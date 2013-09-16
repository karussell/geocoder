package com.graphhopper.geocoder;

import com.github.jillesvangurp.osm2geojson.OsmPostProcessor;
import com.github.jsonj.JsonArray;
import com.github.jsonj.JsonElement;
import com.github.jsonj.JsonObject;
import com.github.jsonj.tools.JsonParser;
import com.google.inject.Inject;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Peter Karich
 */
public class JsonFeeder {

    public static void main(String[] args) {
        Configuration config = new Configuration().reload();
        new JsonFeeder().setConfiguration(config).start();
    }

    public static Client createClient(String cluster, String url, int port) {
        Settings s = ImmutableSettings.settingsBuilder().put("cluster.name", cluster).build();
        TransportClient tmp = new TransportClient(s);
        tmp.addTransportAddress(new InetSocketTransportAddress(url, port));
        return tmp;
    }
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private Client client;
    private String osmType = "osmobject";
    private String osmIndex = "osm";
    @Inject
    private Configuration conf;
    private boolean minimalData = false;

    public JsonFeeder() {
    }

    public JsonFeeder setConfiguration(Configuration configuration) {
        this.conf = configuration;
        return this;
    }

    public void setClient(Client client) {
        if (client == null)
            throw new IllegalArgumentException("client cannot be null");

        this.client = client;
    }

    public void start() {
        // "failed to get node info for..." -> wrong elasticsearch version for client vs. server

        String cluster = conf.getElasticSearchCluster();
        String host = conf.getElasticSearchHost();
        int port = conf.getElasticSearchPort();
        setClient(createClient(cluster, host, port));
        feed();
    }

    public void feed() {
        initIndices();

        OsmPostProcessor processor = new MyOsmPostProcessor(new JsonParser()) {
            @Override
            public Collection<Integer> bulkUpdate(Collection<JsonObject> objects, String indexName, String indexType) {
                // use only one index for all data
                return JsonFeeder.this.bulkUpdate(objects, osmIndex, indexType);
            }
        }.setBulkSize(conf.getFeedBulkSize());
        processor.setDirectory(conf.getIndexDir());
        processor.processNodes();
        processor.processWays();
    }

    public Collection<Integer> bulkUpdate(Collection<JsonObject> objects, String indexName, String indexType) {
        // now using bulk API instead of feeding each doc separate with feedDoc
        BulkRequestBuilder brb = client.prepareBulk();
        for (JsonObject o : objects) {
            String id = o.getString("id");
            if (id == null) {
                logger.warn("Skipped object without id when bulkUpdate:" + o);
                continue;
            }

            try {
                XContentBuilder source = createDoc(o);
                IndexRequest indexReq = Requests.indexRequest(indexName).type(indexType).id(id).source(source);
                brb.add(indexReq);
            } catch (Exception ex) {
                logger.warn("cannot add object " + id + " -> " + o.toString(), ex);
            }
        }
        if (brb.numberOfActions() > 0) {
            BulkResponse rsp = brb.execute().actionGet();
            if (rsp.hasFailures()) {
                List<Integer> list = new ArrayList<Integer>(rsp.getItems().length);
                for (BulkItemResponse br : rsp.getItems()) {
                    if (br.isFailed()) {
                        logger.warn("Cannot index object " + br.getId() + ". Error:" + br.getFailureMessage());
                        list.add(br.getItemId());
                    }
                }
                return list;
            }
        }

        return Collections.emptyList();
    }

    // {"id":"osmnode/1411809098","title":"Bensons Rift",
    //      "geometry":{"type":"Point","coordinates":[-75.9839922,44.3561003]},
    //      "categories":{"osm":["natural:water"]}}
    public XContentBuilder createDoc(JsonObject o) throws IOException {
        XContentBuilder b = JsonXContent.contentBuilder().startObject();

        for (Entry<String, JsonElement> e : o.entrySet()) {
            JsonElement el = e.getValue();
            String key = e.getKey();
            if (key.equalsIgnoreCase("id")) {
                // handled separately -> no need to feed for now
                continue;
            } else if (key.equalsIgnoreCase("geometry")) {
                JsonObject jsonObj = el.asObject();
                String type = jsonObj.getString("type");

                // TODO calculate location if only bounds exist
                
                if ("Point".equalsIgnoreCase(type)) {
                    JsonArray arr = jsonObj.getArray("coordinates");
                    // order is lon,lat
                    b.field("location", new Object[]{arr.get(0), arr.get(1)});

                } else if ("LineString".equalsIgnoreCase(type)) {
                    JsonArray arr = jsonObj.getArray("coordinates");
                    List<Object[]> mainList = new ArrayList<Object[]>();
                    for (JsonArray innerArr : arr.arrays()) {
                        // order is lon,lat
                        mainList.add(new Object[]{innerArr.get(0), innerArr.get(1)});
                    }
                    b.field("bounds", mainList);

                } else if ("Polygon".equalsIgnoreCase(type)) {
                    // polygon is an array of array of array
                    // "geometry":{"type":"Polygon","coordinates":[[[..]]]
                    List<List<Object[]>> mainList = new ArrayList<List<Object[]>>();
                    JsonArray arr = jsonObj.getArray("coordinates");
                    for (JsonArray innerArr : arr.arrays()) {
                        List<Object[]> tmpList = new ArrayList<Object[]>();
                        mainList.add(tmpList);
                        for (JsonArray innerstArr : innerArr.arrays()) {
                            tmpList.add(new Object[]{innerstArr.get(0), innerstArr.get(1)});
                        }
                    }
                    b.field("bounds", mainList);
                } else {
                    throw new IllegalStateException("wrong geometry format:" + key + " -> " + el.toString());
                }
            } else if (key.equalsIgnoreCase("categories")) {
                // TODO type
                b.field("tags", toMap(el.asObject().getObject("osm")));
            } else if (key.equalsIgnoreCase("title")) {
                b.field("title", el.asString());
            } else if (key.equalsIgnoreCase("address")) {
                // object ala {"housenumber":"555","street":"5th Avenue"}
                b.field("address", toMap(el.asObject()));
            } else if (key.equalsIgnoreCase("links")) {
                // array ala  [{"href":"http://www.fairwaymarket.com/"}]
                // only grab the first one
                if (!minimalData && !el.asArray().isEmpty()) {
                    String link = el.asArray().get(0).asObject().get("href").asString();
                    b.field("link", link);
                }
            } else if (key.equalsIgnoreCase("population")) {
                try {
                    long val = Long.parseLong(el.asString());
                    b.field("population", val);
                } catch (NumberFormatException ex) {
                }
            } else {
                if (!minimalData) {
                    b.field(key, el);
                }
                logger.warn("Not explicitely supported " + el.type() + ": " + key + " -> " + el.toString());
            }
        }

        b.endObject();
        return b;
    }

    String[] toArray(JsonArray arr) {
        String[] res = new String[arr.size()];
        int i = 0;
        for (String s : arr.asStringArray()) {
            res[i] = s;
            i++;
        }
        return res;
    }

    Map<String, Object> toMap(JsonObject obj) {
        Map<String, Object> res = new HashMap<String, Object>(obj.size());
        for (Entry<String, JsonElement> e : obj.entrySet()) {
            res.put(e.getKey(), e.getValue().asString());
        }
        return res;
    }

    public void initIndices() {
        initIndex(osmIndex, osmType);
    }

    private void createIndex(String indexName) {
        boolean log = true;
        try {
            client.admin().indices().create(new CreateIndexRequest(indexName)).actionGet();
            if (log)
                logger.info("Created index: " + indexName);
        } catch (Exception ex) {
            if (log)
                logger.info("Index " + indexName + " already exists");
        }
    }

    private void createIndexAndPutMapping(String indexName, String type) {
        boolean log = true;
        try {
            InputStream is = getClass().getResourceAsStream(type + ".json");
            String mappingSource = toString(is);
            client.admin().indices().create(new CreateIndexRequest(indexName).mapping(type, mappingSource)).actionGet();
            if (log)
                logger.info("Created index: " + indexName);
        } catch (Exception ex) {
            if (log)
                logger.info("Index " + indexName + " already exists");
        }
    }

    private void initIndex(String indexName, String type) {
        createIndexAndPutMapping(indexName, type);
//        createIndex(indexName);
//
//        InputStream is = getClass().getResourceAsStream(type + ".json");
//        try {
//            String mappingSource = toString(is);
//            client.admin().indices().putMapping(new PutMappingRequest(indexName).type(type).source(mappingSource)).actionGet();
//        } catch (Exception ex) {
//            throw new RuntimeException(ex);
//        }
    }

    public static String toString(InputStream is) throws IOException {
        if (is == null)
            throw new IllegalArgumentException("stream is null!");

        BufferedReader bufReader = new BufferedReader(new InputStreamReader(is, "UTF-8"));
        StringBuilder sb = new StringBuilder();
        String line;
        while ((line = bufReader.readLine()) != null) {
            sb.append(line);
            sb.append('\n');
        }
        bufReader.close();
        return sb.toString();
    }
}
