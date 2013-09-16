package com.graphhopper.geocoder;

import com.github.jillesvangurp.osm2geojson.OsmPostProcessor;
import com.github.jsonj.JsonArray;
import com.github.jsonj.JsonElement;
import com.github.jsonj.JsonObject;
import com.github.jsonj.tools.JsonParser;
import com.google.inject.Inject;
import com.graphhopper.util.DistanceCalc;
import com.graphhopper.util.DistancePlaneProjection;
import com.graphhopper.util.PointList;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
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
            public Collection<Integer> bulkUpdate(List<JsonObject> objects, String indexName, String indexType) {
                // use only one index for all data
                Collection<Integer> coll = JsonFeeder.this.bulkUpdate(objects, osmIndex, osmType);
                if (!coll.isEmpty()) {
                    Collection<String> ids = new ArrayList(coll.size());
                    for (Integer integ : coll) {
                        ids.add(objects.get(integ).getString("id"));
                    }
                    logger.warn(coll.size() + " problem(s) while feeding " + objects.size() + " object(s)! " + ids);
                }
                return coll;
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
        boolean foundLocation = false;
        boolean foundPopulation = false;
        for (Entry<String, JsonElement> e : o.entrySet()) {
            JsonElement el = e.getValue();
            String key = e.getKey();
            if (key.equalsIgnoreCase("id")) {
                // handled separately -> no need to feed for now
                continue;
            } else if (key.equalsIgnoreCase("geometry")) {
                JsonObject jsonObj = el.asObject();
                String type = jsonObj.getString("type");
                JsonArray arr = jsonObj.getArray("coordinates");
                foundLocation = true;
                double[] middlePoint = null;

                if ("Point".equalsIgnoreCase(type)) {
                    // order is lon,lat
                    middlePoint = new double[]{arr.get(1).asDouble(), arr.get(0).asDouble()};

                } else if ("LineString".equalsIgnoreCase(type)) {
                    middlePoint = GeocoderHelper.calcMiddlePoint(arr);

                } else if ("Polygon".equalsIgnoreCase(type)) {
                    // polygon is an array of array of array
                    // "geometry":{"type":"Polygon","coordinates":[[[..]]]
                    middlePoint = GeocoderHelper.calcCentroid(GeocoderHelper.toPointList(arr));
                    if ("Friedrich-List-Straße".equals(o.get("title").asString()))
                        logger.info("location " + Arrays.toString(middlePoint) + " from " + o.get("title") + ", " + o);

                } else {
                    throw new IllegalStateException("wrong geometry format:" + key + " -> " + el.toString());
                }

                if (middlePoint == null)
                    continue;
                b.field("center", middlePoint);
            } else if (key.equalsIgnoreCase("categories")) {
                b.field("tags", GeocoderHelper.toMap(el.asObject().getObject("osm")));
            } else if (key.equalsIgnoreCase("title")) {
                String title = el.asString();
                if (title.contains("/")) {
                    logger.info(title + " " + o);
                    // force spaces before and after slash
                    title = title.replaceAll("/", " / ");
                    // force spaces after dot
                    title = title.replaceAll("\\.", ". ");
                    title = GeocoderHelper.innerTrim(title);
                }
                b.field("title", title);
            } else if (key.equalsIgnoreCase("type")) {
                b.field("type", el.asString());
            } else if (key.equalsIgnoreCase("address")) {
                // object ala {"housenumber":"555","street":"5th Avenue"}
                b.field("address", GeocoderHelper.toMap(el.asObject()));
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
                    foundPopulation = true;
                } catch (NumberFormatException ex) {
                }
            } else {
                if (!minimalData) {
                    b.field(key, el);
                }
                logger.warn("Not explicitely supported " + el.type() + ": " + key + " -> " + el.toString());
            }
        }

        if (!foundPopulation) {
            b.field("population", 0L);
        }

        if (!foundLocation) {
            throw new IllegalStateException("No location found:" + o.toString());
        }

        b.endObject();
        return b;
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
            String mappingSource = GeocoderHelper.toString(is);
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
}
