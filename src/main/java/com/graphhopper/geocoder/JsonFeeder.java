package com.graphhopper.geocoder;

import com.github.jillesvangurp.osm2geojson.OsmPostProcessor;
import com.github.jillesvangurp.osm2geojson.OsmPostProcessor.JsonWriter;
import com.github.jillesvangurp.osm2geojson.OsmPostProcessor.OsmType;
import com.github.jsonj.JsonArray;
import com.github.jsonj.JsonElement;
import com.github.jsonj.JsonObject;
import com.github.jsonj.tools.JsonParser;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.geo.builders.LineStringBuilder;
import org.elasticsearch.common.geo.builders.PolygonBuilder;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Peter Karich
 */
public class JsonFeeder {

    public static void main(String[] args) {
        new JsonFeeder().start();
    }

    public static Client createClient(String cluster, String url, int port) {
        Settings s = ImmutableSettings.settingsBuilder().put("cluster.name", cluster).build();
        TransportClient tmp = new TransportClient(s);
        tmp.addTransportAddress(new InetSocketTransportAddress(url, port));
        return tmp;
    }
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private Client client;
    private String poiType = "poi";
    private String wayType = "way";
    private String poiIndex = "poi";
    private String wayIndex = "way";

    public void setClient(Client client) {
        this.client = client;
    }

    public void start() {
        // "failed to get node info for..." -> wrong elasticsearch version for client vs. server
        // String cluster = "graphhopper";
        String cluster = "elasticsearch";
        String host = "localhost";
        int port = 9300;
        setClient(createClient(cluster, host, port));
        feed();
    }

    public void feed() {
        final int bulkSize = 1000;
        initIndices();

        OsmPostProcessor processor = new OsmPostProcessor(new JsonParser()) {
            StopWatch sw = new StopWatch().start();
            long counter = 0;

            @Override
            protected JsonWriter createJsonWriter(OsmType type) throws IOException {
                final String tmpType = type.toString().toLowerCase();
                return new JsonWriter() {
                    List<JsonObject> list = new ArrayList<JsonObject>(bulkSize);

                    @Override
                    public void add(JsonObject json) throws IOException {
                        list.add(json);
                        if (list.size() >= bulkSize) {
                            counter += list.size();
                            sw.stop();
                            logger.info("took: " + (float) sw.totalTime().getSeconds() / counter);
                            sw.start();
                            bulkUpdate(list, tmpType, tmpType);
                            list.clear();
                        }
                    }

                    @Override
                    public void close() throws IOException {
                    }
                };
            }
        };
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

                if ("Point".equalsIgnoreCase(type)) {
                    JsonArray arr = jsonObj.getArray("coordinates");
                    // keep lon,lat
                    b.field("location", new Object[]{arr.get(0), arr.get(1)});

                } else if ("LineString".equalsIgnoreCase(type)) {
                    JsonArray arr = jsonObj.getArray("coordinates");
                    List<Object[]> mainList = new ArrayList<Object[]>();
                    for (JsonArray innerArr : arr.arrays()) {
                        // keep lon,lat
                        mainList.add(new Object[]{innerArr.get(0), innerArr.get(1)});
                    }
                    b.field("shape", mainList);

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
                    b.field("shape", mainList);
                } else {
                    throw new IllegalStateException("wrong geometry format:" + key + " -> " + el.toString());
                }
            } else if (key.equalsIgnoreCase("categories")) {
                b.field("categories", el.asObject().getArray("osm"));
            } else if (key.equalsIgnoreCase("title")) {
                b.field("title", el.asString());
            } else if (key.equalsIgnoreCase("address")) {
                // object ala {"housenumber":"555","street":"5th Avenue"}
                b.field("address", el);
            } else if (key.equalsIgnoreCase("links")) {
                // array ala  [{"href":"http://www.fairwaymarket.com/"}]
                b.field("links", el);
            } else {
                b.field(key, el);
                logger.warn("Not explicitely supported " + el.type() + ": " + key + " -> " + el.toString());
            }
        }

        b.endObject();
        return b;
    }

    public void initIndices() {
        initIndex(poiIndex, poiType);
        initIndex(wayIndex, wayType);
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
