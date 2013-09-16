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
    private static DistanceCalc distCalc = new DistancePlaneProjection();

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
        boolean foundLocation = false;
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

                if ("Point".equalsIgnoreCase(type)) {
                    // order is lon,lat
                    b.field("location", new Object[]{arr.get(0), arr.get(1)});

                } else if ("LineString".equalsIgnoreCase(type)) {
                    // pick the point closest to the middle of the road
                    double[] middlePoint = calcMiddlePoint(arr);
                    if (middlePoint == null)
                        continue;
                    b.field("location", middlePoint);

                } else if ("Polygon".equalsIgnoreCase(type)) {
                    // polygon is an array of array of array
                    // "geometry":{"type":"Polygon","coordinates":[[[..]]]
                    double[] middlePoint = calcCentroid(toPointList(arr));
                    if (middlePoint == null)
                        continue;
                    b.field("location", middlePoint);

                    if ("Friedrich-List-Straße".equals(o.get("title")))
                        logger.info("location " + Arrays.toString(middlePoint) + " from " + o.get("title") + ", " + o);

                } else {
                    throw new IllegalStateException("wrong geometry format:" + key + " -> " + el.toString());
                }
            } else if (key.equalsIgnoreCase("categories")) {
                b.field("tags", toMap(el.asObject().getObject("osm")));
            } else if (key.equalsIgnoreCase("title")) {
                b.field("title", el.asString());
            } else if (key.equalsIgnoreCase("type")) {
                b.field("type", el.asString());
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

        if (!foundLocation) {
            throw new IllegalStateException("No location found:" + o.toString());
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

    /**
     * LineString: JsonArray of lon,lat arrays
     */
    private static double[] calcMiddlePoint(JsonArray arr) {
        if (arr.isEmpty())
            return null;

        double lat = Double.MAX_VALUE, lon = Double.MAX_VALUE;
        JsonArray firstCoord = arr.get(0).asArray();
        JsonArray lastCoord = arr.get(arr.size() - 1).asArray();
        double latFirst = firstCoord.get(1).asDouble();
        double lonFirst = firstCoord.get(0).asDouble();
        double latLast = lastCoord.get(1).asDouble();
        double lonLast = lastCoord.get(0).asDouble();
        double latMiddle = (latFirst + latLast) / 2;
        double lonMiddle = (lonFirst + lonLast) / 2;
        double minDist = Double.MAX_VALUE;
        for (JsonArray innerArr : arr.arrays()) {
            double latTmp = innerArr.get(1).asDouble();
            double lonTmp = innerArr.get(0).asDouble();
            double tmpDist = distCalc.calcDist(latMiddle, lonMiddle, latTmp, lonTmp);
            if (minDist > tmpDist) {
                minDist = tmpDist;
                lat = latTmp;
                lon = lonTmp;
            }
        }

        return new double[]{lat, lon};
    }

    /**
     * Polygon: JsonArray or JsonArrays containing lon,lat arrays
     */
    static double[] calcSimpleMean(PointList list) {
        if (list.isEmpty())
            return null;
        double lat = 0, lon = 0;
        int max = list.getSize();
        for (int i = 0; i < max; i++) {
            lat += list.getLatitude(i);
            lon += list.getLongitude(i);
        }
        return new double[]{lat / max, lon / max};
    }

    /**
     * Polygon: JsonArray or JsonArrays containing lon,lat arrays
     */
    static double[] calcCentroid(PointList list) {
        if (list.isEmpty())
            return null;

        // simple average is not too precise 
        // so use http://en.wikipedia.org/wiki/Centroid#Centroid_of_polygon
        double lat = 0, lon = 0;
        double polyArea = 0;

        // lat = y, lon = x
        // TMP(i) = (lon_i * lat_(i+1) - lon_(i+1) * lat_i)
        // A = 1/2 sum_0_to_n-1 TMP(i)
        // lat = C_y = 1/6A sum (lat_i + lat_(i+1) ) * TMP(i)
        // lon = C_x = 1/6A sum (lon_i + lon_(i+1) ) * TMP(i)        

        int max = list.getSize() - 1;
        for (int i = 0; i < max; i++) {
            double tmpLat = list.getLatitude(i);
            double tmpLat_p1 = list.getLatitude(i + 1);
            double tmpLon = list.getLongitude(i);
            double tmpLon_p1 = list.getLongitude(i + 1);
            double TMP = tmpLon * tmpLat_p1 - tmpLon_p1 * tmpLat;
            polyArea += TMP;
            lat += (tmpLat + tmpLat_p1) * TMP;
            lon += (tmpLon + tmpLon_p1) * TMP;
        }
        polyArea /= 2;
        lat = lat / (6 * polyArea);
        lon = lon / (6 * polyArea);

        return new double[]{lat, lon};
    }

    static PointList toPointList(JsonArray arr) {
        if (arr.isEmpty())
            return PointList.EMPTY;

        PointList list = new PointList();
        for (JsonArray innerArr : arr.arrays()) {
            for (JsonArray innerstArr : innerArr.arrays()) {
                double tmpLat = innerstArr.get(1).asDouble();
                double tmpLon = innerstArr.get(0).asDouble();
                list.add(tmpLat, tmpLon);
            }
        }
        return list;
    }
}
