package com.graphhopper.geocoder;

import com.github.jillesvangurp.osm2geojson.OsmPostProcessor;
import com.github.jsonj.JsonArray;
import com.github.jsonj.JsonElement;
import com.github.jsonj.JsonObject;
import com.github.jsonj.tools.JsonParser;
import static com.github.jsonj.tools.JsonBuilder.$;
import static com.github.jsonj.tools.JsonBuilder._;
import static com.github.jsonj.tools.JsonBuilder.array;
import com.google.inject.Inject;
import com.graphhopper.geohash.KeyAlgo;
import com.graphhopper.geohash.SpatialKeyAlgo;
import com.vividsolutions.jts.geom.Point;
import gnu.trove.list.array.TLongArrayList;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Peter Karich
 */
public class JsonFeeder {

    public static void main(String[] args) {
        Configuration config = new Configuration().reload();
        new JsonFeeder(config).start();
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
    private final Configuration config;
    private boolean minimalData;
    private KeyAlgo keyAlgo;

    public JsonFeeder(Configuration c) {
        config = c;
        keyAlgo = new SpatialKeyAlgo(config.getSpatialKeyResolution());
    }

    public void setClient(Client client) {
        if (client == null)
            throw new IllegalArgumentException("client cannot be null");

        this.client = client;
    }

    public void start() {
        // "failed to get node info for..." -> wrong elasticsearch version for client vs. server

        String cluster = config.getElasticSearchCluster();
        String host = config.getElasticSearchHost();
        int port = config.getElasticSearchPort();
        setClient(createClient(cluster, host, port));
        feed();
    }

    public void feed() {
        initIndices();

        OsmPostProcessor processor = new MyOsmPostProcessor(new JsonParser()) {
            boolean dryRun = config.isDryRun();

            @Override public Collection<Integer> bulkUpdate(List<JsonObject> objects, String indexName, String indexType) {
                if (dryRun)
                    return Collections.EMPTY_LIST;
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
        }.setBulkSize(config.getFeedBulkSize());

        minimalData = config.isMinimalDataMode();
        processor.setDirectory(config.getIndexDir());
        processor.processNodes();
        processor.processWays();
        processor.processRelations();
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
                JsonObject obj = createDoc(o);
                IndexRequest indexReq = Requests.indexRequest(indexName).type(indexType).id(id).source(obj.toString());
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
    public JsonObject createDoc(JsonObject o) throws IOException {
        JsonObject result = new JsonObject();
        boolean foundLocation = false;
        boolean foundPopulation = false;
        String name = o.getString("name");
        result.put("name", fixName(name));

        JsonElement type = o.get("type");
        if (type == null)
            logger.warn("no type associated " + o);

        for (Entry<String, JsonElement> e : o.entrySet()) {
            JsonElement el = e.getValue();
            String key = e.getKey();
            if (key.equalsIgnoreCase("id")) {
                // handled separately -> no need to feed for now
                continue;

            } else if (key.equalsIgnoreCase("geometry")) {
                JsonObject jsonObj = el.asObject();
                String geoType = jsonObj.getString("type");
                JsonArray arr = jsonObj.getArray("coordinates");
                foundLocation = true;
                double[] middlePoint = null;

                if ("Point".equalsIgnoreCase(geoType)) {
                    // order is lon,lat, but order of middlePoint is lat,lon
                    middlePoint = new double[]{arr.get(1).asDouble(), arr.get(0).asDouble()};

                } else if ("LineString".equalsIgnoreCase(geoType)) {
                    middlePoint = GeocoderHelper.calcMiddlePoint(arr);

                } else if ("Polygon".equalsIgnoreCase(geoType)) {
                    // A polygon is defined by a list of a list of points. The first and last points in each list must be the same (the polygon must be closed).
                    // The first array represents the outer boundary of the polygon (unsupported: the other arrays represent the interior shapes (holes))
                    List<Point> pointList = GeocoderHelper.polygonToPointList(arr.get(0).asArray());
                    if (pointList.size() < 4)
                        continue;

                    middlePoint = GeocoderHelper.calcCentroid(pointList);
                    JsonArray boundary = simplify(pointList, arr.get(0).asArray());

                    if (boundary.size() > 3) {
                        JsonArray polyBoundary = array();
                        polyBoundary.add(boundary);
                        result.put("has_bounds", true);
                        result.put("bounds", $(_("type", "polygon"),
                                _("coordinates", polyBoundary)));
                    }

                } else if ("MultiPolygon".equalsIgnoreCase(geoType)) {
                    // multipolygon is an array of array of coordinates (array)
                    // "geometry":{"type":"MultiPolygon","coordinates":[ (first polygon) [[lon, lat], ..], (next) [[lon, lat], ..]]                    

                    JsonArray coordinates = array();
                    for (JsonArray polyArr : arr.arrays()) {
                        JsonArray outerBoundary = polyArr.get(0).asArray();
                        List<Point> pointList = GeocoderHelper.polygonToPointList(outerBoundary);
                        if (pointList.size() < 4)
                            continue;

                        outerBoundary = simplify(pointList, outerBoundary);
                        if (outerBoundary.size() > 3) {
                            JsonArray polyBoundary = array();
                            polyBoundary.add(outerBoundary);
                            coordinates.add(polyBoundary);
                        }
                    }

                    if (coordinates.isEmpty())
                        continue;

                    // TODO Collections.sort(arr);
                    // pick middle point from largest polygon == first polygon
                    middlePoint = GeocoderHelper.calcCentroid(GeocoderHelper.polygonToPointList(
                            coordinates.get(0).asArray().get(0).asArray()));

                    result.put("has_bounds", true);
                    result.put("bounds", $(_("type", "multipolygon"), _("coordinates", coordinates)));

                } else {
                    throw new IllegalStateException("wrong geometry format:" + key + " -> " + el.toString());
                }

                if (middlePoint != null)
                    // lon,lat
                    result.put("center", array(middlePoint[1], middlePoint[0]));

            } else if (key.equalsIgnoreCase("center_node")) {
                // a relation normally has a center_node associated -> could make fetching easier/faster
                result.put("center_node", el);

            } else if (key.equalsIgnoreCase("categories")) {
                // no need for now
                // b.field("tags", GeocoderHelper.toMap(el.asObject().getObject("osm")));                            
            } else if (key.equalsIgnoreCase("name")) {
                // already done
            } else if (key.equalsIgnoreCase("names")) {
                JsonObject obj = el.asObject();
                JsonObject names = result.getOrCreateObject("names");
                for (Map.Entry<String, JsonElement> entry : obj.entrySet()) {
                    names.put(entry.getKey(), fixName(entry.getValue().asString()));
                }

            } else if (key.equalsIgnoreCase("population")) {
                result.put("population", el);
                foundPopulation = true;

            } else if (key.equalsIgnoreCase("is_in")) {
                result.put("is_in", el);

            } else if (key.equalsIgnoreCase("type")) {
                result.put("type", el);

            } else if (key.equalsIgnoreCase("address")) {
                // object ala {"housenumber":"555","street":"5th Avenue"}
                result.put("address", el);

            } else if (key.equalsIgnoreCase("link")) {
                result.put("link", el);

            } else if (key.equalsIgnoreCase("wikipedia")) {
                result.put("wikipedia", el);

            } else if (key.equalsIgnoreCase("admin_level")) {
                result.put("admin_level", el);

            } else {
                if (!minimalData)
                    result.put(key, el);

                logger.warn("Not explicitely supported " + el.type() + ": " + key + " -> " + el.toString());
            }
        }

        if (!foundPopulation)
            result.put("population", 0L);

        if (!foundLocation)
            throw new IllegalStateException("No location found:" + o.toString());

        return result;
    }

    String fixName(String name) {
        if (name.contains("/")) {
            // logger.info(title + " " + o);
            // force spaces before and after slash
            name = name.replaceAll("/", " / ");
            // force spaces after dot and comma
            name = name.replaceAll("\\.", ". ");
            name = name.replaceAll("\\,", ", ");
            name = GeocoderHelper.innerTrim(name);
        }
        return name;
    }

    int counter = 0;
    float res = 0;

    /**
     * A very simple simplify algorithm. Create the spatial key of a point and
     * compare to the previous one. If identical -> skip. So, if the resolution
     * is very low only a few points are added to the rboundaryesulting array.
     */
    public JsonArray simplify(List<Point> pointList, JsonArray orig) {
        // skip simplify if small boundary
        if (orig.size() < config.getSmallBoundary())
            return orig;

        JsonArray outerBoundary = new JsonArray();
        int max = pointList.size();
        TLongArrayList keys = new TLongArrayList(max);
        int LAST_N = 3;
        LOOP:
        for (int i = 0; i < max; i++) {
            Point p = pointList.get(i);
            long key = keyAlgo.encode(p.getY(), p.getX());
            keys.add(key);

            // Do not skip ends of list, otherwise we get: IllegalArgumentException[Points of LinearRing do not form a closed linestring];
            if (i > 0 && i < max - 1) {
                // reduce probability that a boundary intersects via comparing LAST_N elements
                for (int tmp = i - 1, ii = 0; tmp > 1 && ii < LAST_N; ii++, tmp--) {
                    if (keys.get(tmp) == key)
                        continue LOOP;
                }
            }
            outerBoundary.add(array(p.getX(), p.getY()));
        }
        if (outerBoundary.size() < 4) {
            logger.warn("reduced multi too much: " + outerBoundary.size() + " vs. original " + orig.size());
            return orig;
        }

        res += (float) outerBoundary.size() / max;
        counter++;
        if (counter % 100 == 0)
            logger.info("simplified:" + res / counter);
        return outerBoundary;
    }

    public void initIndices() {
        initIndex(osmIndex, osmType);
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
    }
}
