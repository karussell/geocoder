package com.graphhopper.geocoder;

import com.graphhopper.util.PointList;
import com.graphhopper.util.shapes.BBox;
import com.graphhopper.util.shapes.GHPoint;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.optimize.OptimizeRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

/**
 * A Junkie needs drugs... and OpenStreetMaps needs better relation ships of the
 * areas (city, town, village, hamlet, locality) and its streets.
 *
 * @author Peter Karich
 */
public class RelationShipFixer extends BaseES {

    public static void main(String[] args) {
        Configuration config = new Configuration().reload();
        new RelationShipFixer(config, BaseES.createClient(config)).start();
    }

    private final long keepTimeInMinutes;

    private RelationShipFixer(Configuration config, Client client) {
        super(config, client);
        keepTimeInMinutes = config.getKeepInMinutes();
    }

    public void start() {
        logger.info("start!");
        // TODO copyIsInIntoName();

        BoundaryIndex index = assignBoundaryToParent();
        logger.info("updateEntries! index.size:" + index.size());
        updateEntries(index);

        logger.info("TODO updateUnassignedEntries");
        // if no boundary matched => calculate closest via distance to city, village, ...
        // TODO updateUnassignedEntries();
        if (config.doOptimize()) {
            logger.info("Optimizing ...");
            client.admin().indices().optimize(new OptimizeRequest(osmIndex).maxNumSegments(1)).actionGet();
        }
    }

    /**
     * get all boundaries and merge with its parents
     */
    private BoundaryIndex assignBoundaryToParent() {
        // TODO how to determine this upfront -> stored in elasticsearch from JsonFeeder?        
        BBox bbox = new BBox(9.84375, 13.820801, 50.415519, 52.268157);
        final BoundaryIndex index = new BoundaryIndex(bbox, 10000);
        SearchResponse rsp = createScan(FilterBuilders.termFilter("has_boundary", true)).get();
        scroll(rsp, new SimpleExecute() {

            @Override public void handle(SearchHit scanSearchHit,
                    List<IndexRequest> toFeed, List<DeleteRequest> toDelete) {
                current++;

                String boundaryId = scanSearchHit.getId();
                Map<String, Object> boundarySource = scanSearchHit.getSource();
                String name = (String) boundarySource.get("name");
                Map bounds = (Map) boundarySource.get("bounds");
                if (bounds == null)
                    throw new IllegalStateException("has_boundary but no bounds!?" + boundaryId + ", " + name);

                String centerNode = (String) boundarySource.get("center_node");
                if (centerNode == null) {
                    logger.warn("skipping boundary " + boundaryId + ". no center_node!? " + name);
                    return;
                }
                centerNode = "osmnode/" + centerNode;
                SearchResponse rsp = client.prepareSearch(osmIndex).
                        setQuery(QueryBuilders.idsQuery(osmType).addIds(centerNode)).
                        get();

                if (rsp.getHits().getHits().length != 1) {
                    logger.warn("center_node not found!? " + centerNode);
                    return;
                }

                SearchHit parent = rsp.getHits().getHits()[0];
                String parentId = parent.getId();
                Map<String, Object> parentSource = parent.getSource();
                if (parentSource.containsKey("bounds")) {
                    logger.info("Parent " + parentId + " already contains boundary. It was: " + boundaryId);
                } else {
                    parentSource.put("bounds", bounds);

                    if (!parentSource.containsKey("admin_level")) {
                        Integer adminLevel = (Integer) boundarySource.get("admin_level");
                        parentSource.put("admin_level", adminLevel);
                    }

                    if (!parentSource.containsKey("wikipedia")) {
                        String wikipedia = (String) boundarySource.get("wikipedia");
                        parentSource.put("wikipedia", wikipedia);
                    }

                    if (!parentSource.containsKey("type_rank")) {
                        String typeRank = (String) boundarySource.get("type_rank");
                        parentSource.put("type_rank", typeRank);
                    }

                    parentSource.put("has_fixed_boundary", true);
                    toFeed.add(new IndexRequest(osmIndex, osmType, parentId).source(parentSource));
                }

                List isIn = (List) parentSource.get("is_in");
                if (isIn == null)
                    logger.warn("is_in is null for " + parentId + ", " + boundaryId);
                else {
                    GHPoint centerPoint = new GHPoint();
                    List center = (List) parentSource.get("center");
                    if (center != null) {
                        centerPoint.lat = (Double) center.get(1);
                        centerPoint.lon = (Double) center.get(0);
                    } else {
                        logger.warn("center is null for " + parentId + ", " + boundaryId);
                    }
                    List coordinates = (List) bounds.get("coordinates");
                    List<PointList> polygonsToFeed = new ArrayList<PointList>(coordinates.size());
                    for (Object o : coordinates) {
                        List poly = (List) o;
                        PointList list = GeocoderHelper.polygonListToPointList(poly);
                        if (!list.isEmpty())
                            polygonsToFeed.add(list);
                    }
                    index.add(new Info(parentId + "|" + boundaryId, centerPoint, polygonsToFeed, isIn));
                }

                toDelete.add(new DeleteRequest(osmIndex, osmType, boundaryId));
            }
        });
        flush();
        return index;
    }

    SearchRequestBuilder createScan(FilterBuilder filter) {
        SearchRequestBuilder srb = client.prepareSearch(osmIndex).
                setTypes(osmType).
                setSize(config.getFeedBulkSize()).
                setSearchType(SearchType.SCAN).
                setScroll(TimeValue.timeValueMinutes(keepTimeInMinutes));

        if (filter != null)
            srb.setFilter(filter);

        return srb;
    }

    String[] entries = {"city", "town", "borough", "village", "hamlet", "locality", "bus_stop", "motorway_junction"};

    /**
     * Fetch all entries (streets, POIs, unassigned stuff) to determine the
     * associated boundary with BoundaryIndex.search and feed the updated entry
     * which should then contain is_in information and the city/village etc
     */
    private void updateEntries(final BoundaryIndex index) {
        FilterBuilder filter = FilterBuilders.notFilter(FilterBuilders.existsFilter("is_in"));
        SearchResponse rsp = createScan(filter).get();
        scroll(rsp, new SimpleExecute() {

            @Override public void handle(SearchHit scanSearchHit,
                    List<IndexRequest> toFeed, List<DeleteRequest> toDelete) {
                current++;

                String id = scanSearchHit.getId();
                if (id.contains("4259951"))
                    id = id;
                Map<String, Object> source = scanSearchHit.getSource();
                List centerCoord = (List) source.get("center");
                if (centerCoord == null || centerCoord.size() != 2) {
                    logger.warn(id + " object has no center or center has not 2 entries: " + centerCoord);
                    return;
                }

                Double lat = (Double) centerCoord.get(1);
                Double lon = (Double) centerCoord.get(0);

                // TODO which info object should we select?
                // we need to build a hierarchy of boundaries as we cannot rely on the fact that boundary always have is_in values
                Collection<Info> list = index.searchContaining(lat, lon);
                if (list.isEmpty()) {
                    // logger.warn("no boundaries found for " + id);
                    return;
                } else if (list.size() == 2) {
                    logger.warn("more than one boundary found for " + id + " -> " + list);
                }
                Info info = list.iterator().next();
                String name = (String) source.get("name");
                if (name != null)
                    source.put("orig_name", name);

                source.put("name", getName(name, info.getIsIn()));
                source.put("is_in", info.getIsIn());
                // logger.info("boundary matched " + id + " -> " + info.toString());
                toFeed.add(new IndexRequest(osmIndex, osmType, id).source(source));
            }

            String getName(String name, List<String> isIn) {
                if (name == null)
                    name = "";

                String previous = name.trim();
                for (int i = 0; i < isIn.size(); i++) {
                    String tmp = isIn.get(i).trim();
                    if (tmp.equalsIgnoreCase(previous))
                        continue;

                    if (name.isEmpty())
                        name = tmp;
                    else
                        name += ", " + tmp;
                    previous = tmp;
                }
                return name.trim();
            }
        });
        flush();
    }

    private void flush() {
        client.admin().indices().flush(new FlushRequest(osmIndex)).actionGet();
    }

    public class SimpleExecute {

        long total;
        long current;
        long lastTime;
        float timePerCall = -1;

        protected void init(SearchResponse nextScroll) {
            total = nextScroll.getHits().getTotalHits();
            if (timePerCall >= 0) {
                timePerCall = (float) (System.nanoTime() - lastTime) / 1000000;
            }
            lastTime = System.nanoTime();
        }

        /**
         * @param scanSearchHit input
         * @param toFeed output which should be feeded
         */
        public void handle(SearchHit scanSearchHit, List<IndexRequest> toFeed, List<DeleteRequest> toDelete) {
        }

        protected void logInfo() {
            if (current % 10 == 0)
                logger.info((float) current * 100 / total + "% -> time/call:" + timePerCall);
        }
    }

    public static interface Rewriter {

        Map<String, Object> rewrite(Map<String, Object> input);
    }

    public void scroll(SearchResponse rsp, SimpleExecute exec) {
        while (true) {
            rsp = client.prepareSearchScroll(rsp.getScrollId()).
                    setScroll(TimeValue.timeValueMinutes(keepTimeInMinutes)).get();
            if (rsp.getHits().hits().length == 0)
                break;

            exec.init(rsp);

            List<IndexRequest> toIndex = new ArrayList<IndexRequest>();
            List<DeleteRequest> toDelete = new ArrayList<DeleteRequest>();
            for (SearchHit sh : rsp.getHits().getHits()) {
                exec.handle(sh, toIndex, toDelete);
            }
            BulkRequestBuilder brb = client.prepareBulk();
            for (IndexRequest ir : toIndex) {
                brb.add(ir);
            }
            if (toIndex.size() == 0)
                return;

//            for (DeleteRequest dr : toDelete) {
//                brb.add(dr);
//            }
            BulkResponse bulkRsp = brb.get();
            int failed = 0;
            for (BulkItemResponse bur : bulkRsp.getItems()) {
                if (bur.isFailed())
                    failed++;
            }
            if (failed > 0)
                logger.warn(failed + " objects failed to reindex!");
        }
    }
}
