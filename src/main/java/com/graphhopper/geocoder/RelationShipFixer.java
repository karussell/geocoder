package com.graphhopper.geocoder;

import java.util.Map;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
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
        assignBoundaryToParent();
        updateEntries();

        // TODO
        // if no boundary matched => calculate closest via distance to city, village, ...
    }

    /**
     * get all boundaries and merge with its parents
     */
    private void assignBoundaryToParent() {
        SearchResponse rsp = createScan(FilterBuilders.termFilter("has_boundary", true)).get();
        scroll(rsp, new SimpleExecute() {

            @Override public void handle(SearchHit scanSearchHit) {
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
                try {
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

                        parentSource.put("has_boundary", true);
                        // TODO can we omit get() here?
                        client.index(new IndexRequest(osmIndex, osmType, parentId).source(parentSource)).get();
                    }
                } catch (Exception ex) {
                    logger.error("Problem while feeding parent " + parentId + " of boundary " + boundaryId, ex);
                }

                try {
                    client.delete(new DeleteRequest(osmIndex, osmType, boundaryId)).get();
                } catch (Exception ex) {
                    logger.error("Problem while deleting " + boundaryId, ex);
                }
            }
        });
        client.admin().indices().flush(new FlushRequest(osmIndex)).actionGet();
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
    private void updateEntries() {
        FilterBuilder filter = FilterBuilders.notFilter(FilterBuilders.existsFilter("is_in"));
        SearchResponse rsp = createScan(filter).get();
        scroll(rsp, new SimpleExecute() {

            @Override public void handle(SearchHit scanSearchHit) {
                current++;

            }
        });
    }

    abstract class SimpleExecute implements Execute {

        long total;
        long current;
        long lastTime;
        float timePerCall = -1;

        @Override public void init(SearchResponse nextScroll) {
            total = nextScroll.getHits().getTotalHits();
            if (timePerCall >= 0) {
                timePerCall = (float) (System.nanoTime() - lastTime) / 1000000;
            }
            lastTime = System.nanoTime();
        }

        protected void logInfo() {
            if (current % 10 == 0)
                logger.info((float) current * 100 / total + "% -> time/call:" + timePerCall);
        }
    }

    public static interface Rewriter {

        Map<String, Object> rewrite(Map<String, Object> input);
    }

    public static interface Execute {

        void init(SearchResponse nextScroll);

        void handle(SearchHit sh);
    }

    public void scroll(SearchResponse rsp, Execute exec) {
        while (true) {
            rsp = client.prepareSearchScroll(rsp.getScrollId()).
                    setScroll(TimeValue.timeValueMinutes(keepTimeInMinutes)).get();
            if (rsp.getHits().hits().length == 0)
                break;

            exec.init(rsp);
            for (SearchHit sh : rsp.getHits().getHits()) {
                exec.handle(sh);
            }
        }
    }

    void bulky(SearchResponse rsp, Rewriter rewriter) {
        int collectedResults = 0;
        int failed = 0;
        long total = rsp.getHits().totalHits();
        BulkRequestBuilder brb = client.prepareBulk();
        for (SearchHit sh : rsp.getHits().getHits()) {
            String id = sh.getId();
            Map<String, Object> sourceMap = rewriter.rewrite(sh.getSource());
            IndexRequest indexReq = Requests.indexRequest(osmIndex).type(osmType).id(id).source(sourceMap);
            brb.add(indexReq);
        }
        BulkResponse bulkRsp = brb.get();
        for (BulkItemResponse bur : bulkRsp.getItems()) {
            if (bur.isFailed())
                failed++;
        }
        collectedResults += rsp.getHits().hits().length;
        logger.debug("Progress " + collectedResults + "/" + total
                + ", failed:" + failed);
    }
}
