package com.graphhopper.geocoder;

import java.util.List;
import java.util.Map;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
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
        assignEntriesWithBounds();

        assignEntriesWithoutBounds();
    }

    /**
     * a) get all entries with bounds. has_bounds:true => get bounds,
     * center_node and eventually admin_level
     *
     * b) get is_in stuff from center_node => "query":
     * "_id:\"osmnode/29927546\"" => name: Pirna with is_in tags (=> set bounds)
     * or 30361883 => Bautzen
     *
     * c) get all in bounds except bounds+village+town+city+... set is_in array
     *
     * somehow merge boundary+city/town (and remove boundary)
     */
    private void assignEntriesWithBounds() {
        // TODO it would be good if we could sort by area size to update small areas first
        // and bigger areas will ignore already assigned streets

        // TODO report progress
        SearchResponse rsp = createScan(FilterBuilders.termFilter("is_boundary", true)).get();
        scroll(rsp, new Execute() {

            @Override public void init(SearchResponse nextScroll) {
            }

            @Override public void handle(SearchHit sh) {
                String name = (String) sh.getSource().get("name");
                String centerNode = (String) sh.getSource().get("center_node");
                Integer adminLevel = (Integer) sh.getSource().get("admin_level");
                Map bounds = (Map) sh.getSource().get("bounds");
                List coords = (List) bounds.get("coordinates");                
                System.out.println(name + ", " + centerNode + ", " + adminLevel + ", " + coords);

                // TODO FilterBuilders.geoPolygonFilter("areaFilter").addPoint(, );
                // FilterBuilders.geoBoundingBoxFilter(name)
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

    /**
     * a) get all villages etc without bounds
     *
     * b) determine radius from population, type
     *
     * c) query with radius and radius*1.1 (eventually until no longer
     * increasing? max==5) use facets as additional indicator if radius too big
     * (e.g. facet=assigned_address => if increasing number => radius is already
     * too big)
     *
     * d) assign village+is_in to the returned streets+pois but exclude stuff
     * also assigned
     */
    private void assignEntriesWithoutBounds() {
        // "query": "type:village OR type:city OR type:town OR type:hamlet OR type:locality"
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
