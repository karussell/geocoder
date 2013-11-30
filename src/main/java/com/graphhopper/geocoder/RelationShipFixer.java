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
import org.elasticsearch.index.query.GeoPolygonFilterBuilder;
import org.elasticsearch.index.query.OrFilterBuilder;
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

        SearchResponse rsp = createScan(FilterBuilders.termFilter("is_boundary", true)).get();
        scroll(rsp, new Execute() {

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

            @Override public void handle(SearchHit scanSearchHit) {
                current++;
                String name = (String) scanSearchHit.getSource().get("name");
                String centerNode = (String) scanSearchHit.getSource().get("center_node");
                Integer adminLevel = (Integer) scanSearchHit.getSource().get("admin_level");
                Map bounds = (Map) scanSearchHit.getSource().get("bounds");

                // System.out.println(name + ", " + centerNode + ", " + adminLevel + ", " + coords);
                if (current % 10 == 0)
                    logger.info((float) current * 100 / total + "% -> time/call:" + timePerCall);

                // TODO fetch not only 10!
                SearchResponse rsp = client.prepareSearch(osmIndex).
                        setFilter(getFilterFromCoordinates(bounds)).
                        setQuery(QueryBuilders.matchAllQuery()).
                        get();
                for (SearchHit boundarySH : rsp.getHits().getHits()) {
                    logger.info(boundarySH.toString());
                }
            }

            private FilterBuilder getFilterFromCoordinates(Map bounds) {
                List coords = (List) bounds.get("coordinates");
                String type = (String) bounds.get("type");

                if (type.equalsIgnoreCase("polygon")) {
                    return getGeoFilter(coords);
                } else if (type.equalsIgnoreCase("multipolygon")) {
                    OrFilterBuilder filter = FilterBuilders.orFilter();
                    for (Object o : coords) {
                        filter.add(getGeoFilter((List) o));
                    }
                    return filter;
                } else
                    throw new IllegalStateException("unknown type " + type);
            }

            GeoPolygonFilterBuilder getGeoFilter(List polygon) {
                GeoPolygonFilterBuilder filter = FilterBuilders.geoPolygonFilter("areaFilter");
                if(polygon.size() != 1)
                    throw new IllegalStateException("polygon size does not match " + polygon.size());
                for (Object o : (List) polygon.get(0)) {
                    List coord = (List) o;
                    if (coord.size() != 2)
                        throw new IllegalStateException("WHAT? " + coord);

                    filter.addPoint((Double) coord.get(1), (Double) coord.get(0));
                }
                return filter;
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
