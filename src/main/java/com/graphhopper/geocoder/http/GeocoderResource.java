package com.graphhopper.geocoder.http;

import com.google.inject.Inject;
import com.graphhopper.geocoder.QueryHandler;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Geocoding query.
 *
 * @author Peter Karich
 */
@Produces("application/json; charset=UTF-8")
@javax.ws.rs.Path("/geocoder")
public class GeocoderResource {

    @Inject
    private QueryHandler queryHandler;
    private final Logger logger = LoggerFactory.getLogger(getClass());

    @GET
    public Map<String, Object> getGeocode(@QueryParam("q") String address,
            @QueryParam("suggest") boolean suggest,
            @DefaultValue("10")
            @QueryParam("size") int size
    /*@QueryParam("locale") String locale, 
     @QueryParam("boostNear") Coord boostNearPoint, 
     @QueryParam("maxBounds") BBox maxBounds*/) {

        long start = System.nanoTime();
        Map<String, Object> json = new HashMap<String, Object>();
        SearchResponse rsp;
        if (suggest)
            rsp = queryHandler.suggest(address, size);            
        else
            rsp = queryHandler.doRequest(address, size);
            
        long total = 0;
        List<Map<String, Object>> results = new ArrayList<Map<String, Object>>();
        if (rsp != null) {
            for (SearchHit sh : rsp.getHits().getHits()) {
                results.add(sh.getSource());
            }
            total = rsp.getHits().getTotalHits();
        }

        json.put("total", total);
        json.put("hits", results);
        long end = System.nanoTime();
        float took = (float) (end - start) / 1000000;
        json.put("took", took);
        logger.info("q=" + address + "&suggest=" + suggest + " # took:" + took + " total:" + total);
        return json;
    }
}
