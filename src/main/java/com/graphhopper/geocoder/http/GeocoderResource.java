package com.graphhopper.geocoder.http;

import com.google.inject.Inject;
import com.graphhopper.geocoder.QueryHandler;
import com.graphhopper.util.shapes.BBox;
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
import org.elasticsearch.search.SearchHits;
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
    public Map<String, Object> getGeocode(
            @QueryParam("q") String address,
            @DefaultValue("false")
            @QueryParam("suggest") boolean suggest,
            @DefaultValue("10")
            @QueryParam("size") int size,
            @DefaultValue("false")
            @QueryParam("withBounds") boolean withBounds
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
        int searchSize = size * 2;
        List<Map<String, Object>> results = new ArrayList<Map<String, Object>>();
        if (rsp != null) {
            SearchHit[] sHits = rsp.getHits().getHits();
            for (int i = 0; i < size && i < sHits.length; i++) {
                SearchHit sh = sHits[i];
                if (withBounds) {
                    Map bounds = (Map) sh.getSource().get("bounds");
                    if (bounds != null) {
                        if (bounds.get("type").equals("polygon"))
                            bounds.put("type", "Polygon");
                        else if (bounds.get("type").equals("mulipolygon"))
                            bounds.put("type", "MultiPolygon");
                        else if (bounds.get("type").equals("linestring"))
                            bounds.put("type", "LineString");
                        sh.getSource().put("bounds", bounds);
                    }
                } else
                    sh.getSource().remove("bounds");

                results.add(sh.getSource());
            }
            total = rsp.getHits().getTotalHits();

            if (sHits.length > 0) {
                BBox bbox = BBox.INVERSE.clone();
                for (SearchHit sh : rsp.getHits().getHits()) {
                    List center = (List) sh.getSource().get("center");
                    if (center == null)
                        continue;
                    double lat = (Double) center.get(1);
                    double lon = (Double) center.get(0);
                    if (lat > bbox.maxLat)
                        bbox.maxLat = lat;

                    if (lat < bbox.minLat)
                        bbox.minLat = lat;

                    if (lon > bbox.maxLon)
                        bbox.maxLon = lon;

                    if (lon < bbox.minLon)
                        bbox.minLon = lon;
                }
                double epsilon = 0.0001;
                bbox.maxLat += epsilon;
                bbox.minLat -= epsilon;
                bbox.maxLon += epsilon;
                bbox.minLon -= epsilon;
                json.put("approx_bbox", bbox.toGeoJson());
            }
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
