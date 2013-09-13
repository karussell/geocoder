package com.graphhopper.geocoder.http;

import com.github.jsonj.JsonObject;
import com.google.inject.Inject;
import com.graphhopper.geocoder.QueryHandler;
import javax.ws.rs.GET;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;

/**
 * Simplistic version of matching: it does a 2*n+1 query instead of an optimized
 * bucket storing + search.
 *
 * @author Peter Karich
 */
@Produces(MediaType.APPLICATION_JSON)
@javax.ws.rs.Path("/geocoding")
public class GeocoderResource {

    @Inject
    private QueryHandler queryHandler;

    @GET
    public JsonObject getGeocode(@QueryParam("name") String address) {
        SearchResponse rsp = queryHandler.doRequest(address);
        JsonObject json = new JsonObject();
        int i = 0;
        for (SearchHit sh : rsp.getHits().getHits()) {
            json.put("h" + i, sh.getSourceAsString());
        }
        json.put("hits", rsp.getHits().getTotalHits());
        return json;
    }
}