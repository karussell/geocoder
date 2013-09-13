package com.graphhopper.geocoder;

import com.google.inject.Inject;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;

/**
 * @author Peter Karich
 */
public class QueryHandler {

    private String wayIndex = "way";
    private String wayType = "way";
    private String poiIndex = "poi";
    private String poiType = "poi";
    @Inject
    private Client client;

    public QueryHandler setClient(Client client) {
        this.client = client;
        return this;
    }

    public SearchResponse doRequest(String query) {
        QueryBuilder builder = QueryBuilders.queryString(query).
                defaultField("title").
                defaultOperator(QueryStringQueryBuilder.Operator.AND);
        SearchResponse rsp = client.prepareSearch(poiIndex).setTypes(poiType).setQuery(builder).execute().actionGet();
        return rsp;
    }
}