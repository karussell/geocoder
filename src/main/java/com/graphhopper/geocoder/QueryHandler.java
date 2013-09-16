package com.graphhopper.geocoder;

import com.google.inject.Inject;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.search.sort.SortOrder;

/**
 * @author Peter Karich
 */
public class QueryHandler {

    private String osmIndex = "osm";
    private String osmType = "osmobject";
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
        SearchResponse rsp = client.prepareSearch(osmIndex).setTypes(osmType).
                setQuery(builder).
                addSort("population", SortOrder.ASC).
                execute().actionGet();
        return rsp;
    }
}
