package com.graphhopper.geocoder;

import com.google.inject.Inject;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.search.sort.SortOrder;

/**
 * @author Peter Karich
 */
public class QueryHandler extends BaseES {

    @Inject
    public QueryHandler(Configuration config, Client client) {
        super(config, client);
    }

    public SearchResponse doRequest(String query, int size) {
        if (query == null || query.isEmpty())
            return null;
        QueryBuilder builder = QueryBuilders.matchQuery("name", query).minimumShouldMatch("3<90%").fuzziness(0.8);
        SearchRequestBuilder srb = _doSearch(builder, size);
        return srb.get();
    }

    public SearchResponse suggest(String query, int size) {        
        if (query == null || query.isEmpty())
            return null;
        // TODO replace via tokenizer
        int index = query.lastIndexOf(" ");
        String front = "";
        String end = query;
        if (index > 0) {
            front = query.substring(0, index);
            end = query.substring(index + 1);
        }
        QueryBuilder builder = QueryBuilders.prefixQuery("name", end.toLowerCase());
        if (!front.isEmpty()) {
            // not fuzzy as suggest should be stricter when filtering
            builder = QueryBuilders.boolQuery().
                    must(builder).
                    must(QueryBuilders.matchQuery("name", front).minimumShouldMatch("3<90%"));
        }
        SearchRequestBuilder srb = _doSearch(builder, size);
        return srb.get();
    }

    private SearchRequestBuilder _doSearch(QueryBuilder query, int size) {
        return client.prepareSearch(osmIndex).setTypes(osmType).
                setSize(size).
                setQuery(query).
                addSort("population", SortOrder.DESC).
                addSort("type_rank", SortOrder.DESC);
    }

    public SearchResponse rawRequest(String query) {
        QueryBuilder builder = QueryBuilders.queryString(query).
                defaultField("name").
                defaultOperator(QueryStringQueryBuilder.Operator.AND);
        SearchResponse rsp = client.prepareSearch(osmIndex).setTypes(osmType).
                setQuery(builder).
                execute().actionGet();
        return rsp;
    }
}
