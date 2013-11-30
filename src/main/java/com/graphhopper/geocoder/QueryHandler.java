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
public class QueryHandler extends BaseES {

    @Inject
    public QueryHandler(Configuration config, Client client) {
        super(config, client);
    }

    public SearchResponse doRequest(String query, int size) {
        if (query == null || query.isEmpty())
            return null;
        QueryBuilder builder = QueryBuilders.fuzzyQuery("name", query).maxExpansions(10);
        return _doSearch(builder, size);
    }

    public SearchResponse suggest(String query, int size) {
        // using ngram technic: http://stackoverflow.com/questions/9421358/filename-search-with-elasticsearch/9432450#9432450
        if (query == null || query.isEmpty())
            return null;
        // TODO replace via tokenizer
        int index = query.lastIndexOf(" ");
        String front = "";
        String end = query;
        if (index > 0) {
            front = query.substring(0, index);
            end = query.substring(index).trim();
        }
        QueryBuilder builder = QueryBuilders.prefixQuery("name", end);
        if (!front.isEmpty()) {
            builder = QueryBuilders.boolQuery().
                    must(builder).
                    must(QueryBuilders.fuzzyQuery("name", front).maxExpansions(10));
        }
        return _doSearch(builder, size);
    }

    private SearchResponse _doSearch(QueryBuilder builder, int size) {
        return client.prepareSearch(osmIndex).setTypes(osmType).
                setSize(size).
                setQuery(builder).
                addSort("population", SortOrder.DESC).
                execute().actionGet();
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
