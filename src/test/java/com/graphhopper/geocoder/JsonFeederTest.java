package com.graphhopper.geocoder;

import com.github.jsonj.JsonArray;
import com.github.jsonj.JsonObject;
import static com.github.jsonj.tools.JsonBuilder.$;
import static com.github.jsonj.tools.JsonBuilder._;
import static com.github.jsonj.tools.JsonBuilder.array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.testng.annotations.*;
import static org.hamcrest.MatcherAssert.assertThat;
import org.elasticsearch.index.query.QueryBuilders;
import static org.hamcrest.Matchers.*;

/**
 *
 * @author Peter Karich
 */
public class JsonFeederTest extends AbstractNodesTests {

    protected Client client;
    private String wayIndex = "way";
    private String wayType = "way";
    private String poiIndex = "poi";
    private String poiType = "poi";
    private JsonFeeder feeder;
    private QueryHandler queryHandler;

    @BeforeClass public void createNodes() throws Exception {
        startNode("node1");
        client = client("node1");
    }

    @AfterClass public void closeNodes() {
        client.close();
        closeAllNodes();
    }

    @BeforeTest
    public void setUp() {
        feeder = new JsonFeeder();
        feeder.setClient(client);
        feeder.initIndices();
        queryHandler = new QueryHandler().setClient(client);
    }

    @Test
    public void testFeedPoint() {
        List<JsonObject> list = new ArrayList<JsonObject>();
        JsonArray coordinates = array(-11, 11);
        JsonObject geo = $(_("type", "Point"), _("coordinates", coordinates));
        list.add($(_("id", "osmnode/123"), _("geometry", geo), _("title", "testing it today")));

        Collection<Integer> res = feeder.bulkUpdate(list, poiIndex, poiType);
        assertThat(res.size(), equalTo(0));
        refresh(poiIndex);
        assertThat(client.prepareCount(poiIndex).execute().actionGet().getCount(), equalTo(1L));

        SearchResponse rsp = queryHandler.doRequest("today");
        assertThat(rsp.getHits().getTotalHits(), equalTo(1L));
    }

//    {"id":"osmway/100198671","title":"Depaula Chevrolet Hummer",
//       "geometry":{"type":"Polygon","coordinates":[[[-73.7882444,42.6792747],[-73.7880386,42.6790894],[-73.7880643,42.6790714],[-73.7879537,42.6789736],[-73.7877787,42.6789659],[-73.7876115,42.6790637],[-73.7876423,42.6790997],[-73.787156,42.6794008],[-73.7870608,42.6793184],[-73.7866569,42.6795603],[-73.7869734,42.6798459],[-73.78703,42.6798124],[-73.7871689,42.6799385],[-73.7882444,42.6792747]]]},
//       "categories":{"osm":["building:yes","shop:car","building"]},
//       "address":{"city":"Albany","country":"US","housenumber":"785","postcode":"12206","street":"Central Avenue"}
//    }
    @Test
    public void testFeedLineString() {
        List<JsonObject> list = new ArrayList<JsonObject>();
        JsonArray coordinates = array();
        coordinates.add(array(11, 11));
        coordinates.add(array(22, 22));
        coordinates.add(array(33, 33));
        JsonObject geo = $(_("type", "LineString"), _("coordinates", coordinates));
        list.add($(_("id", "osmway/123"), _("geometry", geo), _("title", "testing it today")));

        Collection<Integer> res = feeder.bulkUpdate(list, wayIndex, wayType);
        assertThat(res.size(), equalTo(0));
        refresh(wayIndex);
        assertThat(client.prepareCount(wayIndex).execute().actionGet().getCount(), equalTo(1L));

        QueryBuilder builder = QueryBuilders.queryString("today").
                defaultField("title").
                defaultOperator(QueryStringQueryBuilder.Operator.AND);
        SearchResponse rsp = client.prepareSearch(wayIndex).setTypes(wayType).setQuery(builder).execute().actionGet();
        assertThat(rsp.getHits().getTotalHits(), equalTo(1L));

        ShapeBuilder query = ShapeBuilder.newEnvelope().topLeft(-122.88, 48.62).bottomRight(-122.82, 48.54);

        // TODO
//        rsp = client.prepareSearch().setQuery(filteredQuery(matchAllQuery(),
//                geoIntersectionFilter("location", query)))
//                .execute().actionGet();
//        assertThat(rsp.getHits().getTotalHits(), equalTo(1L));
    }

    protected void refresh(String indexName) {
        client.admin().indices().refresh(new RefreshRequest(indexName)).actionGet();
    }
}
