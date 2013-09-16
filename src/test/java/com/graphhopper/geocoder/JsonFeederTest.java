package com.graphhopper.geocoder;

import com.github.jsonj.JsonArray;
import com.github.jsonj.JsonObject;
import static com.github.jsonj.tools.JsonBuilder.$;
import static com.github.jsonj.tools.JsonBuilder._;
import static com.github.jsonj.tools.JsonBuilder.array;
import com.graphhopper.util.PointList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.*;
import static org.junit.Assert.*;

/**
 *
 * @author Peter Karich
 */
public class JsonFeederTest extends AbstractNodesTests {

    protected static Client client;
    private String osmIndex = "osm";
    private String osmType = "osmobject";
    private JsonFeeder feeder;
    private QueryHandler queryHandler;

    @BeforeClass public static void createNodes() throws Exception {
        startNode("node1");
        client = client("node1");
    }

    @AfterClass public static void closeNodes() {
        client.close();
        closeAllNodes();
    }

    @Before
    public void setUp() {
        feeder = new JsonFeeder();
        feeder.setClient(client);
        feeder.initIndices();
        queryHandler = new QueryHandler().setClient(client);
    }

    @After
    public void tearDown() {
        deleteAll(osmIndex);
    }

    @Test
    public void testFeedPoint() {
        List<JsonObject> list = new ArrayList<JsonObject>();
        JsonArray coordinates = array(-11, 11);
        JsonObject geo = $(_("type", "Point"), _("coordinates", coordinates));
        list.add($(_("id", "osmnode/123"), _("geometry", geo), _("title", "testing it today")));

        Collection<Integer> res = feeder.bulkUpdate(list, osmIndex, osmType);
        assertEquals(res.toString(), res.size(), 0);
        refresh(osmIndex);
        assertEquals(client.prepareCount(osmIndex).execute().actionGet().getCount(), 1);

        SearchResponse rsp = queryHandler.doRequest("today");
        assertEquals(rsp.getHits().getTotalHits(), 1);
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

        Collection<Integer> res = feeder.bulkUpdate(list, osmIndex, osmType);
        assertEquals(res.size(), 0);
        refresh(osmIndex);
        assertEquals(client.prepareCount(osmIndex).execute().actionGet().getCount(), 1);

        SearchResponse rsp = queryHandler.doRequest("today");
        assertEquals(rsp.getHits().getTotalHits(), 1);

        // TODO
//        ShapeBuilder query = ShapeBuilder.newEnvelope().topLeft(-122.88, 48.62).bottomRight(-122.82, 48.54);
//        rsp = client.prepareSearch().setQuery(filteredQuery(matchAllQuery(),
//                geoIntersectionFilter("location", query)))
//                .execute().actionGet();
//        assertThat(rsp.getHits().getTotalHits(), equalTo(1L));
    }

    @Test
    public void testCalcCentroid() {
        PointList list = new PointList();
        // a roundabout as polygon, here we can see best the difference of centroid + simpleMean
        list.parseJSON("[9.160359,48.694102],[9.1604046,48.6941125],[9.1604343,48.6941271],[9.1604574,48.6941436],"
                + "[9.1604667,48.6941638],[9.1604699,48.6941843],[9.1604615,48.6942038],[9.1604435,48.6942203],"
                + "[9.1604245,48.6942363],[9.1603981,48.6942448],[9.1603657,48.6942496],[9.1603367,48.694249],"
                + "[9.1602975,48.6942395],[9.1602863,48.6942343],[9.1602655,48.69422],[9.1602529,48.6942051],"
                + "[9.1602459,48.694189],[9.160245,48.694169],[9.1602634,48.6941365],[9.1602936,48.6941168],"
                + "[9.160335,48.6941032],[9.160359,48.694102]");
        double[] res = JsonFeeder.calcCentroid(list);
        assertEquals(48.694130, res[0], 1e-5);
        assertEquals(9.1603476, res[1], 1e-5);

        res = JsonFeeder.calcSimpleMean(list);
        assertEquals(48.694180, res[0], 1e-5);
        assertEquals(9.1603476, res[1], 1e-5);
    }

    protected void refresh(String indexName) {
        client.admin().indices().refresh(new RefreshRequest(indexName)).actionGet();
    }

    protected void deleteAll(String indexName) {
        client.prepareDeleteByQuery(indexName).
                setQuery(QueryBuilders.matchAllQuery()).
                execute().actionGet();
        refresh(indexName);
    }
}
