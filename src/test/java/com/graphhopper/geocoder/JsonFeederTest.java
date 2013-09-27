package com.graphhopper.geocoder;

import com.github.jsonj.JsonArray;
import com.github.jsonj.JsonObject;
import static com.github.jsonj.tools.JsonBuilder.$;
import static com.github.jsonj.tools.JsonBuilder._;
import static com.github.jsonj.tools.JsonBuilder.array;
import com.github.jsonj.tools.JsonParser;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
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
        list.add($(_("id", "osmnode/123"), _("geometry", geo), _("name", "testing it today")));

        Collection<Integer> res = feeder.bulkUpdate(list, osmIndex, osmType);
        assertEquals(res.toString(), 0, res.size());
        refresh(osmIndex);
        assertEquals(client.prepareCount(osmIndex).execute().actionGet().getCount(), 1);

        SearchResponse rsp = queryHandler.doRequest("today");
        assertEquals(1, rsp.getHits().getTotalHits());
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
        list.add($(_("id", "osmway/123"), _("geometry", geo), _("name", "testing it today")));

        Collection<Integer> res = feeder.bulkUpdate(list, osmIndex, osmType);
        assertEquals(0, res.size());
        refresh(osmIndex);
        assertEquals(client.prepareCount(osmIndex).execute().actionGet().getCount(), 1);

        SearchResponse rsp = queryHandler.doRequest("today");
        assertEquals(1, rsp.getHits().getTotalHits());

        // TODO
//        ShapeBuilder query = ShapeBuilder.newEnvelope().topLeft(-122.88, 48.62).bottomRight(-122.82, 48.54);
//        rsp = client.prepareSearch().setQuery(filteredQuery(matchAllQuery(),
//                geoIntersectionFilter("location", query)))
//                .execute().actionGet();
//        assertThat(rsp.getHits().getTotalHits(), equalTo(1L));
    }

    @Test
    public void testFeedRelation() {
        JsonArray coordinates = array();
        coordinates.add(array(11, 11));
        coordinates.add(array(22, 22));
        coordinates.add(array(33, 33));        
        JsonObject obj = MyOsmPostProcessorTest.createRelationObj();

        MyOsmPostProcessor postProc = new MyOsmPostProcessor(new JsonParser());
        obj = postProc.interpretTags(obj, obj);
        
        List<JsonObject> list = new ArrayList<JsonObject>();
        list.add(obj);
        Collection<Integer> res = feeder.bulkUpdate(list, osmIndex, osmType);
        assertEquals(res.size(), 0);
        refresh(osmIndex);
        
        SearchResponse rsp = queryHandler.rawRequest("has_boundary:true");
        assertEquals(1, rsp.getHits().getTotalHits(), 1);
        
        rsp = queryHandler.rawRequest("has_boundary:false");
        assertEquals(0, rsp.getHits().getTotalHits());
        
        rsp = queryHandler.rawRequest("admin_level:7");
        assertEquals(1, rsp.getHits().getTotalHits());
        
        rsp = queryHandler.rawRequest("admin_level:6");
        assertEquals(0, rsp.getHits().getTotalHits());
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
