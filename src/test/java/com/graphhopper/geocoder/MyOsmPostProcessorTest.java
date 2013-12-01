package com.graphhopper.geocoder;

import com.github.jsonj.JsonArray;
import com.github.jsonj.JsonObject;
import com.github.jsonj.tools.JsonParser;
import static com.github.jsonj.tools.JsonBuilder.$;
import static com.github.jsonj.tools.JsonBuilder._;
import static com.github.jsonj.tools.JsonBuilder.array;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Peter Karich
 */
public class MyOsmPostProcessorTest {

    public static JsonObject createTestObject() {
        JsonObject tags = $(
                _("boundary", "administrative"),
                _("admin_level", "7"));

        return $(_("id", "osmway/333"),
                _("tags", tags),
                _("title", "testing it today"),
                _("admin_centre", "1237"));
    }

    public static JsonObject createPolygon() {
        JsonArray outerBoundary = array();
        outerBoundary.add(array(11, 11));
        outerBoundary.add(array(22, 11));
        outerBoundary.add(array(22, 22));
        outerBoundary.add(array(11, 22));
        outerBoundary.add(array(11, 11));
        JsonArray coordinates = array();
        coordinates.add(outerBoundary);
        JsonObject geo = $(_("type", "Polygon"), _("coordinates", coordinates));
        JsonObject obj = createTestObject();
        obj.put("geometry", geo);
        return obj;
    }

    public static JsonObject createMultiPolygon() {
        JsonArray coordinates = array();

        JsonArray outerBoundary = array();
        outerBoundary.add(array(11, 11));
        outerBoundary.add(array(22, 11));
        outerBoundary.add(array(22, 22));
        outerBoundary.add(array(11, 22));
        outerBoundary.add(array(11, 11));
        JsonArray poly = array();
        poly.add(outerBoundary);
        coordinates.add(poly);

        outerBoundary = array();
        outerBoundary.add(array(4, 4));
        outerBoundary.add(array(5, 4));
        outerBoundary.add(array(5, 5));
        outerBoundary.add(array(4, 5));
        outerBoundary.add(array(4, 4));
        poly = array();
        poly.add(outerBoundary);
        coordinates.add(poly);

        JsonObject geo = $(_("type", "MultiPolygon"), _("coordinates", coordinates));
        JsonObject obj = createTestObject();
        obj.put("geometry", geo);
        return obj;
    }

    @Test
    public void testInterpretTags() {
        JsonObject obj = createPolygon();

        MyOsmPostProcessor postProc = new MyOsmPostProcessor(new JsonParser());
        obj = postProc.interpretTags(obj, obj);

        assertEquals("boundary", obj.getString("type"));
        assertEquals("1237", obj.getString("center_node"));
        assertEquals("7", obj.getString("admin_level"));
        assertEquals("testing it today", obj.getString("name"));

        // out of admin_level bounds 6-8
        obj = createPolygon();
        obj.getObject("tags").put("admin_level", "5");
        obj = postProc.interpretTags(obj, obj);
        assertNull(obj);
        
        // city with bounds
        obj = createPolygon();
        obj.getObject("tags").put("place", "city");
        obj = postProc.interpretTags(obj, obj);
        assertEquals("city", obj.getString("type"));
        assertEquals("7", obj.getString("admin_level"));
        assertNotNull(obj.get("geometry"));
    }
}
