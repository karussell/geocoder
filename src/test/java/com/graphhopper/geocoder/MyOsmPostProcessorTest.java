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

    public static JsonObject createRelationObj() {
        JsonArray coordinates = array();
        coordinates.add(array(11, 11));
        coordinates.add(array(22, 22));
        coordinates.add(array(33, 33));
        coordinates.add(array(11, 11));
        JsonObject geo = $(_("type", "LineString"), _("coordinates", coordinates));
        JsonObject tags = $(
                _("boundary", "administrative"),
                _("admin_level", "7"));

        return $(_("id", "osmway/123"),
                _("geometry", geo),
                _("tags", tags),
                _("title", "testing it today"),
                _("admin_centre", "1237"));
    }

    @Test
    public void testInterpretTags() {
        JsonObject obj = createRelationObj();

        MyOsmPostProcessor postProc = new MyOsmPostProcessor(new JsonParser());
        obj = postProc.interpretTags(obj, obj);

        assertEquals("boundary", obj.getString("type"));
        assertEquals("1237", obj.getString("center_node"));
        assertEquals("7", obj.getString("admin_level"));
        assertEquals("testing it today", obj.getString("name"));

        // out of admin_level bounds 6-8
        obj = createRelationObj();
        obj.getObject("tags").put("admin_level", "5");
        obj = postProc.interpretTags(obj, obj);
        assertNull(obj);
    }
}
