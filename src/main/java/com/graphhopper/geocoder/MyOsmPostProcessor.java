package com.graphhopper.geocoder;

import static com.github.jsonj.tools.JsonBuilder.*;
import com.github.jillesvangurp.osm2geojson.OsmPostProcessor;
import com.github.jsonj.JsonArray;
import com.github.jsonj.JsonElement;
import com.github.jsonj.JsonObject;
import com.github.jsonj.tools.JsonParser;
import com.graphhopper.util.StopWatch;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Peter Karich
 */
public class MyOsmPostProcessor extends OsmPostProcessor {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private StopWatch sw = new StopWatch().start();
    private long counter = 0;
    private int bulkSize;

    public MyOsmPostProcessor(JsonParser jsonParser) {
        super(jsonParser);
    }

    public MyOsmPostProcessor setBulkSize(int bulkSize) {
        this.bulkSize = bulkSize;
        return this;
    }

    public Collection<Integer> bulkUpdate(List<JsonObject> objects, String indexName, String indexType) {
        return Collections.EMPTY_LIST;
    }

    @Override
    protected OsmPostProcessor.JsonWriter createJsonWriter(OsmPostProcessor.OsmType type) throws IOException {
        final String tmpType = type.toString().toLowerCase();
        return new OsmPostProcessor.JsonWriter() {
            List<JsonObject> list = new ArrayList<JsonObject>(bulkSize);

            @Override
            public void add(JsonObject json) throws IOException {
                list.add(json);
                if (list.size() >= bulkSize) {
                    counter += list.size();
//                    sw.stop();
//                    logger.info("took: " + (float) sw.getSeconds() / counter);
//                    sw.start();
                    bulkUpdate(list, tmpType, tmpType);
                    list.clear();
                }
            }

            @Override
            public void close() throws IOException {
                // feed remaining in the list!
                counter += list.size();
                bulkUpdate(list, tmpType, tmpType);
                list.clear();
            }
        };
    }

    @Override
    protected JsonObject interpretTags(JsonObject input, JsonObject mainJson) {
        JsonObject tags = input.getObject("tags");
        input.remove("tags");
        JsonObject address = new JsonObject();
        JsonObject names = new JsonObject();
        JsonObject osmCategories = new JsonObject();
        String local_place = null;
        for (Map.Entry<String, JsonElement> entry : tags.entrySet()) {
            String tagName = entry.getKey();
            String value = entry.getValue().asString();
            if (tagName.startsWith("addr:")) {
                // http://wiki.openstreetmap.org/wiki/Key:addr                
                String addrKey = entry.getKey().substring(5);
                // skip not necessary address data
                if (addrKey.equals("interpolation") || addrKey.equals("inclusion"))
                    // TODO use them to associate numbers to way somehow!?
                    // http://wiki.openstreetmap.org/wiki/Addresses#Using_interpolation
                    continue;
                address.put(addrKey, value);
            } else if (tagName.startsWith("name:")) {
                String language = tagName.substring(5);
                names.put(language, value);
            } else if (tagName.endsWith(":place")) {
                local_place = value;
            }
        }

        String value = tags.getString("leisure");
        if (value != null)
            osmCategories.put("leisure", value);

        value = tags.getString("amenity");
        if (value != null)
            osmCategories.put("amenity", value);

        value = tags.getString("natural");
        if (value != null)
            osmCategories.put("natural", value);

        value = tags.getString("historic");
        if (value != null)
            osmCategories.put("historic", value);

        value = tags.getString("cuisine");
        if (value != null)
            osmCategories.put("cuisine", value);

        value = tags.getString("junction");
        if (value != null)
            osmCategories.put("junction", value);

        value = tags.getString("tourism");
        if (value != null)
            osmCategories.put("tourism", value);

        value = tags.getString("shop");
        if (value != null)
            osmCategories.put("shop", value);

        value = tags.getString("building");
        if (value != null)
            osmCategories.put("building", value);

        // 'type' is either place (city|town|village), highway (primary|secondary) or boundary.
        // If more than one of these is used in OSM (which makes no sense) then highway over place 
        // and boundary over highway is preferred
        String type = null;
        value = tags.getString("place");
        if (value != null) {
            type = value;
            osmCategories.put("place", value);
        }

        value = tags.getString("highway");
        if (value != null) {
            // prefer highway so overwrite e.g. the place=hamlet or locality
            type = value;
            osmCategories.put("highway", value);
        }

        String border_type = tags.getString("border_type");

        boolean isAreaAdminBound = false;
        value = tags.getString("boundary");
        if ("administrative".equals(value) && notState(local_place) && notState(border_type)) {
            isAreaAdminBound = true;
            // prefer bounds over highway or place
            type = "boundary";
        }

        JsonElement name = mainJson.get("title");
        if (type == null || name == null)
            return null;
        mainJson.put("type", type);
        // I don't like title -> use name instead                
        mainJson.put("name", name.asString());
        mainJson.remove("title");

        if (!names.isEmpty())
            mainJson.put("names", names);

        if (!osmCategories.isEmpty())
            mainJson.put("categories", $(_("osm", osmCategories)));

        value = tags.getString("admin_level");
        if (value != null && isAreaAdminBound)
            try {
                int adminLevel = Integer.parseInt(value);
                // 9 (city districts)
                // 8 (cities, villages, hamlets)
                // accept 7 (towns) 
                // accept 6: cities do not (often?) have boundaries
                if (adminLevel < 6 || adminLevel > 9)
                    return null;
                mainJson.put("admin_level", adminLevel);
            } catch (NumberFormatException ex) {
                logger.warn("cannot parse adminlevel:" + value, ex);
            }

        String centerNode = mainJson.getString("admin_centre");
        if (centerNode != null) {
            mainJson.put("center_node", centerNode);
            mainJson.remove("admin_centre");
        }

        JsonElement val = tags.get("website");
        if (val != null)
            mainJson.put("link", val.asString());

        val = tags.get("wikipedia");
        if (val != null) {
            String str = val.asString();
            int index = str.indexOf(":");
            if (index > 0) {
                String language = str.substring(0, index);
                String tmpValue = str.substring(index + 1).replaceAll("\\ ", "_");
                String url = "https://" + language + ".wikipedia.org/wiki/" + GeocoderHelper.encodeUrl(tmpValue);
                mainJson.put("wikipedia", url);
            }
        }

        if (address.get("postcode") != null) {
            val = tags.get("postal_code");
            if (val == null)
                val = tags.get("openGeoDB:postal_codes");
            if (val != null)
                address.put("postcode", val.asString());
        }

        if (address.size() > 0)
            mainJson.put("address", address);

        val = tags.get("population");
        if (val == null)
            val = tags.get("openGeoDB:population");

        if (val != null) {
            try {
                long longVal = Long.parseLong(val.asString());
                mainJson.put("population", longVal);
            } catch (NumberFormatException ex) {
            }
        }

        val = tags.get("is_in");
        if (val == null)
            val = tags.get("openGeoDB:is_in");

        if (val != null) {
            JsonArray arr = array();
            String strs[];
            if (val.asString().contains(";")) {
                strs = val.asString().split("\\;");
            } else {
                strs = val.asString().split("\\,");
            }

            for (String str : strs) {
                if (!str.trim().isEmpty())
                    arr.add(str.trim());
            }
            if (!arr.isEmpty())
                mainJson.put("is_in", arr);
        }
        return mainJson;
    }

    private boolean notState(String val) {
        return !"county".equals(val) && !"state".equals(val);
    }
}
