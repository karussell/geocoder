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
                    sw.stop();
                    logger.info("took: " + (float) sw.getSeconds() / counter);
                    sw.start();
                    bulkUpdate(list, tmpType, tmpType);
                    list.clear();
                }
            }

            @Override
            public void close() throws IOException {
            }
        };
    }

    @Override
    protected JsonObject interpretTags(JsonObject input, JsonObject geoJson) {
        JsonObject tags = input.getObject("tags");
        JsonObject address = new JsonObject();
        JsonObject name = new JsonObject();
        JsonObject osmCategories = new JsonObject();
        // type is either a place (city|town|village) or a street (primary|secondary)
        // if both is used in OSM (which makes no sense) then street is preferred
        String type = null;
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
                name.getOrCreateArray(language).add(value);
            } else {
                if (tagName.equals("highway")) {
                    if (type != null)
                        logger.warn("Overwrite type " + type + " with " + value + " for " + input);

                    // rare OSM issue
                    // But prefer highway so overwrite e.g. the place=hamlet or locality
                    type = value;
                    osmCategories.put(tagName, value);

                } else if (tagName.equals("leisure")) {
                    osmCategories.put(tagName, value);

                } else if (tagName.equals("amenity")) {
                    osmCategories.put(tagName, value);

                } else if (tagName.equals("natural")) {
                    osmCategories.put(tagName, value);

                } else if (tagName.equals("historic")) {
                    osmCategories.put(tagName, value);

                } else if (tagName.equals("cuisine")) {
                    osmCategories.put(tagName, value);

                } else if (tagName.equals("junction")) {
                    osmCategories.put(tagName, value);

                } else if (tagName.equals("tourism")) {
                    osmCategories.put(tagName, value);

                } else if (tagName.equals("shop")) {
                    osmCategories.put(tagName, value);

                } else if (tagName.equals("building")) {
                    osmCategories.put(tagName, value);

                } else if (tagName.equals("admin-level")) {
                    osmCategories.put(tagName, value);

                } else if (tagName.equals("boundary")) {
                    if (!value.equals("adminstrative"))
                        continue;
                    osmCategories.put(tagName, value);

                } else if (tagName.equals("place")) {
                    if (type != null) {
                        // prefer highway tag
                        logger.warn("Skipping place " + value + " for " + input);
                        continue;
                    }

                    type = value;
                    osmCategories.put(tagName, value);
                }
            }
        }

        if (type == null)
            return null;

        // skip uncategorizable stuff
        if (osmCategories.isEmpty())
            return null;

        geoJson.put("categories", $(_("osm", osmCategories)));
        if (address.size() > 0)
            geoJson.put("address", address);

        geoJson.put("type", type);

        JsonElement val = tags.get("website");
        if (val != null)
            geoJson.put("link", val.asString());

        val = tags.get("wikipedia");
        if (val != null) {
            String str = val.asString();
            int index = str.indexOf(":");
            if (index > 0) {
                String language = str.substring(0, index);
                String value = str.substring(index + 1).replaceAll("\\ ", "_");
                String url = "http://" + language + ".wikipedia.org/wiki/" + GeocoderHelper.encodeUrl(value);
                geoJson.put("wikipedia", url);
            }
        }

        val = tags.get("population");
        if (val != null) {
            try {
                long longVal = Long.parseLong(val.asString());
                geoJson.put("population", longVal);
            } catch (NumberFormatException ex) {
            }
        }

        val = tags.get("is_in");
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
                geoJson.put("is_in", arr);
        }
        return geoJson;
    }
}
