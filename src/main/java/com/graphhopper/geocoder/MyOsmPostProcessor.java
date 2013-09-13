package com.graphhopper.geocoder;

import static com.github.jsonj.tools.JsonBuilder.*;
import com.github.jillesvangurp.osm2geojson.OsmPostProcessor;
import com.github.jsonj.JsonElement;
import com.github.jsonj.JsonObject;
import com.github.jsonj.tools.JsonParser;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.elasticsearch.common.StopWatch;
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

    public Collection<Integer> bulkUpdate(Collection<JsonObject> objects, String indexName, String indexType) {
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
                    logger.info("took: " + (float) sw.totalTime().getSeconds() / counter);
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
        for (Map.Entry<String, JsonElement> entry : tags.entrySet()) {
            String tagName = entry.getKey();
            String value = entry.getValue().asString();
            if (tagName.startsWith("addr:")) {
                address.put(entry.getKey().substring(5), value);
            } else if (tagName.startsWith("name:")) {
                String language = tagName.substring(5);
                name.getOrCreateArray(language).add(value);
            } else {
                if (tagName.equals("highway")) {
                    // osmCategories.add("street");
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

                } else if (tagName.equals("tourism")) {
                    osmCategories.put(tagName, value);

                } else if (tagName.equals("shop")) {
                    osmCategories.put(tagName, value);

                } else if (tagName.equals("building")) {
                    osmCategories.put(tagName, value);

                } else if (tagName.equals("admin-level")) {
                    osmCategories.put(tagName, value);

                } else if (tagName.equals("place")) {
                    osmCategories.put(tagName, value);
                }
            }
        }

        // skip uncategorizable stuff
        if (osmCategories.isEmpty())
            return null;

        geoJson.put("categories", $(_("osm", osmCategories)));
        if (address.size() > 0) {
            geoJson.put("address", address);
        }

        Object val = tags.get("population");
        if (val != null)
            geoJson.put("population", val);
        return geoJson;
    }
}
