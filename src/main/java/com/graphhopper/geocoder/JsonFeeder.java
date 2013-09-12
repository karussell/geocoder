package com.graphhopper.geocoder;

import com.github.jillesvangurp.osm2geojson.OsmPostProcessor;
import com.github.jsonj.tools.JsonParser;

/**
 * @author Peter Karich
 */
public class JsonFeeder {

    public static void main(String[] args) {
        new JsonFeeder().feed();
    }

    public void feed() {
        OsmPostProcessor processor = new OsmPostProcessor(new JsonParser());
        processor.processNodes();
        processor.processWays();
    }
}
