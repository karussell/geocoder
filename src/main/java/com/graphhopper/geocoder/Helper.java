package com.graphhopper.geocoder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Peter Karich
 */
public class Helper {
    
    private static Logger logger = LoggerFactory.getLogger(Helper.class);

    public static boolean isEmpty(String str) {
        return str == null || str.trim().length() == 0;
    }

    public static void loadProperties(Map<String, String> map, Reader tmpReader) throws IOException {
        BufferedReader reader = new BufferedReader(tmpReader);
        String line;
        try {
            while ((line = reader.readLine()) != null) {
                if (line.startsWith("//") || line.startsWith("#")) {
                    continue;
                }

                if (Helper.isEmpty(line)) {
                    continue;
                }

                int index = line.indexOf("=");
                if (index < 0) {
                    logger.warn("Skipping configuration at line:" + line);
                    continue;
                }

                String field = line.substring(0, index);
                String value = line.substring(index + 1);
                map.put(field, value);
            }
        } finally {
            reader.close();
        }
    }
}
