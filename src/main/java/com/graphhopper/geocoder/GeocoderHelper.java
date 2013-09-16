package com.graphhopper.geocoder;

import com.github.jsonj.JsonArray;
import com.github.jsonj.JsonElement;
import com.github.jsonj.JsonObject;
import com.graphhopper.util.DistanceCalc;
import com.graphhopper.util.DistancePlaneProjection;
import com.graphhopper.util.PointList;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Peter Karich
 */
public class GeocoderHelper {

    private static DistanceCalc distCalc = new DistancePlaneProjection();

    /**
     * remove more than two spaces or newlines
     */
    public static String innerTrim(String str) {
        if (str.isEmpty())
            return "";

        StringBuilder sb = new StringBuilder();
        boolean previousSpace = false;
        for (int i = 0; i < str.length(); i++) {
            char c = str.charAt(i);
            if (c == ' ' || (int) c == 9 || c == '\n') {
                previousSpace = true;
                continue;
            }

            if (previousSpace)
                sb.append(' ');

            previousSpace = false;
            sb.append(c);
        }
        return sb.toString().trim();
    }

    public static String[] toArray(JsonArray arr) {
        String[] res = new String[arr.size()];
        int i = 0;
        for (String s : arr.asStringArray()) {
            res[i] = s;
            i++;
        }
        return res;
    }

    public static Map<String, Object> toMap(JsonObject obj) {
        Map<String, Object> res = new HashMap<String, Object>(obj.size());
        for (Map.Entry<String, JsonElement> e : obj.entrySet()) {
            res.put(e.getKey(), e.getValue().asString());
        }
        return res;
    }

    public static String toString(InputStream is) throws IOException {
        if (is == null)
            throw new IllegalArgumentException("stream is null!");

        BufferedReader bufReader = new BufferedReader(new InputStreamReader(is, "UTF-8"));
        StringBuilder sb = new StringBuilder();
        String line;
        while ((line = bufReader.readLine()) != null) {
            sb.append(line);
            sb.append('\n');
        }
        bufReader.close();
        return sb.toString();
    }

    /**
     * Picks the point closest to the middle of the road/LineString. Input is a
     * JsonArray of lon,lat arrays
     */
    public static double[] calcMiddlePoint(JsonArray arr) {
        if (arr.isEmpty())
            return null;

        double lat = Double.MAX_VALUE, lon = Double.MAX_VALUE;
        JsonArray firstCoord = arr.get(0).asArray();
        JsonArray lastCoord = arr.get(arr.size() - 1).asArray();
        double latFirst = firstCoord.get(1).asDouble();
        double lonFirst = firstCoord.get(0).asDouble();
        double latLast = lastCoord.get(1).asDouble();
        double lonLast = lastCoord.get(0).asDouble();
        double latMiddle = (latFirst + latLast) / 2;
        double lonMiddle = (lonFirst + lonLast) / 2;
        double minDist = Double.MAX_VALUE;
        for (JsonArray innerArr : arr.arrays()) {
            double latTmp = innerArr.get(1).asDouble();
            double lonTmp = innerArr.get(0).asDouble();
            double tmpDist = distCalc.calcDist(latMiddle, lonMiddle, latTmp, lonTmp);
            if (minDist > tmpDist) {
                minDist = tmpDist;
                lat = latTmp;
                lon = lonTmp;
            }
        }

        return new double[]{lat, lon};
    }

    /**
     * Polygon: JsonArray or JsonArrays containing lon,lat arrays
     */
    static double[] calcSimpleMean(PointList list) {
        if (list.isEmpty())
            return null;
        double lat = 0, lon = 0;
        int max = list.getSize();
        for (int i = 0; i < max; i++) {
            lat += list.getLatitude(i);
            lon += list.getLongitude(i);
        }
        return new double[]{lat / max, lon / max};
    }

    /**
     * Polygon: JsonArray or JsonArrays containing lon,lat arrays
     */
    static double[] calcCentroid(PointList list) {
        if (list.isEmpty())
            return null;

        // simple average is not too precise 
        // so use http://en.wikipedia.org/wiki/Centroid#Centroid_of_polygon
        double lat = 0, lon = 0;
        double polyArea = 0;

        // lat = y, lon = x
        // TMP(i) = (lon_i * lat_(i+1) - lon_(i+1) * lat_i)
        // A = 1/2 sum_0_to_n-1 TMP(i)
        // lat = C_y = 1/6A sum (lat_i + lat_(i+1) ) * TMP(i)
        // lon = C_x = 1/6A sum (lon_i + lon_(i+1) ) * TMP(i)        

        int max = list.getSize() - 1;
        for (int i = 0; i < max; i++) {
            double tmpLat = list.getLatitude(i);
            double tmpLat_p1 = list.getLatitude(i + 1);
            double tmpLon = list.getLongitude(i);
            double tmpLon_p1 = list.getLongitude(i + 1);
            double TMP = tmpLon * tmpLat_p1 - tmpLon_p1 * tmpLat;
            polyArea += TMP;
            lat += (tmpLat + tmpLat_p1) * TMP;
            lon += (tmpLon + tmpLon_p1) * TMP;
        }
        polyArea /= 2;
        lat = lat / (6 * polyArea);
        lon = lon / (6 * polyArea);

        return new double[]{lat, lon};
    }

    public static PointList toPointList(JsonArray arr) {
        if (arr.isEmpty())
            return PointList.EMPTY;

        PointList list = new PointList();
        for (JsonArray innerArr : arr.arrays()) {
            for (JsonArray innerstArr : innerArr.arrays()) {
                double tmpLat = innerstArr.get(1).asDouble();
                double tmpLon = innerstArr.get(0).asDouble();
                list.add(tmpLat, tmpLon);
            }
        }
        return list;
    }
}
