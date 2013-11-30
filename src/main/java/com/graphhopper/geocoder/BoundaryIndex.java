package com.graphhopper.geocoder;

import com.graphhopper.geohash.KeyAlgo;
import com.graphhopper.geohash.LinearKeyAlgo;
import com.graphhopper.util.DistanceCalc;
import com.graphhopper.util.DistancePlaneProjection;
import com.graphhopper.util.PointList;
import com.graphhopper.util.shapes.BBox;
import com.graphhopper.util.shapes.GHPoint;
import static java.lang.Math.cos;
import static java.lang.Math.toRadians;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Peter Karich
 */
public class BoundaryIndex {

    private static final DistanceCalc distCalc = new DistancePlaneProjection();
    private final List<List<Info>> boundaries;
    private final KeyAlgo keyAlgo;
    private final double deltaLat, deltaLon;

    public BoundaryIndex(BBox b) {

        // every tile is 10*10km^2 big
        double dist = 10 * 1000;
        double maxLat = distCalc.calcDist(b.minLat, b.minLon, b.minLat, b.maxLon);
        double maxLon = distCalc.calcDist(b.minLat, b.minLon, b.maxLat, b.minLon);
        int latTiles = (int) (maxLat / dist);
        int lonTiles = (int) (maxLon / dist);
        if (latTiles == 0)
            latTiles++;
        if (lonTiles == 0)
            lonTiles++;

        keyAlgo = new LinearKeyAlgo(latTiles, lonTiles).setBounds(b);
        boundaries = new ArrayList<List<Info>>(latTiles * lonTiles);
        deltaLat = (b.maxLat - b.minLat) / latTiles;
        deltaLon = (b.maxLon - b.minLon) / lonTiles;
    }

    public void add(Info info) {
        int key = (int) keyAlgo.encode(info.center.lat, info.center.lon);
        List<Info> list = boundaries.get(key);
        if (list == null) {
            list = new ArrayList<Info>();
            boundaries.set(key, list);
        }
        list.add(info);
    }

    public List<Info> search(double queryLat, double queryLon) {
        List<Info> res = new ArrayList<Info>();

        // search also around the matching tiles => 9 tiles
        double maxLat = queryLat + deltaLat;
        double maxLon = queryLon + deltaLon;
        for (double tmpLat = queryLat - deltaLat; tmpLat <= maxLat; tmpLat += deltaLat) {
            for (double tmpLon = queryLon - deltaLon; tmpLon <= maxLon; tmpLon += deltaLon) {
                int keyPart = (int) keyAlgo.encode(tmpLat, tmpLon);
                res.addAll(boundaries.get(keyPart));
            }
        }
        return res;
    }

    public static class Info {

        final GHPoint center;
        // every PointList represents a polygon
        final List<PointList> polygons;
        final BBox bbox;
        final List<String> isIn;

        public Info(GHPoint center, List<PointList> polygons, List<String> isIn) {
            this.center = center;
            this.polygons = polygons;
            this.isIn = isIn;
            bbox = BBox.INVERSE.clone();
            for (PointList pl : polygons) {
                int size = pl.size();
                for (int index = 0; index < size; index++) {
                    double lat = pl.getLatitude(index);
                    double lon = pl.getLongitude(index);
                    if (lat > bbox.maxLat)
                        bbox.maxLat = lat;

                    if (lat < bbox.minLat)
                        bbox.minLat = lat;

                    if (lon > bbox.maxLon)
                        bbox.maxLon = lon;

                    if (lon < bbox.minLon)
                        bbox.minLon = lon;
                }
            }
        }

        public boolean contains(double queryLat, double queryLon) {
            if (!bbox.contains(queryLat, queryLon))
                return false;

            double factor = Math.cos(Math.toRadians(queryLat));
            queryLon *= factor;

            for (PointList pl : polygons) {
                int size = pl.size();
                if (size == 0)
                    continue;

                boolean contains = false;
                // http://stackoverflow.com/a/2922778/194609
                for (int i = 0, j = size - 1; i < size; j = i++) {
                    double latI = pl.getLatitude(i), lonI = pl.getLongitude(i) * factor;
                    double latJ = pl.getLatitude(j), lonJ = pl.getLongitude(j) * factor;
                    if (((latI > queryLat) != (latJ > queryLat))
                            && (queryLon < (lonJ - lonI) * (queryLat - latI) / (latJ - latI) + lonI))
                        contains = !contains;
                }
                if (contains)
                    return true;
            }
            return false;
        }

        public double calculateDistance(double lat, double lon) {
            return distCalc.calcDist(center.lat, center.lon, lat, lon);
        }
    }
}
