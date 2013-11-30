package com.graphhopper.geocoder;

import com.graphhopper.geohash.KeyAlgo;
import com.graphhopper.geohash.LinearKeyAlgo;
import com.graphhopper.util.DistanceCalc;
import com.graphhopper.util.DistancePlaneProjection;
import com.graphhopper.util.PointList;
import com.graphhopper.util.shapes.BBox;
import com.graphhopper.util.shapes.GHPoint;
import gnu.trove.procedure.TIntProcedure;
import gnu.trove.set.hash.TIntHashSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

/**
 * @author Peter Karich
 */
public class BoundaryIndex {

    private static final DistanceCalc distCalc = new DistancePlaneProjection();
    private final List<List<Info>> boundaries;
    private final KeyAlgo keyAlgo;
    private final double deltaLat, deltaLon;

    public BoundaryIndex(BBox b, double distance) {

        // every tile is 10*10km^2 big
        // double dist = 10 * 1000;
        double maxLat = distCalc.calcDist(b.minLat, b.minLon, b.minLat, b.maxLon);
        double maxLon = distCalc.calcDist(b.minLat, b.minLon, b.maxLat, b.minLon);
        int latTiles = (int) (maxLat / distance);
        int lonTiles = (int) (maxLon / distance);
        if (latTiles == 0)
            latTiles++;
        if (lonTiles == 0)
            lonTiles++;

        keyAlgo = new LinearKeyAlgo(latTiles, lonTiles).setBounds(b);
        int max = latTiles * lonTiles;
        // reserve space
        boundaries = new ArrayList<List<Info>>(max);
        for (int i = 0; i < max; i++) {
            boundaries.add(null);
        }
        deltaLat = (b.maxLat - b.minLat) / latTiles;
        deltaLon = (b.maxLon - b.minLon) / lonTiles;
    }

    public void add(final Info info) {
        // determine all indices (keys) where we should store the polygon
        TIntHashSet allKeys = new TIntHashSet();
        for (PointList pl : info.polygons) {
            for (int i = 0; i < pl.size(); i++) {
                allKeys.add((int) keyAlgo.encode(pl.getLatitude(i), pl.getLongitude(i)));
            }
        }

        // now add only once to the lists
        allKeys.forEach(new TIntProcedure() {

            @Override
            public boolean execute(int key) {
                List<Info> list = boundaries.get(key);
                if (list == null) {
                    list = new ArrayList<Info>();
                    boundaries.set(key, list);
                }
                list.add(info);
                return true;
            }
        });
    }

    public Collection<Info> searchContaining(double queryLat, double queryLon) {
        Collection<Info> res = new HashSet<Info>();

        // search around the matching tiles => 9 tiles
        double maxLat = queryLat + deltaLat;
        double maxLon = queryLon + deltaLon;
        for (double tmpLat = queryLat - deltaLat; tmpLat <= maxLat; tmpLat += deltaLat) {
            for (double tmpLon = queryLon - deltaLon; tmpLon <= maxLon; tmpLon += deltaLon) {
                // 1. filter by key (which is similar to a bounding box)
                int keyPart = (int) keyAlgo.encode(tmpLat, tmpLon);
                List<Info> list = boundaries.get(keyPart);
                if (list != null) {
                    for (Info info : list) {
                        // skip if already containing
                        if (res.contains(info))
                            continue;
                        // 2. filter by more precise 'contains' algorithm
                        if (info.contains(queryLat, queryLon))
                            res.add(info);
                    }
                }
            }
        }
        return res;
    }

    /**
     * Search the closest info object to the specified query coordinates.
     *
     * @param maxDist return the info object ONLY IF the calculated distance is smaller
     * than maxDist
     */
    public Info searchClosest(double queryLat, double queryLon, double maxDist) {
        Info closest = null;
        double distance = Double.MAX_VALUE;

        // search around the matching tiles => 9 tiles
        double maxLat = queryLat + deltaLat;
        double maxLon = queryLon + deltaLon;
        for (double tmpLat = queryLat - deltaLat; tmpLat <= maxLat; tmpLat += deltaLat) {
            for (double tmpLon = queryLon - deltaLon; tmpLon <= maxLon; tmpLon += deltaLon) {
                int keyPart = (int) keyAlgo.encode(tmpLat, tmpLon);
                List<Info> list = boundaries.get(keyPart);
                if (list != null) {
                    for (Info info : list) {
                        double tmpDistance = info.calculateDistance(tmpLat, tmpLon);
                        if (tmpDistance < distance) {
                            distance = tmpDistance;
                            closest = info;
                        }
                    }
                }
            }
        }
        if (maxDist < distance)
            return null;
        return closest;
    }

    public static class Info {

        final GHPoint center;
        // every PointList represents a polygon
        final List<PointList> polygons;
        final BBox bbox;
        final List<String> isIn;
        final double area;

        public Info(GHPoint center, List<PointList> polygons, List<String> isIn) {
            this.center = center;
            this.polygons = polygons;
            this.isIn = isIn;
            bbox = BBox.INVERSE.clone();
            double tmpArea = 0;
            for (PointList pl : polygons) {
                int size = pl.size();
                if (!pl.toGHPoint(0).equals(pl.toGHPoint(size - 1)))
                    throw new IllegalStateException("polygon should end and start with same point " + isIn);

                tmpArea += GeocoderHelper.calcAreaGH(pl);
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
            area = tmpArea;
        }

        public boolean contains(double queryLat, double queryLon) {
            if (!bbox.contains(queryLat, queryLon))
                return false;

            for (PointList pl : polygons) {
                int size = pl.size();
                if (size == 0)
                    continue;

                boolean contains = false;
                // http://stackoverflow.com/a/2922778/194609
                for (int i = 0, j = size - 1; i < size; j = i++) {
                    double latI = pl.getLatitude(i), lonI = pl.getLongitude(i);
                    double latJ = pl.getLatitude(j), lonJ = pl.getLongitude(j);
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

        @Override
        public String toString() {
            return center + " " + isIn;
        }
    }
}
