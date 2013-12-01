package com.graphhopper.geocoder;

import com.graphhopper.geohash.KeyAlgo;
import com.graphhopper.geohash.LinearKeyAlgo;
import com.graphhopper.util.DistanceCalc;
import com.graphhopper.util.DistancePlaneProjection;
import com.graphhopper.util.PointList;
import com.graphhopper.util.shapes.BBox;
import gnu.trove.procedure.TIntProcedure;
import gnu.trove.set.hash.TIntHashSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import org.elasticsearch.common.collect.IdentityHashSet;

/**
 * @author Peter Karich
 */
public class BoundaryIndex {

    private static final DistanceCalc distCalc = new DistancePlaneProjection();
    private final List<List<Info>> boundaries;
    private final KeyAlgo keyAlgo;
    private final double deltaLat, deltaLon;
    private int size;

    /**
     * @param b the maximum bounding box of this index
     * @param distance the width of the tiles used in this index. Unit is meter.
     */
    public BoundaryIndex(BBox b, double distance) {

        // every tile is 10*10km^2 big
        // double dist = 10 * 1000;
        double maxLatDist = distCalc.calcDist(b.minLat, b.minLon, b.minLat, b.maxLon);
        double maxLonDist = distCalc.calcDist(b.minLat, b.minLon, b.maxLat, b.minLon);
        int latTiles = (int) (maxLatDist / distance);
        int lonTiles = (int) (maxLonDist / distance);
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
        size++;
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
    
    public int size() {
        return size;
    }

    public Collection<Info> searchContaining(double queryLat, double queryLon) {
        Collection<Info> res = new IdentityHashSet<Info>();

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
     * @param maxDist return the info object ONLY IF the calculated distance is
     * smaller than maxDist
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
}
