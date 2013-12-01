package com.graphhopper.geocoder;

import com.graphhopper.util.DistanceCalc;
import com.graphhopper.util.DistancePlaneProjection;
import com.graphhopper.util.PointList;
import com.graphhopper.util.shapes.BBox;
import com.graphhopper.util.shapes.GHPoint;
import java.util.List;

/**
 * @author Peter Karich
 */
public class Info {

    private static final DistanceCalc distCalc = new DistancePlaneProjection();
    private final GHPoint center;
    // every PointList represents a polygon
    final List<PointList> polygons;
    private final BBox bbox;
    private final List<String> isIn;
    private final double area;

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

    public List<String> getIsIn() {
        return isIn;
    }

    public double calculateDistance(double lat, double lon) {
        return distCalc.calcDist(center.lat, center.lon, lat, lon);
    }

    @Override
    public String toString() {
        return center + " " + isIn;
    }
}
