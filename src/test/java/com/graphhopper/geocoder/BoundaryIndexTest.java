package com.graphhopper.geocoder;

import com.graphhopper.util.PointList;
import com.graphhopper.util.shapes.BBox;
import com.graphhopper.util.shapes.GHPoint;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Peter Karich
 */
public class BoundaryIndexTest {

    @Test
    public void testSearch() {
        BBox bbox = new BBox(-4, 4, -4, 4);
        BoundaryIndex index = new BoundaryIndex(bbox, 100 * 1000);

        List<PointList> polygons = new ArrayList<PointList>();
        PointList pl1 = new PointList(3);
        pl1.add(0, 0);
        pl1.add(1, 1);
        pl1.add(0, 2);
        pl1.add(0, 0);
        polygons.add(pl1);
        List<String> isInList = new ArrayList<String>();
        BoundaryIndex.Info info = new BoundaryIndex.Info(new GHPoint(), polygons, isInList);
        index.add(info);

        polygons = new ArrayList<PointList>();
        PointList pl2 = new PointList(3);
        pl2.add(0, -1);
        pl2.add(1, 0);
        pl2.add(0, 1);
        pl2.add(0, -1);
        polygons.add(pl2);
        info = new BoundaryIndex.Info(new GHPoint(), polygons, isInList);
        index.add(info);

        Collection<BoundaryIndex.Info> matchingBounds = index.searchContaining(0.8, 0.5);
        assertEquals(0, matchingBounds.size());
        
        matchingBounds = index.searchContaining(0.5, 1);
        assertEquals(1, matchingBounds.size());

        matchingBounds = index.searchContaining(0.2, 0.5);
        assertEquals(2, matchingBounds.size());
    }

    @Test
    public void testContainsPoint() {
        GHPoint point = new GHPoint();
        List<PointList> polygons = new ArrayList<PointList>();
        PointList pl1 = new PointList(3);
        pl1.add(0, 0);
        pl1.add(1, 1);
        pl1.add(0, 2);
        pl1.add(0, 0);
        polygons.add(pl1);
        List<String> isInList = new ArrayList<String>();
        BoundaryIndex.Info info = new BoundaryIndex.Info(point, polygons, isInList);
        assertFalse(info.contains(-0.1, 0));
        assertFalse(info.contains(0.5, 0.2));
        assertTrue(info.contains(0.5, 0.9));

        polygons.clear();
        PointList pl2 = new PointList(3);
        pl2.add(0, -1);
        pl2.add(1, 0);
        pl2.add(0, 1);
        pl2.add(0, -1);
        polygons.add(pl2);
        info = new BoundaryIndex.Info(point, polygons, isInList);
        assertFalse(info.contains(-0.1, 0));
        assertTrue(info.contains(0.5, 0.2));
        assertFalse(info.contains(0.5, 0.9));

        polygons.clear();
        polygons.add(pl1);
        polygons.add(pl2);
        info = new BoundaryIndex.Info(point, polygons, isInList);
        assertFalse(info.contains(-0.1, 0));
        assertTrue(info.contains(0.3, 0.5));
        assertFalse(info.contains(0.6, 0.5));
    }
}
