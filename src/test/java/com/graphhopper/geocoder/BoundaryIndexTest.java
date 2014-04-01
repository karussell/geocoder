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
        PointList pl1 = new PointList(3, false);
        pl1.add(0, 0);
        pl1.add(1, 1);
        pl1.add(0, 2);
        pl1.add(0, 0);
        polygons.add(pl1);
        List<String> isInList = new ArrayList<String>();
        Info info = new Info("1", new GHPoint(), polygons, isInList);
        index.add(info);

        polygons = new ArrayList<PointList>();
        PointList pl2 = new PointList(3, false);
        pl2.add(0, -1);
        pl2.add(1, 0);
        pl2.add(0, 1);
        pl2.add(0, -1);
        polygons.add(pl2);
        info = new Info("2", new GHPoint(), polygons, isInList);
        index.add(info);

        Collection<Info> matchingBounds = index.searchContaining(0.8, 0.5);
        assertEquals(0, matchingBounds.size());
        
        matchingBounds = index.searchContaining(0.5, 1);
        assertEquals(1, matchingBounds.size());

        matchingBounds = index.searchContaining(0.2, 0.5);
        assertEquals(2, matchingBounds.size());
    }
}
