/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.graphhopper.geocoder;

import com.graphhopper.util.PointList;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Peter Karich
 */
public class GeocoderHelperTest {

    @Test
    public void testCalcCentroid() {
        PointList list = new PointList();
        // a roundabout as polygon, here we can see best the difference of centroid + simpleMean
        list.parseJSON("[9.160359,48.694102],[9.1604046,48.6941125],[9.1604343,48.6941271],[9.1604574,48.6941436],"
                + "[9.1604667,48.6941638],[9.1604699,48.6941843],[9.1604615,48.6942038],[9.1604435,48.6942203],"
                + "[9.1604245,48.6942363],[9.1603981,48.6942448],[9.1603657,48.6942496],[9.1603367,48.694249],"
                + "[9.1602975,48.6942395],[9.1602863,48.6942343],[9.1602655,48.69422],[9.1602529,48.6942051],"
                + "[9.1602459,48.694189],[9.160245,48.694169],[9.1602634,48.6941365],[9.1602936,48.6941168],"
                + "[9.160335,48.6941032],[9.160359,48.694102]");
        double[] res = GeocoderHelper.calcCentroid(list);
        assertEquals(48.694130, res[0], 1e-5);
        assertEquals(9.1603476, res[1], 1e-5);

        res = GeocoderHelper.calcSimpleMean(list);
        assertEquals(48.694180, res[0], 1e-5);
        assertEquals(9.1603476, res[1], 1e-5);
    }
}
