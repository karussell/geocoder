package org.placerefs.gazetteer;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.index.ItemVisitor;
import com.vividsolutions.jts.index.strtree.STRtree;
import java.awt.Color;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.Insets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.RepaintManager;

/**
 * Part of the SInteliGIS Temporal Expression Tagger, with minor modifications.
 * Stands under new BSD license. See the project page for more information
 * http://code.google.com/p/stemptag/
 */
public class ConcaveHullBuilder {

    private static GeometryFactory gf = new GeometryFactory();
    private Random rand = new Random();

    public static void main(String[] args) {
        ConcaveHullBuilder hull = new ConcaveHullBuilder();
        hull.demo();
    }

    public void setSeed(long seed) {
        rand = new Random(seed);
    }

    private void demo() {

        /**
         * Make an irregular polygon and generate random points within it to
         * test the concave hull algorithm
         */
        final int numPoints = 200;

        Coordinate[] vertices = {
            new Coordinate(0, 0),
            new Coordinate(100, 100),
            new Coordinate(0, 250),
            new Coordinate(200, 300),
            new Coordinate(200, 450),
            new Coordinate(350, 450),
            new Coordinate(350, 200),
            new Coordinate(250, 200),
            new Coordinate(350, 100),
            new Coordinate(0, 0)
        };

        Polygon poly = gf.createPolygon(gf.createLinearRing(vertices), null);
        Envelope env = poly.getEnvelopeInternal();

        List<Point> points = new ArrayList<Point>(numPoints);
        while (points.size() < numPoints) {
            Coordinate c = new Coordinate();
            c.x = rand.nextDouble() * env.getWidth() + env.getMinX();
            c.y = rand.nextDouble() * env.getHeight() + env.getMinY();
            Point p = gf.createPoint(c);
            if (poly.contains(p)) {
                points.add(p);
            }
        }

        List<double[]> edges = getConcaveHull(points, 50d);

        display((int) (env.getWidth() * 1.1), (int) (env.getHeight() * 1.1),
                poly, Color.GRAY,
                points, Color.RED,
                edges, Color.RED);
    }

    /**
     * Identify a concave hull using the simple alpha-shape algorithm described
     * in: <blockquote> Shen Wei (2003) Building boundary extraction based on
     * LIDAR point clouds data. The International Archives of the
     * Photogrammetry, Remote Sensing and Spatial Information Sciences. Vol.
     * XXXVII. Part B3b. Beijing 2008 </blockquote>
     *
     * @param points the point cloud
     * @param alpha the single parameter for the algorithm
     * @return a List of double array (x1,y1,x2,y2) boundary segments for the
     * concave hull
     */
    public List<double[]> getConcaveHull(List<Point> points, double alpha) {
        double alpha2 = 2 * alpha;
        STRtree index = new STRtree();
        for (Point p : points) {
            index.insert(p.getEnvelopeInternal(), p);
        }
        index.build();
        List<double[]> edges = new ArrayList<double[]>();
        for (Point p1 : points) {
            Envelope qEnv = new Envelope(
                    p1.getX() - alpha2, p1.getX() + alpha2,
                    p1.getY() - alpha2, p1.getY() + alpha2);
            PointVisitor visitor = new PointVisitor(p1, alpha2);
            index.query(qEnv, visitor);
            if (visitor.plist.size() < 2) {
                break;
            }
            visitor.plist.remove(p1);
            boolean[] used = new boolean[visitor.plist.size()];
            int numPts = visitor.plist.size();
            double qAlpha = alpha * alpha;
            while (numPts > 0) {
                Point p2;
                while (true) {
                    int pindex = rand.nextInt(visitor.plist.size());
                    if (!used[pindex]) {
                        p2 = visitor.plist.get(pindex);
                        used[pindex] = true;
                        numPts--;
                        break;
                    }
                }
                Point pcentre = createCircle(p1, p2, alpha);
                boolean onBoundary = true;
                for (Point vp : visitor.plist) {
                    if (vp != p2) {
                        double dx = pcentre.getX() - vp.getX();
                        double dy = pcentre.getY() - vp.getY();
                        if (dx * dx + dy * dy <= qAlpha) {
                            onBoundary = false;
                            break;
                        }
                    }
                }
                if (onBoundary) {
                    edges.add(new double[]{
                                p1.getCoordinate().x, p1.getCoordinate().y,
                                p2.getCoordinate().x, p2.getCoordinate().y});
                }
            }
        }
        return edges;
    }

    /**
     * Calculate the centre coordinates of a circle of radius alpha that has
     * point coordinates c1 and c2 on its circumference.
     *
     * @param c1 first circumference point
     * @param c2 second circumference point
     * @param alpha radius
     * @return a Coordinate representing the circle centre
     */
    private static Point createCircle(Point p1, Point p2, double alpha) {
        Coordinate centre = new Coordinate();

        double dx = (p2.getX() - p1.getX());
        double dy = (p2.getY() - p1.getY());
        double s2 = dx * dx + dy * dy;

        double h = Math.sqrt(alpha * alpha / s2 - 0.25d);

        centre.x = p1.getX() + dx / 2 + h * dy;
        centre.y = p1.getY() + dy / 2 + h * (p1.getX() - p2.getX());

        return gf.createPoint(centre);
    }

    /**
     * Display demo results
     *
     * @param poly the polygon used to generate the point cloud
     * @param polyCol display colour for the polygon
     * @param points the point cloud
     * @param pointCol display colour for the points
     * @param edges concave hull boundary segments as a List of LineStrings
     * @param edgeCol display colour for the segments
     */
    private void display(int w, int h,
            final Polygon poly, final Color polyCol,
            final List<Point> points, final Color pointCol,
            final List<double[]> edges, final Color edgeCol) {

        JFrame frame = new JFrame("Concave hull demo");

        JPanel panel = new JPanel() {
            final int ow = 2;
            final int ow2 = 4;

            @Override
            protected void paintComponent(Graphics g) {
                super.paintComponent(g);
                Graphics2D g2 = (Graphics2D) g;

                g2.setColor(polyCol);
                LineString ring = poly.getExteriorRing();
                Coordinate clast = ring.getCoordinateN(0);
                for (int i = 1; i < ring.getNumPoints(); i++) {
                    Coordinate c = ring.getCoordinateN(i);
                    g2.drawLine((int) clast.x, (int) clast.y, (int) c.x, (int) c.y);
                    clast = c;
                }

                g2.setColor(pointCol);
                for (Point p : points) {
                    int x = (int) Math.round(p.getX());
                    int y = (int) Math.round(p.getY());
                    g2.fillOval(x - ow, y - ow, ow2, ow2);
                }

                g2.setColor(edgeCol);
                Geometry polyEnv = poly.getEnvelope();
                for (double[] l : edges) {
                    int x0 = (int) l[0];
                    int y0 = (int) l[1];
                    int x1 = (int) l[2];
                    int y1 = (int) l[3];

                    g2.drawLine(x0, y0, x1, y1);
                }
            }
        };

        panel.setBackground(Color.WHITE);

        // disable double buffering for debugging drawing
        RepaintManager repaintManager = RepaintManager.currentManager(panel);
        repaintManager.setDoubleBufferingEnabled(false);

        frame.getContentPane().add(panel);
        Insets insets = frame.getInsets();
        frame.setSize(w + insets.left + insets.right, h + insets.bottom + insets.top);
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.setVisible(true);
    }
}

class PointVisitor implements ItemVisitor {

    public List<Point> plist = new ArrayList<Point>();
    private double maxDist;
    private Point refP;

    PointVisitor(Point refP, double maxDist) {
        this.refP = refP;
        this.maxDist = maxDist;
    }

    @Override
    public void visitItem(Object o) {
        if (refP.isWithinDistance((Point) o, maxDist)) {
            plist.add((Point) o);
        }
    }
}
