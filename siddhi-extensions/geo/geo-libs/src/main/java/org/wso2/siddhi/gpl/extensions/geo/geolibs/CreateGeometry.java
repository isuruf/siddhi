package org.wso2.siddhi.gpl.extensions.geo.geolibs;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.prep.PreparedGeometry;
import com.vividsolutions.jts.geom.prep.PreparedGeometryFactory;
import org.geotools.geojson.geom.GeometryJSON;
import org.geotools.geometry.jts.JTSFactoryFinder;

import java.io.IOException;
import java.io.StringWriter;

/**
 * Created by isuru on 2/27/15.
 */
public class CreateGeometry {


    public static final double TO_DEGREE = 110574.61087757687;
    private static final String COORDINATES = "coordinates";
    private static final String RADIUS = "radius";
    private static GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory();
    //JTSFactoryFinder.getGeometryFactory(new Hints(Hints.CRS, DefaultGeographicCRS.WGS84));
    private static GeometryJSON geometryJSON = new GeometryJSON(10);

    public static Geometry geometryFromJSON(String strGeometry) {
        if (strGeometry.contains(RADIUS)) {
            JsonObject jsonObject = new JsonParser().parse(strGeometry).getAsJsonObject();
            JsonArray jLocCoordinatesArray = jsonObject.getAsJsonArray(COORDINATES);
            Coordinate coords = new Coordinate(Double.parseDouble(jLocCoordinatesArray.get(0)
                    .toString()), Double.parseDouble(jLocCoordinatesArray.get(1).toString()));
            Point point = geometryFactory.createPoint(coords); // create the points for GeoJSON file points
            double radius = Double.parseDouble(jsonObject.get(RADIUS).toString()) / TO_DEGREE; //convert to degrees
            return point.buffer(radius); //draw the buffer
        } else {
            try {
                return geometryJSON.read(strGeometry.replace("'", "\""));
            } catch (IOException e) {
                throw new RuntimeException("Failed to create a geometry from given str " + strGeometry, e);
            }
        }
    }

    public static String geometrytoJSON(Geometry geometry) {
        StringWriter sw = new StringWriter();
        try {
            geometryJSON.write(geometry, sw);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create a json string from given geometry " + geometry, e);
        }
        return sw.toString();
    }

    public static PreparedGeometry preparedGeometryFromJSON(String strGeometry) {
        return PreparedGeometryFactory.prepare(geometryFromJSON(strGeometry));
    }

    public static Point createPoint(double longitude, double latitude) {
        return geometryFactory.createPoint(new Coordinate(longitude, latitude));
    }

    public static Geometry createGeometry(Object data) {
        if (data instanceof Geometry) {
            return (Geometry) data;
        } else {
            return geometryFromJSON(data.toString());
        }
    }
}