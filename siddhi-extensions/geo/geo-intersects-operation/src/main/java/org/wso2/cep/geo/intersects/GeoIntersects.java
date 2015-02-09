package org.wso2.cep.geo.intersects;

import com.vividsolutions.jts.geom.*;
import org.apache.log4j.Logger;
import org.geotools.geojson.geom.GeometryJSON;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.wso2.siddhi.core.config.SiddhiContext;
import org.wso2.siddhi.core.exception.QueryCreationException;
import org.wso2.siddhi.core.executor.function.FunctionExecutor;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.Attribute.Type;
import org.wso2.siddhi.query.api.extension.annotation.SiddhiExtension;

import java.io.IOException;

@SiddhiExtension(namespace = "geo", function = "intersects")
public class GeoIntersects extends FunctionExecutor {

    Logger log = Logger.getLogger(GeoIntersects.class);
    private GeometryFactory geometryFactory;
    private Geometry geometry;

    @Override
    public void init(Attribute.Type[] types, SiddhiContext siddhiContext) {
        if (types.length != 2) {
            throw new QueryCreationException(
                    "Not enough number of method arguments");
        } else {
            geometryFactory = JTSFactoryFinder.getGeometryFactory();
            if (types[0] != Attribute.Type.STRING)
                throw new QueryCreationException("First parameter should be a geojson feature string");
            if (types[1] != Attribute.Type.STRING)
                throw new QueryCreationException("Second parameter should be a geojson feature string");
            Object str = attributeExpressionExecutors.get(1)
                    .execute(null);
            if (str == null) {
                throw new QueryCreationException("Second Parameter should be independent of events");
            }
            String strGeometry = (String) str;
            geometry = createGeometry(strGeometry);
        }
    }

    public static Geometry createGeometry(String str) {
        GeometryJSON j = new GeometryJSON();
        try {
            return j.read(str.replace("'", "\""));
        } catch (IOException e) {
            throw new RuntimeException("Failed to create a geometry from given str " + str, e);
        }
    }

    @Override
    protected Object process(Object obj) {
        Object functionParams[] = (Object[]) obj;
        String geometry = (String) functionParams[0];
        return createGeometry(geometry).intersects(this.geometry);
    }

    public Type getReturnType() {
        return Attribute.Type.BOOL;
    }

    public void destroy() {
    }
}
