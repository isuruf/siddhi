package org.wso2.siddhi.gpl.extensions.geo.internal.util;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.prep.PreparedGeometry;
import org.wso2.siddhi.core.exception.ExecutionPlanCreationException;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.gpl.extensions.geo.internal.CreateGeometry;
import org.wso2.siddhi.query.api.definition.Attribute;

/**
 * Created by isuru on 2/25/15.
 */
public abstract class GeoOperation {
    boolean isPoint;
    PreparedGeometry geometry;

    public void init(ExpressionExecutor[] attributeExpressionExecutors, int offset) {
        if (attributeExpressionExecutors[offset].getReturnType() != Attribute.Type.DOUBLE) {
            isPoint = true;
            if (attributeExpressionExecutors[offset + 1].getReturnType() != Attribute.Type.DOUBLE)
                throw new ExecutionPlanCreationException("Latitude and Longitude must be provided as double values");
            ++offset;
        } else if (attributeExpressionExecutors[offset].getReturnType() != Attribute.Type.STRING) {
            isPoint = false;
        } else {
            throw new ExecutionPlanCreationException((offset + 1) +
                    " parameter should be a string for a geometry or a double for a latitude");
        }
        ++offset;
        if (attributeExpressionExecutors[offset].getReturnType() != Attribute.Type.STRING)
            throw new ExecutionPlanCreationException("Last parameter should be a GeoJSON geometry string");
        if (attributeExpressionExecutors[offset] instanceof ConstantExpressionExecutor) {
            String strGeometry = attributeExpressionExecutors[offset].execute(null).toString();
            geometry = CreateGeometry.preparedGeometryFromJSON(strGeometry);
        }
    }

    public boolean process(Object[] data){
        Geometry currentGeometry;
        if(isPoint){
            double latitude = (Double) data[0];
            double longitude = (Double) data[1];
            currentGeometry = CreateGeometry.createPoint(latitude, longitude);
        } else {
            currentGeometry = CreateGeometry.geometryFromJSON(data[0].toString());
        }
        if (geometry != null) {
            return operation(currentGeometry, geometry, data);
        } else {
            return operation(currentGeometry, CreateGeometry.geometryFromJSON(data[2].toString()), data);
        }
    }
    protected abstract Boolean operation(Geometry a, Geometry b, Object[] data);
    protected abstract Boolean operation(Geometry a, PreparedGeometry b, Object[] data);
}
