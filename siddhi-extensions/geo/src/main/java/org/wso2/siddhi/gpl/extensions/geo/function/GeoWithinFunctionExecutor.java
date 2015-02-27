package org.wso2.siddhi.gpl.extensions.geo.function;

import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.prep.PreparedGeometry;
import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.exception.ExecutionPlanCreationException;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.function.FunctionExecutor;
import org.wso2.siddhi.gpl.extensions.geo.internal.CreateGeometry;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.Attribute.Type;

import java.util.List;

public class GeoWithinFunctionExecutor extends FunctionExecutor {

    private PreparedGeometry geometry;
    public GeoWithinFunctionExecutor() {
        this.geometry = null;
    }

    public GeoWithinFunctionExecutor(PreparedGeometry geometry){
        this.geometry = geometry;
    }
    /**
     * The initialization method for FunctionExecutor
     *
     * @param attributeExpressionExecutors are the executors of each attributes in the function
     * @param executionPlanContext         SiddhiContext
     */
    @Override
    public void init(List<ExpressionExecutor> attributeExpressionExecutors, ExecutionPlanContext executionPlanContext) {
        if (attributeExpressionExecutors.size() != 3) {
            throw new ExecutionPlanCreationException(
                    "Not enough number of method arguments");
        } else {
            if (attributeExpressionExecutors.get(0).getReturnType() != Attribute.Type.DOUBLE
                    || attributeExpressionExecutors.get(1).getReturnType() != Attribute.Type.DOUBLE)
                throw new ExecutionPlanCreationException("latitude and longitude must be provided as double values");
            if (attributeExpressionExecutors.get(2).getReturnType() != Attribute.Type.STRING)
                throw new ExecutionPlanCreationException("geometry parameter should be a geojson feature string");
            if(attributeExpressionExecutors.get(2) instanceof ConstantExpressionExecutor) {
                String strGeometry = attributeExpressionExecutors.get(2).execute(null).toString();
                geometry = CreateGeometry.preparedGeometryFromJSON(strGeometry);
            }
        }
    }

    /**
     * The main executions method which will be called upon event arrival
     *
     * @param data the runtime values of the attributeExpressionExecutors
     * @return
     */
    @Override
    protected Object execute(Object[] data) {
        double latitude = (Double) data[0];
        double longitude = (Double) data[1];
        /* Creating a point */
        Point point = CreateGeometry.createPoint(latitude, longitude);
        if (geometry != null) {
            return geometry.contains(point);
        } else {
            return CreateGeometry.geometryFromJSON(data[2].toString()).contains(point);
        }
    }

    @Override
    protected Object execute(Object data) {
        throw new IllegalStateException("geoIsWithin cannot execute data " + data);
    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }

    @Override
    public ExpressionExecutor cloneExecutor() {
        return new GeoWithinFunctionExecutor(geometry);
    }

    public Type getReturnType() {
        return Attribute.Type.BOOL;
    }

}
