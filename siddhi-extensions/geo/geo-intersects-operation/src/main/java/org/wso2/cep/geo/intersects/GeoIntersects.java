package org.wso2.cep.geo.intersects;

import com.vividsolutions.jts.geom.prep.PreparedGeometry;
import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.exception.ExecutionPlanCreationException;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.function.FunctionExecutor;
import org.wso2.siddhi.gpl.extensions.geo.geolibs.CreateGeometry;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.Attribute.Type;
import org.wso2.siddhi.query.api.extension.annotation.SiddhiExtension;

import java.util.List;

@SiddhiExtension(namespace = "geo", function = "intersects")
public class GeoIntersects extends FunctionExecutor {

    private PreparedGeometry geometry;

    /**
     * The initialization method for FunctionExecutor
     *
     * @param attributeExpressionExecutors are the executors of each attributes in the function
     * @param executionPlanContext         SiddhiContext
     */
    @Override
    public void init(List<ExpressionExecutor> attributeExpressionExecutors, ExecutionPlanContext executionPlanContext) {
        if (attributeExpressionExecutors.size() != 2) {
            throw new ExecutionPlanCreationException("Not enough number of method arguments");
        } else {
            if (attributeExpressionExecutors.get(0).getReturnType() != Attribute.Type.STRING)
                throw new ExecutionPlanCreationException("First parameter should be a geojson feature string");
            if (attributeExpressionExecutors.get(1).getReturnType() != Attribute.Type.STRING)
                throw new ExecutionPlanCreationException("Second parameter should be a geojson feature string");
            Object str = attributeExpressionExecutors.get(1).execute(null);
            if (str == null) {
                throw new ExecutionPlanCreationException("Second Parameter should be independent of events");
            }
            String strGeometry = (String) str;
            geometry = CreateGeometry.preparedGeometryFromJSON(strGeometry);
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
        String geometry = (String) data[0];
        return this.geometry.intersects(CreateGeometry.geometryFromJSON(geometry));
    }

    @Override
    protected Object execute(Object data) {
        String geometry = (String) data;
        return this.geometry.intersects(CreateGeometry.geometryFromJSON(geometry));
    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }

    @Override
    public ExpressionExecutor cloneExecutor() {
        return null;
    }

    public Type getReturnType() {
        return Attribute.Type.BOOL;
    }
}
