package org.wso2.siddhi.gpl.extensions.geo.function;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.prep.PreparedGeometry;
import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.exception.ExecutionPlanCreationException;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.function.FunctionExecutor;
import org.wso2.siddhi.gpl.extensions.geo.internal.CreateGeometry;
import org.wso2.siddhi.query.api.definition.Attribute.Type;

public abstract class AbstractGeoOperationExecutor extends FunctionExecutor {

    private PreparedGeometry geometry = null;
    boolean isPoint;

    /**
     * The initialization method for FunctionExecutor, this method will be called before the other methods
     *
     * @param attributeExpressionExecutors are the executors of each function parameters
     * @param executionPlanContext         the context of the execution plan
     */
    @Override
    protected void init(ExpressionExecutor[] attributeExpressionExecutors, ExecutionPlanContext executionPlanContext) {
        int length = attributeExpressionExecutors.length;
        if (length == 3) {
            isPoint = true;
            if (attributeExpressionExecutors[0].getReturnType() != Type.DOUBLE
                    || attributeExpressionExecutors[1].getReturnType() != Type.DOUBLE)
                throw new ExecutionPlanCreationException("Latitude and Longitude must be provided as double values");
        } else if (length == 2) {
            isPoint = false;
            if (attributeExpressionExecutors[0].getReturnType() != Type.STRING)
                throw new ExecutionPlanCreationException("First parameter should be a GeoJSON geometry string");
        } else {
            throw new ExecutionPlanCreationException(
                    "Not enough number of method arguments. Usage " +
                            "geo:operation(latitude string, longitude string, geometry string) or" +
                            "geo:operation(geometry string, geometry string) or" +
                            "");
        }
        if (attributeExpressionExecutors[length - 1].getReturnType() != Type.STRING)
            throw new ExecutionPlanCreationException("Last parameter should be a GeoJSON geometry string");
        if (attributeExpressionExecutors[length - 1] instanceof ConstantExpressionExecutor) {
            String strGeometry = attributeExpressionExecutors[length - 1].execute(null).toString();
            geometry = CreateGeometry.preparedGeometryFromJSON(strGeometry);
        }
    }

    /**
     * The serializable state of the element, that need to be
     * persisted for the reconstructing the element to the same state
     * on a different point of time
     *
     * @return stateful objects of the element as an array
     */
    @Override
    public Object[] currentState() {
        return new Object[0];
    }

    /**
     * The serialized state of the element, for reconstructing
     * the element to the same state as if was on a previous point of time.
     *
     * @param state the stateful objects of the element as an array on
     *              the same order provided by currentState().
     */
    @Override
    public void restoreState(Object[] state) {

    }

    /**
     * The main executions method which will be called upon event arrival
     *
     * @param data the runtime values of the attributeExpressionExecutors
     * @return
     */
    @Override
    protected Object execute(Object[] data) {
        Geometry currentGeometry;
        if(isPoint){
            double latitude = (Double) data[0];
            double longitude = (Double) data[1];
            currentGeometry = CreateGeometry.createPoint(latitude, longitude);
        } else {
            currentGeometry = CreateGeometry.geometryFromJSON(data[0].toString());
        }
        if (geometry != null) {
            return operation(currentGeometry, geometry);
        } else {
            return operation(currentGeometry, CreateGeometry.geometryFromJSON(data[2].toString()));
        }
    }

    protected abstract Boolean operation(Geometry a, Geometry b);
    protected abstract Boolean operation(Geometry a, PreparedGeometry b);

    /**
     * The main execution method which will be called upon event arrival
     * which has zero or one function parameter
     *
     * @param data null if the function parameter count is zero or
     *             runtime data value of the function parameter
     * @return the function result
     */
    @Override
    protected Object execute(Object data) {
        throw new IllegalStateException(this.getClass().getCanonicalName()+" cannot execute data " + data);
    }

    /**
     * This will be called only once, to acquire required resources
     * after initializing the system and before processing the events.
     */
    @Override
    public void start() {

    }

    /**
     * This will be called only once, to release the acquired resources
     * before shutting down the system.
     */
    @Override
    public void stop() {

    }

    //TODO: look into cloning


    public Type getReturnType() {
        return Type.BOOL;
    }

}
