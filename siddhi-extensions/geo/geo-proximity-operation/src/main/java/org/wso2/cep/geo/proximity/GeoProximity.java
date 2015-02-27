package org.wso2.cep.geo.proximity;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;
import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.exception.ExecutionPlanCreationException;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.function.FunctionExecutor;
import org.wso2.siddhi.gpl.extensions.geo.geolibs.CreateGeometry;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.extension.annotation.SiddhiExtension;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@SiddhiExtension(namespace = "geo", function = "geoProximity")
public class GeoProximity extends FunctionExecutor {

    ConcurrentHashMap<String, Double> closeSpatialObjects = new ConcurrentHashMap<String, Double>();
    ConcurrentHashMap<String, Point> pointList = new ConcurrentHashMap<String, Point>();

    public Attribute.Type getReturnType() {
        return Attribute.Type.STRING;
    }

    /**
     * The initialization method for FunctionExecutor
     *
     * @param attributeExpressionExecutors are the executors of each attributes in the function
     * @param executionPlanContext         SiddhiContext
     */
    @Override
    public void init(List<ExpressionExecutor> attributeExpressionExecutors, ExecutionPlanContext executionPlanContext) {
        if (attributeExpressionExecutors.size() != 6) {
            throw new ExecutionPlanCreationException("Expected 6 arguments. Received " + attributeExpressionExecutors.size());
        }
        for (int i = 0; i < 3; i++) {
            if (attributeExpressionExecutors.get(i).getReturnType() != Attribute.Type.DOUBLE) {
                throw new ExecutionPlanCreationException("Argument no " + (i + 1) + " must be of type double. Found "
                        + attributeExpressionExecutors.get(i).getReturnType());
            }
        }
        for (int i = 3; i < 6; i++) {
            if (attributeExpressionExecutors.get(i).getReturnType() != Attribute.Type.DOUBLE
                    && attributeExpressionExecutors.get(i).getReturnType() != Attribute.Type.STRING) {
                throw new ExecutionPlanCreationException("Argument no " + (i + 1) + " must be of type double. Found "
                        + attributeExpressionExecutors.get(i).getReturnType());
            }
        }

    }

    @Override
    public ExpressionExecutor cloneExecutor() {
        return null;
    }

    @Override
    protected synchronized Object execute(Object[] data) {
        ArrayList<String> IDList = new ArrayList<String>();
        double proximityDist = ((Double) data[0]) / (CreateGeometry.TO_DEGREE);
        double latitude = (Double) data[1];
        double longitude = (Double) data[2];
        String currentId = data[3].toString();
        double currentTime = Double.parseDouble(data[4].toString());
        double giventime = Double.parseDouble(data[5].toString()) * 1000; // Time taken in UI front-end is seconds so here we convert it to milliseconds
        String previousId;
        double timediff;

        Point currentPoint = CreateGeometry.createPoint(latitude, longitude);
        pointList.put(currentId, currentPoint);

        // iterate through the list of all available vehicles
        for (Map.Entry<String, Point> entry : pointList.entrySet()) {
            previousId = entry.getKey();
            String compositeKey = makeCompositeKey(currentId, previousId);
            Geometry previousPoint = entry.getValue(); // get the the position of the vehicle

            if (!previousId.equalsIgnoreCase(currentId)) { // if the point is of another vehicle
                if (currentPoint.isWithinDistance(previousPoint, proximityDist)) { // if the two vehicle are in close proximity

                    if (!closeSpatialObjects.containsKey(compositeKey)) {
                        // check for how long they have been close
                        // NOTE: NEED TO RESTRUCTURE!!!!!
                        closeSpatialObjects.put(compositeKey, currentTime);
                    }

                    double timecheck = closeSpatialObjects.get(compositeKey);
                    timediff = currentTime - timecheck;

                    if (timediff >= giventime) {
                        // if the time difference for being in close proximity is less than the user
                        // input time period, output true else false
                        IDList.add(previousId);
                    }
                } else {
                    closeSpatialObjects.remove(compositeKey);
                }
            }
        }
        return generateOutput(IDList);
    }

    @Override
    protected Object execute(Object data) {
        throw new IllegalStateException("GeoInStationary cannot execute data " + data);
    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }

    /**
     * generates the final output string *
     */
    public String generateOutput(ArrayList<String> myList) {
        if (!myList.isEmpty()) {
            StringBuilder finalOutput = new StringBuilder("true,");
            finalOutput.append(myList.get(0));
            for (int i = 1; i < myList.size(); i++) {
                finalOutput.append(",").append(myList.get(i));
            }
            return finalOutput.toString();
        } else {
            return "false";
        }
    }

    public String makeCompositeKey(String key1, String key2) {
        if (key1.compareToIgnoreCase(key2) < 0) {
            return key1 + "-" + key2;
        } else {
            return key2 + "-" + key1;
        }
    }

}
