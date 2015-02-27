/*
 * Copyright (c) 2005-2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.wso2.cep.geo.stationary;

import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.prep.PreparedGeometry;
import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.exception.ExecutionPlanCreationException;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.function.FunctionExecutor;
import org.wso2.siddhi.gpl.extensions.geo.geolibs.CreateGeometry;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.extension.annotation.SiddhiExtension;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/*Check whether a spatial object
* is in a defined stationary
* */
@SiddhiExtension(namespace = "geo", function = "withinstationary")
public class GeoInStationary extends FunctionExecutor {

    private PreparedGeometry geometry;
    private ConcurrentHashMap<String, Double> geoSyncMap = new ConcurrentHashMap<String, Double>();

    /**
     * The initialization method for FunctionExecutor
     *
     * @param attributeExpressionExecutors are the executors of each attributes in the function
     * @param executionPlanContext         SiddhiContext
     */
    @Override
    public void init(List<ExpressionExecutor> attributeExpressionExecutors, ExecutionPlanContext executionPlanContext) {
        if (attributeExpressionExecutors.size() != 6) {
            throw new ExecutionPlanCreationException("Not enough number of method arguments");
        } else {
            if (attributeExpressionExecutors.get(0).getReturnType() != Attribute.Type.DOUBLE
                    || attributeExpressionExecutors.get(1).getReturnType() != Attribute.Type.DOUBLE)
                throw new ExecutionPlanCreationException("latitude and longitude must be provided as double values");
            if (attributeExpressionExecutors.get(2).getReturnType() != Attribute.Type.STRING)
                throw new ExecutionPlanCreationException("geometry parameter should be a geojson feature string");
            String strGeometry = (String) attributeExpressionExecutors.get(2)
                    .execute(null);
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

        double latitude = (Double) data[0];
        double longitude = (Double) data[1];
        String currentId = data[3].toString();
        double currentTime = Double.parseDouble(data[4].toString());
        boolean withinStationary = false;
        boolean inStationary = false;
        double givenTime = Double.parseDouble(data[5].toString()) * 1000; // Time take in UI front-end is seconds so here we convert it to milliseconds

        //Creating a point
        Point currentPoint = CreateGeometry.createPoint(latitude, longitude);

        if (geometry.contains(currentPoint)) {
            withinStationary = true;
        }
        if (withinStationary) {
            if (!geoSyncMap.containsKey(currentId)) {// if the object not already within the stationary
                geoSyncMap.put(currentId, currentTime);
            }
            double previousTime = geoSyncMap.get(currentId);
            double timeDiff = currentTime - previousTime;
            if (timeDiff >= givenTime) { // if the time difference is more than or equal to given time then generate the alert
                inStationary = true;
            }
        } else {
            geoSyncMap.remove(currentId);
        }
        return inStationary;
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

    @Override
    public ExpressionExecutor cloneExecutor() {
        return null;
    }

    public Attribute.Type getReturnType() {
        return Attribute.Type.BOOL;
    }

}
