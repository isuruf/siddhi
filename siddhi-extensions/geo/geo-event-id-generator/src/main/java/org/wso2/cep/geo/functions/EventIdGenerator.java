package org.wso2.cep.geo.functions;

/*
*  Copyright (c) 2005-2010, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
*  WSO2 Inc. licenses this file to you under the Apache License,
*  Version 2.0 (the "License"); you may not use this file except
*  in compliance with the License.
*  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/

import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.function.FunctionExecutor;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.util.Arrays;
import java.util.UUID;

public class EventIdGenerator extends FunctionExecutor {
    /**
     * The initialization method for FunctionExecutor, this method will be called before the other methods
     *
     * @param attributeExpressionExecutors are the executors of each function parameters
     * @param executionPlanContext         the context of the execution plan
     */
    @Override
    protected void init(ExpressionExecutor[] attributeExpressionExecutors, ExecutionPlanContext executionPlanContext) {

    }

    /**
     * Used to collect the serializable state of the processing element, that need to be
     * persisted for the reconstructing the element to the same state on a different point of time
     *
     * @return stateful objects of the processing element as an array
     */
    @Override
    public Object[] currentState() {
        return new Object[0];
    }

    /**
     * Used to restore serialized state of the processing element, for reconstructing
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
        throw new IllegalStateException("generateEventId cannot execute two data " + Arrays.deepToString(data));
    }

    @Override
    protected Object execute(Object data) {
        return UUID.randomUUID().toString();
    }

    /**
     * Return type of the custom function mentioned
     *
     * @return
     */
    @Override
    public Attribute.Type getReturnType() {
        return Attribute.Type.STRING;
    }

    @Override
    public void start() {
        //Nothing to start
    }

    @Override
    public void stop() {
        //nothing to stop
    }
}
