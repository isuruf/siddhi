package org.wso2.cep.geo.notify;

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
import org.wso2.siddhi.core.exception.ExecutionPlanCreationException;
import org.wso2.siddhi.core.exception.OperationNotSupportedException;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.function.FunctionExecutor;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.extension.annotation.SiddhiExtension;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

@SiddhiExtension(namespace = "geo", function = "needToNotify")
public class NotifyAlert extends FunctionExecutor {

    Boolean sendFirst = false;
    ConcurrentHashMap<String, String> informationBuffer = new ConcurrentHashMap<String, String>();

    /**
     * The initialization method for FunctionExecutor
     *
     * @param attributeExpressionExecutors are the executors of each attributes in the function
     * @param executionPlanContext         SiddhiContext
     */
    @Override
    public void init(List<ExpressionExecutor> attributeExpressionExecutors, ExecutionPlanContext executionPlanContext) {
        for (ExpressionExecutor expressionExecutor : attributeExpressionExecutors) {
            if (!(expressionExecutor.getReturnType() == Attribute.Type.STRING)) {
                throw new ExecutionPlanCreationException("Information should be a string value");
            }
        }
        if (attributeExpressionExecutors.size() == 3) {
            String options = (String) attributeExpressionExecutors.get(2)
                    .execute(null);
            if(options.contains("sendFirst")) {
                sendFirst = true;
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
        Boolean returnValue = false;
        String id = (String) data[0];
        String currentInformation = (String) data[1];
        if (!informationBuffer.containsKey(id)) {
            returnValue = sendFirst;
        } else if (!informationBuffer.get(id).equals(currentInformation)) {
            returnValue = true;
        }
        informationBuffer.put(id, currentInformation);
        return returnValue;
    }

    @Override
    protected Object execute(Object data) {
        throw new IllegalStateException("needToNotify cannot execute data " + data);
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

    @Override
    public Attribute.Type getReturnType() {
        return Attribute.Type.BOOL;
    }


}
