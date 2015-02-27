package org.wso2.siddhi.core.query.processor.window;

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

import org.apache.log4j.Logger;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.window.WindowProcessor;
import org.wso2.siddhi.query.api.extension.annotation.SiddhiExtension;

/*
* TODO: 1. Where is "outStreamDefinition" (spatialEventsInnerStream) reside in the CEP?
*       3. Ability to get current active `geo_` pattern execution plans count for pre initialize the static var in `observer` class
* */

@SiddhiExtension(namespace = "geo", function = "subscribeExecutionPlan")
public class SubscribeExecutionPlanWindowProcessor extends WindowProcessor {
    private static Logger logger = Logger.getLogger(SubscribeExecutionPlanWindowProcessor.class);
    Boolean initialized = false;
    public SubscribeExecutionPlanWindowProcessor() {

    }

    @Override
    protected void init(ExpressionExecutor[] inputExecutors) {
        logger.info("Calling init");
        if (!initialized) {
            upCount();
            this.initialized = true;
        }
    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor, StreamEventCloner streamEventCloner) {
        nextProcessor.process(streamEventChunk);
    }

    @Override
    protected WindowProcessor cloneWindowProcessor() {
        return null;
    }

    public void destroy() {
        downCount();
    }

    void upCount() {
        ExecutionPlansCount.upCount();
        logger.info("upCountNumberOfExecutionPlans current count after update = " + ExecutionPlansCount.getNumberOfExecutionPlans());
    }

    void downCount() {
        ExecutionPlansCount.downCount();
        logger.info("downCountNumberOfExecutionPlans current count after update = " + ExecutionPlansCount.getNumberOfExecutionPlans());
    }

}