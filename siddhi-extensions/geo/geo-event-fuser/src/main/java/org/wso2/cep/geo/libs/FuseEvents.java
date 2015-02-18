package org.wso2.cep.geo.libs;

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

import org.wso2.siddhi.core.config.SiddhiContext;
import org.wso2.siddhi.core.event.StreamEvent;
import org.wso2.siddhi.core.event.in.InEvent;
import org.wso2.siddhi.core.event.in.InListEvent;
import org.wso2.siddhi.core.query.QueryPostProcessingElement;
import org.wso2.siddhi.core.query.processor.window.WindowProcessor;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.expression.Expression;
import org.wso2.siddhi.query.api.expression.Variable;
import org.wso2.siddhi.query.api.extension.annotation.SiddhiExtension;

import java.util.*;

@SiddhiExtension(namespace = "geo", function = "eventsFunion")
public class FuseEvents extends WindowProcessor {

    int eventIdPosition = 0;
    int statePosition = 0;
    int informationPosition = 0;
    HashMap<String, ArrayList<InEvent>> eventsBuffer = null;

    @Override
    /**
     *This method called when processing an event
     */
    protected void processEvent(InEvent inEvent) {
        acquireLock();
        try {
            doProcessing(inEvent);
        } finally

        {
            releaseLock();
        }

    }

    @Override
    /**
     *This method called when processing an event list
     */
    protected void processEvent(InListEvent inListEvent) {

        for (int i = 0; i < inListEvent.getActiveEvents(); i++) {
            InEvent inEvent = (InEvent) inListEvent.getEvent(i);
            processEvent(inEvent);
        }
    }

    @Override
    /**
     * This method iterate through the events which are in window
     */
    public Iterator<StreamEvent> iterator() {
        return null;
    }

    @Override
    /**
     * This method iterate through the events which are in window but used in distributed processing
     */
    public Iterator<StreamEvent> iterator(String s) {
        return null;
    }

    @Override
    /**
     * This method used to return the current state of the window, Used for persistence of data
     */
    protected Object[] currentState() {
        return new Object[]{eventsBuffer};
    }

    @Override
    /**
     * This method is used to restore from the persisted state
     */
    protected void restoreState(Object[] objects) {
    }

    @Override
    /**
     * Method called when initialising the extension
     */
    protected void init(Expression[] expressions,
                        QueryPostProcessingElement queryPostProcessingElement,
                        AbstractDefinition abstractDefinition, String s, boolean b,
                        SiddhiContext siddhiContext) {
        eventsBuffer = new HashMap<String, ArrayList<InEvent>>();
        if (expressions.length == 1) {
            eventIdPosition = abstractDefinition.getAttributePosition(
                    ((Variable) expressions[0]).getAttributeName());
            statePosition = 7;
            informationPosition = 8;
            log.info("This method is deprecated. Taking default values of state and information attributes as 7 and 8." +
                    " Use geo:eventsFunion(id, state, information) to give the state and information attributes.");
        } else if (expressions.length == 3) {
            eventIdPosition = abstractDefinition.getAttributePosition(
                    ((Variable) expressions[0]).getAttributeName());
            statePosition = abstractDefinition.getAttributePosition(
                    ((Variable) expressions[1]).getAttributeName());
            informationPosition = abstractDefinition.getAttributePosition(
                    ((Variable) expressions[2]).getAttributeName());
        } else {
            log.error("Parameters count is not matching, there should be three parameters ");
        }

    }

    private void doProcessing(InEvent event) {
        String eventId = (String) event.getData(eventIdPosition);

        if (eventsBuffer.containsKey(eventId)) {
            eventsBuffer.get(eventId).add(event);
            if (eventsBuffer.get(eventId).size() == getDeployedExecutionPlansCount()) {
                // Do the fusion here and return combined event
                InEvent fusedEvent = eventsFuser(event);
                nextProcessor.process(fusedEvent);
                eventsBuffer.remove(eventId);
            }

        } else if (getDeployedExecutionPlansCount().equals(1)) { // This is a special case, where we do not need to fuse(combine) multiple events(because actually we don't get multiple events) so just doing a pass through
            nextProcessor.process(event);
        } else {
            ArrayList<InEvent> buffer = new ArrayList<InEvent>();
            buffer.add(event);
            eventsBuffer.put(eventId, buffer);
        }

    }

    @Override
    public void destroy() {
    }

    public Integer getDeployedExecutionPlansCount() {
        return ExecutionPlansCount.getNumberOfExecutionPlans();
    }

    public InEvent eventsFuser(InEvent event) {
        /*
        * --For reference--
        * -Precedence of states-
        * goes low to high from LHS to RHS
        *
        * OFFLINE < NORMAL < WARNING < ALERT
        * */

        /*
        * --For reference--
        *   Higher the index higher the Precedence
        *   States are in all caps to mimics that states not get on the way
        *
        * */

        String[] statesArray = new String[]{"OFFLINE", "NORMAL", "WARNING", "ALERTED"};
        List<String> states = Arrays.asList(statesArray);

        Object[] data = event.getData();

        String finalState = "";
        String information = "";

        String eventId = (String) event.getData(eventIdPosition);
        ArrayList<InEvent> receivedEvents = eventsBuffer.get(eventId);

        String alertStrings = "";
        String warningStrings = ""; // TODO: what if no warnings came ?

        Integer currentStateIndex = -1;

        for (InEvent receivedEvent : receivedEvents) {
            String thisState = (String) receivedEvent.getData(statePosition);
            Integer thisStateIndex = states.indexOf(thisState);

            if (thisStateIndex > currentStateIndex) { // TODO: `this` and `current` little bit confusing??
                finalState = thisState;
                currentStateIndex = thisStateIndex;
            }

            if (thisState.equals("ALERTED")) {
                alertStrings += "," + (String) receivedEvent.getData(informationPosition);
            } else if (thisState.equals("WARNING")) {
                warningStrings += "," + (String) receivedEvent.getData(informationPosition);
            }
        }

        if (finalState.equals("NORMAL")) {
            information = "Normal driving pattern";
        } else {
            if (!alertStrings.isEmpty()) {
                information = "Alerts: " + alertStrings;
            }
            if (!warningStrings.isEmpty()) {
                information += " | " + "Warnings: " + warningStrings;
            }
        }


        Object[] dataOut = data.clone();
        dataOut[informationPosition] = information;
        dataOut[statePosition] = finalState;

        return new InEvent(event.getStreamId(), System.currentTimeMillis(), dataOut);
    }
}
