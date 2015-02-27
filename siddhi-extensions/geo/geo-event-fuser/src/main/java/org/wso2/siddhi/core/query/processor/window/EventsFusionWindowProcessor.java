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

import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.exception.ExecutionPlanCreationException;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.VariableExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static org.wso2.siddhi.core.util.SiddhiConstants.*;

public class EventsFusionWindowProcessor extends WindowProcessor {

    private int[] eventIdPosition;
    private int[] statePosition;
    private int[] informationPosition;
    private HashMap<String, ArrayList<StreamEvent>> eventsBuffer = new HashMap<String, ArrayList<StreamEvent>>();
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
    private static final List<String> states = Arrays.asList("OFFLINE", "NORMAL", "WARNING", "ALERTED");

    public EventsFusionWindowProcessor(int[] eventIdPosition, int[] statePosition, int[] informationPosition) {
        this.eventIdPosition = eventIdPosition;
        this.statePosition = statePosition;
        this.informationPosition = informationPosition;
    }

    public EventsFusionWindowProcessor() {

    }

    @Override
    protected WindowProcessor cloneWindowProcessor() {

        return new EventsFusionWindowProcessor(eventIdPosition, statePosition, informationPosition);
    }

    @Override
    protected void init(ExpressionExecutor[] inputExecutors) {
        this.inputExecutors = inputExecutors;
        if (inputExecutors.length != 3) {
            throw new ExecutionPlanCreationException("Usage eventsFusion(id, state, information)");
        }
        eventIdPosition = ((VariableExpressionExecutor) inputExecutors[0]).getPosition();
        statePosition = ((VariableExpressionExecutor) inputExecutors[1]).getPosition();
        informationPosition = ((VariableExpressionExecutor) inputExecutors[2]).getPosition();
    }

    @Override
    protected synchronized void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor, StreamEventCloner streamEventCloner) {
        while (streamEventChunk.hasNext()) {
            StreamEvent streamEvent = streamEventChunk.next();

            String eventId = (String) inputExecutors[0].execute(streamEvent);

            if (eventsBuffer.containsKey(eventId)) {
                ArrayList<StreamEvent> events = eventsBuffer.get(eventId);
                events.add(streamEventCloner.copyStreamEvent(streamEvent));
                if (events.size() == getDeployedExecutionPlansCount()) {
                    // Do the fusion here and return combined event
                    StreamEvent fusedEvent = streamEventCloner.copyStreamEvent(streamEvent);
                    eventsFuser(fusedEvent, events);
                    streamEventChunk.insertBeforeCurrent(fusedEvent);
                    events.clear();
                }
                streamEventChunk.remove();
            } else if (getDeployedExecutionPlansCount().equals(1)) { // This is a special case, where we do not need to fuse(combine) multiple events(because actually we don't get multiple events) so just doing a pass through
            } else {
                ArrayList<StreamEvent> buffer = new ArrayList<StreamEvent>(getDeployedExecutionPlansCount());
                buffer.add(streamEventCloner.copyStreamEvent(streamEvent));
                eventsBuffer.put(eventId, buffer);
                streamEventChunk.remove();
            }
        }
        nextProcessor.process(streamEventChunk);
    }

    public Integer getDeployedExecutionPlansCount() {
        return ExecutionPlansCount.getNumberOfExecutionPlans();
    }

    public void eventsFuser(StreamEvent event, ArrayList<StreamEvent> receivedEvents) {

        String finalState;
        String information = "";
        StringBuilder alertStrings = new StringBuilder();
        StringBuilder warningStrings = new StringBuilder(); // TODO: what if no warnings came ?

        Integer maxStateIndex = -1;

        for (StreamEvent receivedEvent : receivedEvents) {
            String thisState = (String) receivedEvent.getAttribute(statePosition);
            Integer thisStateIndex = states.indexOf(thisState);

            if (thisStateIndex > maxStateIndex) {
                maxStateIndex = thisStateIndex;
            }

            if (thisState.equals("ALERTED")) {
                if (alertStrings.length() > 0) {
                    alertStrings.append(",");
                } else {
                    alertStrings.append("Alerts: ");
                }
                alertStrings.append((String) receivedEvent.getAttribute(informationPosition));
            } else if (thisState.equals("WARNING")) {
                if (warningStrings.length() > 0) {
                    warningStrings.append(",");
                }  else {
                    warningStrings.append("Warnings: ");
                }
                warningStrings.append(receivedEvent.getAttribute(informationPosition));
            }
        }
        finalState = states.get(maxStateIndex);
        if (finalState.equals("NORMAL")) {
            information = "Normal driving pattern";
        } else {
            if (alertStrings.length() > 0) {
                information = alertStrings.toString();
            }
            if (warningStrings.length() > 0) {
                if (alertStrings.length() > 0) {
                    information += " | " + warningStrings;
                } else {
                    information = warningStrings.toString();
                }
            }
        }
        setEventAttribute(event, information, informationPosition);
        setEventAttribute(event, finalState, statePosition);
    }

    private void setEventAttribute(StreamEvent streamEvent, Object data, int[] position) {
        switch (position[STREAM_ATTRIBUTE_TYPE_INDEX]) {
            case BEFORE_WINDOW_DATA_INDEX:
                streamEvent.setBeforeWindowData(data, position[STREAM_ATTRIBUTE_INDEX]);
                break;
            case OUTPUT_DATA_INDEX:
                streamEvent.setOutputData(data, position[STREAM_ATTRIBUTE_INDEX]);
                break;
            case ON_AFTER_WINDOW_DATA_INDEX:
                streamEvent.setOnAfterWindowData(data, position[STREAM_ATTRIBUTE_INDEX]);
                break;
            default:
                throw new IllegalStateException("STREAM_ATTRIBUTE_TYPE_INDEX cannot be " +
                        position[STREAM_ATTRIBUTE_TYPE_INDEX]);
        }
    }

}
