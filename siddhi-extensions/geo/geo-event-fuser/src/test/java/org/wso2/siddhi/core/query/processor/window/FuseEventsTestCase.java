package org.wso2.siddhi.core.query.processor.window;

import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.config.SiddhiContext;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;

import java.util.*;

public class FuseEventsTestCase {
    private static SiddhiManager siddhiManager;
    private static Logger logger = Logger.getLogger(FuseEventsTestCase.class);
    private static List<Object[]> data;
    private int eventCount = 0;

    @BeforeClass
    public static void setUp() throws Exception {
        logger.info("Init Siddhi");// Create Siddhi Manager
        siddhiManager = new SiddhiManager();
        SiddhiContext siddhiContext = siddhiManager.getSiddhiContext();

        Map<String, Class> classList = new HashMap<String, Class>();
        classList.put("geo:eventsFusion", EventsFusionWindowProcessor.class);
        classList.put("geo:subscribeExecutionPlan", SubscribeExecutionPlanWindowProcessor.class);
        siddhiContext.setSiddhiExtensions(classList);

        data = new ArrayList<Object[]>(4);
        data.add(new Object[]{"km-4354", 12.56d, 56.32d, UUID.randomUUID().toString(), "NORMAL", "NORMAL driving pattern"});
        data.add(new Object[]{"km-4355", 12.56d, 56.32d, UUID.randomUUID().toString(), "WARNING", "NOT NORMAL driving pattern"});
        data.add(new Object[]{"km-4356", 12.56d, 56.32d, UUID.randomUUID().toString(), "ALERTED", "NOT NORMAL driving pattern"});
        data.add(new Object[]{"km-4357", 12.56d, 56.32d, UUID.randomUUID().toString(), "NORMAL", "NORMAL driving pattern"});
    }

    @Test
    public void testProcess() throws Exception {
        logger.info("TestProcess");
        String stream = "@config(async = 'true') define stream dataIn (id string, latitude double, longitude double, eventId string, state string, information string);";

        String eventsFusionPlan =
                "@info(name = 'query1') from dataOut#window.geo:eventsFusion(eventId, state, information)" +
                "select id, eventId, latitude, longitude, timeStamp, speed, heading, state , information, 'Testing' as notify\n" +
                "insert into dataFusedOut;";

        String executionPlan = stream;

        // Add multiple execution plans which are using the same input stream(dataIn) and outputting to same output stream(dataOut) ,allowing create same copy of the event coming from dataIn stream
        for (int i = 0; i < 3; i++) {
            String query = "from dataIn#window.geo:subscribeExecutionPlan() \n" +
                    "select id, latitude, longitude, 1412236 as timeStamp, 12.6 as speed, 123.12 as heading, eventId, state, concat(information, "+i+") as information\n" +
                    "insert into dataOut;";
            executionPlan += query;
        }
        executionPlan += eventsFusionPlan;

        long start = System.currentTimeMillis();
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);
        long end = System.currentTimeMillis();
        logger.info(String.format("Time to create ExecutionPlanRunTime: [%f sec]", ((end - start) / 1000f)));
        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                for(Event event:inEvents) {
                    System.out.println(event);
                }
                eventCount += inEvents.length;
            }
        });
        executionPlanRuntime.start();
        generateEvents(executionPlanRuntime);
        Thread.sleep(1000);
        Assert.assertEquals(4, eventCount);
    }

    private void generateEvents(ExecutionPlanRuntime executionPlanRuntime) throws Exception {
        InputHandler inputHandler = executionPlanRuntime.getInputHandler("dataIn");
        for (Object[] dataLine : data) {
            inputHandler.send(dataLine);
        }
    }
}