package org.wso2.cep.geo.functions;

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
import org.wso2.siddhi.core.util.EventPrinter;

import java.util.*;

public class UUIDGeneratorTestCase {
    private static Logger logger = org.apache.log4j.Logger.getLogger(UUIDGeneratorTestCase.class);
    private static List<Object[]> data;
    private static SiddhiManager siddhiManager;

    @BeforeClass
    public static void setUp() throws Exception {
        logger.info("Init Siddhi");// Create Siddhi Manager
        siddhiManager = new SiddhiManager();
        SiddhiContext siddhiContext = siddhiManager.getSiddhiContext();

        Map<String, Class> classList = new HashMap<String, Class>();
        classList.put("geo:generateEventId", EventIdGenerator.class);
        siddhiContext.setSiddhiExtensions(classList);
    }

    @BeforeClass
    public static void loadData() throws Exception {
        data = new ArrayList<Object[]>(4);
        data.add(new Object[]{"km-4354", "8.641987", "79.35022", "32.21"});
        data.add(new Object[]{"km-4354", "8.549587", "79.36022", "12.21"});
        data.add(new Object[]{"km-4354", "8.569867", "79.37022", "56.21"});
        data.add(new Object[]{"km-4354", "8.564879", "79.33022", "29.21"});
    }

    @Test
    public void testProcess() throws Exception {
        logger.info("TestProcess");
        String executionPlan = "@config(async = 'true') define stream dataIn (id string, latitude double, longitude double, speed float);"
                + "@info(name = 'query1') from dataIn " +
                "select id, latitude, longitude, speed, geo:generateEventId() as eventId\n" +
                "insert into dataOut;";

        long start = System.currentTimeMillis();
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);
        long end = System.currentTimeMillis();
        logger.info(String.format("Time to create ExecutionPlanRunTime: [%f sec]", ((end - start) / 1000f)));
        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                for (Event event : inEvents) {
                    String uuid = (String) event.getData(4);
                    logger.info("UUID = " + uuid);
                    Assert.assertEquals(true, isUUID(uuid));
                }
            }
        });
        executionPlanRuntime.start();
        generateEvents(executionPlanRuntime);
        Thread.sleep(1000);
    }

    private void generateEvents(ExecutionPlanRuntime executionPlanRuntime) throws Exception {
        InputHandler inputHandler = executionPlanRuntime.getInputHandler("dataIn");
        for (Object[] dataLine : data) {
            inputHandler.send(dataLine);
        }
    }

    public boolean isUUID(String string) {
        try {
            UUID.fromString(string);
            return true;
        } catch (Exception ex) {
            return false;
        }
    }
}