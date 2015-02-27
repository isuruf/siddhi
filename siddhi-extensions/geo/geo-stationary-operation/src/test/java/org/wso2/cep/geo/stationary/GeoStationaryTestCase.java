package org.wso2.cep.geo.stationary;

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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GeoStationaryTestCase {
    private static SiddhiManager siddhiManager;
    private static Logger logger = Logger.getLogger(GeoStationaryTestCase.class);
    private static List<Object[]> data;
    private static List<Boolean> expectedResult;
    private static int eventCount = 0;

    @BeforeClass
    public static void setUp() throws Exception {
        logger.info("Init Siddhi");// Create Siddhi Manager
        siddhiManager = new SiddhiManager();
        SiddhiContext siddhiContext = siddhiManager.getSiddhiContext();

        Map<String, Class> classList = new HashMap<String, Class>();
        classList.put("geo:withinstationary", GeoInStationary.class);
        siddhiContext.setSiddhiExtensions(classList);

        data = new ArrayList<Object[]>();
        expectedResult = new ArrayList<Boolean>();

        data.add(new Object[]{-1d, -1d, "1", 123.5d});
        expectedResult.add(false);
        data.add(new Object[]{0.5d, 0.5d, "1", 124.5d});
        expectedResult.add(false);
        data.add(new Object[]{0.5d, -1d, "2", 125.0d});
        expectedResult.add(false);
        data.add(new Object[]{0.75d, 0.25d, "1", 125.5d});
        expectedResult.add(true);
        data.add(new Object[]{0.5d, 0.25d, "2", 125.5d});
        expectedResult.add(false);
        data.add(new Object[]{0.75d, 1.25d, "1", 126.5d});
        expectedResult.add(false);
        data.add(new Object[]{0.5d, 0.75d, "2", 126.5d});
        expectedResult.add(true);

    }

    @Test
    public void testProcess() throws Exception {
        logger.info("TestProcess");
        String executionPlan = "@config(async = 'true') define stream dataIn (latitude double, longitude double, " +
                "id string, timestamp double);"
                + "@info(name = 'query1') from dataIn " +
                "select id, latitude, longitude, " +
                "geo:withinstationary(latitude, longitude, \"{'type':'Polygon','coordinates':[[[0, 0],[0, 1],[1, 1],[1, 0],[0, 0]]]}\"," +
                "id, timestamp, 0.001) as isWithin \n" +
                "insert into dataOut";

        long start = System.currentTimeMillis();
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);
        long end = System.currentTimeMillis();
        logger.info(String.format("Time to create ExecutionPlanRunTime: [%f sec]", ((end - start) / 1000f)));
        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                for (Event event : inEvents) {
                    Boolean intersects = (Boolean) event.getData(3);
                    Assert.assertEquals(expectedResult.get(eventCount++), intersects);
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
}