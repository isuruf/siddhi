package org.wso2.cep.geo.intersects;

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

public class GeoIntersectsTestCase {
    private static SiddhiManager siddhiManager;
    private static Logger logger = org.apache.log4j.Logger.getLogger(GeoIntersectsTestCase.class);
    private static List<Object[]> data;
    private static List<Boolean> expectedResult;
    private static int eventCount = 0;

    @BeforeClass
    public static void setUp() throws Exception {
        logger.info("Init Siddhi");// Create Siddhi Manager
        siddhiManager = new SiddhiManager();
        SiddhiContext siddhiContext = siddhiManager.getSiddhiContext();

        Map<String, Class> classList = new HashMap<String, Class>();
        classList.put("geo:intersects", GeoIntersects.class);
        siddhiContext.setSiddhiExtensions(classList);

        data = new ArrayList<Object[]>();
        expectedResult = new ArrayList<Boolean>();

        data.add(new Object[]{"1","{'type':'Polygon','coordinates':[[[0.5, 0.5],[0.5, 1.5],[1.5, 1.5],[1.5, 0.5],[0.5, 0.5]]]}"});
        expectedResult.add(true);
        data.add(new Object[]{"1","{'type':'Circle','coordinates':[-1, -1], 'radius':221148}"});
        expectedResult.add(true);
        data.add(new Object[]{"1","{'type':'Point','coordinates':[2, 0]}"});
        expectedResult.add(false);
        data.add(new Object[]{"1","{'type':'Polygon','coordinates':[[[2, 2],[2, 1],[1, 1],[1, 2],[2, 2]]]}"});
        expectedResult.add(true);

    }

    @Test
    public void testProcess() throws Exception {
        logger.info("TestProcess");
        String executionPlan = "@config(async = 'true') define stream dataIn (id string, geometry string);"
                + "@info(name = 'query1') from dataIn " +
                "select id, geometry, \n" +
                "geo:intersects(geometry, \"{'type':'Polygon','coordinates':[[[0, 0],[0, 1],[1, 1],[1, 0],[0, 0]]]}\") as isWithin \n" +
                "insert into dataOut";

        long start = System.currentTimeMillis();
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);
        long end = System.currentTimeMillis();
        logger.info(String.format("Time to create ExecutionPlanRunTime: [%f sec]", ((end - start) / 1000f)));
        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                for (Event event : inEvents) {
                    Boolean intersects = (Boolean) event.getData(2);
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