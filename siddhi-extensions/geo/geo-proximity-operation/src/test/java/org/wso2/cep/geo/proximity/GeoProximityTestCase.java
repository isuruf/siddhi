package org.wso2.cep.geo.proximity;

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
import org.wso2.siddhi.gpl.extensions.geo.geolibs.CreateGeometry;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GeoProximityTestCase {
    private static SiddhiManager siddhiManager;
    private static Logger logger = org.apache.log4j.Logger.getLogger(GeoProximityTestCase.class);
    private static List<Object[]> data;
    private static List<String> expectedResult;
    private static int eventCount = 0;

    @BeforeClass
    public static void setUp() throws Exception {
        logger.info("Init Siddhi");// Create Siddhi Manager
        siddhiManager = new SiddhiManager();
        SiddhiContext siddhiContext = siddhiManager.getSiddhiContext();

        Map<String, Class> classList = new HashMap<String, Class>();
        classList.put("geo:geoProximity", GeoProximity.class);
        siddhiContext.setSiddhiExtensions(classList);

        data = new ArrayList<Object[]>();
        expectedResult = new ArrayList<String>();

        data.add(new Object[]{"1", 234.345d, 100.786d, 6.9876d});
        expectedResult.add("false");
        data.add(new Object[]{"2", 244.345d, 100.786d, 6.9876d});
        expectedResult.add("false");
        data.add(new Object[]{"3", 244.345d, 100.786d, 6.9876d});
        expectedResult.add("false");
        data.add(new Object[]{"1", 254.345d, 100.786d, 6.9876d});
        expectedResult.add("false");
        data.add(new Object[]{"2", 254.345d, 100.786d, 6.9876d});
        expectedResult.add("false");
        data.add(new Object[]{"3", 254.345d, 5d, 100d});
        expectedResult.add("false");
        data.add(new Object[]{"1", 264.345d, 100.786d, 6.9876d});
        expectedResult.add("true,2");
        data.add(new Object[]{"2", 274.345d, 100.786d, 6.9876d});
        expectedResult.add("true,1");
        data.add(new Object[]{"5", 274.345d, 100.786d, 6.9876d});
        expectedResult.add("false");
        data.add(new Object[]{"5", 294.345d, 100.786d, 6.9876d});
        expectedResult.add("true");
        data.add(new Object[]{"1", 294.345d, 5d, 100d});
        expectedResult.add("false");

    }

    @Test
    public void testProcess() throws Exception {
        logger.info("TestProcess");
        String executionPlan = "@config(async = 'true') define stream cseEventStream ( id string , time double, longitude double, latitude double);"
                + "@info(name = 'query1') from cseEventStream "
                + "select id, time, geo:geoProximity(25d,latitude,longitude,id,time,0.020d ) as tt "
                + "insert into StockQuote;";

        long start = System.currentTimeMillis();
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);
        long end = System.currentTimeMillis();
        logger.info(String.format("Time to create ExecutionPlanRunTime: [%f sec]", ((end - start) / 1000f)));
        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                for (Event event : inEvents) {
                    String geoProximity = (String) event.getData(2);
                    Assert.assertTrue(geoProximity.contains(expectedResult.get(eventCount++)));
                }
            }
        });
        executionPlanRuntime.start();
        generateEvents(executionPlanRuntime);
        Thread.sleep(1000);
        /*
        Point point = CreateGeometry.createPoint(0,0);
        Point point2 = CreateGeometry.createPoint(1,1);
        double radius = 1.01;
        Geometry buffer = point.buffer(radius);
        boolean b;
        start = System.currentTimeMillis();
        for(int i=0;i<10000;i++)
            b = point2.within(buffer);
        end = System.currentTimeMillis();
        System.out.println(end-start);
        start = System.currentTimeMillis();
        for(int i=0;i<10000;i++)
            b = point2.isWithinDistance(point, radius);
        end = System.currentTimeMillis();
        System.out.println(end-start);*/

    }

    private void generateEvents(ExecutionPlanRuntime executionPlanRuntime) throws Exception {
        InputHandler inputHandler = executionPlanRuntime.getInputHandler("cseEventStream");
        for (Object[] dataLine : data) {
            inputHandler.send(dataLine);
        }
    }
}