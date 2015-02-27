package org.wso2.cep.geo;

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

public class GeoIsWithinTestCase {
    private static SiddhiManager siddhiManager;
    private static Logger logger = org.apache.log4j.Logger.getLogger(GeoIsWithinTestCase.class);
    private static List<Object[]> data;
    private static List<Boolean> expectedResult;
    private static int eventCount = 0;

    @BeforeClass
    public static void setUp() throws Exception {
        logger.info("Init Siddhi");// Create Siddhi Manager
        siddhiManager = new SiddhiManager();
        SiddhiContext siddhiContext = siddhiManager.getSiddhiContext();

        Map<String, Class> classList = new HashMap<String, Class>();
        classList.put("geo:iswithin", GeoWithin.class);
        siddhiContext.setSiddhiExtensions(classList);

        data = new ArrayList<Object[]>();
        expectedResult = new ArrayList<Boolean>();

        data.add(new Object[]{"km-4354", 6.9270786d, 79.861243d, UUID.randomUUID().toString(), "NORMAL", "NORMAL driving pattern"});
        expectedResult.add(true);
        data.add(new Object[]{"km-4354", 6.91049338d, 79.85399723d, UUID.randomUUID().toString(), "NORMAL", "NOT NORMAL driving pattern"});
        expectedResult.add(false);
        data.add(new Object[]{"km-4354", 38.54816542d, -118.19091797d, UUID.randomUUID().toString(), "NORMAL", "NORMAL driving pattern"});
        expectedResult.add(false);
        data.add(new Object[]{"km-4354", 6.93017582d, 79.8625803d, UUID.randomUUID().toString(), "NORMAL", "NOT NORMAL driving pattern"});
        expectedResult.add(true);
        data.add(new Object[]{"km-4354", 33.58402124d, -80.81010818d, UUID.randomUUID().toString(), "NORMAL", "NORMAL driving pattern"});
        expectedResult.add(false);
        data.add(new Object[]{"km-4354", 6.92455235d, 79.86094952d, UUID.randomUUID().toString(), "NORMAL", "NORMAL driving pattern"});
        expectedResult.add(true);
    }

    @Test
    public void testProcess() throws Exception {
        logger.info("TestProcess");
        String executionPlan = "@config(async = 'true') define stream dataIn (id string, latitude double, longitude double, eventId string, state string, information string);"
            + "@info(name = 'query1') from dataIn " +
            "select id, latitude, longitude, eventId, state, information, \n" +
            "geo:iswithin(longitude,latitude,\"{'type':'Polygon','coordinates':[[[79.85395431518555,6.915009335274164],[79.85395431518555,6.941081755563143],[79.88382339477539,6.941081755563143],[79.88382339477539,6.915009335274164],[79.85395431518555,6.915009335274164]]]}\") as isWithin \n" +
            "insert into dataOut";

        long start = System.currentTimeMillis();
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);
        long end = System.currentTimeMillis();
        logger.info(String.format("Time to create ExecutionPlanRunTime: [%f sec]", ((end - start) / 1000f)));
        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                for (Event event : inEvents) {
                    Boolean isWithin = (Boolean) event.getData(6);
                    Assert.assertEquals(expectedResult.get(eventCount++), isWithin);
                }
            }
        });
        executionPlanRuntime.start();
        generateEvents(executionPlanRuntime);
        Thread.sleep(1000);
        /*boolean b;
        Geometry geometry = CreateGeometry.geometryFromJSON("{'type':'Polygon','coordinates':[[[79.85395431518555,6.915009335274164],[79.85395431518555,6.941081755563143],[79.88382339477539,6.941081755563143],[79.88382339477539,6.915009335274164],[79.85395431518555,6.915009335274164]]]}");
        Point point = CreateGeometry.createPoint(79.85, 6.92);
        Coordinate c = new Coordinate(79.85, 6.92);
        IndexedPointInAreaLocator a = new IndexedPointInAreaLocator(geometry);
        PreparedGeometry prep = PreparedGeometryFactory.prepare(geometry);
        start = System.currentTimeMillis();
        for(int i=0;i<100000;i++)
            b = point.within(geometry);
        end = System.currentTimeMillis();
        System.out.println(end-start);
        start = System.currentTimeMillis();
        for(int i=0;i<100000;i++)
            b = a.locate(c) == Location.INTERIOR;
        end = System.currentTimeMillis();
        System.out.println(end-start);
        start = System.currentTimeMillis();
        for(int i=0;i<100000;i++)
            b = prep.contains(point);
        end = System.currentTimeMillis();
        System.out.println(end-start);*/

    }

    private void generateEvents(ExecutionPlanRuntime executionPlanRuntime) throws Exception {
        InputHandler inputHandler = executionPlanRuntime.getInputHandler("dataIn");
        for (Object[] dataLine : data) {
            inputHandler.send(dataLine);
        }
    }
}