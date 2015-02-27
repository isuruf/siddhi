package org.wso2.cep.geo.notify;

import org.apache.log4j.Logger;
import org.junit.*;
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

public class NotifyAlertTestCase{
    private static SiddhiManager siddhiManager;
    private static Logger logger = org.apache.log4j.Logger.getLogger(NotifyAlertTestCase.class);
    private static List<Object[]> data;
    private static List<Boolean> expectedResult;
    private static int eventCount = 0;

    @BeforeClass
    public static void setUp() throws Exception {
        logger.info("Init Siddhi");// Create Siddhi Manager
        siddhiManager = new SiddhiManager();
        SiddhiContext siddhiContext = siddhiManager.getSiddhiContext();

        Map<String, Class> classList = new HashMap<String, Class>();
        classList.put("geo:needToNotify", NotifyAlert.class);
        siddhiContext.setSiddhiExtensions(classList);

        data = new ArrayList<Object[]>();
        expectedResult = new ArrayList<Boolean>();

        data.add(new Object[]{"km-4354", "12.56", "56.32", "12345684", "NORMAL", "NORMAL driving pattern"});
        expectedResult.add(false);

        data.add(new Object[]{"km-4354", "12.56", "56.32", "12345684", "NORMAL", "NOT NORMAL driving pattern"});
        expectedResult.add(true);

        data.add(new Object[]{"km-4354", "12.56", "56.32", "12345684", "NORMAL", "NOT NORMAL driving pattern"});
        expectedResult.add(false);

        data.add(new Object[]{"km-4354", "12.56", "56.32", "12345684", "NORMAL", "NORMAL driving pattern"});
        expectedResult.add(true);
    }

    @Test
    public void testProcess() throws Exception {
        logger.info("TestProcess");
        String executionPlan = "@config(async = 'true') define stream dataIn (id string, latitude double, longitude double, eventId string, state string, information string);"
                + "@info(name = 'query1') from dataIn \n" +
                "select id ,latitude ,longitude ,eventId ,state ,information , geo:needToNotify(id,information) as notify\n" +
                "insert into dataOut;";

        long start = System.currentTimeMillis();
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);
        long end = System.currentTimeMillis();
        logger.info(String.format("Time to create ExecutionPlanRunTime: [%f sec]", ((end - start) / 1000f)));
        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                for (Event event : inEvents) {
                    Boolean notify = (Boolean) event.getData(6);
                    Assert.assertEquals(expectedResult.get(eventCount++), notify);
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