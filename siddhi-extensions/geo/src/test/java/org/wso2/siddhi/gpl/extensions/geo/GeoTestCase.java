package org.wso2.siddhi.gpl.extensions.geo;

import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.stream.input.InputHandler;

import java.util.ArrayList;

/**
 * Created by isuru on 2/27/15.
 */
public abstract class GeoTestCase {
    protected static SiddhiManager siddhiManager;
    private static Logger logger = Logger.getLogger(GeoTestCase.class);
    protected static ArrayList<Object[]> data;
    protected static ArrayList<Boolean> expectedResult;

    @BeforeClass
    public static void setUp() throws Exception {
        logger.info("Init Siddhi");// Create Siddhi Manager
        siddhiManager = new SiddhiManager();
        data = new ArrayList<Object[]>();
        expectedResult = new ArrayList<Boolean>();
    }

    protected void generateEvents(ExecutionPlanRuntime executionPlanRuntime) throws Exception {
        InputHandler inputHandler = executionPlanRuntime.getInputHandler("dataIn");
        for (Object[] dataLine : data) {
            inputHandler.send(dataLine);
        }
    }
}
