package org.wso2.cep.r;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.wso2.cep.r.RScriptTransformProcessor;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.config.SiddhiConfiguration;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.siddhi.query.api.QueryFactory;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.expression.Expression;
import org.wso2.siddhi.query.api.query.Query;
import org.wso2.siddhi.query.api.query.output.stream.OutStream;


public class RScriptTestCase {
	static final Logger log = Logger.getLogger(RScriptTestCase.class);

	private int count;
	private boolean eventArrived;
	private static SiddhiConfiguration siddhiConfiguration;
	private double value1;
	private double value2;
	@Before
	public void init() {
		count = 0;
		eventArrived = false;
	}
	@BeforeClass
    public static void setUp() throws Exception {
		log.info("R:runScript Tests");
		siddhiConfiguration = new SiddhiConfiguration();

		List<Class> extensions = new ArrayList<Class>(1);
		extensions.add(RScriptTransformProcessor.class);
		siddhiConfiguration.setSiddhiExtensions(extensions);
        
	}
	
	@Test
	public void testRScript1() throws InterruptedException {
		log.info("R:runScript test1");
		SiddhiManager siddhiManager = new SiddhiManager(siddhiConfiguration);
		siddhiManager.defineStream(QueryFactory.createStreamDefinition()
				.name("weather").attribute("time", Attribute.Type.LONG)
				.attribute("temp", Attribute.Type.DOUBLE));

		String script =  "c <- sum(time); m <- sum(temp) ;";
		
		  Query query = QueryFactory.createQuery();
	        query.from(
	                QueryFactory.inputStream("weather").
	                        transform("R", "runScript", Expression.value(script), Expression.value("2"), Expression.value("m,c"))
	        );
	        query.select(
	                QueryFactory.outputSelector().
	                        select("m", Expression.variable("m")).
	                        select("c", Expression.variable("c"))
	        );
	        query.insertInto("weatherOutput", OutStream.OutputEventsFor.ALL_EVENTS);
		
		
		String queryReference = siddhiManager.addQuery(query);
		siddhiManager.addCallback(queryReference, new QueryCallback() {
			@Override
			public void receive(long timeStamp, Event[] inEvents,
					Event[] removeEvents) {
				EventPrinter.print(timeStamp, inEvents, removeEvents);
				if (inEvents != null) {

					for (Event event : inEvents) {
						value1 = (Double) event.getData1();
						value2 = (Double) event.getData0();
	                }
					count++;
				}
			}
		});

		InputHandler inputHandler = siddhiManager.getInputHandler("weather");
		inputHandler.send(new Object[] { 10l, 55.6d });
		inputHandler.send(new Object[] { 20l, 65.6d });
		inputHandler.send(new Object[] { 30l, 75.6d });
		Thread.sleep(1000);
		Assert.assertEquals("Only one event must arrive", 1, count);
		Assert.assertEquals("Value 1 returned", (10 + 20) + 0.0, value1, 1e-4);
		Assert.assertEquals("Value 2 returned", (55.6 + 65.6), value2, 1e-4);
		siddhiManager.shutdown();
	}
	
	@Test
	public void testRScript2() throws InterruptedException {
		log.info("R:runScript test2");
		SiddhiManager siddhiManager = new SiddhiManager(siddhiConfiguration);
		siddhiManager.defineStream(QueryFactory.createStreamDefinition()
				.name("weather").attribute("time", Attribute.Type.LONG)
				.attribute("temp", Attribute.Type.DOUBLE));

		String script = "\" "
				+ "c <- sum(time); m <- sum(temp) ;\"";
		String query = "from weather#transform.R:runScript("+script+", \"2s\", \"m,c\") "+
						"select * "+
						"insert into weatherOutput";

		String queryReference = siddhiManager.addQuery(query);
		siddhiManager.addCallback(queryReference, new QueryCallback() {
			@Override
			public void receive(long timeStamp, Event[] inEvents,
					Event[] removeEvents) {
				EventPrinter.print(timeStamp, inEvents, removeEvents);
				if (inEvents != null) {

					for (Event event : inEvents) {
						value1 = (Double) event.getData1();
						value2 = (Double) event.getData0();
	                }
					count++;
				}
			}
		});

		InputHandler inputHandler = siddhiManager.getInputHandler("weather");
		inputHandler.send(new Object[] { 10l, 55.6d });
		inputHandler.send(new Object[] { 20l, 65.6d });
		Thread.sleep(2500);
		inputHandler.send(new Object[] { 30l, 75.6d });
		Thread.sleep(1000);
		Assert.assertEquals("Only one event must arrive", 1, count);
		Assert.assertEquals("Value 1 returned", (10 + 20 + 30) + 0.0, value1, 1e-4);
		Assert.assertEquals("Value 2 returned", (55.6 + 65.6 + 75.6), value2, 1e-4);
		siddhiManager.shutdown();
	}
	
	
	@Test
	public void testRScript3() throws InterruptedException {
		log.info("R:runScript test3");
		SiddhiManager siddhiManager = new SiddhiManager(siddhiConfiguration);
		siddhiManager.defineStream(QueryFactory.createStreamDefinition()
				.name("weather").attribute("time", Attribute.Type.LONG)
				.attribute("temp", Attribute.Type.DOUBLE));

		String script =  "c <- sum(time); m <- sum(temp) ;";
		
		  Query query = QueryFactory.createQuery();
	        query.from(
	                QueryFactory.inputStream("weather").
	                        transform("R", "runScript", Expression.value(script), Expression.value("1s"), Expression.value("m,c"))
	        );
	        query.select(
	                QueryFactory.outputSelector().
	                        select("m", Expression.variable("m")).
	                        select("c", Expression.variable("c"))
	        );
	        query.insertInto("weatherOutput", OutStream.OutputEventsFor.ALL_EVENTS);

		String queryReference = siddhiManager.addQuery(query);
		siddhiManager.addCallback(queryReference, new QueryCallback() {
			@Override
			public void receive(long timeStamp, Event[] inEvents,
					Event[] removeEvents) {
				EventPrinter.print(timeStamp, inEvents, removeEvents);
				if (inEvents != null) {

					for (Event event : inEvents) {
						value1 = (Double) event.getData1();
						value2 = (Double) event.getData0();
	                }
					count++;
				}
			}
		});

		InputHandler inputHandler = siddhiManager.getInputHandler("weather");
		inputHandler.send(new Object[] { 10l, 55.6d });
		inputHandler.send(new Object[] { 20l, 65.6d });
		Thread.sleep(1500);
		inputHandler.send(new Object[] { 30l, 55.6d });
		inputHandler.send(new Object[] { 40l, 65.6d });
		Thread.sleep(1500);
		inputHandler.send(new Object[] { 50l, 75.6d });
		Thread.sleep(1000);
		
		Assert.assertEquals("Only two events must arrive", 2, count);
		Assert.assertEquals("Value 1 returned", (40 + 50) + 0.0, value1, 1e-4);
		Assert.assertEquals("Value 2 returned", (65.6 + 75.6), value2, 1e-4);
		siddhiManager.shutdown();
	}
}
