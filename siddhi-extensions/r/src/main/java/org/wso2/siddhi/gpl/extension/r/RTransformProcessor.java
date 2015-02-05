package org.wso2.siddhi.gpl.extension.r;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.rosuda.REngine.REXP;
import org.rosuda.REngine.REXPDouble;
import org.rosuda.REngine.REXPInteger;
import org.rosuda.REngine.REXPLogical;
import org.rosuda.REngine.REXPString;
import org.rosuda.REngine.REngine;
import org.rosuda.REngine.REngineException;
import org.rosuda.REngine.RList;
import org.rosuda.REngine.JRI.JRIEngine;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.event.in.InEvent;
import org.wso2.siddhi.core.event.in.InListEvent;
import org.wso2.siddhi.core.event.in.InStream;
import org.wso2.siddhi.core.exception.QueryCreationException;
import org.wso2.siddhi.core.query.processor.transform.TransformProcessor;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.compiler.SiddhiCompiler;
import org.wso2.siddhi.query.compiler.exception.SiddhiParserException;

public abstract class RTransformProcessor extends TransformProcessor {

	boolean isTime = true;
	long lastRun;
	long period;

	List<Attribute> eventAttributes;
	List<Attribute> outputAttributes;
	List<InEvent> eventList = new ArrayList<InEvent>();

	REXP outputs;
	REXP script;
	REXP env;

	REngine re;
	static Logger log = Logger.getLogger(RTransformProcessor.class);

	protected void initialize(String scriptString, String period, String outputString) {
		try {
			// Get the JRIEngine or create one
			re = JRIEngine.createEngine();
			// Create a new R environment
			env = re.newEnvironment(null, true);
		} catch (Exception e) {
			throw new QueryCreationException("Unable to create a new session in R", e);
		}

		try {
			if (period.endsWith("s")) {
				this.period = Integer.parseInt(period.substring(0, period.length() - 1).trim()) * 1000;
			} else if (period.endsWith("min")) {
				this.period = Integer.parseInt(period.substring(0, period.length() - 3).trim()) * 60 * 1000;
			} else if (period.endsWith("h")) {
				this.period = Integer.parseInt(period.substring(0, period.length() - 1).trim()) * 60 * 60 * 1000;
			} else {
				this.period = Integer.parseInt(period);
				isTime = false;
			}
			lastRun = System.currentTimeMillis();
			this.outStreamDefinition = SiddhiCompiler.parseStreamDefinition("define stream ROutputStream(" +
			                                                                outputString + ")");

		} catch (NumberFormatException e) {
			throw new QueryCreationException("Unsupported value for the period given " + period, e);
		} catch (SiddhiParserException e) {
			throw new QueryCreationException("Could not parse the output variables string. Usage: \"a string, b int\"" +
			                                         ". Found: \"" + outputString + "\"", e);
		}

		eventAttributes = inStreamDefinition.getAttributeList();
		outputAttributes = outStreamDefinition.getAttributeList();

		StringBuilder sb = new StringBuilder("list(");
		for (int i = 0; i < outputAttributes.size(); i++) {
			sb.append(outputAttributes.get(i).getName());
			if (i != outputAttributes.size() - 1) {
				sb.append(",");
			}
		}
		sb.append(")");

		try {
			// Parse the output list expression
			outputs = re.parse(sb.toString(), false);
			// Parse the script
			script = re.parse(scriptString, false);
		} catch (REngineException e) {
			throw new QueryCreationException("Unable to parse the script: " + scriptString, e);
		}
		lastRun = System.currentTimeMillis();
	}

	@Override
	protected InStream processEvent(InEvent inEvent) {
		eventList.add(inEvent);
		if (isTime) {
			if (System.currentTimeMillis() >= lastRun + period) {
				lastRun = System.currentTimeMillis();
				return runScript(inEvent.getStreamId());
			}
		} else {
			if (eventList.size() == period) {
				return runScript(inEvent.getStreamId());
			}
		}
		return null;
	}

	private InEvent runScript(String streamId) {
		try {
			REXP eventData;
			Attribute attr;
			for (int j = 0; j < eventAttributes.size(); j++) {
				attr = eventAttributes.get(j);
				switch (attr.getType()) {
					case DOUBLE:
						eventData = doubleToREXP(eventList, j);
						break;
					case FLOAT:
						eventData = floatToREXP(eventList, j);
						break;
					case INT:
						eventData = intToREXP(eventList, j);
						break;
					case STRING:
						eventData = stringToREXP(eventList, j);
						break;
					case LONG:
						eventData = longToREXP(eventList, j);
						break;
					case BOOL:
						eventData = boolToREXP(eventList, j);
						break;
					default:
						continue;
				}
				re.assign(attr.getName(), eventData, env);
			}
			re.eval(script, env, false);
		} catch (Exception e) {
			throw new QueryCreationException("Unable to evaluate the script", e);
		}

		try {
			RList out = re.eval(outputs, env, true).asList();
			REXP result;
			Object[] data = new Object[out.size()];
			for (int i = 0; i < out.size(); i++) {
				result = ((REXP) out.get(i));
				switch (outputAttributes.get(i).getType()) {
					case BOOL:
						if (result.isLogical()) {
							data[i] = (result.asInteger() == 1);
							break;
						}
					case INT:
						if (result.isNumeric()) {
							data[i] = result.asInteger();
							break;
						}
					case LONG:
						if (result.isNumeric()) {
							data[i] = (long) result.asDouble();
							break;
						}
					case FLOAT:
						if (result.isNumeric()) {
							data[i] = ((Double) result.asDouble()).floatValue();
							break;
						}
					case DOUBLE:
						if (result.isNumeric()) {
							data[i] = result.asDouble();
							break;
						}
					case STRING:
						if (result.isString()) {
							data[i] = result.asString();
							break;
						}
					default:
						throw new QueryCreationException("Mismatch in returned and expected output. Expected: " +
						                                         outputAttributes.get(i).getType() + " Returned: " + 
						                                         result.asNativeJavaObject().getClass().getCanonicalName());
				}
			}
			eventList.clear();
			return new InEvent(streamId, System.currentTimeMillis(), data);
		} catch (Exception e) {
			throw new QueryCreationException("Mismatch in returned output and expected output", e);
		}
	}

	@Override
	protected InStream processEvent(InListEvent inListEvent) {
		InListEvent transformedListEvent = new InListEvent();
		for (Event event : inListEvent.getEvents()) {
			if (event instanceof InEvent) {
				InStream inStream = processEvent((InEvent) event);
				if (inStream != null) {
					transformedListEvent.addEvent((Event) inStream);
				}
			}
		}
		return transformedListEvent;
	}

	@Override
	protected Object[] currentState() {
		Object[] objects = { eventList };
		return objects;
	}

	@Override
	protected void restoreState(Object[] objects) {
		eventList = (List<InEvent>) objects[0];
	}

	@Override
	public void destroy() {
		re.close();
	}

	private REXP doubleToREXP(List<InEvent> list, int index) {
		double[] arr = new double[list.size()];
		for (int i = 0; i < list.size(); i++) {
			arr[i] = (Double) list.get(i).getData(index);
		}
		return new REXPDouble(arr);
	}

	private REXP floatToREXP(List<InEvent> list, int index) {
		double[] arr = new double[list.size()];
		for (int i = 0; i < list.size(); i++) {
			arr[i] = (Float) list.get(i).getData(index);
		}
		return new REXPDouble(arr);
	}

	private REXP intToREXP(List<InEvent> list, int index) {
		int[] arr = new int[list.size()];
		for (int i = 0; i < list.size(); i++) {
			arr[i] = (Integer) list.get(i).getData(index);
		}
		return new REXPInteger(arr);
	}

	private REXP longToREXP(List<InEvent> list, int index) {
		double[] arr = new double[list.size()];
		for (int i = 0; i < list.size(); i++) {
			arr[i] = ((Long) list.get(i).getData(index)).doubleValue();
		}
		return new REXPDouble(arr);
	}

	private REXP stringToREXP(List<InEvent> list, int index) {
		String[] arr = new String[list.size()];
		for (int i = 0; i < list.size(); i++) {
			arr[i] = (String) list.get(i).getData(index);
		}
		return new REXPString(arr);
	}

	private REXP boolToREXP(List<InEvent> list, int index) {
		boolean[] arr = new boolean[list.size()];
		for (int i = 0; i < list.size(); i++) {
			arr[i] = (Boolean) list.get(i).getData(index);
		}
		return new REXPLogical(arr);
	}
}