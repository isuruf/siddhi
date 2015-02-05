package org.wso2.siddhi.gpl.extension.r;

import java.util.List;

import org.wso2.siddhi.core.config.SiddhiContext;
import org.wso2.siddhi.core.exception.QueryCreationException;
import org.wso2.siddhi.core.executor.expression.ExpressionExecutor;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
import org.wso2.siddhi.query.api.expression.Expression;
import org.wso2.siddhi.query.api.expression.constant.StringConstant;
import org.wso2.siddhi.query.api.extension.annotation.SiddhiExtension;

@SiddhiExtension(namespace = "R", function = "runScript")
public class RScriptTransformProcessor extends RTransformProcessor {
	@Override
	protected void init(Expression[] expressions, List<ExpressionExecutor> expressionExecutors,
	                    StreamDefinition inStreamDefinition, StreamDefinition outStreamDefinition,
	                    String elementId, SiddhiContext siddhiContext) {

		if (expressions.length != 3) {
			throw new QueryCreationException("Wrong number attributes given. Expected 3, found " +
			                                 expressions.length);
		}
		String scriptString;
		String period;
		String outputString;

		try {
			scriptString = ((StringConstant) expressions[0]).getValue();
		} catch (ClassCastException e) {
			throw new QueryCreationException("First parameter should be of type string. Found " +
			                                 (expressions[0]).getClass().getCanonicalName() + "\n" +
			                                 "Usage: runScript(script:string, period:string, outputVariables:string)");
		}
		try {
			period = ((StringConstant) expressions[1]).getValue();
		} catch (ClassCastException e) {
			throw new QueryCreationException("Second parameter should be of type string. Found " +
			                                 (expressions[1]).getClass().getCanonicalName() + "\n" +
			                                 "Usage: runScript(script:string, period:string, outputVariables:string)");
		}
		try {
			outputString = ((StringConstant) expressions[2]).getValue();
		} catch (ClassCastException e) {
			throw new QueryCreationException("Third parameter should be of type string. Found " +
			                                 (expressions[2]).getClass().getCanonicalName() + "\n" +
			                                 "Usage: runScript(script:string, period:string, outputVariables:string)");
		}
		initialize(scriptString, period, outputString);
	}
}