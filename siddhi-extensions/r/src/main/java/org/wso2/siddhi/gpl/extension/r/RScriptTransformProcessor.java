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
	protected void init(Expression[] expressions,
			List<ExpressionExecutor> expressionExecutors,
			StreamDefinition inStreamDefinition,
			StreamDefinition outStreamDefinition, String elementId,
			SiddhiContext siddhiContext) {

		if (expressions.length != 3) {
			throw new QueryCreationException(
					"Wrong number attributes given. Expected 3, found "
							+ expressions.length);
		}
		String scriptString;
		String time;
		String outputString;

		try {
			scriptString = ((StringConstant) expressions[0]).getValue();
		} catch (ClassCastException e) {
			throw new QueryCreationException(
					"First parameter should be of type string", e);
		}
		try {
			time = ((StringConstant) expressions[1]).getValue().trim();
		} catch (ClassCastException e) {
			throw new QueryCreationException(
					"Second parameter should be of type string", e);
		}
		try {
			outputString = ((StringConstant) expressions[2]).getValue();
		} catch (ClassCastException e) {
			throw new QueryCreationException(
					"Third parameter should be of type string", e);
		}

		initialize(scriptString, time, outputString);
	}
}