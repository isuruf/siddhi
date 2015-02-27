package org.wso2.siddhi.gpl.extensions.geo.stream;

import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.event.ComplexEvent;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.event.stream.populater.StreamEventPopulater;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.stream.StreamProcessor;
import org.wso2.siddhi.gpl.extensions.geo.internal.util.GeoOperation;
import org.wso2.siddhi.gpl.extensions.geo.internal.util.WithinOperation;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.Attribute.Type;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class GeoStationaryStreamProcessor extends StreamProcessor {

    private GeoOperation geoOperation;
    private Set<String> set = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

    /**
     * The init method of the StreamProcessor, this method will be called before other methods
     *
     * @param inputDefinition              the incoming stream definition
     * @param attributeExpressionExecutors the executors of each function parameters
     * @param executionPlanContext         the context of the execution plan
     * @return the additional output attributes introduced by the function
     */
    @Override
    protected List<Attribute> init(AbstractDefinition inputDefinition, ExpressionExecutor[] attributeExpressionExecutors, ExecutionPlanContext executionPlanContext) {
        geoOperation = new WithinOperation();
        geoOperation.init(attributeExpressionExecutors, 1, attributeExpressionLength);
        ArrayList<Attribute> attributeList = new ArrayList<Attribute>(1);
        attributeList.add(new Attribute("within", Type.BOOL));
        return attributeList;
    }

    /**
     * This will be called only once, to acquire required resources
     * after initializing the system and before processing the events.
     */
    @Override
    public void start() {

    }

    /**
     * This will be called only once, to release the acquired resources
     * before shutting down the system.
     */
    @Override
    public void stop() {

    }

    /**
     * The serializable state of the element, that need to be
     * persisted for the reconstructing the element to the same state
     * on a different point of time
     *
     * @return stateful objects of the element as an array
     */
    @Override
    public Object[] currentState() {
        return new Object[0];
    }

    /**
     * The serialized state of the element, for reconstructing
     * the element to the same state as if was on a previous point of time.
     *
     * @param state the stateful objects of the element as an array on
     *              the same order provided by currentState().
     */
    @Override
    public void restoreState(Object[] state) {

    }

    @Override
    protected void process(ComplexEventChunk complexEventChunk, Processor nextProcessor, StreamEventCloner streamEventCloner, StreamEventPopulater streamEventPopulater) {
        while (complexEventChunk.hasNext()) {
            ComplexEvent complexEvent = complexEventChunk.next();

            Object[] data = new Object[attributeExpressionLength - 1];
            for (int i = 1; i < attributeExpressionLength; i++) {
                data[i-1] = attributeExpressionExecutors[i].execute(complexEvent);
            }
            boolean within = (Boolean)geoOperation.process(data);

            String id = attributeExpressionExecutors[0].execute(complexEvent).toString();
            if (set.contains(id)) {
                if (!within) {
                    //alert out
                    streamEventPopulater.populateStreamEvent(complexEvent, new Object[]{false});
                    set.remove(id);
                } else {
                    complexEventChunk.remove();
                }
            } else {
                if (within) {
                    //alert in
                    streamEventPopulater.populateStreamEvent(complexEvent, new Object[]{true});
                    set.add(id);
                } else {
                    complexEventChunk.remove();
                }
            }
        }
        nextProcessor.process(complexEventChunk);
    }
}
