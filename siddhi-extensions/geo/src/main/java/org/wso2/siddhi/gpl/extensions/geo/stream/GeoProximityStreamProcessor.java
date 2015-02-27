package org.wso2.siddhi.gpl.extensions.geo.stream;

import com.vividsolutions.jts.geom.Geometry;
import org.geotools.util.MapEntry;
import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.event.ComplexEvent;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.event.stream.populater.StreamEventPopulater;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.stream.StreamProcessor;
import org.wso2.siddhi.gpl.extensions.geo.internal.util.GeoOperation;
import org.wso2.siddhi.gpl.extensions.geo.internal.util.WithinDistanceOperation;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.Attribute.Type;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class GeoProximityStreamProcessor extends StreamProcessor {

    private GeoOperation geoOperation;
    private ConcurrentHashMap<String, Geometry> map = new ConcurrentHashMap<String, Geometry>();
    private Set<MapEntry<String, String>> set = Collections.newSetFromMap(
            new ConcurrentHashMap<MapEntry<String, String>, Boolean>());

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
        this.geoOperation = new WithinDistanceOperation();
        this.geoOperation.init(attributeExpressionExecutors, 1, attributeExpressionLength - 1);
        ArrayList<Attribute> attributeList = new ArrayList<Attribute>();
        if (this.geoOperation.isPoint == 1) {
            attributeList.add(new Attribute("otherLatitude", Type.DOUBLE));
            attributeList.add(new Attribute("otherLongitude", Type.DOUBLE));
        } else {
            attributeList.add(new Attribute("otherGeometry", Type.STRING));
        }
        attributeList.add(new Attribute("proximity", Type.BOOL));
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
            StreamEvent streamEvent = (StreamEvent) complexEventChunk.next();
            Geometry currentGeometry, previousGeometry;
            Object[] data = new Object[attributeExpressionLength - 1];
            for (int i = 1; i < attributeExpressionLength; i++) {
                data[i - 1] = attributeExpressionExecutors[i].execute(streamEvent);
            }
            String currentId = attributeExpressionExecutors[0].execute(streamEvent).toString();
            currentGeometry = geoOperation.getCurrentGeometry(data);
            currentGeometry.setUserData(new Object[]{currentId, data[data.length - 1]});
            for (Map.Entry<String, Geometry> entry : map.entrySet()) {
                if (!entry.getKey().equals(currentId)) {
                    previousGeometry = entry.getValue();
                    processData(currentGeometry, currentId, previousGeometry, entry.getKey(), streamEvent);
                }
            }

        }
        nextProcessor.process(complexEventChunk);
    }

    private void processData(Geometry currentGeometry, String currentId,
                             Geometry previousGeometry, String previousId,
                             StreamEvent streamEvent) {
        boolean within = (Boolean) geoOperation.operation(currentGeometry, previousGeometry);
        MapEntry<String, String> pair = new MapEntry<String, String>(currentId, previousId);
        boolean contains = set.contains(pair);
        if (contains) {
            if (!within) {
                //alert out
                streamEventPopulater.populateStreamEvent(streamEventCloner.copyStreamEvent(streamEvent),
                        new Object[]{currentGeometry.toText(), previousGeometry.toText(), false});
                set.remove(pair);
            }
        } else {
            if (within) {
                //alert in
                streamEventPopulater.populateStreamEvent(streamEventCloner.copyStreamEvent(streamEvent),
                        new Object[]{currentGeometry.toText(), previousGeometry.toText(), true});
                set.add(pair);
            }
        }
    }
}
