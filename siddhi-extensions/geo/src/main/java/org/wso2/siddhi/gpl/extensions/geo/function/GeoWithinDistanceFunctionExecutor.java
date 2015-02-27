package org.wso2.siddhi.gpl.extensions.geo.function;

import org.wso2.siddhi.gpl.extensions.geo.internal.util.WithinOperation;

public class GeoWithinDistanceFunctionExecutor extends AbstractGeoOperationExecutor {
    public GeoWithinDistanceFunctionExecutor() {
        this.geoOperation = new WithinOperation();
    }
}
