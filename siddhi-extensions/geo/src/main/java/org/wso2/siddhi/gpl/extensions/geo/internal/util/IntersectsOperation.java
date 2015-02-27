package org.wso2.siddhi.gpl.extensions.geo.internal.util;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.prep.PreparedGeometry;

/**
 * Created by isuru on 2/25/15.
 */
public class IntersectsOperation extends GeoOperation {
    @Override
    protected Boolean operation(Geometry a, Geometry b, Object[] data) {
        return a.within(b);
    }

    @Override
    protected Boolean operation(Geometry a, PreparedGeometry b, Object[] data) {
        return b.contains(a);
    }
}
