package org.wso2.siddhi.gpl.extension.r;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.wso2.siddhi.core.config.SiddhiConfiguration;


public class RTransformTestCase {
	static final Logger log = Logger.getLogger(RTransformTestCase.class);

	protected static SiddhiConfiguration siddhiConfiguration;

	@BeforeClass
    public static void setUp() throws Exception {
		log.info("R:runSource Tests");
		siddhiConfiguration = new SiddhiConfiguration();

		List<Class> extensions = new ArrayList<Class>(1);
		extensions.add(RSourceTransformProcessor.class);
		extensions.add(RScriptTransformProcessor.class);
		siddhiConfiguration.setSiddhiExtensions(extensions);
		log.info("RTransform tests");
	}

	
}
