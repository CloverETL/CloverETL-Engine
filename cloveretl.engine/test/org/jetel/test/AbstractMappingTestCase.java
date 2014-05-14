/*
 * CloverETL Engine - Java based ETL application framework.
 * Copyright (c) Javlin, a.s. (info@cloveretl.com).  Use is subject to license terms.
 *
 * www.cloveretl.com
 */
package org.jetel.test;

import java.util.List;

import org.jetel.graph.TransformationGraph;
import org.jetel.graph.runtime.GraphRuntimeContext;

/**
 * @author jan.michalica (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 23.8.2012
 */
public class AbstractMappingTestCase extends CloverTestCase {

	protected void runMappingAndExpect(String mappingGraph, Object input, Object expectedOutput) throws Exception {
		
		Object value = runMapping(mappingGraph, input);
		assertEquals(expectedOutput, value);
	}
	
	protected Object runMapping(String mappingGraph, Object input) throws Exception {
		
		TransformationGraph graph = createTransformationGraph("test-data/mapping/" + mappingGraph,
				new GraphRuntimeContext());
		Object result = null;
		try {
			graph.getDictionary().setValue("inputPayload", input);
			runGraph(graph);
			result = getOutput(graph);
		} finally {
			graph.free();
		}
		return result;
	}
	
	protected Object getOutput(TransformationGraph graph) {
		
		List<?> list = (List<?>)graph.getDictionary().getValue("outputPayload");
		if (list.size() == 1) {
			return list.get(0);
		}
		return list;
	}
}
