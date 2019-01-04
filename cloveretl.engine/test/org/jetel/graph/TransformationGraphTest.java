/*
 * jETeL/CloverETL - Java based ETL application framework.
 * Copyright (c) Javlin, a.s. (info@cloveretl.com)
 *  
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */
package org.jetel.graph;

import java.io.FileInputStream;

import org.jetel.graph.runtime.EngineInitializer;
import org.jetel.graph.runtime.GraphRuntimeContext;
import org.jetel.graph.runtime.WatchDog;
import org.jetel.graph.runtime.WatchDogFuture;
import org.jetel.main.runGraph;
import org.jetel.test.CloverTestCase4;
import org.jetel.test.SuspendLoggingRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * @author Milan (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 4. 1. 2019
 */
public class TransformationGraphTest extends CloverTestCase4 {

	// suspend logging from WatchDog, the graph is expected to fail
	@Rule
	public final TestRule suspendLogging = new SuspendLoggingRule(WatchDog.class);

	@Test
	public void testFileDescriptorLeak_CLO15616() throws Exception {
		TransformationGraph graph;
		try (FileInputStream fis = new FileInputStream("./test-data/FileDescriptorLeak.grf")) {
			graph = TransformationGraphXMLReaderWriter.loadGraph(fis, new GraphRuntimeContext());
		}
		
		final Edge edge = graph.getEdges().values().iterator().next();
		
		final Edge edgeSpy = Mockito.spy(edge);
		Mockito.doAnswer(new Answer<Object>() { // override preExecute()

			@Override
			public Object answer(InvocationOnMock invocation) throws Throwable {
				Object result = invocation.callRealMethod();
				EdgeBase edgeBaseSpy = Mockito.spy(edgeSpy.getEdgeBase()); // spy on the edge base 
				edgeSpy.setEdge(edgeBaseSpy);
				return result;
			}
			
		}).when(edgeSpy).preExecute();

		// a bit of a hack, replace the original edge in the graph with the spy
		edge.getWriter().addOutputPort(0, edgeSpy);
		edge.getReader().addInputPort(0, edgeSpy);
		graph.addEdge(edgeSpy);
		
		EngineInitializer.initGraph(graph);

		GraphRuntimeContext runtimeContext = new GraphRuntimeContext();
		WatchDogFuture future = runGraph.executeGraph(graph, runtimeContext);
		future.get(); // the graph is expected to fail

		// postExecute() must be invoked even if the graph fails
		Mockito.verify(edgeSpy.getEdgeBase(), Mockito.times(1)).postExecute();
	}

}
