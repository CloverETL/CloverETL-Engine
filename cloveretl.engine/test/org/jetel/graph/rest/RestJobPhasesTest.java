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
package org.jetel.graph.rest;

import org.jetel.component.HelloWorldComponent;
import org.jetel.enums.EdgeTypeEnum;
import org.jetel.graph.Edge;
import org.jetel.graph.EdgeFactory;
import org.jetel.graph.JobType;
import org.jetel.graph.Phase;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.runtime.EngineInitializer;
import org.jetel.graph.runtime.GraphRuntimeContext;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataFieldType;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.metadata.DataRecordParsingType;
import org.jetel.test.CloverTestCase;
import org.jetel.util.RestJobUtils;

/**
 * @author jan.michalica (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 22.3.2017
 */
public class RestJobPhasesTest extends CloverTestCase {

	
	public void testRestJobAnalysisAndPhases() throws Exception {
		
		DataRecordMetadata metadata = new DataRecordMetadata("rc1", DataRecordParsingType.DELIMITED);
		metadata.addField(new DataFieldMetadata("f1", DataFieldType.STRING, "|"));
		
		Edge edge = EdgeFactory.newEdge("e1", metadata);
		edge.setEdgeType(EdgeTypeEnum.DIRECT_FAST_PROPAGATE);
	
		TransformationGraph graph = new TransformationGraph();
		graph.setStaticJobType(JobType.RESTJOB);
		GraphRuntimeContext ctx = new GraphRuntimeContext();
		ctx.setUseJMX(false);
		graph.setInitialRuntimeContext(ctx);
		
		HelloWorldComponent input = new HelloWorldComponent("HELLO1");
		input.setGreeting("Hello from job input!");
		input.setPartOfRestInput(true);
		
		HelloWorldComponent body = new HelloWorldComponent("HELLO2");
		
		HelloWorldComponent barrier = new HelloWorldComponent("HELLOBARRIER");
		barrier.setGreeting("Hello from barrier");
		barrier.setType(RestJobUtils.REST_JOB_OUTPUT_TYPE);
		barrier.setPartOfRestOutput(true);
		barrier.addOutputPort(0, edge);
		
		HelloWorldComponent output = new HelloWorldComponent("HELLO3");
		output.setGreeting("Hello from job output!");
		output.setPartOfRestOutput(true);
		output.addInputPort(0, edge);
		
		Phase main = new Phase(7);
		main.addNode(input, output, body, barrier);
	
		graph.addPhase(main);
		graph.addEdge(edge);
		
		EngineInitializer.initGraph(graph, ctx);
		
		assertEquals("REST job input in wrong phase", main.getPhaseNum(), input.getPhaseNum());;
		assertTrue("REST job output in wrong phase", main.getNodes().containsValue(barrier));
		assertEquals("Unexpected count of phases", 2, graph.getPhases().length);
		
		runGraph(graph);
	}
}
