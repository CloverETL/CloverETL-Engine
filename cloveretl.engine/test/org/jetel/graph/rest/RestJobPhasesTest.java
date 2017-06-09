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
import org.jetel.graph.JobType;
import org.jetel.graph.Phase;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.runtime.EngineInitializer;
import org.jetel.graph.runtime.GraphRuntimeContext;
import org.jetel.test.CloverTestCase;

/**
 * @author jan.michalica (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 22.3.2017
 */
public class RestJobPhasesTest extends CloverTestCase {

	
	public void testRestJobAnalysisAndPhases() throws Exception {
		
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
		barrier.setType("RESTJOB_OUTPUT");
		barrier.setPartOfRestOutput(true);
		
		HelloWorldComponent output = new HelloWorldComponent("HELLO3");
		output.setGreeting("Hello from job output!");
		output.setPartOfRestOutput(true);
		
		Phase main = new Phase(7);
		main.addNode(input, output, body, barrier);
		graph.addPhase(main);
		
		EngineInitializer.initGraph(graph, ctx);
		
		assertTrue("REST job output in wrong phase", main.getNodes().containsValue(barrier));
		assertEquals("Unexpected count of phases", 3, graph.getPhases().length);
		
		runGraph(graph);
	}
}
