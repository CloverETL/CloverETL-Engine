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
	
	private static final Phase INIT_PHASE = new Phase(Phase.INITIAL_PHASE_ID);
	private static final Phase FINAL_PHASE = new Phase(Phase.FINAL_PHASE_ID);
	private TransformationGraph restJob;
	private DataRecordMetadata metadata;
	private HelloWorldComponent restJobInput;
	private HelloWorldComponent restJobOutput;
	private Phase mainPhase;
	
	@Override
	protected void setUp() throws Exception {
		super.setUp();
		restJob = new TransformationGraph();
		restJob.setStaticJobType(JobType.RESTJOB);
		GraphRuntimeContext ctx = new GraphRuntimeContext();
		ctx.setUseJMX(false);
		restJob.setInitialRuntimeContext(ctx);
		metadata = new DataRecordMetadata("record1", DataRecordParsingType.DELIMITED);
		metadata.addField(new DataFieldMetadata("field1", DataFieldType.STRING, "|"));
		restJobInput = new HelloWorldComponent("RESTJOB_INPUT");
		restJobInput.setPartOfRestInput(true);
		restJobInput.setType(RestJobUtils.REST_JOB_INPUT_TYPE);
		restJobInput.setGreeting("Hello from rest job input!");
		restJobOutput = new HelloWorldComponent("RESTJOB_OUTPUT");
		restJobOutput.setPartOfRestOutput(true);
		restJobOutput.setType(RestJobUtils.REST_JOB_OUTPUT_TYPE);
		restJobOutput.setGreeting("Hello from rest job output!");
		mainPhase = new Phase(7);
		mainPhase.addNode(restJobInput, restJobOutput);
		restJob.addPhase(mainPhase);
	}
	
	private void generateResponse() throws Exception {
		if (restJobOutput.getOutPorts().size() == 0) {
			Edge out2res = createEdge("out2res");
			restJobOutput.addOutputPort(0, out2res);
			HelloWorldComponent restJobResponse = new HelloWorldComponent("RESTJOB_RESPONSE");
			restJobResponse.setGreeting("Hello from rest job response");
			restJobResponse.setPartOfRestOutput(true);
			restJobResponse.addInputPort(0, out2res);
			mainPhase.addNode(restJobResponse);
			restJob.addEdge(out2res);
		}
	}
	
	private Edge createEdge(String id) {
		Edge edge = EdgeFactory.newEdge(id, metadata);
		edge.setEdgeType(EdgeTypeEnum.DIRECT_FAST_PROPAGATE);
		return edge;
	}

	public void testJobWithBody() throws Exception {
		generateResponse();
		
		HelloWorldComponent body = new HelloWorldComponent("HELLO_WORLD");
		body.setGreeting("Hello from body");
		
		mainPhase.addNode(body);
	
		EngineInitializer.initGraph(restJob, restJob.getRuntimeContext());
		
		checkPhases(2, mainPhase, mainPhase);
		
		runGraph(restJob);
	}
	
	public void testEmptyJobWithCustomResponse() throws Exception {
		EngineInitializer.initGraph(restJob, restJob.getRuntimeContext());
		
		checkPhases(3, INIT_PHASE, FINAL_PHASE);
	}
	
	public void testEmptyJobWithGeneratedResponse() throws Exception {
		generateResponse();
		
		EngineInitializer.initGraph(restJob, restJob.getRuntimeContext());
		
		checkPhases(2, mainPhase, mainPhase);
	}
	
	public void testJobWith3Phases() throws Exception {
		generateResponse();
		
		HelloWorldComponent checkData = new HelloWorldComponent("DATA_CHECK");
		checkData.setGreeting("Hello from data check");
		Phase checkPhase = new Phase(3);
		checkPhase.addNode(checkData);
		restJob.addPhase(checkPhase);
		
		HelloWorldComponent loadData = new HelloWorldComponent("DATA_LOAD");
		loadData.setGreeting("Hello from data load");
		mainPhase.addNode(loadData);
		
		HelloWorldComponent saveData = new HelloWorldComponent("DATA_SAVE");
		saveData.setGreeting("Hello from create response");
		Phase savePhase = new Phase(5);
		savePhase.addNode(saveData);
		restJob.addPhase(savePhase);
		
		EngineInitializer.initGraph(restJob, restJob.getRuntimeContext());
		
		checkPhases(4, checkPhase, mainPhase);
	}
	
	public void testJobWith3PhasesInputConnected() throws Exception {
		generateResponse();
		
		HelloWorldComponent checkData = new HelloWorldComponent("DATA_CHECK");
		checkData.setGreeting("Hello from data check");
		Phase checkPhase = new Phase(3);
		checkPhase.addNode(checkData);
		restJob.addPhase(checkPhase);
		
		Edge in2load = createEdge("in2load");
		restJobInput.addOutputPort(0, in2load);
		HelloWorldComponent loadData = new HelloWorldComponent("DATA_LOAD");
		loadData.setGreeting("Hello from data load");
		loadData.addInputPort(0, in2load);
		mainPhase.addNode(loadData);
		restJob.addEdge(in2load);
		
		HelloWorldComponent saveData = new HelloWorldComponent("DATA_SAVE");
		saveData.setGreeting("Hello from create response");
		Phase savePhase = new Phase(5);
		savePhase.addNode(saveData);
		restJob.addPhase(savePhase);
		
		EngineInitializer.initGraph(restJob, restJob.getRuntimeContext());
		
		checkPhases(4, checkPhase, mainPhase);
	}
	
	public void testJobWith3PhasesAndRequiredParametersValidator() throws Exception {
		generateResponse();
		
		HelloWorldComponent validator = new HelloWorldComponent("RESTJOB_VALIDATOR");
		validator.setPartOfRestInput(true);
		validator.setType("RESTJOB_VALIDATOR");
		validator.setGreeting("Hello from rest job input!");
		mainPhase.addNode(validator);
		
		HelloWorldComponent checkData = new HelloWorldComponent("DATA_CHECK");
		checkData.setGreeting("Hello from data check");
		Phase checkPhase = new Phase(3);
		checkPhase.addNode(checkData);
		restJob.addPhase(checkPhase);
		
		HelloWorldComponent loadData = new HelloWorldComponent("DATA_LOAD");
		loadData.setGreeting("Hello from data load");
		mainPhase.addNode(loadData);
		
		HelloWorldComponent saveData = new HelloWorldComponent("DATA_SAVE");
		saveData.setGreeting("Hello from create response");
		Phase savePhase = new Phase(5);
		savePhase.addNode(saveData);
		restJob.addPhase(savePhase);
		
		EngineInitializer.initGraph(restJob, restJob.getRuntimeContext());
		
		checkPhases(5, checkPhase, mainPhase);
	}
	
	private void checkPhases(int count, Phase input, Phase output) {
		assertEquals("Unexpected count of phases", count, restJob.getPhases().length);
		assertEquals("REST job input in wrong phase", input.getPhaseNum(), restJobInput.getPhaseNum());
		assertEquals("REST job output in wrong phase", output.getPhaseNum(), restJobOutput.getPhaseNum());
	}
}
