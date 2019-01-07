package org.jetel.test;

import java.io.FileNotFoundException;
import java.util.concurrent.ExecutionException;

import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.GraphConfigurationException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.runtime.GraphRuntimeContext;

import junit.framework.TestCase;

public abstract class CloverTestCase extends TestCase {

	public final InitEngineRule initEngineRule = new InitEngineRule(getCloverPropertiesFile());

	public CloverTestCase() {
		super();
	}

	public CloverTestCase(String name) {
		super(name);
	}

	@Override
	protected void setUp() throws Exception {
		initEngine();
	}
	
	protected void initEngine() {
		initEngineRule.before();
	}

	protected String getCloverPropertiesFile() {
		return null;
	}
	
	protected TransformationGraph createTransformationGraph(String path, GraphRuntimeContext context)
		throws FileNotFoundException, GraphConfigurationException, XMLConfigurationException, ComponentNotReadyException {
		return TestUtils.createTransformationGraph(path, context);
	}
	
	protected Result runGraph(TransformationGraph graph) throws ExecutionException, InterruptedException {
		return TestUtils.runGraph(graph);
	}
	
}
