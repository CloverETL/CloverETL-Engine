package org.jetel.test;

import java.io.FileNotFoundException;
import java.util.concurrent.ExecutionException;

import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.GraphConfigurationException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.runtime.GraphRuntimeContext;
import org.junit.Rule;
import org.junit.rules.TestRule;

/**
 * A copy of CloverTestCase rewritten for JUnit4.
 * Engine initialization was moved to {@link InitEngineRule}.
 * Utility methods were moved to {@link TestUtils}.
 * 
 * @author Milan (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 4. 1. 2019
 */
public abstract class CloverTestCase4 {
	
	@Rule
	public final TestRule initEngine = new InitEngineRule(getCloverPropertiesFile());

	/**
	 * Override to provide a custom location of the engine configuration file.
	 * 
	 * @return
	 */
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
