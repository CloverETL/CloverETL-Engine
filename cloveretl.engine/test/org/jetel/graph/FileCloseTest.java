package org.jetel.graph;

import java.io.File;
import java.io.FileInputStream;
import java.util.concurrent.Future;

import junit.framework.TestCase;
import junit.framework.TestFailure;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.runtime.EngineInitializer;
import org.jetel.graph.runtime.GraphRuntimeContext;
import org.jetel.graph.runtime.IThreadManager;
import org.jetel.graph.runtime.SimpleThreadManager;
import org.jetel.graph.runtime.WatchDog;
import org.jetel.main.runGraph;

/*
 * Copyright (c) 2004-2005 Javlin Consulting s.r.o. All rights reserved.
 * 
 */

public class FileCloseTest extends TestCase {

	private final static String EXAMPLE_PATH = "examples/simpleExamples/";

	static Log logger = LogFactory.getLog(FileCloseTest.class);

	public void testSortData() throws Exception {
		testFileClosed("graph/graphSortData.grf", "data-out/orders.sorted");
	}

	public void testSequence() {
		testFileClosed("graph/graphSequence.grf", "seq/sequence.dat");
	}

	public void testFileClosed(String graphName, String outputName) {

		final File graphFile = new File(EXAMPLE_PATH, graphName);
		final File outputFile = new File(EXAMPLE_PATH, outputName);
		logger.info("output file " + outputFile.getAbsolutePath());

		if (outputFile.exists()) {
			assertTrue(outputFile.delete());
		}

		EngineInitializer.initEngine("plugins", null, null);

		final GraphRuntimeContext runtimeContext = new GraphRuntimeContext();
		runtimeContext.addAdditionalProperty("PROJECT_DIR", EXAMPLE_PATH);
		runtimeContext.addAdditionalProperty("PROJECT", ".");
		runtimeContext.setUseJMX(false);

		TransformationGraph graph = null;
		Future<Result> futureResult = null;

		try {
			graph = TransformationGraphXMLReaderWriter.loadGraph(new FileInputStream(graphFile), runtimeContext.getAdditionalProperties());
			graph.setDebugMode(false);
		} catch (Exception e) {
			fail("Error in graph loading: " + e);
		}

		try {
			EngineInitializer.initGraph(graph, runtimeContext);
		} catch (ComponentNotReadyException e) {
			fail("Error in graph initialization: " + e);
		}

		try {
			IThreadManager threadManager = new SimpleThreadManager();
			WatchDog watchDog = new WatchDog(graph, runtimeContext);
			futureResult = threadManager.executeWatchDog(watchDog);
		} catch (Exception e) {
			fail("Error in graph execution: " + e);
		}

		Result result = Result.N_A;
		try {
			result = futureResult.get();
		} catch (Exception e) {
			fail("Error during graph processing: " + e);
		}

		assertEquals(Result.FINISHED_OK, result);

		System.out.println("Transformation graph is freeing.\n");
		graph.free();

		assertTrue(outputFile.exists());
		assertTrue(outputFile.delete());
		logger.info(outputFile.getAbsolutePath() + " deleted");

	}

}
