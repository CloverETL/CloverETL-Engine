package org.jetel.graph;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.runtime.EngineInitializer;
import org.jetel.graph.runtime.GraphExecutor;
import org.jetel.graph.runtime.GraphRuntimeContext;


/*
 * Copyright (c) 2004-2005 Javlin Consulting s.r.o. All rights reserved.
 * 
 */

public class ResetTest {

	/**
	 * 1. param should be plugins location
	 * 2. param should be graph file location
	 * @param args
	 * @throws MalformedURLException
	 * @throws IOException
	 */
	public static void main(String[] args) throws MalformedURLException, IOException {

		Log logger = LogFactory.getLog(ResetTest.class);

		// "../cloveretl.engine/plugins"
		EngineInitializer.initEngine(args[0], null);

		GraphRuntimeContext runtimeContext = new GraphRuntimeContext();
		runtimeContext.addAdditionalProperty("PROJECT_DIR", "."); // "/home/mvarecha/workspace/ex"
		runtimeContext.setUseJMX(true);

		GraphExecutor graphExecutor = new GraphExecutor();

		TransformationGraph graph = null;
		try {
			// "file:/home/mvarecha/workspace/ex/graphJms.grf"
			graph = GraphExecutor.loadGraph(new FileInputStream(args[1]), null);
		} catch (Exception e) {
			logger.error("Error in graph loading !", e);
			System.exit(-1);
		}

		try {
			graphExecutor.initGraph(graph);
		} catch (ComponentNotReadyException e) {
			logger.error("Error in graph initialization !", e);
			System.exit(-1);
		}

		for(int i = 0; i < 5; i++) {

			Future<Result> futureResult = null;
			try {
				futureResult = graphExecutor.runGraph(graph, runtimeContext);
			} catch (Exception e) {
				logger.error("Error in graph execution !", e);
				System.exit(-1);
			}

			Result result = Result.N_A;
			try {
				result = futureResult.get();
			} catch (Exception e) {
				logger.error("Error during graph processing !", e);
				System.exit(-1);
			}

			switch (result) {
			case FINISHED_OK:
				// everything O.K.
				System.out.println("Execution of graph successful !");
				break;
			case ABORTED:
				// execution was ABORTED !!
				System.err.println("Execution of graph aborted !");
				System.exit(result.code());
				break;
			default:
				System.err.println("Execution of graph failed !");
			System.exit(result.code());
			}

			try {
				graph.reset();
			} catch (ComponentNotReadyException e) {
				System.err.println("Graph reseting failed !");
				System.exit(-1);
			}
		}

		System.out.println("Transformation graph is freeing.");
		graph.free();
		System.out.println("Graph executor is terminating.");
		graphExecutor.free();
	}
}