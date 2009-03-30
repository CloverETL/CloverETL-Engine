package org.jetel.graph;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.concurrent.Future;

import junit.framework.AssertionFailedError;
import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.runtime.EngineInitializer;
import org.jetel.graph.runtime.GraphRuntimeContext;
import org.jetel.graph.runtime.IThreadManager;
import org.jetel.graph.runtime.SimpleThreadManager;
import org.jetel.graph.runtime.WatchDog;


/*
 * Copyright (c) 2004-2005 Javlin Consulting s.r.o. All rights reserved.
 * 
 */

public class ResetTest extends TestCase{

	private final static String EXAMPLE_PATH = "examples/simpleExamples/";
	
	static Log logger = LogFactory.getLog(ResetTest.class);
	private GraphRuntimeContext runtimeContext;

	private final static String PROJECT_DIR = "examples/simpleExamples/";
	private final static String GRAPHS_DIR = "graph";
	private final static String[] OUT_DIRS = {"data-out/", "data-tmp/", "seq/"};
	
	@Override
	protected void setUp() throws Exception {
		super.setUp();
		EngineInitializer.initEngine( "plugins", null, null);

		runtimeContext = new GraphRuntimeContext();
		runtimeContext.addAdditionalProperty("PROJECT_DIR", EXAMPLE_PATH);
		runtimeContext.addAdditionalProperty("PROJECT", ".");
		runtimeContext.setUseJMX(false);
	}
	
	public void testAllExamples() throws Exception {
		File[] graphFile = (new File(PROJECT_DIR + GRAPHS_DIR)).listFiles(new FileFilter(){
			public boolean accept(File pathname) {
				return pathname.getName().endsWith(".grf")
					&& !pathname.getName().endsWith("graphSimpleLookup.grf") // ok, uses lookup free in transform attribute
					&& !pathname.getName().equals("graphJoinData.grf") // ok, uses class file that is not created
				 	&& !pathname.getName().equals("graphJoinHash.grf") // ok, uses class file that is not created
				 	&& !pathname.getName().equals("graphOrdersReformat.grf") // ok, uses class file that is not created
					&& !pathname.getName().equals("graphParametrizedLookup.grf") // ok, wrong path to lookup
					&& !pathname.getName().equals("graphRunGraph.grf") // ok, wrong path to output file
					&& !pathname.getName().equals("graphSystemExecute.grf") // ok, wrong path
					&& !pathname.getName().equals("graphAspellLookup.grf") // ok, use commercial components
					&& !pathname.getName().equals("graphPersistentLookup.grf") // ok, use commercial components
					&& !pathname.getName().equals("graphPersistentLookup2.grf") // ok, use commercial components
					&& !pathname.getName().equals("graphSystemExecuteWin.grf"); // ok, graph for Windows
			}
		});
		
		for (int i = 0; i < graphFile.length; i++) {
			try {
				testExample(graphFile[i]);
			} catch (AssertionFailedError e) {
				//if (!graphFile[i].getName().contains("graphIntersectData.grf")) // issue 870
				fail(graphFile[i] + ": " + e.getMessage());
			} catch (Exception e) {
				fail(graphFile[i] + ": " + e.getMessage());
			}
		}
	}
	
	public void testExample(File file) throws Exception {
		TransformationGraph graph = null;
		Future<Result> futureResult = null;
			System.out.println("Analizing graph " + file.getName());
			try {
				graph = TransformationGraphXMLReaderWriter.loadGraph(new FileInputStream(file), runtimeContext.getAdditionalProperties());
				graph.setDebugMode(false);
			} catch (Exception e) {
				fail("Error in graph loading: " + e);
			}

			try {
				EngineInitializer.initGraph(graph, runtimeContext);
			} catch (ComponentNotReadyException e) {
				fail("Error in graph initialization: " + e);
			}

			for(int i = 0; i < 5; i++) {

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
					fail();
				}

				if (i < 4) {
					try {
						graph.reset();
					} catch (ComponentNotReadyException e) {
						fail("Graph reseting failed: " + e);
					}
				}
			}

			System.out.println("Transformation graph is freeing.\n");
			graph.free();
			
	}

	@Override
	protected void tearDown() throws Exception {
		super.tearDown();
		System.out.println("Graph executor is terminating.");
		for (String outDir : OUT_DIRS) {
			File outDirFile = new File(PROJECT_DIR + outDir);
			File[] file = outDirFile.listFiles();
			for (int i = 0; i < file.length; i++) {
				file[i].delete();
			}
		}
	}
	
	/**
	 * 1. param should be plugins location
	 * 2. param should be graph file location
	 * @param args
	 * @throws MalformedURLException
	 * @throws IOException
	 */
	public static void main(String[] args) throws Exception {
		
		// "../cloveretl.engine/plugins"
		EngineInitializer.initEngine(args[0], null, null);

		GraphRuntimeContext runtimeContext = new GraphRuntimeContext();
		runtimeContext.addAdditionalProperty("PROJECT_DIR", "examples/extExamples/");
		runtimeContext.setUseJMX(false);

		TransformationGraph graph = null;
		try {
			graph = TransformationGraphXMLReaderWriter.loadGraph(new FileInputStream(args[1]), runtimeContext.getAdditionalProperties());
		} catch (Exception e) {
			logger.error("Error in graph loading !", e);
			System.exit(-1);
		}

		try {
			EngineInitializer.initGraph(graph, runtimeContext);
		} catch (ComponentNotReadyException e) {
			logger.error("Error in graph initialization !", e);
			System.exit(-1);
		}

		for(int i = 0; i < 5; i++) {

			Future<Result> futureResult = null;
			try {
		        IThreadManager threadManager = new SimpleThreadManager();
		        WatchDog watchDog = new WatchDog(graph, runtimeContext);
				futureResult = threadManager.executeWatchDog(watchDog);
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

			if (i < 4) {
				try {
					graph.reset();
				} catch (ComponentNotReadyException e) {
					System.err.println("Graph reseting failed !");
					System.exit(-1);
				}
			}
		}

		System.out.println("Transformation graph is freeing.");
		graph.free();
		System.out.println("Graph executor is terminating.");
	}
}

