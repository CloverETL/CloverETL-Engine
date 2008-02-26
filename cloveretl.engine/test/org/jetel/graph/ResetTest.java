package org.jetel.graph;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.Future;

import junit.framework.AssertionFailedError;
import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.runtime.EngineInitializer;
import org.jetel.graph.runtime.GraphExecutor;
import org.jetel.graph.runtime.GraphRuntimeContext;


/*
 * Copyright (c) 2004-2005 Javlin Consulting s.r.o. All rights reserved.
 * 
 */

public class ResetTest extends TestCase{

	private final static String EXAMPLE_PATH = "./examples";
	
	static Log logger = LogFactory.getLog(ResetTest.class);
	private GraphRuntimeContext runtimeContext;
	private GraphExecutor graphExecutor;
	private Properties graphProperties = new Properties();

	private HashMap<File, Error> fails;

	private HashMap<File, Exception> errors;
	
	@Override
	protected void setUp() throws Exception {
		super.setUp();
		EngineInitializer.initEngine( "../plugins", null, null);

		runtimeContext = new GraphRuntimeContext();
		runtimeContext.addAdditionalProperty("PROJECT_DIR", EXAMPLE_PATH); 
		runtimeContext.setUseJMX(true);

		graphExecutor = new GraphExecutor();
		graphProperties.put(TransformationGraph.PROJECT_DIR_PROPERTY, EXAMPLE_PATH);
	}
	
	public void testAllExamples() throws Exception {
		File[] graphFile = (new File(".")).listFiles(new FileFilter(){

			public boolean accept(File pathname) {
				return pathname.getName().endsWith(".grf") && !pathname.getName().equals("graphXMLExtract.grf")
				&& !pathname.getName().equals("graphXPathReader.grf");
			}
			
		});
		
		fails = new HashMap<File, Error>();
		errors = new HashMap<File, Exception>();
		
		for (int i = 0; i < graphFile.length; i++) {
			try {
				testExample(graphFile[i]);
			} catch (AssertionFailedError e) {
				fails.put(graphFile[i], e);
			}catch (Exception e) {
				errors.put(graphFile[i], e);
			}
		}
		
		System.out.println("\nFails:\n");
		for (File file : fails.keySet()) {
			System.out.println(file);
		}
	}
	
	public void testFails() throws Exception {
		BufferedReader in = new BufferedReader(new FileReader("../test/org/jetel/graph/fails.txt"));
		ArrayList<File> fileList = new ArrayList<File>();
		String line;
		while( (line = in.readLine()) != null){
			fileList.add(new File(line));
		}
		
		for (File file : fileList) {
			try {
				testExample(file);
			} catch (AssertionFailedError e) {
				System.err.println(file.toString() + e);
			}catch(Exception e){
				
			}
		}
	}
	
	public void testExample(File file) throws Exception {
		TransformationGraph graph = null;
		Future<Result> futureResult = null;
			System.out.println("Analizing graph " + file.getName());
			try {
				graph = GraphExecutor.loadGraph(new FileInputStream(file), graphProperties);
			} catch (Exception e) {
				fail("Error in graph loading: " + e);
			}

			try {
				graphExecutor.initGraph(graph);
			} catch (ComponentNotReadyException e) {
				fail("Error in graph initialization: " + e);
			}

			for(int i = 0; i < 5; i++) {

				try {
					futureResult = graphExecutor.runGraph(graph, runtimeContext);
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
		graphExecutor.free();
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
		graphExecutor.free();
	}
}

