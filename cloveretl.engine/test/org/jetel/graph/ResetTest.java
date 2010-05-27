package org.jetel.graph;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Future;

import junit.framework.TestCase;
import junit.framework.TestResult;

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

	private final static String[] EXAMPLE_PATH = {
		"../cloveretl.examples/SimpleExamples/", 
		"../cloveretl.examples/AdvancedExamples/",
		"../cloveretl.examples/CTLFunctionsTutorial/",
		"../cloveretl.examples/DataProfiling/",
		"../cloveretl.examples/ExtExamples/",
		"../cloveretl.test.scenarios/",
		"../cloveretl.examples.commercial/",
		"../cloveretl.examples/CompanyTransactionsTutorial/"
		};
	
	static Log logger = LogFactory.getLog(ResetTest.class);
	private GraphRuntimeContext runtimeContext;

	private final static String GRAPHS_DIR = "graph";
	private final static String[] OUT_DIRS = {"data-out/", "data-tmp/", "seq/"};
	
	private final static String LOG_FILE_NAME = "reset.log";
	
	private FileWriter log_file = null;
	
	/**
	 * We suppose that this test is running from cloveretl.engine directory. 
	 * If not, this variable has to be change to point to the cloveretl.engine directory. 
	 */
	private final static File current_directory = new File(".");
	private final static String SCENARIOS_RELATIVE_PATH = "/../cloveretl.test.scenarios";
	
	private Map<String, Exception> errors = new HashMap<String, Exception>();

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		
		try {
			log_file = new FileWriter(LOG_FILE_NAME);
		} catch (Exception e) {
			System.err.println("Log file cannot be created:");
			e.printStackTrace();
		}
		
		EngineInitializer.initEngine( "..", null, null);
		
		runtimeContext = new GraphRuntimeContext();
		runtimeContext.addAdditionalProperty("PROJECT", ".");
		runtimeContext.addAdditionalProperty("CONN_DIR", current_directory.getAbsolutePath() + SCENARIOS_RELATIVE_PATH + "/conn");
		runtimeContext.setUseJMX(false);
		
		errors.clear();
	}
	
	public void testAllExamples(){
		for (int i = 0; i < EXAMPLE_PATH.length; i++) {
			File[] graphFile = (new File(EXAMPLE_PATH[i] + GRAPHS_DIR)).listFiles(new FileFilter() {
				public boolean accept(File pathname) {
					return pathname.getName().endsWith(".grf") 
//							&& !pathname.getName().endsWith("graphSimpleLookup.grf") // ok, uses lookup free in transform attribute
							&& !pathname.getName().startsWith("TPCH")// ok, performance tests - last very long
							&& !pathname.getName().contains("Performance")// ok, performance tests - last very long
							&& !pathname.getName().equals("graphJoinData.grf") // ok, uses class file that is not created
							&& !pathname.getName().equals("graphJoinHash.grf") // ok, uses class file that is not created
							&& !pathname.getName().equals("graphOrdersReformat.grf") // ok, uses class file that is not created
							&& !pathname.getName().equals("graphDataGeneratorExt.grf") // ok, uses class file that is not created
							&& !pathname.getName().equals("graphApproximativeJoin.grf") // ok, uses class file that is not created
							&& !pathname.getName().equals("graphDBJoin.grf") // ok, uses class file that is not created
							&& !pathname.getName().equals("conversionNum2num.grf") // ok, should fail
							&& !pathname.getName().equals("outPortWriting.grf") // ok, should fail
							&& !pathname.getName().equals("graphDb2Load.grf") // ok, can only work with db2 client
							&& !pathname.getName().equals("graphMsSqlDataWriter.grf") // ok, can only work with MsSql client
							&& !pathname.getName().equals("graphMysqlDataWriter.grf") // ok, can only work with MySql client
							&& !pathname.getName().equals("graphOracleDataWriter.grf") // ok, can only work with Oracle client
							&& !pathname.getName().equals("graphInformixDataWriter.grf") // ok, can only work with informix server
							&& !pathname.getName().equals("graphInfobrightDataWriter.grf") // ok, can only work with infobright server
							&& !pathname.getName().equals("graphSystemExecuteWin.grf") // ok, graph for Windows
							&& !pathname.getName().equals("graphLdapReader_Uninett.grf") // ok, invalid server
							&& !pathname.getName().equals("graphSequenceChecker.grf"); // ok, is to fail
							
//TODO these graphs should work in the future:
//							&& !pathname.getName().startsWith("graphLdap") //LDAP server is not configured properly yet
//							&& !pathname.getName().equals("mountainsSybase.grf") //issue 2939
//							&& !pathname.getName().equals("graphJms.grf") //issue 3250
//							&& !pathname.getName().equals("graphGenerateData.grf") //issue 3220
//							&& !pathname.getName().equals("graphJavaExecute.grf") //issue 3220
//							&& !pathname.getName().equals("dateToday.grf") //issue 3220
//							&& !pathname.getName().equals("mathRandom.grf") //issue 3220
//							&& !pathname.getName().equals("mathRandom_boolean.grf") //issue 3220
//							&& !pathname.getName().equals("mathRandom_gaussian.grf") //issue 3220
//							&& !pathname.getName().equals("mathRandom_intWithRange.grf") //issue 3220
//							&& !pathname.getName().equals("mathRandom_intWithoutRange.grf") //issue 3220
//							&& !pathname.getName().equals("mathRandom_longWithoutRange.grf") //issue 3220
//							&& !pathname.getName().equals("graphCheckForeignKey.grf") //issue 3220
//							&& !pathname.getName().equals("graphDBExecuteMySql.grf") //issue 3220
//							&& !pathname.getName().equals("graphDBExecuteMsSql.grf") //issue 3220
//							&& !pathname.getName().equals("graphDBExecuteOracle.grf") //issue 3220
//							&& !pathname.getName().equals("graphDBExecutePostgre.grf") //issue 3220
//							&& !pathname.getName().equals("graphDBUnload.grf") //issue 3220
//							&& !pathname.getName().equals("graphDBUnload2.grf") //issue 3220
//							&& !pathname.getName().equals("graphDBLoad5.grf") //issue 3220
//							&& !pathname.getName().equals("graphDBUnloadUniversal.grf") //issue 3220
//							&& !pathname.getName().equals("bufferedEdge1.grf") //issue 3220
//							&& !pathname.getName().equals("bufferedEdge2.grf") //issue 3220
//							&& !pathname.getName().equals("incrementalReadingDB.grf") //issue 3220
//							&& !pathname.getName().equals("informix.grf") //issue 3220
//							&& !pathname.getName().equals("parallelReaderFunctionalTest.grf") //issue 3220
//							&& !pathname.getName().equals("sort.grf") //issue 3220
//							&& !pathname.getName().equals("transformations.grf") //issue 3220
//							&& !pathname.getName().equals("mountainsPgsql.grf") //issue 3220
//							&& !pathname.getName().equals("A12_XMLExtractTransactionsFamily.grf") //issue 3220
//							&& !pathname.getName().equals("graphXMLExtract.grf") //issue 3220
//							&& !pathname.getName().equals("graphXMLExtractXsd.grf") //issue 3220
//							&& !pathname.getName().equals("mountainsInformix.grf") //issue 2550
//							&& !pathname.getName().equals("graphRunGraph.grf") 
//							&& !pathname.getName().equals("DBJoin.grf");//issue 3285
							
				}
			});
			
			log("Testing graphs in " + EXAMPLE_PATH[i]);
			
			Arrays.sort(graphFile);
			runtimeContext.addAdditionalProperty("PROJECT_DIR", EXAMPLE_PATH[i]);
			// absolute path in PROJECT parameter is required for graphs using Derby database
			runtimeContext.addAdditionalProperty("PROJECT", new File(EXAMPLE_PATH[i]).getAbsolutePath());
			for (int j = 0; j < graphFile.length; j++) {
				if (!graphFile[j].getName().contains("Jms")) {//set LIB_DIR to jdbc drivers directory
					runtimeContext.addAdditionalProperty("LIB_DIR", current_directory.getAbsolutePath() + SCENARIOS_RELATIVE_PATH + "/lib");
				}
				try {
					testExample(graphFile[j]);
//				} catch (AssertionFailedError e) {
//					fail(graphFile[j] + ": " + e.getMessage());
				} catch (Exception e) {
//					if (e.getMessage() == null){
//						e.printStackTrace();
//					}
//					fail(graphFile[j] + ": " + e.getMessage());
					errors.put(graphFile[j].getName(), e);
				}
			}
		}
		
		
		if (!errors.isEmpty()) {
			for (Entry<String, Exception> error : errors.entrySet()) {
				System.err.println(error.getKey() + ": ");
				error.getValue().printStackTrace();
			}
		}
	}
	
	@Override
	public void run(TestResult result) {
		super.run(result);
		if (!errors.isEmpty()) {
			Exception e;
			for (Entry<String, Exception> error : errors.entrySet()) {
				e = new RuntimeException(error.getKey() + " failed.", error.getValue());
				result.addError(this, e);
			}
		}
	}
	
	public void testExample(File file) throws Exception {
		TransformationGraph graph = null;
		Future<Result> futureResult = null;
		log("Analizing graph " + file.getName());
		try {
				graph = TransformationGraphXMLReaderWriter.loadGraph(new FileInputStream(file), runtimeContext.getAdditionalProperties());
				graph.setDebugMode(false);
			} catch (Exception e) {
				log("Error in graph loading: " + e.getMessage());
				errors.put(file.getName(), e);
				return;
			}

			try {
				EngineInitializer.initGraph(graph, runtimeContext);
			} catch (ComponentNotReadyException e) {
				log("Error in graph initialization: " + e.getMessage());
				errors.put(file.getName(), e);
				return;
			}

			for(int i = 0; i < 3; i++) {

				try {
			        IThreadManager threadManager = new SimpleThreadManager();
			        WatchDog watchDog = new WatchDog(graph, runtimeContext);
					futureResult = threadManager.executeWatchDog(watchDog);
				} catch (Exception e) {
					log("Error in graph execution: " + e.getMessage());
					errors.put(file.getName(), e);
					return;
				}

				Result result = Result.N_A;
				try {
					result = futureResult.get();
				} catch (Exception e) {
					log("Error during graph processing: " + e.getMessage());
					errors.put(file.getName(), e);
					return;
				}

				switch (result) {
				case FINISHED_OK:
					// everything O.K.
					log("Execution of graph successful !");
					break;
				case ABORTED:
					// execution was ABORTED !!
					log("Execution of graph aborted !");
					System.exit(result.code());
					break;
				default:
					log("Execution of graph failed !");
//					fail();
					errors.put(file.getName(), new RuntimeException("Execution of graph failed !"));
					return;
				}

				if (i < 2) {
					try {
						graph.reset();
					} catch (ComponentNotReadyException e) {
						log("Graph reseting failed: " + e.getMessage());
						errors.put(file.getName(), e);
						return;
					}
				}
			}

			log("Transformation graph is freeing.\n");
			graph.free();
			
	}

	@Override
	protected void tearDown() throws Exception {
		super.tearDown();
		log_file.close();
		for (int j = 0; j < EXAMPLE_PATH.length; j++) {
			for (String outDir : OUT_DIRS) {
				File outDirFile = new File(EXAMPLE_PATH[j] + outDir);
				File[] file = outDirFile.listFiles();
				for (int i = 0; i < file.length; i++) {
					file[i].delete();
				}
			}
		}
	}
	
	/**
	 * 1. param should be plugins location
	 * 2. param should be graph file location
	 * 3. param (optional) should be project directory, if not exist simpleExamples project is used
	 * @param args
	 * @throws MalformedURLException
	 * @throws IOException
	 */
	public static void main(String[] args) throws Exception {
		
		// ".."
		EngineInitializer.initEngine(args[0], null, null);

		GraphRuntimeContext runtimeContext = new GraphRuntimeContext();
		runtimeContext.addAdditionalProperty("PROJECT_DIR", args.length > 2 ? args[2] : "../cloveretl.examples/SimpleExamples");
		runtimeContext.setUseJMX(false);

		TransformationGraph graph = null;
		try {
			graph = TransformationGraphXMLReaderWriter.loadGraph(new FileInputStream((args.length > 2 ? args[2] + "/" : "") + args[1]), 
					runtimeContext.getAdditionalProperties());
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

//			if (i < 4) {
//				try {
//					graph.reset();
//				} catch (ComponentNotReadyException e) {
//					System.err.println("Graph reseting failed !");
//					e.printStackTrace();
//					System.exit(-1);
//				}
//			}
		}

		System.out.println("Transformation graph is freeing.");
		graph.free();
		System.out.println("Graph executor is terminating.");
		
		
	}
	
	private void log(String message){
		System.out.println(message);
		if (log_file != null) {
			try {
				log_file.write(message);
				log_file.flush();
			} catch (IOException e) {
				System.err.println("Can't write to log file");
				e.printStackTrace();
			}
		}
	}
}

