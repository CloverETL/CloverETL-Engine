package org.jetel.graph;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.util.Arrays;
import java.util.concurrent.Future;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.graph.runtime.EngineInitializer;
import org.jetel.graph.runtime.GraphRuntimeContext;
import org.jetel.graph.runtime.IThreadManager;
import org.jetel.graph.runtime.SimpleThreadManager;
import org.jetel.graph.runtime.WatchDog;
import org.jetel.test.CloverTestCase;
import org.jetel.util.file.FileUtils;

/*
 * Copyright (c) 2004-2005 Javlin Consulting s.r.o. All rights reserved.
 * 
 */
public class ResetTest extends CloverTestCase {

	private final static String SCENARIOS_RELATIVE_PATH = "../../cloveretl.test.scenarios/";
	private final static String[] EXAMPLE_PATH = {
			"../cloveretl.examples/SimpleExamples/",
			"../cloveretl.examples/AdvancedExamples/",
			"../cloveretl.examples/CTL1FunctionsTutorial/",
			"../cloveretl.examples/CTL2FunctionsTutorial/",
			"../cloveretl.examples/DataProfiling/",
			"../cloveretl.examples/ExtExamples/",
			"../cloveretl.test.scenarios/",
			"../cloveretl.examples.commercial/",
			"../cloveretl.examples/CompanyTransactionsTutorial/"
		};
	

	private final static String GRAPHS_DIR = "graph";
	private final static String[] OUT_DIRS = {"data-out/", "data-tmp/", "seq/"};
	
	private final String basePath;
	private final File graphFile;
	private final boolean batchMode;
	
	private boolean cleanUp = true;
	
	private static Log logger = LogFactory.getLog(ResetTest.class);
	
	public static Test suite() {

        final TestSuite suite = new TestSuite();
		
		for (int i = 0; i < EXAMPLE_PATH.length; i++) {
			logger.info("Testing graphs in " + EXAMPLE_PATH[i]);
			final File graphsDir =  new File(EXAMPLE_PATH[i], GRAPHS_DIR);
			if(!graphsDir.exists()){
				throw new IllegalStateException("Graphs directory " + graphsDir.getAbsolutePath() +" not found");
			}
			File[] graphFiles =graphsDir.listFiles(new FileFilter() {
				public boolean accept(File pathname) {
					return pathname.getName().endsWith(".grf") 
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
							&& !pathname.getName().equals("graphPostgreSqlDataWriter.grf") // ok, can only work with postgre client
							&& !pathname.getName().equals("graphInformixDataWriter.grf") // ok, can only work with informix server
							&& !pathname.getName().equals("graphInfobrightDataWriter.grf") // ok, can only work with infobright server
							&& !pathname.getName().equals("graphSystemExecuteWin.grf") // ok, graph for Windows
							&& !pathname.getName().equals("graphLdapReader_Uninett.grf") // ok, invalid server
							&& !pathname.getName().equals("graphSequenceChecker.grf") // ok, is to fail
							&& !pathname.getName().equals("FixedData.grf") // ok, is to fail
							&& !pathname.getName().equals("xpathReaderStates.grf") // ok, is to fail
							&& !pathname.getName().equals("graphDataPolicy.grf") // ok, is to fail
							&& !pathname.getName().equals("conversionDecimal2integer.grf") // ok, is to fail
							&& !pathname.getName().equals("conversionDecimal2long.grf") // ok, is to fail
							&& !pathname.getName().equals("conversionDouble2integer.grf") // ok, is to fail							
							&& !pathname.getName().equals("conversionDouble2long.grf") // ok, is to fail
							&& !pathname.getName().equals("conversionLong2integer.grf"); // ok, is to fail
							
//TODO these graphs should work in the future:
//								&& !pathname.getName().startsWith("graphLdap") //LDAP server is not configured properly yet
//								&& !pathname.getName().equals("mountainsSybase.grf") //issue 2939
//								&& !pathname.getName().equals("graphJms.grf") //issue 3250
//								&& !pathname.getName().equals("graphGenerateData.grf") //issue 3220
//								&& !pathname.getName().equals("graphJavaExecute.grf") //issue 3220
//								&& !pathname.getName().equals("dateToday.grf") //issue 3220
//								&& !pathname.getName().equals("mathRandom.grf") //issue 3220
//								&& !pathname.getName().equals("mathRandom_boolean.grf") //issue 3220
//								&& !pathname.getName().equals("mathRandom_gaussian.grf") //issue 3220
//								&& !pathname.getName().equals("mathRandom_intWithRange.grf") //issue 3220
//								&& !pathname.getName().equals("mathRandom_intWithoutRange.grf") //issue 3220
//								&& !pathname.getName().equals("mathRandom_longWithoutRange.grf") //issue 3220
//								&& !pathname.getName().equals("graphCheckForeignKey.grf") //issue 3220
//								&& !pathname.getName().equals("graphDBExecuteMySql.grf") //issue 3220
//								&& !pathname.getName().equals("graphDBExecuteMsSql.grf") //issue 3220
//								&& !pathname.getName().equals("graphDBExecuteOracle.grf") //issue 3220
//								&& !pathname.getName().equals("graphDBExecutePostgre.grf") //issue 3220
//								&& !pathname.getName().equals("graphDBUnload.grf") //issue 3220
//								&& !pathname.getName().equals("graphDBUnload2.grf") //issue 3220
//								&& !pathname.getName().equals("graphDBLoad5.grf") //issue 3220
//								&& !pathname.getName().equals("graphDBUnloadUniversal.grf") //issue 3220
//								&& !pathname.getName().equals("bufferedEdge1.grf") //issue 3220
//								&& !pathname.getName().equals("bufferedEdge2.grf") //issue 3220
//								&& !pathname.getName().equals("incrementalReadingDB.grf") //issue 3220
//								&& !pathname.getName().equals("informix.grf") //issue 3220
//								&& !pathname.getName().equals("parallelReaderFunctionalTest.grf") //issue 3220
//								&& !pathname.getName().equals("sort.grf") //issue 3220
//								&& !pathname.getName().equals("transformations.grf") //issue 3220
//								&& !pathname.getName().equals("mountainsPgsql.grf") //issue 3220
//								&& !pathname.getName().equals("A12_XMLExtractTransactionsFamily.grf") //issue 3220
//								&& !pathname.getName().equals("graphXMLExtract.grf") //issue 3220
//								&& !pathname.getName().equals("graphXMLExtractXsd.grf") //issue 3220
//								&& !pathname.getName().equals("mountainsInformix.grf") //issue 2550
//								&& !pathname.getName().equals("graphRunGraph.grf") 
//								&& !pathname.getName().equals("DBJoin.grf");//issue 3285
							
				}
			});
			
			Arrays.sort(graphFiles);
			
			for( int j = 0; j < graphFiles.length; j++){
				suite.addTest(new ResetTest(EXAMPLE_PATH[i], graphFiles[j], false, false));
				suite.addTest(new ResetTest(EXAMPLE_PATH[i], graphFiles[j], true, j == graphFiles.length - 1 ? true : false));
			}

		}
			
		return suite;
	 }

	
	@Override
	protected void setUp() throws Exception {
		super.setUp();
		initEngine("test/org/jetel/graph/ResetTestDefault.properties");
	}

	protected static String getTestName(String basePath, File graphFile, boolean batchMode) {
		final StringBuilder ret = new StringBuilder();
		final String n = graphFile.getName();
		int lastDot = n.lastIndexOf('.');
		if (lastDot == -1) {
			ret.append(n);
		} else {
			ret.append(n.substring(0, lastDot));
		}

		if (batchMode) {
			ret.append("-batch");
		} else {
			ret.append("-nobatch");
		}

		return ret.toString();
	}
	 
	protected ResetTest(String basePath, File graphFile, boolean batchMode, boolean cleanup) {
		super(getTestName(basePath, graphFile, batchMode));
		this.basePath = basePath;
		this.graphFile = graphFile;
		this.batchMode = batchMode;
		this.cleanUp = cleanup;
	}
	 
	@Override
	protected void runTest() throws Throwable {
		
		final String beseAbsolutePath = new File(basePath).getAbsolutePath();
		logger.info("Project dir: " + beseAbsolutePath);
		logger.info("Analyzing graph " + graphFile.getPath());
		logger.info("Batch mode: " + batchMode);
		
		final GraphRuntimeContext runtimeContext = new GraphRuntimeContext();
		runtimeContext.setUseJMX(false);
	
		runtimeContext.setContextURL(FileUtils.getFileURL(basePath));
		// absolute path in PROJECT parameter is required for graphs using Derby database
		runtimeContext.addAdditionalProperty("PROJECT", beseAbsolutePath);
		if (!basePath.equals("../cloveretl.test.scenarios/")) {
			runtimeContext.addAdditionalProperty("CONN_DIR", SCENARIOS_RELATIVE_PATH + "/conn");
		}
		if (!graphFile.getName().contains("Jms")) {// set LIB_DIR to jdbc drivers directory
			runtimeContext.addAdditionalProperty("LIB_DIR", SCENARIOS_RELATIVE_PATH + "/lib");
		}

		runtimeContext.setBatchMode(batchMode);
		
		final TransformationGraph graph = TransformationGraphXMLReaderWriter.loadGraph(new FileInputStream(graphFile), runtimeContext);
		try {
			
			graph.setDebugMode(false);

			EngineInitializer.initGraph(graph);

			for (int i = 0; i < 3; i++) {

				final IThreadManager threadManager = new SimpleThreadManager();
				final WatchDog watchDog = new WatchDog(graph, runtimeContext);
				final Future<Result> futureResult = threadManager.executeWatchDog(watchDog);

				Result result = Result.N_A;
				result = futureResult.get();

				switch (result) {
				case FINISHED_OK:
					// everything O.K.
					logger.info("Execution of graph successful !");
					break;
				case ABORTED:
					// execution was ABORTED !!
					logger.info("Execution of graph failed !");
					fail("Execution of graph failed !");
					break;
				default:
					logger.info("Execution of graph failed !");
					fail("Execution of graph failed !");
				}
			}

		} catch (Throwable e) {
			throw new IllegalStateException("Error executing grap " + graphFile);
		} finally {
			if (cleanUp) {
				cleanupData();
			}
			logger.info("Transformation graph is freeing.\n");
			graph.free();
		}
	}


	private void cleanupData() {
		for (String outDir : OUT_DIRS) {
			File outDirFile = new File(basePath, outDir);
			File[] file = outDirFile.listFiles(new FileFilter() {
				
				@Override
				public boolean accept(File f) {
					return f.isFile();
				}
			});
			for (int i = 0; i < file.length; i++) {
				final boolean drt = file[i].delete();
				if (drt) {
					logger.info("Cleanup: deleted file " + file[i].getAbsolutePath());
				} else {
					logger.info("Cleanup: error delete file " + file[i].getAbsolutePath());
				}
			}
		}
	}
	
}

