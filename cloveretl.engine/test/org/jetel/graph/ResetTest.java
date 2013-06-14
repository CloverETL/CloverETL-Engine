package org.jetel.graph;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.Future;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.commons.io.filefilter.AbstractFileFilter;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.graph.runtime.EngineInitializer;
import org.jetel.graph.runtime.GraphRuntimeContext;
import org.jetel.main.runGraph;
import org.jetel.test.CloverTestCase;
import org.jetel.util.file.FileUtils;
import org.jetel.util.string.StringUtils;

/*
 * Copyright (c) 2004-2005 Javlin Consulting s.r.o. All rights reserved.
 * 
 */
public class ResetTest extends CloverTestCase {

	private final static String SCENARIOS_RELATIVE_PATH = "../cloveretl.test.scenarios/";
	private final static String[] EXAMPLE_PATH = {
			"../cloveretl.examples/SimpleExamples/",
			"../cloveretl.examples/AdvancedExamples/",
			"../cloveretl.examples/CTL1FunctionsTutorial/",
			"../cloveretl.examples/CTL2FunctionsTutorial/",
			"../cloveretl.examples/DataProfiling/",
			"../cloveretl.examples/DataSampling/",
			"../cloveretl.examples/ExtExamples/",
			"../cloveretl.examples/RealWorldExamples/",
			"../cloveretl.examples.community/RealWorldExamples/",
			"../cloveretl.examples/WebSiteExamples/",
			"../cloveretl.examples.community/WebSiteExamples/",
			"../cloveretl.test.scenarios/",
			"../cloveretl.examples.commercial/",
			"../cloveretl.examples/CompanyTransactionsTutorial/"
		};
	
	private final static String[] NEEDS_SCENARIOS_CONNECTION = {
		"graphRevenues.grf",
		"graphDBExecuteMsSql.grf",
		"graphDBExecuteMySql.grf",
		"graphDBExecuteOracle.grf",
		"graphDBExecutePostgre.grf",
		"graphDBExecuteSybase.grf",
		"graphInfobrightDataWriterRemote.grf",
		"graphLdapReaderWriter.grf"
	};
	
	private final static String[] NEEDS_SCENARIOS_LIB = {
		"graphDBExecuteOracle.grf",
		"graphDBExecuteSybase.grf",
		"graphLdapReaderWriter.grf"
	};
		
	private final static String GRAPHS_DIR = "graph";
	private final static String TRANS_DIR = "trans";
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
			IOFileFilter fileFilter = new AbstractFileFilter() {
				@Override
				public boolean accept(File file) {
					return file.getName().endsWith(".grf") 
							&& !file.getName().startsWith("TPCH")// ok, performance tests - last very long
							&& !file.getName().contains("Performance")// ok, performance tests - last very long
							&& !file.getName().equals("graphJoinData.grf") // ok, uses class file that is not created
							&& !file.getName().equals("graphJoinHash.grf") // ok, uses class file that is not created
							&& !file.getName().equals("graphOrdersReformat.grf") // ok, uses class file that is not created
							&& !file.getName().equals("graphDataGeneratorExt.grf") // ok, uses class file that is not created
							&& !file.getName().equals("graphApproximativeJoin.grf") // ok, uses class file that is not created
							&& !file.getName().equals("graphDBJoin.grf") // ok, uses class file that is not created
							&& !file.getName().equals("conversionNum2num.grf") // ok, should fail
							&& !file.getName().equals("outPortWriting.grf") // ok, should fail
							&& !file.getName().equals("graphDb2Load.grf") // ok, can only work with db2 client
							&& !file.getName().equals("graphMsSqlDataWriter.grf") // ok, can only work with MsSql client
							&& !file.getName().equals("graphMysqlDataWriter.grf") // ok, can only work with MySql client
							&& !file.getName().equals("graphOracleDataWriter.grf") // ok, can only work with Oracle client
							&& !file.getName().equals("graphPostgreSqlDataWriter.grf") // ok, can only work with postgre client
							&& !file.getName().equals("graphInformixDataWriter.grf") // ok, can only work with informix server
							&& !file.getName().equals("graphInfobrightDataWriter.grf") // ok, can only work with infobright server
							&& !file.getName().equals("graphSystemExecuteWin.grf") // ok, graph for Windows
							&& !file.getName().equals("graphLdapReader_Uninett.grf") // ok, invalid server
							&& !file.getName().equals("graphSequenceChecker.grf") // ok, is to fail
							&& !file.getName().equals("FixedData.grf") // ok, is to fail
							&& !file.getName().equals("xpathReaderStates.grf") // ok, is to fail
							&& !file.getName().equals("graphDataPolicy.grf") // ok, is to fail
							&& !file.getName().equals("conversionDecimal2integer.grf") // ok, is to fail
							&& !file.getName().equals("conversionDecimal2long.grf") // ok, is to fail
							&& !file.getName().equals("conversionDouble2integer.grf") // ok, is to fail							
							&& !file.getName().equals("conversionDouble2long.grf") // ok, is to fail
							&& !file.getName().equals("conversionLong2integer.grf") // ok, is to fail
							&& !file.getName().equals("nativeSortTestGraph.grf") // ok, invalid paths
							&& !file.getName().equals("mountainsInformix.grf") // see issue 2550							
							&& !file.getName().equals("SystemExecuteWin_EchoFromFile.grf") // graph for windows
							&& !file.getName().equals("XLSEncryptedFail.grf") // ok, is to fail
							&& !file.getName().equals("XLSXEncryptedFail.grf") // ok, is to fail
							&& !file.getName().equals("XLSInvalidFile.grf") // ok, is to fail
							&& !file.getName().equals("XLSReaderOrderMappingFail.grf") // ok, is to fail
							&& !file.getName().equals("XLSXReaderOrderMappingFail.grf") // ok, is to fail
							&& !file.getName().equals("XLSWildcardStrict.grf") // ok, is to fail
							&& !file.getName().equals("XLSXWildcardStrict.grf") // ok, is to fail
							&& !file.getName().equals("XLSWildcardControlled1.grf") // ok, is to fail
							&& !file.getName().equals("XLSXWildcardControlled1.grf") // ok, is to fail
							&& !file.getName().equals("XLSWildcardControlled7.grf") // ok, is to fail
							&& !file.getName().equals("XLSXWildcardControlled7.grf") // ok, is to fail
							&& !file.getName().equals("SSWRITER_MultilineInsertIntoTemplate.grf") // uses graph parameter definition from after-commit.ts
							&& !file.getName().equals("SSWRITER_FormatInMetadata.grf") // uses graph parameter definition from after-commit.ts
							&& !file.getName().equals("WSC_NamespaceBindingsDefined.grf") // ok, is to fail
							&& !file.getName().equals("FailingGraph.grf") // ok, is to fail
							&& !file.getName().equals("RunGraph_FailWhenUnderlyingGraphFails.grf") // probably should fail, recheck after added to after-commit.ts
							&& !file.getName().equals("DataIntersection_order_check_A.grf") // ok, is to fail
							&& !file.getName().equals("DataIntersection_order_check_B.grf") // ok, is to fail
							&& !file.getName().equals("UDR_Logging_SFTP_CL1469.grf") // ok, is to fail
							&& !file.getName().startsWith("AddressDoctor") //wrong path to db file, try to fix when AD installed on jenkins machines 
							&& !file.getName().equals("EmailReader_Local.grf") // remove after CL-2167 solved
							&& !file.getName().equals("EmailReader_Server.grf") // remove after CLD-3437 solved (or mail.javlin.eu has valid certificate)
							&& !file.getName().contains("firebird") // remove after CL-2170 solved
							&& !file.getName().startsWith("ListOfRecords_Functions_02_") // remove after CL-2173 solved
							&& !file.getName().equals("UDR_FileURL_OneZipMultipleFilesUnspecified.grf") // remove after CL-2174 solved
							&& !file.getName().equals("UDR_FileURL_OneZipOneFileUnspecified.grf") // remove after CL-2174 solved
							&& !file.getName().startsWith("MapOfRecords_Functions_01_Compiled_") // remove after CL-2175 solved
							&& !file.getName().startsWith("MapOfRecords_Functions_01_Interpreted_") // remove after CL-2176 solved
							&& !file.getName().equals("manyRecords.grf") // remove after CL-1292 implemented
							&& !file.getName().equals("packedDecimal.grf") // remove after CL-1811 solved
							&& !file.getName().equals("SimpleZipWrite.grf") // used by ArchiveFlushTest.java, doesn't make sense to run it separately
							&& !file.getName().equals("XMLExtract_TKLK_003_Back.grf") // needs output from XMLWriter_LKTW_003.grf
							&& !file.getName().equals("SQLDataParser_precision_CL2187.grf") // ok, is to fail
							&& !file.getName().equals("incrementalReadingDB_explicitMapping.grf") // remove after CL-2239 solved
							&& !file.getName().equals("HTTPConnector_get_bodyparams.grf") // ok, is to fail
							&& !file.getName().equals("HTTPConnector_get_error_unknownhost.grf") // ok, is to fail
							&& !file.getName().equals("HTTPConnector_get_error_unknownprotocol.grf") // ok, is to fail
							&& !file.getName().equals("HTTPConnector_get_inputfield.grf") // ok, is to fail
							&& !file.getName().equals("HTTPConnector_get_inputfileURL.grf") // ok, is to fail
							&& !file.getName().equals("HTTPConnector_get_requestcontent.grf") // ok, is to fail
							&& !file.getName().equals("HTTPConnector_post_error_unknownhost.grf") // ok, is to fail
							&& !file.getName().equals("HTTPConnector_post_error_unknownprotocol.grf") // ok, is to fail
							&& !file.getName().equals("HTTPConnector_inputmapping_null_values.grf") // ok, is to fail
							&& !file.getName().equals("HttpConnector_errHandlingNoRedir.grf") // ok, is to fail
							&& !file.getName().equals("XMLExtract_fileURL_not_xml.grf") // ok, is to fail
							&& !file.getName().equals("XMLExtract_charset_invalid.grf") // ok, is to fail
							&& !file.getName().equals("XMLExtract_mappingURL_missing.grf") // ok, is to fail
							&& !file.getName().equals("XMLExtract_fileURL_not_exists.grf") // ok, is to fail
							&& !file.getName().equals("XMLExtract_charset_not_default_fail.grf") // ok, is to fail
							&& !file.getName().equals("RunGraph_differentOutputMetadataFail.grf") // ok, is to fail
							&& !file.getName().equals("LUTPersistent_wrong_metadata.grf") // ok, is to fail
							&& !file.getName().equals("UDW_nonExistingDir_fail_CL-2478.grf") // ok, is to fail
							&& !file.getName().equals("CTL_lookup_put_fail.grf") // ok, is to fail
							&& !file.getName().equals("SystemExecute_printBatchFile.grf") // ok, is to fail
							&& !file.getName().equals("JoinMergeIssue_FailWhenMasterUnsorted.grf") // ok, is to fail
							&& !file.getName().equals("UDW_remoteZipPartitioning_fail_CL-2564.grf") // ok, is to fail
							&& !file.getName().equals("checkConfigTest.grf") // ok, is to fail
							&& !file.getName().equals("DebuggingGraph.grf") // ok, is to fail
							&& !file.getName().equals("graphDebuggingGraph.grf") // ok, is to fail
							&& !file.getName().equals("CompanyChecks.grf") // an example that needs embedded derby
							&& !file.getName().equals("DatabaseAccess.grf") // an example that needs embedded derby
							&& !file.getName().equals("graphDatabaseAccess.grf") // an example that needs embedded derby
							&& !file.getName().startsWith("Proxy_") // allowed to run only on virt-cyan as proxy tests
							&& !file.getName().equals("SandboxOperationHandlerTest.grf") // runs only on server
							&& !file.getName().equals("DenormalizerWithoutInputFile.grf") // probably subgraph not supposed to be executed separately
							&& !file.getName().equals("SimpleSequence_longValue.grf") // needs the sequence to be reset on start
							&& !file.getName().equals("DBLookupTable_negativeResponse_noCache.grf") // remove after CLO-715 solved
							&& !file.getName().equals("BeanWriterReader_employees.grf") // remove after CL-2474 solved
							&& !file.getName().equals("EmptyGraph.grf"); // ok, is to fail
					
				}
			};
			
			IOFileFilter dirFilter = new AbstractFileFilter() {
				@Override
				public boolean accept(File file) {
					return file.isDirectory() && file.getName().equals("hadoop");
				}
			};
			
			@SuppressWarnings("unchecked")
			Collection<File> filesCollection = org.apache.commons.io.FileUtils.listFiles(graphsDir, fileFilter, dirFilter);
			File[] graphFiles = filesCollection.toArray(new File[0]);			
			Arrays.sort(graphFiles);
			
			for(int j = 0; j < graphFiles.length; j++){
				suite.addTest(new ResetTest(EXAMPLE_PATH[i], graphFiles[j], false, false));
				suite.addTest(new ResetTest(EXAMPLE_PATH[i], graphFiles[j], true, j == graphFiles.length - 1 ? true : false));
			}

		}
			
		return suite;
	 }

	
	@Override
	protected void setUp() throws Exception {
		super.setUp();
		initEngine();
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
		
		final String baseAbsolutePath = new File(basePath).getAbsolutePath().replace('\\', '/');
		logger.info("Project dir: " + baseAbsolutePath);
		logger.info("Analyzing graph " + graphFile.getPath());
		logger.info("Batch mode: " + batchMode);
		
		final GraphRuntimeContext runtimeContext = new GraphRuntimeContext();
		runtimeContext.setUseJMX(false);
	
		runtimeContext.setContextURL(FileUtils.getFileURL(FileUtils.appendSlash(baseAbsolutePath))); // context URL should be absolute
		// absolute path in PROJECT parameter is required for graphs using Derby database
		runtimeContext.addAdditionalProperty("PROJECT", baseAbsolutePath);
		if (StringUtils.findString(graphFile.getName(), NEEDS_SCENARIOS_CONNECTION) != -1) {
			final String connDir = new File(SCENARIOS_RELATIVE_PATH + "conn").getAbsolutePath();
			runtimeContext.addAdditionalProperty("CONN_DIR", connDir);
			logger.info("CONN_DIR set to " + connDir);
		}
		if (StringUtils.findString(graphFile.getName(), NEEDS_SCENARIOS_LIB) != -1) {// set LIB_DIR to jdbc drivers directory
			final String libDir = new File(SCENARIOS_RELATIVE_PATH + "lib").getAbsolutePath();
			runtimeContext.addAdditionalProperty("LIB_DIR", libDir);
			logger.info("LIB_DIR set to " + libDir);
		}
		
		// for scenarios graphs, add the TRANS dir to the classpath
		if (basePath.contains("cloveretl.test.scenarios")) {
			runtimeContext.setRuntimeClassPath(new URL[] {FileUtils.getFileURL(FileUtils.appendSlash(baseAbsolutePath) + TRANS_DIR + "/")});
			runtimeContext.setCompileClassPath(runtimeContext.getRuntimeClassPath());
		}

		runtimeContext.setBatchMode(batchMode);
		
		
		final TransformationGraph graph = TransformationGraphXMLReaderWriter.loadGraph(new FileInputStream(graphFile), runtimeContext);
		try {
			
			graph.setDebugMode(false);

			EngineInitializer.initGraph(graph);

			for (int i = 0; i < 3; i++) {

				final Future<Result> futureResult = runGraph.executeGraph(graph, runtimeContext);

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
			throw new IllegalStateException("Error executing grap " + graphFile, e);
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

