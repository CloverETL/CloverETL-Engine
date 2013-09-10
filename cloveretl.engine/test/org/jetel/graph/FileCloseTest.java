package org.jetel.graph;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.parser.DataParser;
import org.jetel.data.parser.TextParserConfiguration;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.runtime.EngineInitializer;
import org.jetel.graph.runtime.GraphRuntimeContext;
import org.jetel.graph.runtime.IThreadManager;
import org.jetel.graph.runtime.SimpleThreadManager;
import org.jetel.graph.runtime.WatchDog;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.metadata.DataRecordMetadataXMLReaderWriter;
import org.jetel.test.CloverTestCase;
import org.jetel.util.MultiFileReader;
import org.jetel.util.file.FileUtils;
import org.jetel.util.property.PropertyRefResolver;

/*
 * Copyright (c) 2004-2005 Javlin Consulting s.r.o. All rights reserved.
 * 
 */

public class FileCloseTest extends CloverTestCase {

	private final static String EXAMPLE_PATH = "../cloveretl.examples/SimpleExamples/";

	static Log logger = LogFactory.getLog(FileCloseTest.class);

	private Properties properties;

	/**
	 * Sets up the fixture, for example, open a network connection.
	 * This method is called before a test is executed.
	 */
	@Override
	protected void setUp() throws Exception {
		super.setUp();
		initEngine();
		
		properties = new Properties();
		properties.load(new FileInputStream(new File(EXAMPLE_PATH + "workspace.prm")));
	}

	
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
		try {
			runtimeContext.setContextURL(FileUtils.getFileURL(FileUtils.appendSlash(EXAMPLE_PATH)));
		} catch (MalformedURLException e1) {
			fail("Invalid project path: " + e1);
		}
		runtimeContext.addAdditionalProperty("PROJECT", ".");
		runtimeContext.setUseJMX(false);

		TransformationGraph graph = null;
		Future<Result> futureResult = null;

		try {
			graph = TransformationGraphXMLReaderWriter.loadGraph(new FileInputStream(graphFile), runtimeContext);
			graph.setDebugMode(false);
		} catch (Exception e) {
			fail("Error in graph loading: " + e);
		}

		try {
			EngineInitializer.initGraph(graph);
		} catch (ComponentNotReadyException e) {
			fail("Error in graph initialization: " + e);
		}

		try {
			IThreadManager threadManager = new SimpleThreadManager();
			WatchDog watchDog = new WatchDog(graph, runtimeContext);
            threadManager.initWatchDog(watchDog);
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

	/**
	 * Test if the MultifileReader releases sources.
	 */
	public void testFileClosed4MultiFileReader() {
		File file1 = null;
		File file2 = null;
		try {
			PropertyRefResolver propertyRefResolver = new PropertyRefResolver(properties);
			String metaDir = EXAMPLE_PATH + propertyRefResolver.resolveRef("${META_DIR}");
			String detaInDir = EXAMPLE_PATH + propertyRefResolver.resolveRef("${DATAIN_DIR}");
			file1 = new File(detaInDir + "/delimited/" + "orders1.dat");
			file2 = new File(detaInDir + "/delimited/" + "orders2.dat");
			duplicateFile(new File(detaInDir + "/delimited/" + "orders.dat"), file1);
			duplicateFile(new File(detaInDir + "/delimited/" + "orders.dat"), file2);
			
			// create metadata
			DataRecordMetadataXMLReaderWriter dataRecordMetadataXMLReaderWriter = new DataRecordMetadataXMLReaderWriter();
			InputStream is = null;
			try {
				is = FileUtils.getInputStream(null,  metaDir + "/delimited/orders.fmt");
			} catch (IOException e) {
				fail(e.getMessage());
			}
			DataRecordMetadata dataRecordMetadata = dataRecordMetadataXMLReaderWriter.read(is);
			
			// create multifile reader
			TextParserConfiguration dataParserCfg = new TextParserConfiguration();
			dataParserCfg.setMetadata(dataRecordMetadata);
			DataParser dataParser = new DataParser(dataParserCfg);
			MultiFileReader multiFileReader = new MultiFileReader(dataParser, null, detaInDir + "/delimited/" + "orders?.dat");

			// test checkconfig
			try {
				multiFileReader.checkConfig(dataRecordMetadata);
			} catch (ComponentNotReadyException e) {
				fail(e.getMessage());
			}
			if (!file1.delete() || !file2.delete()) {
				fail();
			}
			
			// reduplicate files
			duplicateFile(new File(detaInDir + "/delimited/" + "orders.dat"), file1);
			duplicateFile(new File(detaInDir + "/delimited/" + "orders.dat"), file2);
			
			// test init and running
			int i = 0;
			try {
				multiFileReader.init(dataRecordMetadata);
				multiFileReader.preExecute();
				while (multiFileReader.getNext() != null) {i++;};
				
				if (!file1.delete() || !file2.delete()) {
					fail();
				}
				// reduplicate files
				duplicateFile(new File(detaInDir + "/delimited/" + "orders.dat"), file1);
				duplicateFile(new File(detaInDir + "/delimited/" + "orders.dat"), file2);
			} catch (Exception e) {
				fail(e.getMessage());
			}
			
			// test running
			try {
				multiFileReader.init(dataRecordMetadata);
				int j = 0;
				while (multiFileReader.getNext() != null) {
					if (j == i/2) {
						if (!file1.delete()) { //file1???
							fail();
						}
					}
					j++;
				};
				
				if (!file2.delete()) {
					fail();
				}
				// reduplicate files
				duplicateFile(new File(detaInDir + "/delimited/" + "orders.dat"), file1);
				duplicateFile(new File(detaInDir + "/delimited/" + "orders.dat"), file2);
			} catch (Exception e) {
				fail(e.getMessage());
			}

		} finally {
			if (file1 != null && file1.exists()) file1.delete();
			if (file2 != null && file2.exists()) file2.delete();
		}
		
	}

	/**
	 * Copy file from file1 to file2.
	 * @param file1
	 * @param file2
	 */
	private void duplicateFile(File file1, File file2) {
		try {
			InputStream is = new FileInputStream(file1);
			OutputStream os = new FileOutputStream(file2);
			byte[] data = new byte[1024];
			int i;
			while ((i = is.read(data)) != -1) {
				os.write(data, 0, i);
			}
			os.flush();
			os.close();
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}
	
	/**
	 * Test if sources are released when the phase is ended up.
	 */
	public void testFileClosed4Phase() {
		//TODO - doesn't work now
	}
}
