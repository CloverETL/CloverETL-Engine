/*
 * jETeL/CloverETL - Java based ETL application framework.
 * Copyright (c) Javlin, a.s. (info@cloveretl.com)
 *  
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */
package org.jetel.graph.runtime;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.net.MalformedURLException;
import java.util.concurrent.ExecutionException;

import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.GraphConfigurationException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.TransformationGraphXMLReaderWriter;
import org.jetel.test.CloverTestCase;
import org.jetel.util.file.FileUtils;

/**
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 17.6.2013
 */
public class SingleThreadExecutionTest extends CloverTestCase {

	public void testSync() throws MalformedURLException, FileNotFoundException, XMLConfigurationException, GraphConfigurationException, ComponentNotReadyException {
		GraphRuntimeContext runtimeContext = new GraphRuntimeContext();
		runtimeContext.setExecutionType(ExecutionType.SINGLE_THREAD_EXECUTION);
		runtimeContext.setContextURL(FileUtils.getFileURL(FileUtils.appendSlash("data/")));
		TransformationGraph graph = 
				TransformationGraphXMLReaderWriter.loadGraph(new FileInputStream("data/graph/SingleThreadExecutionTest.grf"), runtimeContext);
		EngineInitializer.initGraph(graph);
		
        SingleThreadWatchDog watchDog = new SingleThreadWatchDog(graph, runtimeContext);
        watchDog.init();
		Result result = watchDog.call();

		System.out.println("Result: " + result);
		
		//validate results
		assertEquals(Result.FINISHED_OK, result);
		assertEquals(FileUtils.getStringFromURL(null, "data/supposed-out/SingleThreadExecutionTestOutData1.txt", "UTF-8"),
				FileUtils.getStringFromURL(null, "data/data-out/SingleThreadExecutionTestOutData1.txt", "UTF-8"));
		assertEquals(FileUtils.getStringFromURL(null, "data/supposed-out/SingleThreadExecutionTestOutData2.txt", "UTF-8"),
				FileUtils.getStringFromURL(null, "data/data-out/SingleThreadExecutionTestOutData2.txt", "UTF-8"));
	}

	public void testAsync() throws MalformedURLException, FileNotFoundException, XMLConfigurationException, GraphConfigurationException, ComponentNotReadyException, InterruptedException, ExecutionException {
		GraphRuntimeContext runtimeContext = new GraphRuntimeContext();
		runtimeContext.setContextURL(FileUtils.getFileURL(FileUtils.appendSlash("data/")));
		TransformationGraph graph = 
				TransformationGraphXMLReaderWriter.loadGraph(new FileInputStream("data/graph/SingleThreadExecutionTest.grf"), runtimeContext);
		EngineInitializer.initGraph(graph);
		
        IThreadManager threadManager = new SimpleThreadManager();
        WatchDog watchDog = new WatchDog(graph, runtimeContext);
        threadManager.initWatchDog(watchDog);
		CloverFuture future = threadManager.executeWatchDog(watchDog);
		Result result = future.get();

		System.out.println("Result: " + result);
		assertEquals(Result.FINISHED_OK, result);
	}

	public void testJSONReader_allTypes_formatting() throws MalformedURLException, FileNotFoundException, XMLConfigurationException, GraphConfigurationException, ComponentNotReadyException {
		GraphRuntimeContext runtimeContext = new GraphRuntimeContext();
		runtimeContext.setExecutionType(ExecutionType.SINGLE_THREAD_EXECUTION);
		runtimeContext.setContextURL(FileUtils.getFileURL(FileUtils.appendSlash("../cloveretl.test.scenarios/")));
		TransformationGraph graph = 
				TransformationGraphXMLReaderWriter.loadGraph(new FileInputStream("../cloveretl.test.scenarios/graph/JSONReader_allTypes_formatting.grf"), runtimeContext);
		EngineInitializer.initGraph(graph);
		
        SingleThreadWatchDog watchDog = new SingleThreadWatchDog(graph, runtimeContext);
        watchDog.init();
		Result result = watchDog.call();

		System.out.println("Result: " + result);

		//validate results
		assertEquals(Result.FINISHED_OK, result);
		assertEquals(FileUtils.getStringFromURL(null, "../cloveretl.test.scenarios/supposed-out/JSONReader_allTypes_formatting.txt", "UTF-8"),
				FileUtils.getStringFromURL(null, "../cloveretl.test.scenarios/data-out/JSONReader_allTypes_formatting.txt", "UTF-8"));
	}

	public void testJSONWriter_AllTypes() throws MalformedURLException, FileNotFoundException, XMLConfigurationException, GraphConfigurationException, ComponentNotReadyException {
		GraphRuntimeContext runtimeContext = new GraphRuntimeContext();
		runtimeContext.setExecutionType(ExecutionType.SINGLE_THREAD_EXECUTION);
		runtimeContext.setContextURL(FileUtils.getFileURL(FileUtils.appendSlash("../cloveretl.test.scenarios/")));
		TransformationGraph graph = 
				TransformationGraphXMLReaderWriter.loadGraph(new FileInputStream("../cloveretl.test.scenarios/graph/JSONWriter_AllTypes.grf"), runtimeContext);
		EngineInitializer.initGraph(graph);
		
        SingleThreadWatchDog watchDog = new SingleThreadWatchDog(graph, runtimeContext);
        watchDog.init();
		Result result = watchDog.call();

		System.out.println("Result: " + result);

		//validate results
		assertEquals(Result.FINISHED_OK, result);
		assertEquals(FileUtils.getStringFromURL(null, "../cloveretl.test.scenarios/supposed-out/jsonWriter_AllTypes.json", "UTF-8"),
				FileUtils.getStringFromURL(null, "../cloveretl.test.scenarios/data-out/jsonWriter_AllTypes.json", "UTF-8"));
		assertEquals(FileUtils.getStringFromURL(null, "../cloveretl.test.scenarios/supposed-out/jsonWriter_AllTypesInArray.json", "UTF-8"),
				FileUtils.getStringFromURL(null, "../cloveretl.test.scenarios/data-out/jsonWriter_AllTypesInArray.json", "UTF-8"));
	}
	
}
