package org.jetel.component;

import java.io.InputStream;
import java.nio.channels.Channels;
import java.util.concurrent.Future;

import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.TransformationGraphXMLReaderWriter;
import org.jetel.graph.runtime.EngineInitializer;
import org.jetel.graph.runtime.GraphRuntimeContext;
import org.jetel.main.runGraph;
import org.jetel.test.CloverTestCase;
import org.jetel.util.file.FileUtils;

public class ExtSort2Test extends CloverTestCase {

	public void testExtSort() throws Exception {
		
		String pluginsRoot = "../cloveretl.engine/plugins/";
		String defaultProperties = "../cloveretl.engine/build/bin/defaultProperties";
		String graphDefinitionFile = "../cloveretl.engine/graph/ExtSort2Graph.grf";
//		String graphDefinitionFile = "../cloveretl.engine/graph/ExtSortNativeGraph.grf";

		System.out
				.println("ExtSortTest: Reading graph definition...");

		EngineInitializer.initEngine(pluginsRoot, defaultProperties, null);
		
		InputStream in = Channels.newInputStream(FileUtils.getReadableChannel(
				null, graphDefinitionFile));

		GraphRuntimeContext runtimeContext = new GraphRuntimeContext();
		Future<Result> futureResult = null;

		TransformationGraph graph = null;
		graph = TransformationGraphXMLReaderWriter.loadGraph(in, runtimeContext
				.getAdditionalProperties());
		System.out
				.println("ExtSortTest: Reading graph definition... finished");

		System.out.println("ExtSortTest: Running graph...");
		long startTime = System.currentTimeMillis();

		Result result = Result.N_A;
		EngineInitializer.initGraph(graph, runtimeContext);
		futureResult = runGraph.executeGraph(graph, runtimeContext);
		result = futureResult.get();
		long totalTime = System.currentTimeMillis() - startTime;

		System.out.println("ExtSortTest: Running graph... finished ("  + result + ") in "
				+ (totalTime / 1000) + " s");

		graph.free();

	}
	
}
