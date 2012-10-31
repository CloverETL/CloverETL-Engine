package org.jetel.test;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import junit.framework.TestCase;

import org.apache.commons.io.IOUtils;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.GraphConfigurationException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.TransformationGraphXMLReaderWriter;
import org.jetel.graph.runtime.EngineInitializer;
import org.jetel.graph.runtime.GraphRuntimeContext;
import org.jetel.graph.runtime.SimpleThreadManager;
import org.jetel.graph.runtime.WatchDog;

public abstract class CloverTestCase extends TestCase {

	public CloverTestCase() {
		super();
	}

	public CloverTestCase(String name) {
		super(name);
	}

	private static final String PLUGINS_KEY = "cloveretl.plugins";

	private static final String PLUGINS_DEFAULT_DIR = "..";


	protected void initEngine() {
		initEngine(null);
	}
	
	protected void initEngine(String defaultPropertiesFile) {
		final String pluginsDir;

		final String pluginsProperty = System.getenv(PLUGINS_KEY);
		if (pluginsProperty != null) {
			pluginsDir = pluginsProperty;
		} else {
			pluginsDir = PLUGINS_DEFAULT_DIR;
		}

		System.out.println("Cloveretl plugins: " + pluginsDir);
		EngineInitializer.initEngine(pluginsDir, defaultPropertiesFile, null);
		EngineInitializer.forceActivateAllPlugins();
	}

	@Override
	protected void setUp() throws Exception {
		if (!EngineInitializer.isInitialized()) {
			initEngine(getCloverPropertiesFile());
		}
	}
	
	protected String getCloverPropertiesFile() {
		return null;
	}
	
	protected TransformationGraph createTransformationGraph(String path, GraphRuntimeContext context)
		throws FileNotFoundException, GraphConfigurationException, XMLConfigurationException, ComponentNotReadyException {
		
		InputStream in = new BufferedInputStream(new FileInputStream(path));
		try {
			context.setUseJMX(false);
			TransformationGraph graph = TransformationGraphXMLReaderWriter.loadGraph(in, context);
			EngineInitializer.initGraph(graph, context);
			return graph;
		} finally {
			IOUtils.closeQuietly(in);
		}
	}
	
	protected Result runGraph(TransformationGraph graph)
		throws ExecutionException, InterruptedException {
		
		WatchDog watchDog = new WatchDog(graph, graph.getRuntimeContext());
		watchDog.init();
		SimpleThreadManager manager = new SimpleThreadManager();
		Future<Result> result = manager.executeWatchDog(watchDog);
		Result value = result.get();
		if (value == Result.ERROR) {
			if (watchDog.getCauseException() != null) {
				rethrowRuntime(watchDog.getCauseException());
			}
		}
		return value;
	}
	
	protected static void rethrowRuntime(Throwable throwable) {
		
		if (throwable == null) {
			return;
		}
		if (throwable instanceof RuntimeException) {
			throw (RuntimeException)throwable;
		} else if (throwable instanceof Error) {
			throw (Error)throwable;
		} else {
			throw new RuntimeException(throwable);
		}
	}
}
