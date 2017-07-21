package org.jetel.test;

import junit.framework.TestCase;

import org.jetel.graph.runtime.EngineInitializer;

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

}
