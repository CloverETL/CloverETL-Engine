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
package org.jetel.test;

import org.jetel.graph.runtime.EngineInitializer;
import org.jetel.util.MiscUtils;
import org.junit.rules.ExternalResource;

/**
 * @author Milan (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 7. 1. 2019
 */
public class InitEngineRule extends ExternalResource {

	private static final String PLUGINS_KEY = "cloveretl.plugins";

	private static final String PLUGINS_DEFAULT_DIR = "..";
	
	private final String cloverPropertiesFile;

	public InitEngineRule(String cloverPropertiesFile) {
		this.cloverPropertiesFile = cloverPropertiesFile;
	}
	
	public InitEngineRule() {
		this(null);
	}

	@Override
	public void before() {
		if (!EngineInitializer.isInitialized()) {
			initEngine(cloverPropertiesFile);
		}
	}

	protected void initEngine() {
		initEngine(null);
	}
	
	protected void initEngine(String defaultPropertiesFile) {
		final String pluginsDir;

		final String pluginsProperty = MiscUtils.getEnvSafe(PLUGINS_KEY);
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
