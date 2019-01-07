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

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

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

/**
 * @author Milan (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 7. 1. 2019
 */
public class TestUtils {

	public static TransformationGraph createTransformationGraph(String path, GraphRuntimeContext context)
		throws FileNotFoundException, GraphConfigurationException, XMLConfigurationException, ComponentNotReadyException {
		
		InputStream in = new BufferedInputStream(new FileInputStream(path));
		try {
			context.setUseJMX(false);
			TransformationGraph graph = TransformationGraphXMLReaderWriter.loadGraph(in, context);
			EngineInitializer.initGraph(graph);
			return graph;
		} finally {
			IOUtils.closeQuietly(in);
		}
	}

	public static Result runGraph(TransformationGraph graph)
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

	public static void rethrowRuntime(Throwable throwable) {
		if (throwable == null) {
			return;
		}
		
		if (throwable instanceof RuntimeException) {
			throw (RuntimeException) throwable;
		} else if (throwable instanceof Error) {
			throw (Error) throwable;
		} else {
			throw new RuntimeException(throwable);
		}
	}
}
