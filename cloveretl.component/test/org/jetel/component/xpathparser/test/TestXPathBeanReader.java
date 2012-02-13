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
package org.jetel.component.xpathparser.test;

import java.io.FileInputStream;
import java.util.concurrent.Future;

import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.TransformationGraphXMLReaderWriter;
import org.jetel.graph.runtime.EngineInitializer;
import org.jetel.graph.runtime.GraphRuntimeContext;
import org.jetel.graph.runtime.IThreadManager;
import org.jetel.graph.runtime.SimpleThreadManager;
import org.jetel.graph.runtime.WatchDog;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * TODO add framework do run test graphs - why does not a project defining components
 * include something to test them?!
 * 
 * @author jan.michalica (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 7.12.2011
 */
public class TestXPathBeanReader {

	@BeforeClass
	public static void beforeAll() {
		
		/*
		 * where do I get plugin dir to run the tests?
		 * to require to have GUI checked out just to run test of something that does
		 * not imply GUI existence is insane
		 * TODO FIXME
		 */
		EngineInitializer.initEngine("../cloveretl.gui/lib/plugins" , null, null);
		EngineInitializer.forceActivateAllPlugins();
	}
	
	@Test
	public void testReader() throws Exception {
		
		GraphRuntimeContext context = new GraphRuntimeContext();
		context.setUseJMX(false);
		context.setWaitForJMXClient(false);
		
		TransformationGraph graph = TransformationGraphXMLReaderWriter.loadGraph(
				new FileInputStream("test-data/xpath/beanreadertest.grf"), context);
		
		graph.getDictionary().setValue("input", TestJXPathPushParser.getTestData());
		
		EngineInitializer.initGraph(graph, context);
	
		WatchDog watchDog = new WatchDog(graph, context);
		
		IThreadManager threadManager = new SimpleThreadManager();
		threadManager.initWatchDog(watchDog);
		Future<Result> result = threadManager.executeWatchDog(watchDog);
		
		Assert.assertEquals(Result.FINISHED_OK, result.get());
	}
}
