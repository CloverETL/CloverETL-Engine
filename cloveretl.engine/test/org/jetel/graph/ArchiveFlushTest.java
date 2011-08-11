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
package org.jetel.graph;

import java.io.File;
import java.io.FileInputStream;
import java.util.concurrent.Future;

import org.jetel.graph.runtime.EngineInitializer;
import org.jetel.graph.runtime.GraphRuntimeContext;
import org.jetel.main.runGraph;
import org.jetel.test.CloverTestCase;
import org.jetel.util.file.FileUtils;

/**
 * Tests whether zip archives are accessible after graph free().
 * 
 * @author Raszyk (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 22.6.2011
 */
public class ArchiveFlushTest extends CloverTestCase {
	
	private final static String SCENARIOS_RELATIVE_PATH = "../cloveretl.test.scenarios/";
	
	public void testFlushed() throws Exception {
		initEngine("test/org/jetel/graph/ResetTestDefault.properties");
		final String baseDir = new File(SCENARIOS_RELATIVE_PATH).getAbsolutePath();
		
		GraphRuntimeContext graphCtx = new GraphRuntimeContext();
		graphCtx.setUseJMX(false);
		graphCtx.setContextURL(FileUtils.getFileURL(baseDir));
		graphCtx.addAdditionalProperty("DATAOUT_DIR", new File(SCENARIOS_RELATIVE_PATH + "data-out").getAbsolutePath());
		
		String graphFile = baseDir + "/graph/SimpleZipWrite.grf";
		final TransformationGraph graph = TransformationGraphXMLReaderWriter.loadGraph(new FileInputStream(graphFile), graphCtx);
		graph.setDebugMode(false);
		EngineInitializer.initGraph(graph);
		Future<Result> executeGraph = runGraph.executeGraph(graph, graphCtx);
		executeGraph.get().equals(Result.FINISHED_OK);
		graph.free();
		
		File file = new File(SCENARIOS_RELATIVE_PATH + "data-out/simple-zip-write.zip");
		File file2 = new File(SCENARIOS_RELATIVE_PATH + "data-out/simple-zip-write2.zip");
		assertTrue(file.exists());
		assertFalse(file.isDirectory());
		assertTrue(file.canRead());
		assertTrue(file.canWrite());
		assertTrue(file.renameTo(file2));
		assertTrue(file2.delete());
	}
	
}
