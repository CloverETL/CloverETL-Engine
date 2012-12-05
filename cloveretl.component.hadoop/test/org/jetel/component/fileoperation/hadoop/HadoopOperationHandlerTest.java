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
package org.jetel.component.fileoperation.hadoop;

import static org.junit.Assume.assumeTrue;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Date;

import org.jetel.component.fileoperation.CloverURI;
import org.jetel.component.fileoperation.DefaultOperationHandler;
import org.jetel.component.fileoperation.IOperationHandler;
import org.jetel.component.fileoperation.ObservableHandler;
import org.jetel.component.fileoperation.Operation;
import org.jetel.component.fileoperation.OperationHandlerTestTemplate;
import org.jetel.component.fileoperation.SimpleParameters.CreateParameters;
import org.jetel.component.fileoperation.SimpleParameters.DeleteParameters;
import org.jetel.component.fileoperation.URIUtils;
import org.jetel.component.fileoperation.hadoop.HadoopOperationHandler;
import org.jetel.component.fileoperation.result.CreateResult;
import org.jetel.component.fileoperation.result.DeleteResult;
import org.jetel.component.fileoperation.result.InfoResult;
import org.jetel.graph.ContextProvider;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.TransformationGraphXMLReaderWriter;
import org.jetel.graph.runtime.GraphRuntimeContext;
import org.jetel.util.file.FileUtils;

/**
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created Nov 26, 2012
 */
public class HadoopOperationHandlerTest extends OperationHandlerTestTemplate {

	private static final String TESTING_URI = "hdfs://CDH3U4/tmp/test_fo/";
	
	private static final String HADOOP_TEST_GRAPH = "hadoop-testGraph.grf";
	
	protected HadoopOperationHandler handler = null;
	
	private TransformationGraph graph = null;
	
	@Override
	protected IOperationHandler createOperationHandler() {
		return handler = new HadoopOperationHandler();
	}
	
	protected URI getTestingURI() throws URISyntaxException {
		return new URI(TESTING_URI);
	}
	
	protected String getTestingGraph() {
		return HADOOP_TEST_GRAPH;
	}

	@Override
	protected URI createBaseURI() {
		try {
			URI base = getTestingURI();
			CloverURI tmpDirUri = CloverURI.createURI(base.resolve(String.format("CloverTemp%d/", System.nanoTime())));
			CreateResult result = manager.create(tmpDirUri, new CreateParameters().setDirectory(true));
			assumeTrue(result.success());
			return tmpDirUri.getSingleURI().toURI();
		} catch (URISyntaxException ex) {
			return null;
		}
	}

	@Override
	protected void setUp() throws Exception {
		initEngine();
		
		GraphRuntimeContext context = new GraphRuntimeContext();
		// set graph context URL so that .cfg and .jar files can be loaded in graph.init()
		context.setContextURL(getClass().getResource("."));
		
		InputStream is = null;
		try {
			is = getClass().getResourceAsStream(getTestingGraph()); // get .grf file as a resource
			graph = TransformationGraphXMLReaderWriter.loadGraph(is, context);
			graph.init();
		} finally {
			FileUtils.closeQuietly(is);
			if (graph != null) {
				// register the graph in ContextProvider, so that Hadoop connections can be obtained
				ContextProvider.registerGraph(graph);
			}
		}
		
		super.setUp();
		
		DefaultOperationHandler defaultHandler = new DefaultOperationHandler();
		manager.registerHandler(VERBOSE ? new ObservableHandler(defaultHandler) : defaultHandler);
	}

	@Override
	protected void tearDown() throws Exception {
		Thread.interrupted(); // reset the interrupted flag of the current thread
		DeleteResult result = manager.delete(CloverURI.createURI(baseUri), new DeleteParameters().setRecursive(true));
		if (!result.success()) {
			System.err.println("Failed to delete " + result.getURI(0));
		}
		super.tearDown();
		handler = null;
		if (graph != null) {
			ContextProvider.unregister();
			graph = null;
		}
	}

	@Override
	public void testGetPriority() {
		assertEquals(IOperationHandler.TOP_PRIORITY, handler.getPriority(Operation.delete(HadoopOperationHandler.HADOOP_SCHEME)));
		assertEquals(IOperationHandler.TOP_PRIORITY, handler.getPriority(Operation.create(HadoopOperationHandler.HADOOP_SCHEME)));
		assertEquals(IOperationHandler.TOP_PRIORITY, handler.getPriority(Operation.resolve(HadoopOperationHandler.HADOOP_SCHEME)));
		assertEquals(IOperationHandler.TOP_PRIORITY, handler.getPriority(Operation.info(HadoopOperationHandler.HADOOP_SCHEME)));
		assertEquals(IOperationHandler.TOP_PRIORITY, handler.getPriority(Operation.list(HadoopOperationHandler.HADOOP_SCHEME)));
		assertEquals(IOperationHandler.TOP_PRIORITY, handler.getPriority(Operation.read(HadoopOperationHandler.HADOOP_SCHEME)));
		assertEquals(IOperationHandler.TOP_PRIORITY, handler.getPriority(Operation.write(HadoopOperationHandler.HADOOP_SCHEME)));
	}

	@Override
	public void testCanPerform() {
		assertTrue(handler.canPerform(Operation.delete(HadoopOperationHandler.HADOOP_SCHEME)));
		assertTrue(handler.canPerform(Operation.create(HadoopOperationHandler.HADOOP_SCHEME)));
		assertTrue(handler.canPerform(Operation.resolve(HadoopOperationHandler.HADOOP_SCHEME)));
		assertTrue(handler.canPerform(Operation.info(HadoopOperationHandler.HADOOP_SCHEME)));
		assertTrue(handler.canPerform(Operation.list(HadoopOperationHandler.HADOOP_SCHEME)));
		assertTrue(handler.canPerform(Operation.read(HadoopOperationHandler.HADOOP_SCHEME)));
		assertTrue(handler.canPerform(Operation.write(HadoopOperationHandler.HADOOP_SCHEME)));
	}

	/*
	 * FIXME re-enable when Hadoop servers are properly synchronized 
	 * Overridden - setting last modification date is not supported for directories
	 */
//	@Override
//	public void testCreateDated() throws Exception {
//		CloverURI uri;
//		Date modifiedDate = new Date(10000);
//		
//		{ // does not work very well on FTP, as the timezone knowledge is required
//			uri = relativeURI("datedFile.tmp");
//			long tolerance = 2 * 60 * 1000; // 2 minutes 
//			Date beforeFileWasCreated = new Date(System.currentTimeMillis() - tolerance);
//			assertTrue(manager.create(uri).success());
//			Date afterFileWasCreated = new Date(System.currentTimeMillis() + tolerance);
//			InfoResult info = manager.info(uri);
//			assertTrue(info.isFile());
//			Date fileCreatedDate = info.getLastModified();
//			if (fileCreatedDate != null) {
//				assertTrue(fileCreatedDate.after(beforeFileWasCreated));
//				assertTrue(afterFileWasCreated.after(fileCreatedDate));
//			}
//		}
//
//		uri = relativeURI("topdir1/subdir/subsubdir/file");
//		System.out.println(uri.getAbsoluteURI());
//		assertFalse(String.format("%s already exists", uri), manager.exists(uri));
//		assertTrue(manager.create(uri, new CreateParameters().setMakeParents(true)).success());
//		assertTrue(String.format("%s is a not file", uri), manager.isFile(uri));
//		assertTrue(manager.create(uri, new CreateParameters().setLastModified(modifiedDate)).success());
//		assertEquals("Dates are different", modifiedDate, manager.info(uri).getLastModified());
//		
////		uri = relativeURI("topdir2/subdir/subsubdir/dir2/");
////		System.out.println(uri.getAbsoluteURI());
////		assertFalse(String.format("%s already exists", uri), manager.exists(uri));
////		assertTrue(manager.create(uri, new CreateParameters().setMakeParents(true)).success());
////		assertTrue(String.format("%s is not a directory", uri), manager.isDirectory(uri));
////		assertTrue(manager.create(uri, new CreateParameters().setLastModified(modifiedDate)).success());
////		assertEquals("Dates are different", modifiedDate, manager.info(uri).getLastModified());
//		
//		uri = relativeURI("file");
//		uri = relativeURI("datedFile");
//		System.out.println(uri.getAbsoluteURI());
//		assertFalse(String.format("%s already exists", uri), manager.exists(uri));
//		assertTrue(manager.create(uri, new CreateParameters().setMakeParents(true)).success());
//		assertTrue(String.format("%s is not a file", uri), manager.isFile(uri));
//		assertTrue(manager.create(uri, new CreateParameters().setLastModified(modifiedDate)).success());
//		assertEquals("Dates are different", modifiedDate, manager.info(uri).getLastModified());
//		
////		uri = relativeURI("datedDir1");
////		System.out.println(uri.getAbsoluteURI());
////		assertFalse(String.format("%s already exists", uri), manager.exists(uri));
////		assertTrue(manager.create(uri, new CreateParameters().setDirectory(true).setLastModified(modifiedDate)).success());
////		assertTrue(String.format("%s is not a directory", uri), manager.isDirectory(uri));
////		assertEquals("Dates are different", modifiedDate, manager.info(uri).getLastModified());
//
////		uri = relativeURI("datedDir2/");
////		System.out.println(uri.getAbsoluteURI());
////		assertFalse(String.format("%s already exists", uri), manager.exists(uri));
////		assertTrue(manager.create(uri, new CreateParameters().setLastModified(modifiedDate)).success());
////		assertTrue(String.format("%s is not a directory", uri), manager.isDirectory(uri));
////		assertEquals("Dates are different", modifiedDate, manager.info(uri).getLastModified());
//		
//		{
//			String dirName = "touch";
//			modifiedDate = null; 
//
//			uri = relativeURI(dirName);
//			assertFalse(String.format("%s already exists", uri), manager.exists(uri));
//			
//			uri = relativeURI(dirName + "/file.tmp");
//			System.out.println(uri.getAbsoluteURI());
//			assertTrue(manager.create(uri, new CreateParameters().setMakeParents(true)).success());
//			modifiedDate = manager.info(uri).getLastModified();
//			Thread.sleep(1000);
//			assertTrue(manager.create(uri).success());
//			assertTrue(manager.info(uri).getLastModified().after(modifiedDate));
//			
////			uri = relativeURI(dirName + "/dir/");
////			System.out.println(uri.getAbsoluteURI());
////			assertTrue(manager.create(uri, new CreateParameters().setMakeParents(true)).success());
////			modifiedDate = manager.info(uri).getLastModified();
////			Thread.sleep(1000);
////			assertTrue(manager.create(uri).success());
////			assertTrue(manager.info(uri).getLastModified().after(modifiedDate));
//		}
//	}

//	@Override
//	public void testResolve() throws Exception {
//		super.testResolve();
//		
//		ResolveResult result;
//		
////		result = manager.resolve(relativeURI("**/eclipse.exe"));
////		System.out.println(result);
////		assertTrue(result.success());
////		assertEquals(2, result.successCount());
//		
//		result = manager.resolve(relativeURI("{aaa,bbb}/*"));
//		System.out.println(result);
//		assertTrue(result.success());
//		assertEquals(2, result.successCount());
//
//		result = manager.resolve(relativeURI("*/eclipse-3.[67]"));
//		System.out.println(result);
//		assertTrue(result.success());
//		assertEquals(2, result.successCount());
//
//		result = manager.resolve(relativeURI("*/eclipse-3.[57]"));
//		System.out.println(result);
//		assertTrue(result.success());
//		assertEquals(1, result.successCount());
//
//		result = manager.resolve(relativeURI("subdir/*"));
//		System.out.println(result);
//		assertTrue(result.success());
//		assertEquals(2, result.successCount());
//
//		result = manager.resolve(relativeURI("subdir/*/"));
//		System.out.println(result);
//		assertTrue(result.success());
//		assertEquals(1, result.successCount());
//	}

	@Override
	protected void generate(URI root, int depth) throws IOException {
		int i = 0;
		for ( ; i < 20; i++) {
			String name = String.valueOf(i);
			URI child = URIUtils.getChildURI(root, name);
			manager.create(CloverURI.createSingleURI(child));
		}
	}

}
