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
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.jetel.component.fileoperation.CloverURI;
import org.jetel.component.fileoperation.DefaultOperationHandler;
import org.jetel.component.fileoperation.IOperationHandler;
import org.jetel.component.fileoperation.ObservableHandler;
import org.jetel.component.fileoperation.Operation;
import org.jetel.component.fileoperation.OperationHandlerTestTemplate;
import org.jetel.component.fileoperation.OperationKind;
import org.jetel.component.fileoperation.SimpleParameters.CreateParameters;
import org.jetel.component.fileoperation.SimpleParameters.DeleteParameters;
import org.jetel.component.fileoperation.URIUtils;
import org.jetel.component.fileoperation.result.CreateResult;
import org.jetel.component.fileoperation.result.DeleteResult;
import org.jetel.component.fileoperation.result.InfoResult;
import org.jetel.graph.ContextProvider;
import org.jetel.graph.ContextProvider.Context;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.TransformationGraphXMLReaderWriter;
import org.jetel.graph.runtime.GraphRuntimeContext;
import org.jetel.hadoop.fileoperation.HadoopOperationHandler;
import org.jetel.util.file.FileUtils;

/**
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created Nov 26, 2012
 */
public class HadoopOperationHandlerTest extends OperationHandlerTestTemplate {

	protected static final String CDH412 = "hdfs://CDH412/tmp/test_fo/";
	protected static final String CDH560 = "hdfs://CDH560/tmp/test_fo/";
	protected static final String CDH511 = "hdfs://CDH511/tmp/test_fo/";
	
	private static final String HADOOP_TEST_GRAPH = "hadoop-testGraph.grf";
	
	protected HadoopOperationHandler handler = null;
	
	private TransformationGraph graph = null;
	
	private Context c = null;
	
	private Long timeStamp = null;
	
	@Override
	protected IOperationHandler createOperationHandler() {
		return handler = new HadoopOperationHandler();
	}
	
	protected URI getTestingURI() {
		return URI.create(CDH560);
	}
	
	/*
	 * Used for testing MOVE between two servers.
	 */
	protected URI getRemoteURI() {
		return URI.create(CDH412);
	}
	
	@Override
	public void testMove() throws Exception {
		super.testMove();
		
		// CLO-3505: test moving between two different connections (or even servers)
		URI baseUri2 = null;
		try {
			baseUri2 = createBaseURI(getRemoteURI());
			Map<String, String> texts = new HashMap<String, String>();
			String originalName = "differentConnections.tmp";
			String newName = originalName + ".moved";
			CloverURI source = relativeURI(originalName);
			assertFalse(manager.exists(source));
			String content = "\u017Dlu\u0165ou\u010Dk\u00FD k\u016F\u0148 \u00FAp\u011Bl \u010F\u00E1belsk\u00E9 \u00F3dy";
			texts.put(originalName, content);
			prepareData(texts);
			assertTrue(manager.exists(source));
			CloverURI target = CloverURI.createSingleURI(baseUri2, newName);
			
			assertFalse(manager.move(source, target).success());
			
			// enable moving using DefaultOperationHandler
			DefaultOperationHandler defaultHandler = new DefaultOperationHandler();
			manager.registerHandler(VERBOSE ? new ObservableHandler(defaultHandler) : defaultHandler);
			
			assertTrue(manager.move(source, target).success());
			
			assertFalse(manager.exists(source));
			// file with new name, but in the old location
			assertFalse(manager.exists(relativeURI(newName)));
			assertTrue(manager.exists(target));
			assertEquals(content, read(manager.getInput(target).channel()));
		} finally {
			if (baseUri2 != null) {
				DeleteResult result = manager.delete(CloverURI.createURI(baseUri2), new DeleteParameters().setRecursive(true));
				if (!result.success()) {
					System.err.println("Failed to delete " + result.getURI(0));
				}
			}
		}
		
	}

	protected String getTestingGraph() {
		return HADOOP_TEST_GRAPH;
	}
	
	protected boolean isSynchronized() {
		try {
			CloverURI uri = relativeURI("synchronization-test.tmp");
			int tolerance = 1000;
			Date beforeFileWasCreated = new Date(System.currentTimeMillis() - tolerance);
			CreateResult cr = manager.create(uri);
			Date afterFileWasCreated = new Date(System.currentTimeMillis() + tolerance);
			if (cr.success()) {
				InfoResult ir = manager.info(uri);
				if (ir.success()) {
					Date modificationDate = ir.getLastModified();
					if (modificationDate != null) {
						return modificationDate.after(beforeFileWasCreated) && afterFileWasCreated.after(modificationDate); 
					}
				}
			}
		} catch (Exception e) {
		}
		return false;
	}
	
	protected URI createBaseURI(URI testingUri) {
		CloverURI tmpDirUri = CloverURI.createURI(testingUri.resolve(String.format("CloverTemp%d/", timeStamp)));
		CreateResult result = manager.create(tmpDirUri, new CreateParameters().setDirectory(true).setMakeParents(true));
		if (!result.success()) {
			System.err.println(result.getFirstErrorMessage());
		}
		assumeTrue(result.success());
		return tmpDirUri.getSingleURI().toURI();
	}

	@Override
	protected URI createBaseURI() {
		return createBaseURI(getTestingURI());
	}

	@Override
	protected void setUp() throws Exception {
		c = null;
		timeStamp = System.nanoTime();
		
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
			if (graph != null) {
				// register the graph in ContextProvider, so that Hadoop connections can be obtained
				c = ContextProvider.registerGraph(graph);
			}
			FileUtils.closeQuietly(is);
		}
		
		super.setUp();
		
		DefaultOperationHandler defaultHandler = new DefaultOperationHandler() {

			@Override
			public boolean canPerform(Operation operation) {
				if (operation.kind == OperationKind.MOVE) {
					return false;
				}
				return super.canPerform(operation);
			}
			
		};
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
			ContextProvider.unregister(c);
			graph = null;
		}
		timeStamp = null;
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
	 * Overridden - setting last modification date is not supported for directories
	 */
	@Override
	public void testCreateDated() throws Exception {
		if (!isSynchronized()) {
			System.err.println("The Hadoop server is not synchronized");
			// CLO-6185: execute the test even if the servers are not synchronized
//			return;
		} else {
			System.out.println("The server is properly synchronized, executing testCreateDated()");
		}
		CloverURI uri;
		Date modifiedDate = new Date(10000);
		
		{ // does not work very well on FTP, as the timezone knowledge is required
			uri = relativeURI("datedFile.tmp");
			long tolerance = 2 * 60 * 1000; // 2 minutes 
			Date beforeFileWasCreated = new Date(System.currentTimeMillis() - tolerance);
			assertTrue(manager.create(uri).success());
			Date afterFileWasCreated = new Date(System.currentTimeMillis() + tolerance);
			InfoResult info = manager.info(uri);
			assertTrue(info.isFile());
			Date fileCreatedDate = info.getLastModified();
			if (fileCreatedDate != null) {
				assertTrue(fileCreatedDate.after(beforeFileWasCreated));
				assertTrue(afterFileWasCreated.after(fileCreatedDate));
			}
		}

		uri = relativeURI("topdir1/subdir/subsubdir/file");
		System.out.println(uri.getAbsoluteURI());
		assertFalse(String.format("%s already exists", uri), manager.exists(uri));
		assertTrue(manager.create(uri, new CreateParameters().setMakeParents(true)).success());
		assertTrue(String.format("%s is a not file", uri), manager.isFile(uri));
		assertTrue(manager.create(uri, new CreateParameters().setLastModified(modifiedDate)).success());
		assertEquals("Dates are different", modifiedDate, manager.info(uri).getLastModified());
		
//		uri = relativeURI("topdir2/subdir/subsubdir/dir2/");
//		System.out.println(uri.getAbsoluteURI());
//		assertFalse(String.format("%s already exists", uri), manager.exists(uri));
//		assertTrue(manager.create(uri, new CreateParameters().setMakeParents(true)).success());
//		assertTrue(String.format("%s is not a directory", uri), manager.isDirectory(uri));
//		assertTrue(manager.create(uri, new CreateParameters().setLastModified(modifiedDate)).success());
//		assertEquals("Dates are different", modifiedDate, manager.info(uri).getLastModified());
		
		uri = relativeURI("file");
		uri = relativeURI("datedFile");
		System.out.println(uri.getAbsoluteURI());
		assertFalse(String.format("%s already exists", uri), manager.exists(uri));
		assertTrue(manager.create(uri, new CreateParameters().setMakeParents(true)).success());
		assertTrue(String.format("%s is not a file", uri), manager.isFile(uri));
		assertTrue(manager.create(uri, new CreateParameters().setLastModified(modifiedDate)).success());
		assertEquals("Dates are different", modifiedDate, manager.info(uri).getLastModified());
		
//		uri = relativeURI("datedDir1");
//		System.out.println(uri.getAbsoluteURI());
//		assertFalse(String.format("%s already exists", uri), manager.exists(uri));
//		assertTrue(manager.create(uri, new CreateParameters().setDirectory(true).setLastModified(modifiedDate)).success());
//		assertTrue(String.format("%s is not a directory", uri), manager.isDirectory(uri));
//		assertEquals("Dates are different", modifiedDate, manager.info(uri).getLastModified());

//		uri = relativeURI("datedDir2/");
//		System.out.println(uri.getAbsoluteURI());
//		assertFalse(String.format("%s already exists", uri), manager.exists(uri));
//		assertTrue(manager.create(uri, new CreateParameters().setLastModified(modifiedDate)).success());
//		assertTrue(String.format("%s is not a directory", uri), manager.isDirectory(uri));
//		assertEquals("Dates are different", modifiedDate, manager.info(uri).getLastModified());
		
		{
			String dirName = "touch";
			modifiedDate = null; 

			uri = relativeURI(dirName);
			assertFalse(String.format("%s already exists", uri), manager.exists(uri));
			
			uri = relativeURI(dirName + "/file.tmp");
			System.out.println(uri.getAbsoluteURI());
			assertTrue(manager.create(uri, new CreateParameters().setMakeParents(true)).success());
			modifiedDate = manager.info(uri).getLastModified();
			Thread.sleep(1000);
			assertTrue(manager.create(uri).success());
			assertTrue(manager.info(uri).getLastModified().after(modifiedDate));
			
//			uri = relativeURI(dirName + "/dir/");
//			System.out.println(uri.getAbsoluteURI());
//			assertTrue(manager.create(uri, new CreateParameters().setMakeParents(true)).success());
//			modifiedDate = manager.info(uri).getLastModified();
//			Thread.sleep(1000);
//			assertTrue(manager.create(uri).success());
//			assertTrue(manager.info(uri).getLastModified().after(modifiedDate));
		}
	}

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
	public void testInfo() throws Exception {
		super.testInfo();
		
		String connectionId = getTestingURI().getAuthority();
		
		CloverURI uri;
		InfoResult result;
		
		uri = CloverURI.createURI("hdfs://" + connectionId + "/");
		result = manager.info(uri);
		assertTrue(result.success());
		assertTrue(result.isDirectory());
		assertTrue(result.getName().isEmpty());
		System.out.println(result.getResult());

		uri = CloverURI.createURI("hdfs://" + connectionId);
		result = manager.info(uri);
		assertTrue(result.success());
		assertTrue(result.isDirectory());
		assertTrue(result.getName().isEmpty());
		System.out.println(result.getResult());
	}

	@Override
	protected void generate(URI root, int depth) throws IOException {
		int i = 0;
		for ( ; i < 20; i++) {
			String name = String.valueOf(i);
			URI child = URIUtils.getChildURI(root, name);
			CreateResult createResult = manager.create(CloverURI.createSingleURI(child));
			assumeTrue(createResult.success());
		}
	}

	@Override
	public void testInterruptDelete() throws Exception {
		// disabled, the test fails randomly 
		// the current implementation just delegates the call to HDFS to achieve best performance
	}

}
