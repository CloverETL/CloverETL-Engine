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
package org.jetel.component.fileoperation;

import static org.junit.Assume.assumeTrue;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Date;

import org.jetel.component.fileoperation.SimpleParameters.CreateParameters;
import org.jetel.component.fileoperation.SimpleParameters.DeleteParameters;
import org.jetel.component.fileoperation.result.CreateResult;
import org.jetel.component.fileoperation.result.InfoResult;
import org.jetel.component.fileoperation.result.ListResult;
import org.jetel.component.fileoperation.result.ResolveResult;

public class SFTPOperationHandlerTest extends OperationHandlerTestTemplate {

	private static final String testingUri = "sftp://test:test@koule/home/test/tmp/file_operation_tests/";
//	private static final String testingUri = "sftp://test:test@localhost/";

	@SuppressWarnings("deprecation")
	private SFTPOperationHandler handler = null;

	@SuppressWarnings("deprecation")
	@Override
	protected IOperationHandler createOperationHandler() {
		return handler = new SFTPOperationHandler();
	}
	
	@Override
	protected URI createBaseURI() {
		try {
			URI base = new URI(testingUri);
			SingleCloverURI tmpDirUri = CloverURI.createSingleURI(base, String.format("CloverTemp%d/", System.nanoTime()));
			CreateResult result = manager.create(tmpDirUri, new CreateParameters().setDirectory(true));
			assumeTrue(result.success());
			return tmpDirUri.getAbsoluteURI().toURI();
		} catch (URISyntaxException ex) {
			return null;
		}
	}

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		DefaultOperationHandler defaultHandler = new DefaultOperationHandler();
		manager.registerHandler(VERBOSE ? new ObservableHandler(defaultHandler) : defaultHandler);
	}

	@Override
	protected void tearDown() throws Exception {
		Thread.interrupted(); // reset the interrupted flag of the current thread
		manager.delete(CloverURI.createURI(baseUri), new DeleteParameters().setRecursive(true));
		super.tearDown();
		handler = null;
	}

	@SuppressWarnings("deprecation")
	@Override
	public void testGetPriority() {
//		assertEquals(0, handler.getSpeed(Operation.copy(SFTPOperationHandler.SFTP_SCHEME, SFTPOperationHandler.SFTP_SCHEME)));
		assertEquals(0, handler.getPriority(Operation.move(SFTPOperationHandler.SFTP_SCHEME, SFTPOperationHandler.SFTP_SCHEME)));
		assertEquals(0, handler.getPriority(Operation.delete(SFTPOperationHandler.SFTP_SCHEME)));
		assertEquals(0, handler.getPriority(Operation.create(SFTPOperationHandler.SFTP_SCHEME)));
		assertEquals(0, handler.getPriority(Operation.resolve(SFTPOperationHandler.SFTP_SCHEME)));
		assertEquals(0, handler.getPriority(Operation.info(SFTPOperationHandler.SFTP_SCHEME)));
		assertEquals(0, handler.getPriority(Operation.list(SFTPOperationHandler.SFTP_SCHEME)));
		assertEquals(0, handler.getPriority(Operation.read(SFTPOperationHandler.SFTP_SCHEME)));
		assertEquals(0, handler.getPriority(Operation.write(SFTPOperationHandler.SFTP_SCHEME)));
	}

	@SuppressWarnings("deprecation")
	@Override
	public void testCanPerform() {
//		assertTrue(handler.canPerform(Operation.copy(SFTPOperationHandler.SFTP_SCHEME, SFTPOperationHandler.SFTP_SCHEME)));
		assertTrue(handler.canPerform(Operation.move(SFTPOperationHandler.SFTP_SCHEME, SFTPOperationHandler.SFTP_SCHEME)));
		assertTrue(handler.canPerform(Operation.delete(SFTPOperationHandler.SFTP_SCHEME)));
		assertTrue(handler.canPerform(Operation.create(SFTPOperationHandler.SFTP_SCHEME)));
		assertTrue(handler.canPerform(Operation.resolve(SFTPOperationHandler.SFTP_SCHEME)));
		assertTrue(handler.canPerform(Operation.info(SFTPOperationHandler.SFTP_SCHEME)));
		assertTrue(handler.canPerform(Operation.list(SFTPOperationHandler.SFTP_SCHEME)));
		assertTrue(handler.canPerform(Operation.read(SFTPOperationHandler.SFTP_SCHEME)));
		assertTrue(handler.canPerform(Operation.write(SFTPOperationHandler.SFTP_SCHEME)));
	}

	@Override
	protected void generate(URI root, int depth) throws IOException {
		int i = 0;
		for ( ; i < 20; i++) {
			String name = String.valueOf(i);
			URI child = URIUtils.getChildURI(root, name);
			manager.create(CloverURI.createSingleURI(child));
		}
	}
	
	
	
	@Override
	public void testList() throws Exception {
		super.testList();
		
		CloverURI uri;
		ListResult result;
		
		uri = CloverURI.createURI("sftp://test:test@koule/");
		result = manager.list(uri);
		assertTrue(result.success());
		System.out.println(result.getResult());

		uri = CloverURI.createURI("sftp://test:test@koule");
		result = manager.list(uri);
		assertTrue(result.success());
		System.out.println(result.getResult());
	}
	
	@Override
	public void testInfo() throws Exception {
		super.testInfo();
		
		// CLO-4118:
		CloverURI uri;
		InfoResult result;
		
		uri = CloverURI.createURI("sftp://test:test@koule/");
		result = manager.info(uri);
		assertTrue(result.success());
		assertTrue(result.isDirectory());
		assertTrue(result.getName().isEmpty());
		System.out.println(result.getResult());

		uri = CloverURI.createURI("sftp://test:test@koule");
		result = manager.info(uri);
		assertTrue(result.success());
		assertTrue(result.isDirectory());
		assertTrue(result.getName().isEmpty());
		System.out.println(result.getResult());
	}

	@Override
	public void testResolve() throws Exception {
		super.testResolve();

		CloverURI uri;
		ResolveResult result;
		
		uri = CloverURI.createURI("sftp://test:test@koule/*");
		result = manager.resolve(uri);
		assertTrue(result.success());
		System.out.println(result.getResult());

		uri = CloverURI.createURI("sftp://test:test@koule");
		result = manager.resolve(uri);
		assertTrue(result.success());
		assertEquals(1, result.totalCount());
		System.out.println(result.getResult());
		
		uri = CloverURI.createURI("sftp://badUser:badPassword@badserver/home/test/*.txt");
		result = manager.resolve(uri);
		assertFalse(result.success());
		assertEquals(1, result.totalCount());
	}

	@Override
	public void testCreateDated() throws Exception {
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
		
		uri = relativeURI("topdir2/subdir/subsubdir/dir2/");
		System.out.println(uri.getAbsoluteURI());
		assertFalse(String.format("%s already exists", uri), manager.exists(uri));
		assertTrue(manager.create(uri, new CreateParameters().setMakeParents(true)).success());
		assertTrue(String.format("%s is not a directory", uri), manager.isDirectory(uri));
		assertTrue(manager.create(uri, new CreateParameters().setLastModified(modifiedDate)).success());
//		assertEquals("Dates are different", modifiedDate, manager.info(uri).getLastModified());
		
		uri = relativeURI("file");
		uri = relativeURI("datedFile");
		System.out.println(uri.getAbsoluteURI());
		assertFalse(String.format("%s already exists", uri), manager.exists(uri));
		assertTrue(manager.create(uri, new CreateParameters().setMakeParents(true)).success());
		assertTrue(String.format("%s is not a file", uri), manager.isFile(uri));
		assertTrue(manager.create(uri, new CreateParameters().setLastModified(modifiedDate)).success());
		assertEquals("Dates are different", modifiedDate, manager.info(uri).getLastModified());
		
		uri = relativeURI("datedDir1");
		System.out.println(uri.getAbsoluteURI());
		assertFalse(String.format("%s already exists", uri), manager.exists(uri));
		assertTrue(manager.create(uri, new CreateParameters().setDirectory(true).setLastModified(modifiedDate)).success());
		assertTrue(String.format("%s is not a directory", uri), manager.isDirectory(uri));
//		assertEquals("Dates are different", modifiedDate, manager.info(uri).getLastModified());

		uri = relativeURI("datedDir2/");
		System.out.println(uri.getAbsoluteURI());
		assertFalse(String.format("%s already exists", uri), manager.exists(uri));
		assertTrue(manager.create(uri, new CreateParameters().setLastModified(modifiedDate)).success());
		assertTrue(String.format("%s is not a directory", uri), manager.isDirectory(uri));
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
			assertTrue(after(manager.info(uri).getLastModified(), modifiedDate));
			
			uri = relativeURI(dirName + "/dir/");
			System.out.println(uri.getAbsoluteURI());
			assertTrue(manager.create(uri, new CreateParameters().setMakeParents(true)).success());
			modifiedDate = manager.info(uri).getLastModified();
			Thread.sleep(1000);
			assertTrue(manager.create(uri).success());
			assertTrue(after(manager.info(uri).getLastModified(), modifiedDate));
		}
	}

	@Override
	public void testInterruptDelete() throws Exception {
		// FIXME
	}

	@Override
	public void testInterruptCopy() throws Exception {
		// FIXME
	}

	@Override
	public void testInterruptMove() throws Exception {
		// FIXME
	}

	@Override
	public void testInterruptList() throws Exception {
		// FIXME
	}

	@Override
	public void testSpecialCharacters() throws Exception {
		// FIXME test succeeds, but the filenames are wrong
		// maybe the culprit is koule, which does not have UTF8 locale
	}

	@Override
	public URI getUnreachableUri() {
		return URI.create("sftp://badUser:badPassword@badserver/");
	}

	@Override
	protected long getTolerance() {
		return 1000;
	}
	
	
}
