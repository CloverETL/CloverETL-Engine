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

import org.jetel.component.fileoperation.SimpleParameters.CreateParameters;
import org.jetel.component.fileoperation.SimpleParameters.DeleteParameters;
import org.jetel.component.fileoperation.result.CreateResult;
import org.jetel.component.fileoperation.result.DeleteResult;
import org.jetel.component.fileoperation.result.InfoResult;
import org.jetel.component.fileoperation.result.ListResult;
import org.jetel.component.fileoperation.result.ResolveResult;

public class FTPOperationHandlerTest extends OperationHandlerTestTemplate {
	
	private static final String testingUri = "ftp://test:test@koule/tmp/file_operation_tests/";
//	private static final String testingUri = "ftp://test:test@localhost/";
	
	protected FTPOperationHandler handler = null;
	
	@Override
	protected IOperationHandler createOperationHandler() {
		return handler = new FTPOperationHandler();
	}
	
	@Override
	protected URI createBaseURI() {
		try {
			URI base = new URI(testingUri);
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
	}

	@Override
	public void testGetPriority() {
//		assertEquals(Integer.MAX_VALUE, handler.getPriority(Operation.copy(FTPOperationHandler.FTP_SCHEME, FTPOperationHandler.FTP_SCHEME)));
		assertEquals(Integer.MAX_VALUE, handler.getPriority(Operation.move(FTPOperationHandler.FTP_SCHEME, FTPOperationHandler.FTP_SCHEME)));
		assertEquals(Integer.MAX_VALUE, handler.getPriority(Operation.delete(FTPOperationHandler.FTP_SCHEME)));
		assertEquals(Integer.MAX_VALUE, handler.getPriority(Operation.create(FTPOperationHandler.FTP_SCHEME)));
		assertEquals(Integer.MAX_VALUE, handler.getPriority(Operation.resolve(FTPOperationHandler.FTP_SCHEME)));
		assertEquals(Integer.MAX_VALUE, handler.getPriority(Operation.info(FTPOperationHandler.FTP_SCHEME)));
		assertEquals(Integer.MAX_VALUE, handler.getPriority(Operation.list(FTPOperationHandler.FTP_SCHEME)));
		assertEquals(Integer.MAX_VALUE, handler.getPriority(Operation.read(FTPOperationHandler.FTP_SCHEME)));
		assertEquals(Integer.MAX_VALUE, handler.getPriority(Operation.write(FTPOperationHandler.FTP_SCHEME)));
	}

	@Override
	public void testCanPerform() {
//		assertTrue(handler.canPerform(Operation.copy(FTPOperationHandler.FTP_SCHEME, FTPOperationHandler.FTP_SCHEME)));
		assertTrue(handler.canPerform(Operation.move(FTPOperationHandler.FTP_SCHEME, FTPOperationHandler.FTP_SCHEME)));
		assertTrue(handler.canPerform(Operation.delete(FTPOperationHandler.FTP_SCHEME)));
		assertTrue(handler.canPerform(Operation.create(FTPOperationHandler.FTP_SCHEME)));
		assertTrue(handler.canPerform(Operation.resolve(FTPOperationHandler.FTP_SCHEME)));
		assertTrue(handler.canPerform(Operation.info(FTPOperationHandler.FTP_SCHEME)));
		assertTrue(handler.canPerform(Operation.list(FTPOperationHandler.FTP_SCHEME)));
		assertTrue(handler.canPerform(Operation.read(FTPOperationHandler.FTP_SCHEME)));
		assertTrue(handler.canPerform(Operation.write(FTPOperationHandler.FTP_SCHEME)));
	}
	
	@Override
	public URI getUnreachableUri() {
		return URI.create("ftp://badUser:badPassword@badserver/");
	}

	@Override
	public void testCreateDated() throws Exception {
		// FIXME overridden - setting last modified date is not supported
	}

	@Override
	public void testSpecialCharacters() throws Exception {
		// FIXME overridden - does not work on FTP
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

	public void testRootList() throws Exception {
		CloverURI uri;
		ListResult result;
		
		uri = CloverURI.createURI("ftp://test:test@koule/");
		result = manager.list(uri);
		assertTrue(result.success());
		System.out.println(result.getResult());

		uri = CloverURI.createURI("ftp://test:test@koule");
		result = manager.list(uri);
		assertTrue(result.success());
		System.out.println(result.getResult());
	}
	
	public void testRootInfo() throws Exception {
		CloverURI uri;
		InfoResult result;
		
		uri = CloverURI.createURI("ftp://test:test@koule/");
		result = manager.info(uri);
		assertTrue(result.success());
		assertTrue(result.isDirectory());
		System.out.println(result.getResult());

		uri = CloverURI.createURI("ftp://test:test@koule");
		result = manager.info(uri);
		assertTrue(result.success());
		assertTrue(result.isDirectory());
		System.out.println(result.getResult());
	}
	
	public void testRootResolve() throws Exception {
		CloverURI uri;
		ResolveResult result;
		
		uri = CloverURI.createURI("ftp://test:test@koule/*");
		result = manager.resolve(uri);
		assertTrue(result.success());
		System.out.println(result.getResult());

		uri = CloverURI.createURI("ftp://test:test@koule");
		result = manager.resolve(uri);
		assertTrue(result.success());
		assertEquals(1, result.totalCount());
		System.out.println(result.getResult());
	}

}
