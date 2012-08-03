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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.jetel.component.fileoperation.SimpleParameters.CreateParameters;
import org.jetel.component.fileoperation.SimpleParameters.DeleteParameters;

public class SFTPOperationHandlerTest extends OperationHandlerTestTemplate {

	private static final String testingUri = "sftp://test:test@koule/home/test/tmp/file_operation_tests/";

	private SFTPOperationHandler handler = null;

	@Override
	protected IOperationHandler createOperationHandler() {
		return handler = new SFTPOperationHandler();
	}
	
	@Override
	protected URI createBaseURI() {
		try {
			URI base = new URI(testingUri);
			CloverURI tmpDirUri = CloverURI.createURI(base.resolve(String.format("CloverTemp%d/", System.nanoTime())));
			manager.create(tmpDirUri, new CreateParameters().setDirectory(true));
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
		manager.delete(CloverURI.createURI(baseUri), new DeleteParameters().setRecursive(true));
		super.tearDown();
		handler = null;
	}

	@Override
	public void testGetPriority() {
//		assertEquals(Integer.MAX_VALUE, handler.getSpeed(Operation.copy(SFTPOperationHandler.SFTP_SCHEME, SFTPOperationHandler.SFTP_SCHEME)));
		assertEquals(Integer.MAX_VALUE, handler.getPriority(Operation.move(SFTPOperationHandler.SFTP_SCHEME, SFTPOperationHandler.SFTP_SCHEME)));
		assertEquals(Integer.MAX_VALUE, handler.getPriority(Operation.delete(SFTPOperationHandler.SFTP_SCHEME)));
		assertEquals(Integer.MAX_VALUE, handler.getPriority(Operation.create(SFTPOperationHandler.SFTP_SCHEME)));
		assertEquals(Integer.MAX_VALUE, handler.getPriority(Operation.resolve(SFTPOperationHandler.SFTP_SCHEME)));
		assertEquals(Integer.MAX_VALUE, handler.getPriority(Operation.info(SFTPOperationHandler.SFTP_SCHEME)));
		assertEquals(Integer.MAX_VALUE, handler.getPriority(Operation.list(SFTPOperationHandler.SFTP_SCHEME)));
		assertEquals(Integer.MAX_VALUE, handler.getPriority(Operation.read(SFTPOperationHandler.SFTP_SCHEME)));
		assertEquals(Integer.MAX_VALUE, handler.getPriority(Operation.write(SFTPOperationHandler.SFTP_SCHEME)));
	}

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
	
	
}
