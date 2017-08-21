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
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.jetel.component.fileoperation.SimpleParameters.CreateParameters;
import org.jetel.component.fileoperation.SimpleParameters.DeleteParameters;
import org.jetel.component.fileoperation.result.CreateResult;
import org.jetel.component.fileoperation.result.DeleteResult;
import org.jetel.component.fileoperation.result.ListResult;

public class SMB2OperationHandlerTest extends OperationHandlerTestTemplate {
	
	private static final String BASE_URI = "smb2://domain%3BAdministrator:semafor4@virt-pink/smbtest/";

	protected IOperationHandler handler = null;
	
	@Override
	protected IOperationHandler createOperationHandler() {
		return handler = new SMB2OperationHandler();
	}
	
	protected String getBaseUri() {
		return BASE_URI;
	}
	
	@Override
	protected URI createBaseURI() {
		URI base = URI.create(getBaseUri());
		CloverURI tmpDirUri = CloverURI.createURI(base.resolve(String.format("CloverTemp%d/", System.nanoTime())));
		CreateResult result = manager.create(tmpDirUri, new CreateParameters().setDirectory(true));
		if (result.getFirstError() != null) throw new RuntimeException(result.getFirstError());
		assertTrue(result.getFirstErrorMessage(), result.success());
		return tmpDirUri.getSingleURI().toURI();
	}
	
	@Override
	protected void tearDown() throws Exception {
		Thread.interrupted(); // reset the interrupted flag of the current thread
		DeleteResult result = manager.delete(CloverURI.createURI(baseUri), new DeleteParameters().setRecursive(true));
		if (!result.success()) {
			System.err.println("Failed to delete " + result.getURI(0));
			if (result.getFirstError() != null) {
				result.getFirstError().printStackTrace();
			}
		}
		super.tearDown();
		handler = null;
	}

	@Override
	public void testGetPriority() {
		assertEquals(IOperationHandler.TOP_PRIORITY, handler.getPriority(Operation.copy(SMB2OperationHandler.SMB_SCHEME, SMBOperationHandler.SMB_SCHEME)));
		assertEquals(IOperationHandler.TOP_PRIORITY, handler.getPriority(Operation.move(SMB2OperationHandler.SMB_SCHEME, SMBOperationHandler.SMB_SCHEME)));
		assertEquals(IOperationHandler.TOP_PRIORITY, handler.getPriority(Operation.delete(SMB2OperationHandler.SMB_SCHEME)));
		assertEquals(IOperationHandler.TOP_PRIORITY, handler.getPriority(Operation.create(SMB2OperationHandler.SMB_SCHEME)));
		assertEquals(IOperationHandler.TOP_PRIORITY, handler.getPriority(Operation.resolve(SMB2OperationHandler.SMB_SCHEME)));
		assertEquals(IOperationHandler.TOP_PRIORITY, handler.getPriority(Operation.info(SMB2OperationHandler.SMB_SCHEME)));
		assertEquals(IOperationHandler.TOP_PRIORITY, handler.getPriority(Operation.list(SMB2OperationHandler.SMB_SCHEME)));
		assertEquals(IOperationHandler.TOP_PRIORITY, handler.getPriority(Operation.read(SMB2OperationHandler.SMB_SCHEME)));
		assertEquals(IOperationHandler.TOP_PRIORITY, handler.getPriority(Operation.write(SMB2OperationHandler.SMB_SCHEME)));
	}

	@Override
	public void testCanPerform() {
		assertTrue(handler.canPerform(Operation.copy(SMB2OperationHandler.SMB_SCHEME, SMB2OperationHandler.SMB_SCHEME)));
		assertTrue(handler.canPerform(Operation.move(SMB2OperationHandler.SMB_SCHEME, SMB2OperationHandler.SMB_SCHEME)));
		assertTrue(handler.canPerform(Operation.delete(SMB2OperationHandler.SMB_SCHEME)));
		assertTrue(handler.canPerform(Operation.create(SMB2OperationHandler.SMB_SCHEME)));
		assertTrue(handler.canPerform(Operation.resolve(SMB2OperationHandler.SMB_SCHEME)));
		assertTrue(handler.canPerform(Operation.info(SMB2OperationHandler.SMB_SCHEME)));
		assertTrue(handler.canPerform(Operation.list(SMB2OperationHandler.SMB_SCHEME)));
		assertTrue(handler.canPerform(Operation.read(SMB2OperationHandler.SMB_SCHEME)));
		assertTrue(handler.canPerform(Operation.write(SMB2OperationHandler.SMB_SCHEME)));
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
	protected long getTolerance() {
		return 1000;
	}
	
	@Override
	protected String getRootDirName() {
		return "VIRT-ORANGE";
	}

	public void testAdministrativeShare() throws Exception {
		URI uri = new URI("smb2://administrator:semafor@VIRT-ORANGE/ADMIN$/");
		SingleCloverURI cloverURI = CloverURI.createSingleURI(uri);
		ListResult result = manager.list(cloverURI);
		
		assertTrue(result.successCount() >= 50); // at least say 50 files in the "Windows" directory

		// check that some expected files are in list result
		Set<URI> expectedFiles = new HashSet<URI>(Arrays.asList(
				uri.resolve("Boot"),
				uri.resolve("system"),
				uri.resolve("System32"),
				uri.resolve("explorer.exe"),
				uri.resolve("system.ini"),
				uri.resolve("syconfig.INI")
				));
		
		for (Info i : result.getResult()) {
			expectedFiles.remove(i.getURI());
		}
		assertTrue("Some expected files were not listed: " + expectedFiles, expectedFiles.isEmpty());
	}
}
