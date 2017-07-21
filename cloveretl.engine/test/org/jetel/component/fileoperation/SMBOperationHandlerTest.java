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
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import jcifs.smb.SmbFile;

import org.jetel.component.fileoperation.SimpleParameters.CreateParameters;
import org.jetel.component.fileoperation.SimpleParameters.DeleteParameters;
import org.jetel.component.fileoperation.result.CopyResult;
import org.jetel.component.fileoperation.result.CreateResult;
import org.jetel.component.fileoperation.result.DeleteResult;
import org.jetel.component.fileoperation.result.ListResult;
import org.jetel.component.fileoperation.result.MoveResult;
import org.jetel.component.fileoperation.result.ResolveResult;

public class SMBOperationHandlerTest extends OperationHandlerTestTemplate {
	
	private static final String BASE_URI = "smb://virt-orange%3BSMBTest:p%40ss%7B%2F%7D@VIRT-ORANGE/SMBTestPub/";   // unescaped password: p@ss{/}    and %3B is ';'

	protected IOperationHandler handler = null;
	
	@Override
	protected IOperationHandler createOperationHandler() {
		return handler = new SMBOperationHandler();
	}
	
	@Override
	protected URI createBaseURI() {
		try {
			URI base = new URI(BASE_URI);
			CloverURI tmpDirUri = CloverURI.createURI(base.resolve(String.format("CloverTemp%d/", System.nanoTime())));
			CreateResult result = manager.create(tmpDirUri, new CreateParameters().setDirectory(true));
			if (result.getFirstError() != null) throw new RuntimeException(result.getFirstError());
			assertTrue(result.getFirstErrorMessage(), result.success());
			return tmpDirUri.getSingleURI().toURI();
		} catch (URISyntaxException ex) {
			ex.printStackTrace();
			return null;
		}
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
		assertEquals(IOperationHandler.TOP_PRIORITY, handler.getPriority(Operation.copy(SMBOperationHandler.SMB_SCHEME, SMBOperationHandler.SMB_SCHEME)));
		assertEquals(IOperationHandler.TOP_PRIORITY, handler.getPriority(Operation.move(SMBOperationHandler.SMB_SCHEME, SMBOperationHandler.SMB_SCHEME)));
		assertEquals(IOperationHandler.TOP_PRIORITY, handler.getPriority(Operation.delete(SMBOperationHandler.SMB_SCHEME)));
		assertEquals(IOperationHandler.TOP_PRIORITY, handler.getPriority(Operation.create(SMBOperationHandler.SMB_SCHEME)));
		assertEquals(IOperationHandler.TOP_PRIORITY, handler.getPriority(Operation.resolve(SMBOperationHandler.SMB_SCHEME)));
		assertEquals(IOperationHandler.TOP_PRIORITY, handler.getPriority(Operation.info(SMBOperationHandler.SMB_SCHEME)));
		assertEquals(IOperationHandler.TOP_PRIORITY, handler.getPriority(Operation.list(SMBOperationHandler.SMB_SCHEME)));
		assertEquals(IOperationHandler.TOP_PRIORITY, handler.getPriority(Operation.read(SMBOperationHandler.SMB_SCHEME)));
		assertEquals(IOperationHandler.TOP_PRIORITY, handler.getPriority(Operation.write(SMBOperationHandler.SMB_SCHEME)));
	}

	@Override
	public void testCanPerform() {
		assertTrue(handler.canPerform(Operation.copy(SMBOperationHandler.SMB_SCHEME, SMBOperationHandler.SMB_SCHEME)));
		assertTrue(handler.canPerform(Operation.move(SMBOperationHandler.SMB_SCHEME, SMBOperationHandler.SMB_SCHEME)));
		assertTrue(handler.canPerform(Operation.delete(SMBOperationHandler.SMB_SCHEME)));
		assertTrue(handler.canPerform(Operation.create(SMBOperationHandler.SMB_SCHEME)));
		assertTrue(handler.canPerform(Operation.resolve(SMBOperationHandler.SMB_SCHEME)));
		assertTrue(handler.canPerform(Operation.info(SMBOperationHandler.SMB_SCHEME)));
		assertTrue(handler.canPerform(Operation.list(SMBOperationHandler.SMB_SCHEME)));
		assertTrue(handler.canPerform(Operation.read(SMBOperationHandler.SMB_SCHEME)));
		assertTrue(handler.canPerform(Operation.write(SMBOperationHandler.SMB_SCHEME)));
	}
	
	@Override
	public void testCopy() throws Exception {
		super.testCopy();
		
		CloverURI source;
		CloverURI target;
		CopyResult result;
		
		source = relativeURI("W.TMP");
		if (manager.exists(source)) { // case insensitive file system
			target = relativeURI("w.tmp");
			result = manager.copy(source, target);
			assertFalse(result.success());
			assertTrue(manager.exists(source));
		}
	}

	@Override
	public void testMove() throws Exception {
		super.testMove();
		
		CloverURI source;
		CloverURI target;
		MoveResult result;
		
		source = relativeURI("U.TMP");
		if (manager.exists(source)) { // case insensitive file system
			target = relativeURI("u.tmp");
			result = manager.move(source, target);
			assertFalse(result.success());
			assertTrue(manager.exists(source));
		}
		
	}
	
	@Override
	public void testResolve() throws Exception {
		super.testResolve();
		
		CloverURI uri;
		ResolveResult result;
		
		for (SmbFile file : Arrays.asList(new SmbFile("smb://"))) { // hmmm, creates a bit strange "smb:////" URL, but seems to work anyway...
			if (file.exists()) {
				uri = CloverURI.createURI(file.toString() + "*");
				result = manager.resolve(uri);
				System.out.println(uri);
				assertTrue(result.success());
				System.out.println(result.getResult());

				uri = CloverURI.createURI(file.toString());
				result = manager.resolve(uri);
				System.out.println(uri);
				assertTrue(result.success());
				assertEquals(1, result.totalCount());
				System.out.println(result.getResult());
			}
		}
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
	
	public void testAdministrativeShare() throws Exception {
		URI uri = new URI("smb://administrator:semafor@VIRT-ORANGE/ADMIN$/");
		SingleCloverURI cloverURI = CloverURI.createSingleURI(uri);
		ListResult result = manager.list(cloverURI);
		
		assertTrue(result.successCount() >= 50); // at least say 50 files in the "Windows" directory

		// check that some expected files are in list result
		Set<URI> expectedFiles = new HashSet<URI>(Arrays.asList(
				uri.resolve("Boot/"),
				uri.resolve("system/"),
				uri.resolve("System32/"),
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
