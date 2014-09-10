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

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.AccessDeniedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermissions;

import org.jetel.component.fileoperation.SimpleParameters.CreateParameters;
import org.jetel.component.fileoperation.result.CopyResult;
import org.jetel.component.fileoperation.result.InfoResult;
import org.jetel.component.fileoperation.result.ListResult;
import org.jetel.component.fileoperation.result.MoveResult;
import org.jetel.component.fileoperation.result.ResolveResult;
import org.jetel.util.exec.PlatformUtils;

public class LocalOperationHandlerTest extends OperationHandlerTestTemplate {
	
	private File rootTmpDir = null;
	protected IOperationHandler handler = null;
	
	protected File createTmpDir() {
		try {
			File tmpFile = File.createTempFile("CloverTemp", Long.toString(System.nanoTime()));
			if (tmpFile.delete() && tmpFile.mkdir()) {
				return tmpFile;
			}
		} catch (IOException ex) {}
		return null;
	}
	
	@Override
	protected IOperationHandler createOperationHandler() {
		return handler = new LocalOperationHandler();
	}
	
	@Override
	protected URI createBaseURI() {
		rootTmpDir = createTmpDir();
		if (rootTmpDir != null) {
			File tmp = rootTmpDir;
			try {
				tmp = rootTmpDir.getCanonicalFile();
			} catch (Exception ex) {}
			return tmp.toURI();
		}
		return null;
	}
	
	protected void delete(File file) {
		if (file == null) {
			return;
		}
		if (file.isDirectory()) {
			File[] children = file.listFiles();
			for (File child: children) {
				delete(child);
			}
		}
		if (!file.delete()) {
			System.err.println("Failed to delete " + file);
		}
	}

	@Override
	protected void tearDown() throws Exception {
		super.tearDown();
		delete(rootTmpDir);
		rootTmpDir = null;
	}

	@Override
	public void testGetPriority() {
		assertEquals(IOperationHandler.TOP_PRIORITY, handler.getPriority(Operation.copy(LocalOperationHandler.FILE_SCHEME, LocalOperationHandler.FILE_SCHEME)));
		assertEquals(IOperationHandler.TOP_PRIORITY, handler.getPriority(Operation.move(LocalOperationHandler.FILE_SCHEME, LocalOperationHandler.FILE_SCHEME)));
		assertEquals(IOperationHandler.TOP_PRIORITY, handler.getPriority(Operation.delete(LocalOperationHandler.FILE_SCHEME)));
		assertEquals(IOperationHandler.TOP_PRIORITY, handler.getPriority(Operation.create(LocalOperationHandler.FILE_SCHEME)));
		assertEquals(IOperationHandler.TOP_PRIORITY, handler.getPriority(Operation.resolve(LocalOperationHandler.FILE_SCHEME)));
		assertEquals(IOperationHandler.TOP_PRIORITY, handler.getPriority(Operation.info(LocalOperationHandler.FILE_SCHEME)));
		assertEquals(IOperationHandler.TOP_PRIORITY, handler.getPriority(Operation.list(LocalOperationHandler.FILE_SCHEME)));
		assertEquals(IOperationHandler.TOP_PRIORITY, handler.getPriority(Operation.read(LocalOperationHandler.FILE_SCHEME)));
		assertEquals(IOperationHandler.TOP_PRIORITY, handler.getPriority(Operation.write(LocalOperationHandler.FILE_SCHEME)));
	}

	@Override
	public void testCanPerform() {
		assertTrue(handler.canPerform(Operation.copy(LocalOperationHandler.FILE_SCHEME, LocalOperationHandler.FILE_SCHEME)));
		assertTrue(handler.canPerform(Operation.move(LocalOperationHandler.FILE_SCHEME, LocalOperationHandler.FILE_SCHEME)));
		assertTrue(handler.canPerform(Operation.delete(LocalOperationHandler.FILE_SCHEME)));
		assertTrue(handler.canPerform(Operation.create(LocalOperationHandler.FILE_SCHEME)));
		assertTrue(handler.canPerform(Operation.resolve(LocalOperationHandler.FILE_SCHEME)));
		assertTrue(handler.canPerform(Operation.info(LocalOperationHandler.FILE_SCHEME)));
		assertTrue(handler.canPerform(Operation.list(LocalOperationHandler.FILE_SCHEME)));
		assertTrue(handler.canPerform(Operation.read(LocalOperationHandler.FILE_SCHEME)));
		assertTrue(handler.canPerform(Operation.write(LocalOperationHandler.FILE_SCHEME)));
	}
	
	public void testNativePath() throws Exception {
		
		CloverURI uri;
		File file;
		
		uri = relativeURI("."); // the only platform-independent test
		file = new File(uri.getAbsoluteURI().getSingleURI().toURI());
		uri = CloverURI.createURI(file.getAbsolutePath());
		System.out.println(uri.getAbsoluteURI().getSingleURI().getPath());
		assertTrue(manager.info(uri).isDirectory());
		
		// TODO move these into a graph test that will run on Windows
//		uri = CloverURI.createURI("\\\\LINUXFILE\\share\\milan");
//		System.out.println(uri.getAbsoluteURI().getSingleURI().getPath());
//		assertTrue(manager.info(uri).isDirectory());
//
//		uri = CloverURI.createURI("//LINUXFILE/share/milan");
//		System.out.println(uri.getAbsoluteURI().getSingleURI().getPath());
//		assertTrue(manager.info(uri).isDirectory());
//		
//		uri = CloverURI.createURI("//LINUXFILE/share/m*l?n");
//		System.out.println(uri.getAbsoluteURI().getSingleURI().getPath());
//		ListResult listResult = manager.list(uri);
//		assertTrue(listResult.success());
//		List<Info> infos = listResult.getResult(0);
//		System.out.println(infos);
//		assertFalse(infos.isEmpty());

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
	public void testList() throws Exception {
		super.testList();
		
		CloverURI uri;
		ListResult result;
		for (File file: File.listRoots()) {
			if (file.exists()) {
				uri = CloverURI.createURI(file.toString());
				result = manager.list(uri);
				System.out.println(uri);
				assertTrue(result.success());
				System.out.println(result.getResult());
			}
		}

		if (PlatformUtils.isLinuxPlatform()) {
			uri = relativeURI("permissionsDir/file.tmp");
			assertTrue(manager.create(uri, new CreateParameters().setMakeParents(true)).success());
			uri = relativeURI("permissionsDir");
			Path file = Paths.get(uri.getSingleURI().getAbsoluteURI().toURI());
			try {
				Files.setPosixFilePermissions(file, PosixFilePermissions.fromString("rw-------"));
				result = manager.list(uri);
				assertFalse(result.success());
				assertTrue(result.getFirstError().getCause() instanceof AccessDeniedException);
			} finally {
				Files.setPosixFilePermissions(file, PosixFilePermissions.fromString("rwxrwxrwx"));
			}
		}
	}

	@Override
	public void testInfo() throws Exception {
		super.testInfo();
		
		CloverURI uri;
		InfoResult result;
		for (File file: File.listRoots()) {
			if (file.exists()) {
				uri = CloverURI.createURI(file.toString());
				result = manager.info(uri);
				System.out.println(uri);
				assertTrue(result.success());
				assertTrue(result.isDirectory());
				assertTrue(result.getName().isEmpty());
				System.out.println(result.getResult());
			}
		}
	}

	@Override
	public void testResolve() throws Exception {
		super.testResolve();
		
		CloverURI uri;
		ResolveResult result;
		
		for (File file: File.listRoots()) {
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
	
}
