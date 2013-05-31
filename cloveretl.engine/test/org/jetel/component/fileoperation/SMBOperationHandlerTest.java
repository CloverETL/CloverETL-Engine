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
import java.net.URISyntaxException;
import java.util.Arrays;

import jcifs.smb.SmbFile;

import org.jetel.component.fileoperation.SimpleParameters.CreateParameters;
import org.jetel.component.fileoperation.SimpleParameters.DeleteParameters;
import org.jetel.component.fileoperation.result.CopyResult;
import org.jetel.component.fileoperation.result.CreateResult;
import org.jetel.component.fileoperation.result.DeleteResult;
import org.jetel.component.fileoperation.result.MoveResult;
import org.jetel.component.fileoperation.result.ResolveResult;

public class SMBOperationHandlerTest extends OperationHandlerTestTemplate {
	
	private static final String BASE_URI = "smb://javlin:javlin@VIRT-ORANGE/SMBTestPub/";

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
	
	public void testNativePath() throws Exception {
		
/*		TODO probably remove 
 		CloverURI uri;
		SmbFile file;
		
		uri = relativeURI("."); // the only platform-independent test
		file = new SmbFile(uri.getAbsoluteURI().getSingleURI().toString());
		uri = CloverURI.createURI(file.getCanonicalPath());
		System.out.println(uri.getAbsoluteURI().getSingleURI().getPath());
		assertTrue(manager.info(uri).isDirectory());
*/		
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
		/*
		 * TODO baaaah! do something reasonable or remove!
		 * 
		CloverURI uri;
		ListResult result;
		// TODO cannot list item "javlintest" of new SmbFile("smb://").listFiles() 
		for (SmbFile file : new SmbFile("smb://javlin:javlin@VIRT-ORANGE/").listFiles()) { // TODO may be slow
			printFileInfo(file);
			if (file.exists()) {
				uri = CloverURI.createURI(file.toString());
				result = manager.list(uri);
				System.out.println(uri);
				Exception e = result.getFirstError();
				if (e != null) throw e;
				assertTrue(result.success());
				System.out.println(result.getResult());
			}
		}
		*/
	}

	// TODO remove (or move to SmbFileInfo?)
	public static void printFileInfo(SmbFile file) throws IOException {
		System.out.println("-- " + file);
		System.out.print("   Type: ");
		switch (file.getType()) {
		case SmbFile.TYPE_COMM:
			System.out.println("communications device");
			break;
		case SmbFile.TYPE_FILESYSTEM:
			System.out.println("file system (file or directory)");
			break;
		case SmbFile.TYPE_NAMED_PIPE:
			System.out.println("named pipe");
			break;
		case SmbFile.TYPE_PRINTER:
			System.out.println("printer");
			break;
		case SmbFile.TYPE_SERVER:
			System.out.println("server");
			break;
		case SmbFile.TYPE_SHARE:
			System.out.println("share");
			break;
		case SmbFile.TYPE_WORKGROUP:
			System.out.println("workgroup");
			break;
		}
		System.out.println("   Server/Workgroup: " + file.getServer());
		System.out.println("   Share: " + file.getShare());
		
//		for (ACE a : file.getSecurity()) {
//			System.out.println("   ACE:  " + (!StringUtils.isEmpty(a.getSID().getDomainName()) ? a.getSID().getDomainName() + "\\" : "") + a.getSID().getAccountName() + ", allow=" + a.isAllow() + ", exec=" + ((a.getAccessMask() & ACE.FILE_EXECUTE) != 0));
//			
//		}
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
	
	public void testGetFile() throws IOException {
		// FIXME use unimprovised paths and non-local File
		SingleCloverURI uri = CloverURI.createSingleURI(null, "smb://tkramolis:mojeheslo@virt-pink/tkramolis/test.txt");
		try {
			manager.getFile(uri);
			fail();
		} catch (RuntimeException e) {
		}
		printFileInfo(new SmbFile(uri.toURI().toString()));
		
		uri = CloverURI.createSingleURI(null, "smb://tecra3/UNCTest/test.txt");
		printFileInfo(new SmbFile(uri.toURI().toString()));
		File f = manager.getFile(uri);
		assertTrue(f.exists());
	}
}
