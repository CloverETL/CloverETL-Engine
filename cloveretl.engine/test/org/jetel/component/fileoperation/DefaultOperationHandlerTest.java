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

import static org.junit.Assume.assumeNotNull;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.Channels;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.jetel.component.fileoperation.SimpleParameters.CopyParameters;

public class DefaultOperationHandlerTest extends LocalOperationHandlerTest {
	
	private DefaultOperationHandler handler = null;
	
	private URI baseFtpUri;
	private URI baseHttpUri;
	private URI baseJarUri;
	
	@Override
	protected IOperationHandler createOperationHandler() {
		return handler = new DefaultOperationHandler() {

			@Override
			public boolean canPerform(Operation operation) {
				switch (operation.kind) {
					case RESOLVE:
						return manager.canPerform(Operation.info(operation.scheme()))
								&& manager.canPerform(Operation.list(operation.scheme()));
					default: 
						return super.canPerform(operation);
				}
			}
			
		};
	}
	
	@Override
	protected void setUp() throws Exception {
		super.setUp();
		baseFtpUri = new URI("ftp://test:test@koule/tmp/file_operation_tests/");
		baseHttpUri = new URI("http://milan.blansko.net/uploads/tests/");
		baseJarUri = new URI(String.format("jar:%stest.jar!/", baseUri));
		LocalOperationHandler localHandler = new LocalOperationHandler() {

			@Override
			public boolean canPerform(Operation operation) {
				switch (operation.kind) {
					case COPY:
					case MOVE:
					case RESOLVE:
						return false;
					default: 
						return super.canPerform(operation);
				}
			}
			
		};
		manager.registerHandler(VERBOSE ? new ObservableHandler(localHandler) : localHandler);
		
		URLOperationHandler urlHandler = new URLOperationHandler() {
			@Override
			public boolean canPerform(Operation operation) {
				switch (operation.kind) {
					case RESOLVE:
						return false;
					default: 
						return super.canPerform(operation);
				}
			}
		};
		manager.registerHandler(VERBOSE ? new ObservableHandler(urlHandler) : urlHandler);

		PooledFTPOperationHandler ftpHandler = new PooledFTPOperationHandler() {
			@Override
			public boolean canPerform(Operation operation) {
				switch (operation.kind) {
					case RESOLVE:
						return false;
					default: 
						return super.canPerform(operation);
				}
			}
		};
		manager.registerHandler(VERBOSE ? new ObservableHandler(ftpHandler) : ftpHandler);

		WebdavOperationHandler webdavHandler = new WebdavOperationHandler();
		manager.registerHandler(VERBOSE ? new ObservableHandler(webdavHandler) : webdavHandler);
	
		S3OperationHandler s3handler = new S3OperationHandler();
		manager.registerHandler(VERBOSE ? new ObservableHandler(s3handler) : s3handler);

		SFTPOperationHandler sftpHandler = new SFTPOperationHandler() {

			@Override
			public boolean canPerform(Operation operation) {
				switch (operation.kind) {
					case RESOLVE:
						return false;
					default: 
						return super.canPerform(operation);
				}
			}
			
		};
		manager.registerHandler(VERBOSE ? new ObservableHandler(sftpHandler) : sftpHandler);
	}

	@Override
	protected void tearDown() throws Exception {
		super.tearDown();
		handler = null;
		baseFtpUri = null;
		baseHttpUri = null;
		baseJarUri = null;
	}

	@Override
	public void testGetPriority() {
	}

	@Override
	public void testCanPerform() {
		assertTrue(handler.canPerform(Operation.copy(LocalOperationHandler.FILE_SCHEME, LocalOperationHandler.FILE_SCHEME)));
		assertTrue(handler.canPerform(Operation.move(LocalOperationHandler.FILE_SCHEME, LocalOperationHandler.FILE_SCHEME)));
	}
	
	protected CloverURI relativeFtpURI(String uri) throws URISyntaxException {
		return CloverURI.createRelativeURI(baseFtpUri, uri);
	}

	protected CloverURI relativeHttpURI(String uri) throws URISyntaxException {
		return CloverURI.createRelativeURI(baseHttpUri, uri);
	}

	protected CloverURI relativeJarURI(String uri) throws URISyntaxException {
		return CloverURI.createRelativeURI(baseJarUri, uri);
	}

	public static void zip(InputStream input, OutputStream dest, String entryName) {
		try {
			BufferedInputStream origin = null;
			ZipOutputStream out = new ZipOutputStream(new BufferedOutputStream(dest));
			byte data[] = new byte[IO_BUFFER_SIZE];
			origin = new BufferedInputStream(input, IO_BUFFER_SIZE);
			ZipEntry entry = new ZipEntry(entryName);
			out.putNextEntry(entry);
			int count;
			while ((count = origin.read(data, 0, IO_BUFFER_SIZE)) != -1) {
				out.write(data, 0, count);
			}
			origin.close();
			out.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void testCopy() throws Exception {
		super.testCopy();
		assumeNotNull(baseJarUri);
		String originalContent = "Žluťoučký kůň úpěl ďábelské ódy";
		String newContent = "New content";
		InputStream is = new ByteArrayInputStream(originalContent.getBytes(charset));
		OutputStream os = Channels.newOutputStream(manager.getOutput(relativeFtpURI("FTPReadTest.txt")).channel()); 
		copy(is, os);
		os.close();
		prepareData(relativeURI("FTPWriteTest.tmp"), newContent);
		
		CloverURI source;
		CloverURI target;
		
		String name = "FTPReadTest.txt";
		source = relativeFtpURI(name);
		target = relativeURI("FTP_" + name);
		assertTrue(manager.copy(source, target).success());
		assertEquals(originalContent, read(manager.getInput(target).channel()));
		
		source = relativeURI("FTPWriteT*.tmp");
		target = relativeFtpURI(name);
		assertTrue(manager.copy(source, target).success());
		assertEquals(newContent, read(manager.getInput(target).channel()));
		
		source = relativeURI("FTP_" + name);
		target = relativeFtpURI(name);
		assertTrue(manager.copy(source, target).success());
		assertEquals(originalContent, read(manager.getInput(target).channel()));
		
		source = relativeHttpURI(name);
		target = relativeURI("HTTP_" + name);
		assertTrue(manager.copy(source, target).success());
		assertEquals(originalContent, read(manager.getInput(target).channel()));

//		zip(Channels.newInputStream(manager.getInput(relativeURI("HTTP_" + name)).next()), Channels.newOutputStream(manager.getOutput(relativeURI("test.jar")).next()), "HTTP_" + name);
//		source = relativeJarURI("HTTP_" + name);
//		target = relativeURI("JAR_" + name);
//		assertTrue(manager.copy(source, target));
//		assertEquals(originalContent, read(manager.getInput(target).next()));
		
		source = CloverURI.createURI("ftp://ftproottest:test@koule:66/");
		target = relativeURI("copiedFtpRoot/");
		assertTrue(manager.create(target).success());
		assertTrue(manager.copy(source, target, new CopyParameters().setRecursive(true)).success());
		CloverURI checkUri = CloverURI.createSingleURI(target.getSingleURI().getAbsoluteURI().toURI(), "rootFtpFile.txt");
		assertTrue(manager.exists(checkUri));
	}

	@Override
	public void testMove() throws Exception {
		super.testMove(); // DefaultOperationHandler will perform MOVE operations instead of LocalOperationHandler
	}

	@Override
	public void testInfo() throws Exception {
	}

	@Override
	public void testGetInput() throws Exception {
	}

	@Override
	public void testGetOutput() throws Exception {
	}

	@Override
	public void testDelete() throws Exception {
	}

	@Override
	public void testResolve() throws Exception {
		super.testResolve();
	}

	@Override
	public void testList() throws Exception {
	}

	@Override
	public void testCreate() throws Exception {
	}
	
}
