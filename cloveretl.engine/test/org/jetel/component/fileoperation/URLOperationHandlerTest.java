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

import static org.jetel.component.fileoperation.CloverURI.createURI;
import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.List;

public class URLOperationHandlerTest extends OperationHandlerTestTemplate {
	
	private URLOperationHandler handler = null;
	
	@Override
	protected IOperationHandler createOperationHandler() {
		return handler = new URLOperationHandler();
	}
	
	@Override
	protected URI createBaseURI() {
		return null;
	}
	
	@Override
	protected void setBaseURI() {
	}
	
	@Override
	protected void setUp() throws Exception {
		super.setUp();
		WebdavOperationHandler webdavHandler = new WebdavOperationHandler();
		manager.registerHandler(VERBOSE ? new ObservableHandler(webdavHandler) : webdavHandler);
		S3OperationHandler s3handler = new S3OperationHandler();
		manager.registerHandler(VERBOSE ? new ObservableHandler(s3handler) : s3handler);
	}

	@Override
	protected void tearDown() throws Exception {
		super.tearDown();
		handler = null;
	}

	@Override
	public void testGetPriority() {
	}

	@Override
	public void testCanPerform() {
	}

	@Override
	public void testCopy() throws Exception {
	}

	@Override
	public void testInfo() throws Exception {
		CloverURI uri;
		Info info;

		uri = createURI("ftp://test:test@koule/tmp/file_operation_tests/");
		System.out.println(uri.getAbsoluteURI());
		info = manager.info(uri).getInfo();
		assertNotNull(info);
		assertTrue(info.isFile());
		
		uri = createURI("ftp://test:test@koule/tmp/file_operation_tests/FTPReadTest.txt");
		System.out.println(uri.getAbsoluteURI());
		info = manager.info(uri).getInfo();
		assertNotNull(info);
		assertTrue(info.isFile());

		uri = createURI("ftp://test:test@koule/");
		System.out.println(uri.getAbsoluteURI());
		info = manager.info(uri).getInfo();
		assertNotNull(info);
		assertTrue(info.isFile());
		assertTrue(info.getName().isEmpty());

		uri = createURI("ftp://test:test@koule");
		System.out.println(uri.getAbsoluteURI());
		info = manager.info(uri).getInfo();
		assertNotNull(info);
		assertTrue(info.isFile());
		assertTrue(info.getName().isEmpty());

		uri = createURI("http://www.cloveretl.com/");
		System.out.println(uri.getAbsoluteURI());
		info = manager.info(uri).getInfo();
		assertNotNull(info);
		assertTrue(info.isFile());
		assertTrue(info.getName().isEmpty());

		uri = createURI("http://www.cloveretl.com");
		System.out.println(uri.getAbsoluteURI());
		info = manager.info(uri).getInfo();
		assertNotNull(info);
		assertTrue(info.isFile());
		assertTrue(info.getName().isEmpty());

		uri = createURI("http:(direct:)//www.cloveretl.com/");
		System.out.println(uri.getAbsoluteURI());
		info = manager.info(uri).getInfo();
		assertNotNull(info);
		assertTrue(info.isFile());
		assertTrue(info.getName().isEmpty());

		uri = createURI("http:(direct:)//www.cloveretl.com");
		System.out.println(uri.getAbsoluteURI());
		info = manager.info(uri).getInfo();
		assertNotNull(info);
		assertTrue(info.isFile());
		assertTrue(info.getName().isEmpty());
	}

	@Override
	public void testMove() throws Exception {
	}

	@Override
	public void testGetInput() throws Exception {
		CloverURI uri;
		ReadableByteChannel channel;
		
		String text = "Žluťoučký kůň úpěl ďábelské ódy";

//		uri = createURI("http://milan.blansko.net/uploads/tests/FTPReadTest.txt");
//		channel = manager.getInput(uri).channel();
//		assertEquals(text, read(channel));
//
//		uri = createURI("ftp://milan:nalim@ftp.blansko.net/MILAN/uploads/tests/FTPReadTest.txt");
//		channel = manager.getInput(uri).channel();
//		assertEquals(text, read(channel));

		uri = createURI("ftp://test:test@koule/tmp/file_operation_tests/FTPReadTest.txt");
		channel = manager.getInput(uri).channel();
		assertEquals(text, read(channel));

		uri = createURI("ftp:(direct:)//test:test@koule/tmp/file_operation_tests/FTPReadTest.txt");
		channel = manager.getInput(uri).channel();
		assertEquals(text, read(channel));

		uri = createURI("http://www.cloveretl.com");
		channel = manager.getInput(uri).channel();
		assertTrue(read(channel).length() > 0);

		uri = createURI("http:(direct:)//www.cloveretl.com");
		channel = manager.getInput(uri).channel();
		assertTrue(read(channel).length() > 0);

//		uri = createURI("ftp:(proxysocks://proxytest:proxytest@127.0.0.1:1080)//test:test@koule/tmp/file_operation_tests/FTPReadTest.txt");
//		channel = manager.getInput(uri).channel();
//		assertTrue(read(channel).length() > 0);

//		uri = createURI("http:(proxy://195.70.145.15:8000)//www.cloveretl.com");
//		channel = manager.getInput(uri).channel();
//		assertTrue(read(channel).length() > 0);
		
		uri = createURI("http://anonymous:@koule:22401/repository/default/oracle.dat");
		channel = manager.getInput(uri).channel();
		assertTrue(read(channel).length() > 0);

	}

	@Override
	public void testGetOutput() throws Exception {
		CloverURI uri;
		WritableByteChannel outputChannel;
		ReadableByteChannel inputChannel;
		
		String text = "Žluťoučký kůň úpěl ďábelské ódy";
		String newText = "New Content";

		uri = createURI("ftp://test:test@koule/tmp/file_operation_tests/FTPWriteTest.txt");
		outputChannel = manager.getOutput(uri).channel();
		assertTrue(write(outputChannel, text));
		
		try {
			inputChannel = manager.getInput(uri).channel();
			assertEquals(text, read(inputChannel));
		} catch (IOException ioe) {
			System.err.println("Failed to read from " + uri);
		}

		outputChannel = manager.getOutput(uri).channel();
		assertTrue(write(outputChannel, newText));
		
		try {
			inputChannel = manager.getInput(uri).channel();
			assertEquals(newText, read(inputChannel));
		} catch (IOException ioe) {
			System.err.println("Failed to read from " + uri);
		}

		uri = createURI("ftp:(direct:)//test:test@koule/tmp/file_operation_tests/FTPWriteTestDirectProxy.txt");
		outputChannel = manager.getOutput(uri).channel();
		assertTrue(write(outputChannel, text));
		
		try {
			inputChannel = manager.getInput(uri).channel();
			assertEquals(text, read(inputChannel));
		} catch (IOException ioe) {
			System.err.println("Failed to read from " + uri);
		}

		outputChannel = manager.getOutput(uri).channel();
		assertTrue(write(outputChannel, newText));
		
		try {
			inputChannel = manager.getInput(uri).channel();
			assertEquals(newText, read(inputChannel));
		} catch (IOException ioe) {
			System.err.println("Failed to read from " + uri);
		}
		
//		uri = createURI("http://www.cloveretl.com/documentation/quickstart/index.html");
//		outputChannel = manager.getOutput(uri).channel();
//		assertFalse(write(outputChannel, text)); // not a WebDAV
		
	}

	@Override
	public void testDelete() throws Exception {
	}

	@Override
	public void testResolve() throws Exception {
	}

	@Override
	public void testList() throws Exception {
		CloverURI uri;
		List<Info> result;
		
		uri = createURI("ftp://test:test@koule/tmp/file_operation_tests/;ftp://test:test@koule/tmp/file_operation_tests/FTPReadTest.txt");
		System.out.println(uri.getAbsoluteURI());
		result = manager.list(uri).getResult();
		assertTrue(result != null && !result.isEmpty());
		System.out.println(result);
	}

	@Override
	public void testCreate() throws Exception {
	}

	@Override
	public void testCreateDated() throws Exception {
	}
	
	@Override
	public void testSpecialCharacters() throws Exception {
		String text = "žluťoučký kůň úpěl ďábelské ódy";

		File tmpFile = File.createTempFile("CloverTemp žluťoučký kůň", ".tmp");
		tmpFile.deleteOnExit();
		assumeTrue(tmpFile.exists());
		assumeTrue(write(new FileOutputStream(tmpFile).getChannel(), text));
		
		System.out.println(tmpFile.toURI());
		CloverURI uri = createURI(tmpFile.toURI());
		assertTrue(text.equals(read(manager.getInput(uri).channel())));
		
		tmpFile.delete();
	}

	@Override
	public void testInterruptDelete() throws Exception {
	}

	@Override
	public void testInterruptCopy() throws Exception {
	}

	@Override
	public void testInterruptMove() throws Exception {
	}

	@Override
	public void testInterruptList() throws Exception {
	}
	
}
