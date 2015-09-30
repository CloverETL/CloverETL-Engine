/*
 * CloverETL Engine - Java based ETL application framework.
 * Copyright (c) Javlin, a.s. (info@cloveretl.com).  Use is subject to license terms.
 *
 * www.cloveretl.com
 */
package com.opensys.cloveretl.component;

import static org.junit.Assume.assumeTrue;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Date;

import org.jetel.component.fileoperation.CloverURI;
import org.jetel.component.fileoperation.DefaultOperationHandler;
import org.jetel.component.fileoperation.IOperationHandler;
import org.jetel.component.fileoperation.ObservableHandler;
import org.jetel.component.fileoperation.Operation;
import org.jetel.component.fileoperation.OperationHandlerTestTemplate;
import org.jetel.component.fileoperation.S3CopyOperationHandler;
import org.jetel.component.fileoperation.S3OperationHandler;
import org.jetel.component.fileoperation.SimpleParameters;
import org.jetel.component.fileoperation.URIUtils;
import org.jetel.component.fileoperation.SimpleParameters.CreateParameters;
import org.jetel.component.fileoperation.SimpleParameters.DeleteParameters;
import org.jetel.component.fileoperation.pool.PooledS3Connection;
import org.jetel.component.fileoperation.pool.S3Authority;
import org.jetel.component.fileoperation.result.CreateResult;
import org.jetel.component.fileoperation.result.DeleteResult;
import org.jetel.component.fileoperation.result.ListResult;
import org.jetel.util.ExceptionUtils;
import org.jetel.util.file.FileUtils;

/**
 * This test contains S3 credentials, therefore has been moved to commercial repository.
 * 
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 18. 3. 2015
 */
public class S3OperationHandlerTest extends OperationHandlerTestTemplate {

	private S3OperationHandler handler;
	
	private static final String rootUri = "s3://AKIAJLUEHALGLF23IOYQ:h3GFbUGJa8Put4VCJN4v9nyQAQQk00pUBUU6RXfA@s3.amazonaws.com";
	private static final String testingUri = rootUri + "/cloveretl.engine.test/test-fo/";
	
	@Override
	protected IOperationHandler createOperationHandler() {
		return handler = new S3OperationHandler();
	}

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		DefaultOperationHandler defaultHandler = new DefaultOperationHandler();
		manager.registerHandler(VERBOSE ? new ObservableHandler(defaultHandler) : defaultHandler);
		S3CopyOperationHandler copyHandler = new S3CopyOperationHandler();
		manager.registerHandler(VERBOSE ? new ObservableHandler(copyHandler) : copyHandler);
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
	protected URI createBaseURI() {
		try {
			URI base = new URI(testingUri);
			CloverURI tmpDirUri = CloverURI.createURI(base.resolve(String.format("CloverTemp%d/", System.nanoTime())));
			CreateResult result = manager.create(tmpDirUri, new CreateParameters().setDirectory(true).setMakeParents(true));
			assumeTrue(result.getFirstErrorMessage(), result.success());
			return tmpDirUri.getSingleURI().toURI();
		} catch (URISyntaxException ex) {
			return null;
		}
	}

	@Override
	public void testGetPriority() {
		assertEquals(IOperationHandler.TOP_PRIORITY, handler.getPriority(Operation.copy(S3OperationHandler.S3_SCHEME, S3OperationHandler.S3_SCHEME)));
		assertEquals(IOperationHandler.TOP_PRIORITY, handler.getPriority(Operation.move(S3OperationHandler.S3_SCHEME, S3OperationHandler.S3_SCHEME)));
		assertEquals(IOperationHandler.TOP_PRIORITY, handler.getPriority(Operation.delete(S3OperationHandler.S3_SCHEME)));
		assertEquals(IOperationHandler.TOP_PRIORITY, handler.getPriority(Operation.create(S3OperationHandler.S3_SCHEME)));
		assertEquals(IOperationHandler.TOP_PRIORITY, handler.getPriority(Operation.resolve(S3OperationHandler.S3_SCHEME)));
		assertEquals(IOperationHandler.TOP_PRIORITY, handler.getPriority(Operation.info(S3OperationHandler.S3_SCHEME)));
		assertEquals(IOperationHandler.TOP_PRIORITY, handler.getPriority(Operation.list(S3OperationHandler.S3_SCHEME)));
		assertEquals(IOperationHandler.TOP_PRIORITY, handler.getPriority(Operation.read(S3OperationHandler.S3_SCHEME)));
		assertEquals(IOperationHandler.TOP_PRIORITY, handler.getPriority(Operation.write(S3OperationHandler.S3_SCHEME)));
	}

	@Override
	public void testCanPerform() {
		assertTrue(handler.canPerform(Operation.copy(S3OperationHandler.S3_SCHEME, S3OperationHandler.S3_SCHEME)));
		assertTrue(handler.canPerform(Operation.move(S3OperationHandler.S3_SCHEME, S3OperationHandler.S3_SCHEME)));
		assertTrue(handler.canPerform(Operation.delete(S3OperationHandler.S3_SCHEME)));
		assertTrue(handler.canPerform(Operation.create(S3OperationHandler.S3_SCHEME)));
		assertTrue(handler.canPerform(Operation.resolve(S3OperationHandler.S3_SCHEME)));
		assertTrue(handler.canPerform(Operation.info(S3OperationHandler.S3_SCHEME)));
		assertTrue(handler.canPerform(Operation.list(S3OperationHandler.S3_SCHEME)));
		assertTrue(handler.canPerform(Operation.read(S3OperationHandler.S3_SCHEME)));
		assertTrue(handler.canPerform(Operation.write(S3OperationHandler.S3_SCHEME)));
	}

	// overridden - setting last modified date is not supported
	@Override
	public void testCreateDated() throws Exception {
		CloverURI uri;
		CreateResult result;
		UnsupportedOperationException exception;
				
		uri = relativeURI("lastModified.tmp");
		result = manager.create(uri, new CreateParameters().setMakeParents(true).setLastModified(new Date()));
		assertFalse(result.success());
		assertFalse(manager.exists(uri));
		exception = (UnsupportedOperationException) ExceptionUtils.getRootCause(result.getFirstError());
		assertEquals("Setting last modification date is not supported by S3", exception.getMessage());
		
		uri = relativeURI("lastModifiedDir");
		result = manager.create(uri, new CreateParameters().setMakeParents(true).setDirectory(true).setLastModified(new Date()));
		assertFalse(result.success());
		assertFalse(manager.exists(uri));
		exception = (UnsupportedOperationException) ExceptionUtils.getRootCause(result.getFirstError());
		assertEquals("Setting last modification date is not supported by S3", exception.getMessage());
	}

	@Override
	protected void generate(URI root, int depth) throws IOException {
		int i = 0;
		for ( ; i < 2; i++) {
			String name = String.valueOf(i);
			URI child = URIUtils.getChildURI(root, name);
			manager.create(CloverURI.createSingleURI(child));
		}
	}

	@Override
	public void testList() throws Exception {
		super.testList();
		
		// test listing a newly created bucket - used to throw IllegalArgumentException: Illegal Capacity: -1
		CloverURI emptyBucket = CloverURI.createSingleURI(URI.create(rootUri), "cloveretl.test.empty.bucket");
		try {
			assumeTrue(manager.create(emptyBucket, new CreateParameters().setDirectory(true)).success());
			ListResult listResult = manager.list(emptyBucket);
			assertTrue(ExceptionUtils.stackTraceToString(listResult.getFirstError()), listResult.success());
		} finally {
			DeleteResult deleteResult = manager.delete(emptyBucket, new DeleteParameters().setRecursive(true));
			if (!deleteResult.success()) {
				System.err.println("Failed to delete " + deleteResult.getURI(0));
			}
		}
	}
	
	public void testGetSecretKey() {
		URI uri = URI.create("s3://ACCESSKEY:5XyJ3MFWZKd%2BBJ4C3ushhLQYIXBqbNTSK7EDzXLw@s3.amazonaws.com/jharazim.redshift.etl");
		S3Authority authority = new S3Authority(uri);
		String secretKey = PooledS3Connection.getSecretKey(authority);
		assertEquals("5XyJ3MFWZKd+BJ4C3ushhLQYIXBqbNTSK7EDzXLw", secretKey);
		
		authority = new S3Authority(URI.create("s3://AKIAIN22BDZO35DANLGQ:JazDFBhDlMaJwKO5c6pDSzuKFW0LMTV%2FfVeszyEo@s3.amazonaws.com"));
		assertEquals("JazDFBhDlMaJwKO5c6pDSzuKFW0LMTV/fVeszyEo", PooledS3Connection.getSecretKey(authority));
	}
	
	/**
	 * CLO-6688:
	 * 
	 * @throws IOException
	 */
	public void testLeak() throws IOException {
		for (int i = 0; i < 5; i++) {
			System.out.println("Opening connection #" + (i+1));
			FileUtils.getInputStream(null, rootUri + "/cloveretl.engine.test/employees.dat");
			System.gc();
		}
	}
	
}
