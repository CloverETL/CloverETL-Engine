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

import java.net.URI;
import java.net.URISyntaxException;

import org.jetel.component.fileoperation.SimpleParameters.CreateParameters;
import org.jetel.component.fileoperation.result.CreateResult;
import org.jetel.component.fileoperation.result.InfoResult;
import org.jetel.component.fileoperation.result.ResolveResult;

/**
 * Tests FTP handler on Pure-FTPd, 
 * which supports MLST and MLSD commands. 
 * 
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 29. 4. 2014
 */
public class PooledFTPOperationHandlerPureFTPTest extends PooledFTPOperationHandlerTest {
	
	private static final String testingUri = "ftp://pureftpuser:test@koule:66/tmp/file_operation_tests/";

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
	public void testResolve() throws Exception {
		super.testResolve();

		CloverURI uri;
		ResolveResult result;
		
		// CLO-4062:
		String input = "ftp://wildcards:pas*word@koule:66/";
		uri = CloverURI.createURI(input);
		result = manager.resolve(uri);
		assertEquals(1, result.getResult().size());
		assertEquals(URI.create(input), result.getResult().get(0).toURI());

		input = "ftp://wildcards:pas*word@koule:66/wildcards/*.tmp";
		uri = CloverURI.createURI(input);
		result = manager.resolve(uri);
		assertEquals(1, result.getResult().size());
		assertEquals(URI.create("ftp://wildcards:pas*word@koule:66/wildcards/file.tmp"), result.getResult().get(0).toURI());
		
		input = "ftp://wildcards:pas*word@koule:66/wildcards/nonExistingFile.tmp";
		uri = CloverURI.createURI(input);
		result = manager.resolve(uri);
		assertEquals(1, result.getResult().size());
		assertEquals(URI.create(input), result.getResult().get(0).toURI());
	}

	@Override
	public void testRootInfo() throws Exception {
		CloverURI uri;
		InfoResult result;
		
		uri = CloverURI.createURI("ftp://ftproottest:test@koule:66/");
		result = manager.info(uri);
		assertTrue(result.success());
		assertTrue(result.isDirectory());
		assertTrue(result.getName().isEmpty());
		System.out.println(result.getResult());

		uri = CloverURI.createURI("ftp://ftproottest:test@koule:66");
		result = manager.info(uri);
		assertTrue(result.success());
		assertTrue(result.isDirectory());
		assertTrue(result.getName().isEmpty());
		System.out.println(result.getResult());
	}
}
