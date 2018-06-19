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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.net.URI;
import java.util.Arrays;
import java.util.List;

import org.jetel.component.fileoperation.SimpleParameters.ListParameters;
import org.jetel.component.fileoperation.SimpleParameters.ResolveParameters;
import org.jetel.component.fileoperation.result.ListResult;
import org.jetel.data.Defaults;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 15. 6. 2018
 */
@RunWith(MockitoJUnitRunner.class)
public class FileManagerTest {
	
	private FileManager manager = null;

	@Mock
	private IOperationHandler handler;
	
	@Before
	public void setUp() throws Exception {
		manager = FileManager.getInstance();
		manager.clear();
		manager.registerHandler(handler);
		
		Defaults.DEFAULT_PATH_SEPARATOR_REGEX = ";";
	}
	
	@Test
	public void testListFiles_CLO_13627() throws Exception {
		doReturn(true).when(handler).canPerform(Operation.resolve("ftp"));
		doReturn(true).when(handler).canPerform(Operation.list("ftp"));
		
		List<SingleCloverURIInfo> resolveResult = Arrays.asList( // CLO-13627: the bug was in SimpleInfo class
				new SingleCloverURIInfo(new SimpleInfo("a.txt", URI.create("ftp://localhost/dir/a.txt")).setType(Info.Type.FILE)),
				new SingleCloverURIInfo(new SimpleInfo("b.txt", URI.create("ftp://localhost/dir/b.txt")).setType(Info.Type.FILE))
		);
		doReturn(resolveResult).when(handler).resolve(any(SingleCloverURI.class), any(ResolveParameters.class));
		
		ListResult listResult = manager.list(CloverURI.createRelativeURI(URI.create("ftp://localhost/"), "dir/*.txt"));
		
		assertTrue(listResult.success());
		assertEquals(resolveResult, listResult.getResult());
		
		// listFiles should be invoked just once in FileManager.resolve()
		verify(handler, times(0)).list(any(SingleCloverURI.class), any(ListParameters.class));
	}

}
