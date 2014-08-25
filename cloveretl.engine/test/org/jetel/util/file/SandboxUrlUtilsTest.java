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
package org.jetel.util.file;

import java.net.MalformedURLException;

import org.jetel.test.CloverTestCase;

/**
 * @author Martin Janik, Javlin a.s. &lt;martin.janik@javlin.eu&gt;
 *
 * @version 3.1.0
 * @since 3.1.0
 */
public class SandboxUrlUtilsTest extends CloverTestCase {

	private static final String SANDBOX_URL = SandboxUrlUtils.SANDBOX_PROTOCOL_URL_PREFIX + "default";
	private static final String SANDBOX_GRAPH_FILE = "graph/someGraph.grf";
	private static final String SANDBOX_GRAPH_URL = SANDBOX_URL + '/' + SANDBOX_GRAPH_FILE;

	/**
	 * Test method for {@link org.jetel.util.file.SandboxUrlUtils#isSandboxUrl(java.lang.String)}.
	 */
	public void testIsSandboxUrl() {
		// valid sandbox URLs
		assertTrue(SandboxUrlUtils.isSandboxUrl(SANDBOX_URL));
		assertTrue(SandboxUrlUtils.isSandboxUrl(SANDBOX_GRAPH_URL));

		// invalid sandbox URLs
		assertFalse(SandboxUrlUtils.isSandboxUrl("sandbox.grf"));
		assertFalse(SandboxUrlUtils.isSandboxUrl("file://default/graph/someGraph.grf"));
	}

	/**
	 * Test method for {@link org.jetel.util.file.SandboxUrlUtils#getSandboxName(java.lang.String)}.
	 */
	public void testGetSandboxName() {
		assertEquals("default", SandboxUrlUtils.getSandboxName(SANDBOX_URL));
		assertEquals("default", SandboxUrlUtils.getSandboxName(SANDBOX_GRAPH_URL));
	}

	public void testGetSandboxPath_url() throws MalformedURLException {
		assertEquals("", SandboxUrlUtils.getSandboxPath(FileUtils.getFileURL("sandbox://default")));
		assertEquals("/", SandboxUrlUtils.getSandboxPath(FileUtils.getFileURL("sandbox://default/")));
		assertEquals("/graph/someGraph.grf", SandboxUrlUtils.getSandboxPath(FileUtils.getFileURL("sandbox://default/graph/someGraph.grf")));
		try {
			SandboxUrlUtils.getSandboxPath(FileUtils.getFileURL("/graph/someGraph.grf"));
			assertTrue(false);
		} catch (IllegalArgumentException e) {
			//CORECT
		}
	}
	
	/**
	 * Test method for {@link org.jetel.util.file.SandboxUrlUtils#getRelativeUrl(java.lang.String)}.
	 */
	public void testGetRelativeUrl() {
		assertEquals(".", SandboxUrlUtils.getRelativeUrl(SANDBOX_URL));
		assertEquals(".", SandboxUrlUtils.getRelativeUrl(SANDBOX_URL + '/'));
		assertEquals(SANDBOX_GRAPH_FILE, SandboxUrlUtils.getRelativeUrl(SANDBOX_GRAPH_URL));
	}

	public void testGetSandboxUrl() throws MalformedURLException {
		assertEquals("sandbox://mySandbox/", SandboxUrlUtils.getSandboxUrl("mySandbox", null).toString());
		assertEquals("sandbox://mySandbox/path", SandboxUrlUtils.getSandboxUrl("mySandbox", "path").toString());
		assertEquals("sandbox://mySandbox/path/to/folder/data.txt", SandboxUrlUtils.getSandboxUrl("mySandbox", "path/to/folder/data.txt").toString());
	}
	
	public void testGetSandboxPath() {
		assertEquals("sandbox://xxx/yyy", SandboxUrlUtils.getSandboxPath("xxx", "yyy"));
		assertEquals("sandbox://xxx/yyy", SandboxUrlUtils.getSandboxPath("xxx/", "yyy"));
		assertEquals("sandbox://xxx/yyy/", SandboxUrlUtils.getSandboxPath("xxx/", "yyy/"));
		assertEquals("sandbox://xxx/yyy/", SandboxUrlUtils.getSandboxPath("xxx", "yyy/"));
		assertEquals("sandbox://xxx/y/yy/", SandboxUrlUtils.getSandboxPath("xxx", "y/yy/"));
		assertEquals("sandbox://xxx/yy/y", SandboxUrlUtils.getSandboxPath("xxx", "yy/y"));
	}
	
	public void testJoinSandboxUrls() {
		assertEquals("sandbox://test/data.txt", SandboxUrlUtils.joinSandboxUrls("xxx", "sandbox://test/data.txt"));
		assertEquals("sandbox://test/data.txt", SandboxUrlUtils.joinSandboxUrls("", "sandbox://test/data.txt"));
		assertEquals("sandbox://test/data.txt", SandboxUrlUtils.joinSandboxUrls(null, "sandbox://test/data.txt"));
		
		assertEquals("sandbox://test/some/data.txt", SandboxUrlUtils.joinSandboxUrls("sandbox://test/foo.txt", "some/data.txt"));

		assertEquals("some/data.txt", SandboxUrlUtils.joinSandboxUrls("foo.txt", "some/data.txt")); 
		assertEquals("some/data.txt", SandboxUrlUtils.joinSandboxUrls("", "some/data.txt")); 
		assertEquals("some/data.txt", SandboxUrlUtils.joinSandboxUrls(null, "some/data.txt")); 
	}
	
}
