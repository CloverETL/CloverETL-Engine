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

import junit.framework.TestCase;

/**
 * @author Martin Janik, Javlin a.s. &lt;martin.janik@javlin.eu&gt;
 *
 * @version 3.1.0
 * @since 3.1.0
 */
public class SandboxUtilsTest extends TestCase {

	private static final String SANDBOX_URL = SandboxUtils.SANDBOX_PROTOCOL_URL_PREFIX + "default";
	private static final String SANDBOX_GRAPH_FILE = "graph/someGraph.grf";
	private static final String SANDBOX_GRAPH_URL = SANDBOX_URL + '/' + SANDBOX_GRAPH_FILE;

	/**
	 * Test method for {@link org.jetel.util.file.SandboxUtils#isSandboxUrl(java.lang.String)}.
	 */
	public void testIsSandboxUrl() {
		// valid sandbox URLs
		assertTrue(SandboxUtils.isSandboxUrl(SANDBOX_URL));
		assertTrue(SandboxUtils.isSandboxUrl(SANDBOX_GRAPH_URL));

		// invalid sandbox URLs
		assertFalse(SandboxUtils.isSandboxUrl("sandbox.grf"));
		assertFalse(SandboxUtils.isSandboxUrl("file://default/graph/someGraph.grf"));
	}

	/**
	 * Test method for {@link org.jetel.util.file.SandboxUtils#getSandboxName(java.lang.String)}.
	 */
	public void testGetSandboxName() {
		assertEquals("default", SandboxUtils.getSandboxName(SANDBOX_URL));
		assertEquals("default", SandboxUtils.getSandboxName(SANDBOX_GRAPH_URL));
	}

	/**
	 * Test method for {@link org.jetel.util.file.SandboxUtils#getRelativeUrl(java.lang.String)}.
	 */
	public void testGetRelativeUrl() {
		assertEquals(".", SandboxUtils.getRelativeUrl(SANDBOX_URL));
		assertEquals(".", SandboxUtils.getRelativeUrl(SANDBOX_URL + '/'));
		assertEquals(SANDBOX_GRAPH_FILE, SandboxUtils.getRelativeUrl(SANDBOX_GRAPH_URL));
	}

}
