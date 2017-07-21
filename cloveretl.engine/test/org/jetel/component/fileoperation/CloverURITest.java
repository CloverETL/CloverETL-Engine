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

import java.net.URI;
import java.util.List;

import org.jetel.test.CloverTestCase;
import org.jetel.util.exec.PlatformUtils;

/**
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 6.3.2012
 */
public class CloverURITest extends CloverTestCase {

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		initEngine();
	}

	@Override
	protected void tearDown() throws Exception {
		super.tearDown();
	}

	/**
	 * Test method for {@link org.jetel.component.fileoperation.CloverURI#resolve(org.jetel.component.fileoperation.CloverURI)}.
	 */
	public void testResolve() throws Exception {
		CloverURI baseUri = CloverURI.createURI("file://C:/myDire/?ctory/aaa/");
		CloverURI relativeUri = CloverURI.createURI("bbb");
//		CloverURI resolvedUri = baseUri.resolve(relativeUri);
//		System.out.println(resolvedUri);
		System.out.println(URI.create("ahoj/?nazdar/").resolve("fff"));
//		System.out.println(CloverURI.createURI("file://C:/myDire/?ctory/aaa/", "bbb"));
	}
	
	public void testCreateURI() throws Exception {
		String input = PlatformUtils.isWindowsPlatform() ? "file:/C:/Windows;C:/Windows" : "file:/home/milan;/home/milan";
		MultiCloverURI uri = (MultiCloverURI) CloverURI.createURI(input);
		List<SingleCloverURI> uris = uri.split();
		assertEquals(2, uris.size());
		for (SingleCloverURI u: uris) {
			assertTrue(u.getPath().startsWith("file:"));
		}
	}

}
