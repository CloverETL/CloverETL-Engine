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
package org.jetel.util.protocols.sandbox;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URL;

import org.jetel.data.Defaults;
import org.jetel.graph.runtime.IAuthorityProxy;
import org.jetel.graph.runtime.PrimitiveAuthorityProxy;
import org.jetel.util.classloader.GreedyURLClassLoader;
import org.junit.Ignore;

import junit.framework.TestCase;

/**
 * @author jan.michalica (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 29.8.2012
 */
@Ignore("ignored until CL-2497 resolved")
public class SandboxUrlClassLoaderTest extends TestCase {

	private static boolean afterInit = false;
	
	@Override
	protected void setUp() throws Exception {
		if (!afterInit) {
			Defaults.init();
			afterInit = true;
		}
	}
	
	public void testSanboxUrl() throws Exception {
		
		URL url = new URL(null, "sandbox:../cloveretl.ctlfunction/lib/json.jar", new SandboxStreamHandler());
		URL fileUrl = new File("../cloveretl.ctlfunction/lib/json.jar").getAbsoluteFile().toURI().toURL();
		
		IAuthorityProxy proxy = new PrimitiveAuthorityProxy() {
			
			@Override
			public InputStream getSandboxResourceInput(String storageCode, String path) {
				File file = new File(path);
				try {
					return new BufferedInputStream(new FileInputStream(file));
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
			
		};
		IAuthorityProxy.setDefaultProxy(proxy);
		//ClassLoader loader = new GreedyURLClassLoader(new URL[] {fileUrl});
		ClassLoader loader = new GreedyURLClassLoader(new URL[] {url});
		Class<?> klass = loader.loadClass("org.json.JSONObject");
		
		assertEquals("org.json.JSONObject", klass.getName());
		assertSame(loader, klass.getClassLoader());
	}
}
