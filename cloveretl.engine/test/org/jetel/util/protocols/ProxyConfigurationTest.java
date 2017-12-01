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
package org.jetel.util.protocols;

import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.Proxy;
import java.net.Proxy.Type;
import java.net.URL;

import org.apache.commons.lang3.StringUtils;
import org.jetel.test.CloverTestCase;
import org.jetel.util.file.FileUtils;

/**
 * @author martin (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 24. 10. 2017
 */
public class ProxyConfigurationTest extends CloverTestCase {

	public void testToString() {
		Proxy proxy = new Proxy(Type.HTTP, new InetSocketAddress("koule", 3128));
		assertEquals("proxy://koule:3128", ProxyConfiguration.toString(proxy, null));
		assertEquals("proxy://koule:3128", ProxyConfiguration.toString(proxy, new UserInfo(null)));
		assertEquals("proxy://koule:3128", ProxyConfiguration.toString(proxy, new UserInfo("")));
		assertEquals("proxy://login@koule:3128", ProxyConfiguration.toString(proxy, new UserInfo("login")));
		assertEquals("proxy://user:password@koule:3128", ProxyConfiguration.toString(proxy, new UserInfo("user:password")));
		assertEquals("proxy://user:password@koule:3128", ProxyConfiguration.toString(proxy, new UserInfo("user", "password")));
		assertEquals("proxy://user:password@koule:3128", ProxyConfiguration.toString(proxy, new UserInfo("user", "password")));
		
		proxy = FileUtils.getProxy("proxy://test:test@koule");
		// 8080 is the default port, should we remove it from the proxy configuration string?
		assertEquals("proxy://user:password@koule:8080", ProxyConfiguration.toString(proxy, new UserInfo("user", "password")));
		
		proxy = new Proxy(Type.SOCKS, new InetSocketAddress("koule", 3129));
		assertEquals("proxysocks://user:password@koule:3129", ProxyConfiguration.toString(proxy, new UserInfo("user", "password")));

		assertEquals("direct:", ProxyConfiguration.toString(Proxy.NO_PROXY, null));
		assertEquals("direct:", ProxyConfiguration.toString(Proxy.NO_PROXY, new UserInfo("user:password")));
		assertEquals("direct:", ProxyConfiguration.toString(Proxy.NO_PROXY, new UserInfo("user", "password")));
	}
	
	private boolean isProxy(String fileURL) {
		return ProxyConfiguration.isProxy(fileURL);
	}
	
	public void testIsProxy() {
		assertTrue(isProxy("proxy://user:p%40ssword@hostname:8080"));
		assertTrue(isProxy("proxy://user:p%40ssword@hostname:8080/path"));
		assertTrue(isProxy("proxy://user:p%40ssword@hostname.com"));
		assertTrue(isProxy("proxy://hostname.com"));
		assertTrue(isProxy("proxy://hostname.com:3128"));
		assertTrue(isProxy("proxysocks://user:p%40ssword@hostname.com:8080"));
		assertTrue(isProxy("direct:"));

		assertFalse(isProxy(""));
		assertFalse(isProxy(null));
		assertFalse(isProxy("ftp://user:password@hostname.com"));
	}
	
	private void testGetProxy(Proxy expected, String fileURL) {
		Proxy proxy = ProxyConfiguration.getProxy(fileURL);
		assertEquals(expected, proxy);
		if (expected == Proxy.NO_PROXY) {
			assertSame(expected, proxy);
		}
	}
	
	public void testGetProxy() {
		testGetProxy(new Proxy(Proxy.Type.HTTP, new InetSocketAddress("hostname.com", 8080)), "proxy://hostname.com:8080");
		testGetProxy(new Proxy(Proxy.Type.HTTP, new InetSocketAddress("hostname.com", 8080)), "proxy://hostname.com"); // default port
		testGetProxy(new Proxy(Proxy.Type.HTTP, new InetSocketAddress("hostname", 3128)), "proxy://user:p%40ssword@hostname:3128");

		testGetProxy(new Proxy(Proxy.Type.SOCKS, new InetSocketAddress("hostname.com", 8080)), "proxysocks://hostname.com:8080");
		testGetProxy(new Proxy(Proxy.Type.SOCKS, new InetSocketAddress("hostname.com", 8080)), "proxysocks://hostname.com"); // default port
		testGetProxy(new Proxy(Proxy.Type.SOCKS, new InetSocketAddress("hostname", 3128)), "proxysocks://user:p%40ssword@hostname:3128");

		testGetProxy(Proxy.NO_PROXY, "direct:");

		testGetProxy(null, "");
		testGetProxy(null, null);
		testGetProxy(null, "ftp://hostname.com");
	}
	
	private void testGetProxyURL(String fileURL) throws MalformedURLException {
		URL expected = !StringUtils.isEmpty(fileURL) ? newProxyURL(fileURL) : null;
		URL proxyUrl = ProxyConfiguration.getProxyUrl(fileURL);
		assertEquals(expected, proxyUrl);
		if (expected != null) {
			assertEquals(fileURL, proxyUrl.toExternalForm());
		} else {
			assertNull(proxyUrl);
		}
	}

	private URL newProxyURL(String fileURL) throws MalformedURLException {
		return new URL(null, fileURL, FileUtils.proxyHandler);
	}
	
	public void testGetProxyURL() throws Exception {
		testGetProxyURL("direct:");

		testGetProxyURL("proxy://hostname");
		testGetProxyURL("proxy://hostname.com");
		testGetProxyURL("proxy://hostname.com:8080");
		testGetProxyURL("proxy://hostname.com:3128");
		testGetProxyURL("proxy://user:password@hostname.com:3128");
		testGetProxyURL("proxy://user:p%40ssword@hostname.com:3128/path");

		testGetProxyURL("proxysocks://hostname");
		testGetProxyURL("proxysocks://hostname.com");
		testGetProxyURL("proxysocks://hostname.com:8080");
		testGetProxyURL("proxysocks://hostname.com:3128");
		testGetProxyURL("proxysocks://user:password@hostname.com:3128");
		testGetProxyURL("proxysocks://user:p%40ssword@hostname.com:3128/path");

		testGetProxyURL("");
		testGetProxyURL(null);

		assertEquals(null, ProxyConfiguration.getProxyUrl("ftp://hostname.com"));
	}
	
}
