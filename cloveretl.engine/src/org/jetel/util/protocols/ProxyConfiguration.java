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

import java.io.IOException;
import java.net.Proxy;
import java.net.ProxySelector;
import java.net.SocketAddress;
import java.net.URI;
import java.util.Arrays;
import java.util.List;

import org.jetel.util.file.FileUtils;

public class ProxyConfiguration {
	private final String proxyString;
	private final Proxy proxy;
	private ProxySelector proxySelector;

	public ProxyConfiguration(String proxyString) {
		this.proxyString = proxyString;
		this.proxy = (proxyString != null) ? FileUtils.getProxy(proxyString) : null;
		this.proxySelector = (proxy != null) ? new ProxySelector() {

			@Override
			public List<Proxy> select(URI uri) {
				// this should enforce no proxy even for direct: 
				return Arrays.asList(proxy);
			}

			@Override
			public void connectFailed(URI uri, SocketAddress sa, IOException ioe) {
			}
			
		} : null;
	}
	
	public Proxy getProxy() {
		return proxy;
	}
	
	public ProxySelector getProxySelector() {
		return proxySelector; // if null, ProxySelector.getDefault() will be used
	}
	
	public String getProxyString() {
		return proxyString;
	}
	
}