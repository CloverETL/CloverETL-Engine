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
package org.jetel.util.protocols.sftp;

import java.io.IOException;
import java.net.Proxy;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.util.HashMap;
import java.util.Map;

import org.jetel.graph.ContextProvider;

/**
 * URLStreamHandler for sftp connection.
 * 
 * @author Jan Ausperger, Martin Slama (jan.ausperger@javlinconsulting.cz) (c) Javlin Consulting
 *         (www.javlinconsulting.cz)
 */
public class SFTPStreamHandler extends URLStreamHandler {

	/*
	 * Opened connection are cached for each component (component id is the key for the connection). Cached connection
	 * is re-used if possible. New connection is created just if it's necessary.
	 */
	private final Map<String, SFTPConnection> pool = new HashMap<String, SFTPConnection>();

	@Override
	public URLConnection openConnection(URL url) throws IOException {
		return openConnection(url, null);
	}

	@Override
	public URLConnection openConnection(URL url, Proxy proxy) throws IOException {
		String nodeId = ContextProvider.getComponentId();
		SFTPConnection connection = null;
		if (!connectionExists(url)) {
			connection = new SFTPConnection(url, proxy, this);
			pool.put(nodeId, connection);
		} else {
			connection = pool.get(nodeId);
		}
		connection.setURL(url);
		return connection;
	}

	@Override
	protected void parseURL(URL u, String spec, int start, int limit) {
		super.parseURL(u, spec, start, limit);
		String protocol = u.getProtocol();
		if (!(protocol.equals("sftp") || protocol.equals("scp"))) {
			throw new RuntimeException("Parse error: The URL protocol must be sftp or scp!");
		}
	}

	/**
	 * Removes given connection from the pool.
	 * 
	 * @param connection
	 *            Connection to be removed.
	 * @return True if connection was removed, false otherwise.
	 */
	public boolean removeFromPool(SFTPConnection connection) {
		for (Map.Entry<String, SFTPConnection> entries : pool.entrySet()) {
			if (connection == entries.getValue()) {
				return pool.remove(entries.getKey()) != null;
			}
		}
		return false;
	}

	private boolean connectionExists(URL url) {
		SFTPConnection connection = pool.get(ContextProvider.getComponentId());
		if (connection != null) {
			URL connectionURL = connection.getURL();
			boolean connectionExists = connectionURL.getPort() == url.getPort();
			if (connectionExists && connectionURL.getHost() != null) {
				connectionExists = connectionURL.getHost().equals(url.getHost());
			}
			if (connectionExists && connectionURL.getProtocol() != null) {
				connectionExists = connectionURL.getProtocol().equals(url.getProtocol());
			}
			if (connectionExists && connectionURL.getAuthority() != null) {
				connectionExists = connectionURL.getAuthority().equals(url.getAuthority());
			}
			if (connectionExists && connectionURL.getUserInfo() != null) {
				connectionExists = connectionURL.getUserInfo().equals(url.getUserInfo());
			}
			return connectionExists;
		}
		return false;
	}

}
