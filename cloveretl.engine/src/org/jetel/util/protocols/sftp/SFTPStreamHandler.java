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

/**
 * URLStreamHandler for sftp connection.
 * 
 * @author Jan Ausperger (jan.ausperger@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 */
public class SFTPStreamHandler extends URLStreamHandler {
	
	private static SFTPConnection connection = null;
	
	@Override
	public URLConnection openConnection(URL url) throws IOException {
		return openConnection(url, null);
	}

	@Override
	public URLConnection openConnection(URL url, Proxy proxy) throws IOException {
		if (!connectionExists(url)) {
			connection = new SFTPConnection(url, proxy);
		}
		connection.setURL(url);
		return connection;
	}

    protected void parseURL(URL u, String spec, int start, int limit) {
    	super.parseURL(u, spec, start, limit);
    	String protocol = u.getProtocol();
    	if (!(protocol.equals("sftp") || protocol.equals("scp"))) {
    		throw new RuntimeException("Parse error: The URL protocol must be sftp or scp!");
    	}
    }
    
    private boolean connectionExists(URL url) {
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
