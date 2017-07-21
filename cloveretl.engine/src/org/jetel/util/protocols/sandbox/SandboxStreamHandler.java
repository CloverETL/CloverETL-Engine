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

import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;

import org.jetel.util.file.SandboxUrlUtils;

/**
 * 
 * @author mvarecha
 *
 */
public class SandboxStreamHandler extends URLStreamHandler {

	@Override
	public URLConnection openConnection(URL url) throws IOException {
		return new SandboxConnection(url);
	}
	
    @Override
	protected void parseURL(URL url, String spec, int start, int limit) {
    	super.parseURL(url, spec, start, limit);
    	String path = url.getPath();
    	String query = url.getQuery();
    	if (query!=null && !query.isEmpty()) {
    		StringBuilder pathBuilder = new StringBuilder(path);
    		pathBuilder.append("?").append(query);
    		query = null; // set to null - empty string would cause a "?" to be appended to the URL
    		path = pathBuilder.toString();
    	}
    	setURL(url, url.getProtocol(), url.getHost(), url.getPort(), url.getAuthority(), url.getUserInfo(), path, query, url.getRef());

    	if (!SandboxUrlUtils.isSandboxUrl(url)) {
    		throw new RuntimeException("Parse error: The URL protocol name must be sandbox!");
    	}
    }

}
