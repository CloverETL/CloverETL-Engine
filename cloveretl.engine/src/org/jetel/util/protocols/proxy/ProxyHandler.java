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
package org.jetel.util.protocols.proxy;

import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.util.Arrays;

/**
 * URLHandler for proxy.
 * 
 * @author Jan Ausperger (jan.ausperger@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 */
public class ProxyHandler extends URLStreamHandler {

	@Override
	public URLConnection openConnection(URL url) throws IOException {
		throw new UnsupportedOperationException("The opentConnection method is not supported for the ProxyHandler!");
	}
	
    @Override
	protected void parseURL(URL u, String spec, int start, int limit) {
    	super.parseURL(u, spec, start, limit);

    	String protocol = u.getProtocol();
    	if (!acceptProtocol(protocol)) {
    		throw new RuntimeException("Parse error: The URL protocol have to be one of " + Arrays.toString(ProxyProtocolEnum.values()));
    	}
    }

    public static boolean acceptProtocol(String protocol) {
    	return ProxyProtocolEnum.fromString(protocol) != null;
    }
    
}
