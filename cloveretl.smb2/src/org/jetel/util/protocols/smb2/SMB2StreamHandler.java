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
package org.jetel.util.protocols.smb2;

import java.io.IOException;
import java.net.Proxy;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;

/**
 * 
 * @author krivanekm
 */
public class SMB2StreamHandler extends URLStreamHandler {
	
	public static final SMB2StreamHandler INSTANCE = new SMB2StreamHandler();

	@Override
	protected URLConnection openConnection(URL u) throws IOException {
		return new SMB2URLConnection(u);
	}

	@Override
	protected URLConnection openConnection(URL u, Proxy p) throws IOException {
		return openConnection(u);
	}

	@Override
	protected void parseURL(URL u, String spec, int start, int limit) {
		super.parseURL(u, spec, start, limit);
		String protocol = u.getProtocol();
		if (!(protocol.equals(SMB2PathResolver.SMB2_PROTOCOL))) {
			throw new RuntimeException(String.format("Parse error: The URL protocol must be '%s'!", SMB2PathResolver.SMB2_PROTOCOL));
		}
	}

}
