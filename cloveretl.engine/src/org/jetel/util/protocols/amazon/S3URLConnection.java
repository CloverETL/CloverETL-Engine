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
package org.jetel.util.protocols.amazon;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Proxy;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;

import org.jetel.component.fileoperation.pool.Authority;
import org.jetel.component.fileoperation.pool.PooledS3Connection;
import org.jetel.component.fileoperation.pool.S3Authority;
import org.jetel.util.protocols.AbstractURLConnection;
import org.jetel.util.protocols.ProxyAuthenticable;
import org.jetel.util.protocols.ProxyConfiguration;
import org.jetel.util.protocols.UserInfo;

/**
 * S3 {@link URLConnection} that uses connection pooling.
 * 
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 19. 3. 2015
 */
public class S3URLConnection extends AbstractURLConnection<S3Authority> implements ProxyAuthenticable {
	
	private static URI toURI(URL url) throws IOException {
		try {
			return url.toURI();
		} catch (URISyntaxException e) {
			throw new IOException(e);
		}
	}
	
	/**
	 * @param url - plain URL without proxy
	 * @param proxy - {@link Proxy} instance
	 */
	public S3URLConnection(URL url, Proxy proxy) throws IOException {
		super(url, new S3Authority(toURI(url), ProxyConfiguration.toString(proxy, null)));
	}

	@Override
	protected PooledS3Connection connect(Authority authority) throws IOException {
		return (PooledS3Connection) super.connect(authority);
	}

	@Override
	public InputStream getInputStream() throws IOException {
		PooledS3Connection pooledConnection = connect(authority);
		return pooledConnection.getInputStream(getAuthority().getUri());
	}

	@Override
	public OutputStream getOutputStream() throws IOException {
		PooledS3Connection pooledConnection = connect(authority);
		return pooledConnection.getOutputStream(getAuthority().getUri());
	}

	@Override
	public void setProxyCredentials(UserInfo userInfo) {
		authority = authority.setProxyCredentials(userInfo);
	}

	
}
