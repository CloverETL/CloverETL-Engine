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
package org.jetel.component.fileoperation.pool;

import java.net.URI;
import java.net.URL;

public class DefaultAuthority extends AbstractAuthority implements Authority {
	
	public final String proxyString;
	
	public DefaultAuthority(String protocol, String userInfo, String host, int port, String proxyString) {
		super(protocol, userInfo, host, port);
		this.proxyString = proxyString;
	}

	public DefaultAuthority(URI uri, String proxy) {
		this(uri.getScheme(), uri.getUserInfo(), uri.getHost(), uri.getPort(), proxy);
	}
	
	public DefaultAuthority(URI uri) {
		this(uri, null);
	}

	public DefaultAuthority(URL url, String proxy) {
		this(url.getProtocol(), url.getUserInfo(), url.getHost(), url.getPort(), proxy);
	}

	public DefaultAuthority(URL url) {
		this(url, null);
	}
	
	/**
	 * @return the proxy
	 */
	@Override
	public String getProxyString() {
		return proxyString;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((host == null) ? 0 : host.hashCode());
		result = prime * result + port;
		result = prime * result
				+ ((protocol == null) ? 0 : protocol.hashCode());
		result = prime * result + ((proxyString == null) ? 0 : proxyString.hashCode());
		result = prime * result
				+ ((userInfo == null) ? 0 : userInfo.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		DefaultAuthority other = (DefaultAuthority) obj;
		if (host == null) {
			if (other.host != null)
				return false;
		} else if (!host.equals(other.host))
			return false;
		if (port != other.port)
			return false;
		if (protocol == null) {
			if (other.protocol != null)
				return false;
		} else if (!protocol.equals(other.protocol))
			return false;
		if (proxyString == null) {
			if (other.proxyString != null)
				return false;
		} else if (!proxyString.equals(other.proxyString))
			return false;
		if (userInfo == null) {
			if (other.userInfo != null)
				return false;
		} else if (!userInfo.equals(other.userInfo))
			return false;
		return true;
	}


	
}
