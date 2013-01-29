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

/**
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created Jan 29, 2013
 */
public abstract class AbstractAuthority implements Authority {

	public final String protocol;
	public final String userInfo;
	public final String host;
	public final int port;
	
	public AbstractAuthority(String protocol, String userInfo, String host, int port) {
		this.protocol = protocol;
		this.userInfo = userInfo;
		this.host = host;
		this.port = port;
	}

	public AbstractAuthority(URL url) {
		this(url.getProtocol(), url.getUserInfo(), url.getHost(), url.getPort());
	}

	public AbstractAuthority(URI uri) {
		this(uri.getScheme(), uri.getUserInfo(), uri.getHost(), uri.getPort());
	}

	/**
	 * @return the protocol
	 */
	public String getProtocol() {
		return protocol;
	}

	/**
	 * @return the userInfo
	 */
	public String getUserInfo() {
		return userInfo;
	}

	/**
	 * @return the host
	 */
	public String getHost() {
		return host;
	}

	/**
	 * @return the port
	 */
	public int getPort() {
		return port;
	}

	@Override
	public int hashCode() {
		String protocol = getProtocol();
		String host = getHost();
		int port = getPort();
		String userInfo = getUserInfo();
		String proxyString = getProxyString();
		
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
		AbstractAuthority other = (AbstractAuthority) obj;

		String protocol = getProtocol();
		String host = getHost();
		int port = getPort();
		String userInfo = getUserInfo();
		String proxyString = getProxyString();

		String otherProtocol = other.getProtocol();
		String otherHost = other.getHost();
		int otherPort = other.getPort();
		String otherUserInfo = other.getUserInfo();
		String otherProxyString = other.getProxyString();

		if (host == null) {
			if (otherHost != null)
				return false;
		} else if (!host.equals(otherHost))
			return false;
		if (port != otherPort)
			return false;
		if (protocol == null) {
			if (otherProtocol != null)
				return false;
		} else if (!protocol.equals(otherProtocol))
			return false;
		if (proxyString == null) {
			if (otherProxyString != null)
				return false;
		} else if (!proxyString.equals(otherProxyString))
			return false;
		if (userInfo == null) {
			if (otherUserInfo != null)
				return false;
		} else if (!userInfo.equals(otherUserInfo))
			return false;
		return true;
	}


}
