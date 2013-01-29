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

import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URL;

import org.jetel.util.protocols.UserInfo;
import org.jetel.util.protocols.proxy.ProxyProtocolEnum;

public class URLAuthority extends AbstractAuthority implements Authority {
	
	private final URL url;
	private final Proxy proxy;
	private UserInfo proxyCredentials;
	private String proxyString = null;
	
	public URLAuthority(URL url, Proxy proxy) {
		this.url = url;
		this.proxy = proxy;
	}

	public URLAuthority(URL url, Proxy proxy, UserInfo proxyCredentials) {
		this(url, proxy);
		this.proxyCredentials = proxyCredentials;
	}

	@Override
	public String getProtocol() {
		return url.getProtocol();
	}

	@Override
	public String getUserInfo() {
		return url.getUserInfo();
	}

	@Override
	public String getHost() {
		return url.getHost();
	}

	@Override
	public int getPort() {
		return url.getPort();
	}

	public void setProxyCredentials(UserInfo proxyCredentials) {
		this.proxyCredentials = proxyCredentials;
		this.proxyString = null; // the proxyString might have changed
	}

	@Override
	public String getProxyString() {
		if (proxy == null) {
			return null;
		} else if (proxyString == null) {
			ProxyProtocolEnum type = null;
			switch (this.proxy.type()) {
			case DIRECT:
				type = ProxyProtocolEnum.NO_PROXY;
				break;
			case HTTP:
				type = ProxyProtocolEnum.PROXY_HTTP;
				break;
			case SOCKS:
				type = ProxyProtocolEnum.PROXY_SOCKS;
				break;
			}
			StringBuilder sb = new StringBuilder();
			sb.append(type.toString()).append("://");
			if (proxy.type() != Proxy.Type.DIRECT) {
				if ((proxyCredentials != null) && (proxyCredentials.getUserInfo() != null)) {
					sb.append(proxyCredentials.getUserInfo());
				}
				InetSocketAddress address = (InetSocketAddress) proxy.address();
				sb.append(address.getHostName()).append(':').append(address.getPort());
			}
			proxyString = sb.toString();
		}
		
		return proxyString;
	}

}