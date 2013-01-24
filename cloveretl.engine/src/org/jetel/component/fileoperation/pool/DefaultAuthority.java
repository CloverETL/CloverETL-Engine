package org.jetel.component.fileoperation.pool;

import java.net.URI;
import java.net.URL;

public class DefaultAuthority implements Authority {
	public final String protocol;
	public final String userInfo;
	public final String host;
	public final int port;
	public final String proxyString;
	
	public DefaultAuthority(String protocol, String userInfo, String host, int port, String proxyString) {
		this.protocol = protocol;
		this.userInfo = userInfo;
		this.host = host;
		this.port = port;
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
	 * @return the scheme
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

	/**
	 * @return the proxy
	 */
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
