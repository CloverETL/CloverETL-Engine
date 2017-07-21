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
package org.jetel.util.protocols;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.Proxy;
import java.net.ProxySelector;
import java.net.SocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;

import org.jetel.util.file.FileUtils;
import org.jetel.util.protocols.proxy.ProxyProtocolEnum;

public class ProxyConfiguration {
	
	private static final int DEFAULT_PORT = 8080;
	
	private final String proxyString;
	private final Proxy proxy;
	private ProxySelector proxySelector;
	
	private String host = null;
	private int port = -1; // port number not specified
	
	private UserInfo userInfo;

	public ProxyConfiguration(String proxyString) {
		this.proxyString = proxyString;
		
		URL url = null;
		if (proxyString != null) {
	    	try {
	    		// parse hostname and port
	    		url = FileUtils.getFileURL(proxyString);
				this.host = url.getHost();
				this.port = url.getPort();
			} catch (MalformedURLException e) {}
		}
		
		this.proxy = getProxy(url);
	}
	
	private ProxyConfiguration(String host, int port, String user, String password) {
		this.proxy = getProxy(ProxyProtocolEnum.PROXY_HTTP, host, port);
		this.userInfo = new UserInfo(user, password);
		this.proxyString = null;
		
		this.host = host;
		this.port = port;
	}
	
	public boolean isProxyUsed() {
		return (proxy != null) && (proxy != Proxy.NO_PROXY);
	}
	
	public Proxy getProxy() {
		return proxy;
	}
	
	public ProxySelector getProxySelector() {
		if (proxySelector == null) {
			proxySelector = (proxy != null) ? new ProxySelector() {

				@Override
				public List<Proxy> select(URI uri) {
					// this should enforce no proxy even for direct: 
					return Arrays.asList(proxy);
				}

				@Override
				public void connectFailed(URI uri, SocketAddress sa, IOException ioe) {
				}
				
			} : null;
		}
		return proxySelector; // if null, ProxySelector.getDefault() will be used
	}
	
	public String getProxyString() {
		return proxyString;
	}
	
	public String getHost() {
		return host;
	}

	public int getPort() {
		return port;
	}

	/**
	 * @see ProxyConfiguration#ProxyConfiguration(String, int, String, String)
	 * @return
	 */
	public UserInfo getUserInfo() {
		if (userInfo == null) {
			if (proxyString != null) {
				try {
					URI uri = new URI(proxyString);
					String userInfo = uri.getUserInfo();
					this.userInfo = new UserInfo(userInfo);
				} catch (URISyntaxException ex) {
					// prevent the userInfo from being recomputed
					this.userInfo = new UserInfo(null);
				}
			} else {
				this.userInfo = new UserInfo(null);
			}
		}
		return userInfo;
	}
	
	public String getProxyUser() {
		return getUserInfo().getUser();
	}
	
	public String getProxyPassword() {
		return getUserInfo().getPassword();
	}
	
	public static ProxyConfiguration getSystemConfiguration(String protocol) {
		protocol = protocol.toLowerCase();
		String proxyHost = System.getProperty(protocol + ".proxyHost"); // null means any host
		String proxyPortString = System.getProperty(protocol + ".proxyPort");
		int proxyPort = -1; // -1 means any port
		if (proxyPortString != null) {
			proxyPort = Integer.parseInt(proxyPortString);
		}
		return new ProxyConfiguration(proxyHost, proxyPort, System.getProperty(protocol + ".proxyUser"), System.getProperty(protocol + ".proxyPassword"));
	}
	
    /**
     * Creates an proxy from the file url string.
     * @param fileURL
     * @return
     */
    public static Proxy getProxy(String fileURL) {
    	try {
    		// create an url
    		URL url = FileUtils.getFileURL(fileURL);
			return getProxy(url);
		} catch (MalformedURLException e) {
			return null;
		}
	}
    
    private static Proxy getProxy(URL proxyUrl) {
    	if (proxyUrl == null) {
    		return null;
    	}
		// get proxy type
		ProxyProtocolEnum proxyProtocolEnum;
    	if ((proxyProtocolEnum = ProxyProtocolEnum.fromString(proxyUrl.getProtocol())) == null) {
    		return null;
    	}
    	
    	return getProxy(proxyProtocolEnum, proxyUrl.getHost(), proxyUrl.getPort());
    }

    /**
     * Creates a proxy from the proxy type, hostname and port.
     * If the port number is negative, 8080 is used.
     * 
     * If proxy type is "direct", returns {@link Proxy#NO_PROXY}.
     * 
     * Returns <code>null</code> if host is <code>null</code>.
     * 
     * @param proxyProtocolEnum	- proxy type
     * @param host				- hostname
     * @param port				- port (if negative, 8080 is used)
     * 
     * @return {@link java.net.Proxy} instance or <code>null</code>
     */
    private static Proxy getProxy(ProxyProtocolEnum proxyProtocolEnum, String host, int port) {
		// no proxy
    	if (proxyProtocolEnum == ProxyProtocolEnum.NO_PROXY) {
    		return Proxy.NO_PROXY;
    	}
    	if (host == null) {
    		return null;
    	}
    	// create a proxy
    	SocketAddress addr = new InetSocketAddress(host, port < 0 ? DEFAULT_PORT : port);
    	Proxy proxy = new Proxy(Proxy.Type.valueOf(proxyProtocolEnum.getProxyString()), addr);
		return proxy;
    }

}