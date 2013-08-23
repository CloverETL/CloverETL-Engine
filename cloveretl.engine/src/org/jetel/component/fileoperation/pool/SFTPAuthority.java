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

import java.io.File;
import java.io.FileFilter;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URI;
import java.net.URL;
import java.util.HashSet;
import java.util.Set;

import org.jetel.util.file.FileUtils;
import org.jetel.util.protocols.UserInfo;
import org.jetel.util.protocols.proxy.ProxyProtocolEnum;

public class SFTPAuthority extends AbstractAuthority implements Authority {
	
	/**
	 * The name of the directory where to look for private keys. 
	 */
	private static final String SSH_KEYS_DIR = "ssh-keys";

	private static final FileFilter KEY_FILE_FILTER = new FileFilter() {

		@Override
		public boolean accept(File pathname) {
			return pathname.getName().toLowerCase().endsWith(".key");
		}
		
	};

	private final Proxy proxy;
	private UserInfo proxyCredentials;
	private String proxyString = null;
	private Set<String> privateKeys = null;
	
	public SFTPAuthority(URL url, Proxy proxy) {
		super(url);
		this.proxy = proxy;
		loadPrivateKeys();
	}

	public SFTPAuthority(URL url, Proxy proxy, UserInfo proxyCredentials) {
		this(url, proxy);
		this.proxyCredentials = proxyCredentials;
	}

	public SFTPAuthority(URI uri, Proxy proxy) {
		super(uri);
		this.proxy = proxy;
		loadPrivateKeys();
	}

	public SFTPAuthority(URI uri, Proxy proxy, UserInfo proxyCredentials) {
		this(uri, proxy);
		this.proxyCredentials = proxyCredentials;
	}
	
	private void loadPrivateKeys() {
		File file = FileUtils.getJavaFile(null, SSH_KEYS_DIR);
		if ((file != null) && file.isDirectory()) {
			File[] keys = file.listFiles(KEY_FILE_FILTER);
			if ((keys != null) && (keys.length > 0)) {
				this.privateKeys = new HashSet<String>(keys.length);
				for (File key: keys) {
					this.privateKeys.add(key.getAbsolutePath());
				}
			}
		}
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
					sb.append(proxyCredentials.getUserInfo()).append('@');
				}
				InetSocketAddress address = (InetSocketAddress) proxy.address();
				sb.append(address.getHostName()).append(':').append(address.getPort());
			}
			proxyString = sb.toString();
		}
		
		return proxyString;
	}

	/**
	 * @return the privateKeys
	 */
	public Set<String> getPrivateKeys() {
		return privateKeys;
	}

	/*
	 * Overridden to include privateKeys.
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((privateKeys == null) ? 0 : privateKeys.hashCode());
		return result;
	}

	/*
	 * Overridden to include privateKeys.
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		SFTPAuthority other = (SFTPAuthority) obj;
		if (privateKeys == null) {
			if (other.privateKeys != null)
				return false;
		} else if (!privateKeys.equals(other.privateKeys))
			return false;
		return true;
	}

}