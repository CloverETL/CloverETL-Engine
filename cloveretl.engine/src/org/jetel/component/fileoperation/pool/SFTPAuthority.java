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
import java.net.MalformedURLException;
import java.net.Proxy;
import java.net.URI;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.jetel.component.fileoperation.CloverURI;
import org.jetel.component.fileoperation.FileManager;
import org.jetel.component.fileoperation.Info;
import org.jetel.component.fileoperation.result.InfoResult;
import org.jetel.component.fileoperation.result.ListResult;
import org.jetel.graph.ContextProvider;
import org.jetel.util.file.FileUtils;
import org.jetel.util.protocols.UserInfo;
import org.jetel.util.protocols.proxy.ProxyProtocolEnum;
import org.jetel.util.string.StringUtils;

public class SFTPAuthority extends AbstractAuthority implements Authority {
	
	private static final FileManager manager = FileManager.getInstance();
	
	/**
	 * The name of the directory where to look for private keys. 
	 */
	private static final String SSH_KEYS_DIR = "ssh-keys";
	
	public static final String CONTEXT_PARAMETER_KEY_SSH_KEYS_DIR = "CONTEXT_PARAMETER_KEY_SSH_KEYS_DIR";
	
	private static boolean accept(String filename) {
		return filename.toLowerCase().endsWith(".key");
	}
	
	private static String getKeyName(String filename) {
		return filename.substring(0, filename.length() - 4);
	}

	private static final FileFilter KEY_FILE_FILTER = new FileFilter() {

		@Override
		public boolean accept(File file) {
			return SFTPAuthority.accept(file.getName());
		}
		
	};

	private final Proxy proxy;
	private UserInfo proxyCredentials;
	private String proxyString = null;
	private Map<String, URI> privateKeys = null;
	
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
		// CLO-5529: do not load private keys if password is specified
		if (!StringUtils.isEmpty(userInfo) && (userInfo.indexOf(':') >= 0)) {
			String[] parts = userInfo.split(":");
			String password = (parts.length > 1) ? parts[1] : "";
			if (!StringUtils.isEmpty(password)) {
				return;
			}
		}
		
		try {
			URL url = null;
			
			// try SSH keys directory path from thread context
			String sshKeysDir = (String)ContextProvider.getContextParameter(CONTEXT_PARAMETER_KEY_SSH_KEYS_DIR);
			if (sshKeysDir != null){
				url = FileUtils.getFileURL((URL)null, sshKeysDir);
			} else {
				// ssh-key subdir of project root
				url = FileUtils.getFileURL(ContextProvider.getContextURL(), SSH_KEYS_DIR);
			}
			
			File file = null;
			try {
				// optimization, conversion to File usually works at runtime
				file = FileUtils.convertUrlToFile(url);
			} catch (MalformedURLException e) {}
			
			if (file != null) {
				if (file.isDirectory()) {
					File[] keys = file.listFiles(KEY_FILE_FILTER);
					if ((keys != null) && (keys.length > 0)) {
						this.privateKeys = new HashMap<String, URI>(keys.length);
						for (File key: keys) {
							this.privateKeys.put(getKeyName(key.getName()), key.toURI());
						}
					}
				}
			} else { // CLO-6175: conversion to File failed, try listing the directory using FileManager
				CloverURI uri = CloverURI.createSingleURI(url.toURI());
				InfoResult dirInfo = manager.info(uri);
				if (dirInfo.isDirectory()) {
					ListResult listResult = manager.list(uri);
					if (listResult.success()) {
						List<Info> files = listResult.getResult();
						Map<String, URI> keys = new HashMap<>(files.size());
						for (Info info: files) {
							String name = info.getName();
							if (accept(name)) {
								keys.put(getKeyName(name), info.getURI());
							}
						}
						if (!keys.isEmpty()) {
							this.privateKeys = keys;
						}
					}
				}
			}
			
		} catch (Exception ex) {}
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
	public Map<String, URI> getPrivateKeys() {
		return privateKeys;
	}

	/*
	 * Overridden to include privateKeys.
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + Objects.hashCode(privateKeys);
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