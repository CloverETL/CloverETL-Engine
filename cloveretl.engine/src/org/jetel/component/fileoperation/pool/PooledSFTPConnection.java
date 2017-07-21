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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.Proxy.Type;
import java.net.URI;
import java.net.URLDecoder;
import java.text.MessageFormat;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.util.file.FileUtils;
import org.jetel.util.protocols.sftp.URLUserInfo;
import org.jetel.util.protocols.sftp.URLUserInfoIteractive;
import org.jetel.util.string.StringUtils;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Proxy;
import com.jcraft.jsch.ProxyHTTP;
import com.jcraft.jsch.ProxySOCKS4;
import com.jcraft.jsch.ProxySOCKS5;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.UserInfo;

public class PooledSFTPConnection extends AbstractPoolableConnection {
	
	/**
	 * Async close timeout (seconds).
	 */
	private static final int CLOSE_TIMEOUT = 10; // 10 seconds should be enough to close an input stream
	
	private static final int DEFAULT_PORT = 22;

	private static final Log log = LogFactory.getLog(PooledSFTPConnection.class);

	// standard encoding for URLDecoder
	// see http://www.w3.org/TR/html40/appendix/notes.html#non-ascii-chars
	private static final String ENCODING = "UTF-8";

	private Session session = null;
	private ChannelSftp channel = null;
	
	// CLO-2533: use a private lock to minimize the chance of deadlocking
	private Object lock = new Object();
	
	private static final ThreadFactory THREAD_FACTORY = new ThreadFactory() {

		@Override
		public Thread newThread(Runnable r) {
			Thread t = new Thread(r);
			t.setName("SFTP stream closer " + t.getId());
			return t;
		}
		
	};
	
	public PooledSFTPConnection(Authority authority) {
		super(authority);
	}
	
	private Proxy[] getProxies() {
		String proxyString = authority.getProxyString();
		if (!StringUtils.isEmpty(proxyString)) {
			java.net.Proxy proxy = FileUtils.getProxy(authority.getProxyString());
			if ((proxy != null) && (proxy.type() != Type.DIRECT)) {
				URI proxyUri = URI.create(proxyString);
				String hostName = proxyUri.getHost();
				int port = proxyUri.getPort();
				String userInfo = proxyUri.getUserInfo();
				org.jetel.util.protocols.UserInfo proxyCredentials = null;
				if (userInfo != null) {
					proxyCredentials = new org.jetel.util.protocols.UserInfo(userInfo);
				}
				switch (proxy.type()) {
				case HTTP:
					ProxyHTTP proxyHttp = (port >= 0) ? new ProxyHTTP(hostName, port) : new ProxyHTTP(hostName);
					if (proxyCredentials != null) {
						proxyHttp.setUserPasswd(proxyCredentials.getUser(), proxyCredentials.getPassword());
					}
					return new Proxy[] {proxyHttp};
				case SOCKS:
					ProxySOCKS4 proxySocks4 = (port >= 0) ? new ProxySOCKS4(hostName, port) : new ProxySOCKS4(hostName);
					ProxySOCKS5 proxySocks5 = (port >= 0) ? new ProxySOCKS5(hostName, port) : new ProxySOCKS5(hostName);
					if (proxyCredentials != null) {
						proxySocks4.setUserPasswd(proxyCredentials.getUser(), proxyCredentials.getPassword());
						proxySocks5.setUserPasswd(proxyCredentials.getUser(), proxyCredentials.getPassword());
					}
					return new Proxy[] {proxySocks5, proxySocks4};
				}
			}
		}
		
		return new Proxy[1];
	}

	/**
	 * Decodes string.
	 * @param s
	 * @return
	 */
	private String decodeString(String s) {
		try {
			return URLDecoder.decode(s, ENCODING);
		} catch (UnsupportedEncodingException e) {
			return s;
		}
	}

	private String[] getUserInfo() {
		String userInfo = authority.getUserInfo();
		if (userInfo == null) return new String[] {""};
		return decodeString(userInfo).split(":");
	}
	
	private Set<String> getPrivateKeys() {
		return ((SFTPAuthority) authority).getPrivateKeys();
	}

	public ChannelSftp getChannelSftp() throws JSchException {
		if ((channel == null) || !channel.isConnected()) {
			channel = (ChannelSftp) session.openChannel(authority.getProtocol());
			channel.connect();
			log.trace("Connection successful");
		}
		return channel;
	}

	@Override
	public void close() throws IOException {
		try {
			disconnect();
		} catch (Exception e) {
			throw new IOException(e);
		}
	}
	
	private Session getSession() throws IOException {
		JSch jsch = new JSch();
		Set<String> keys = getPrivateKeys();
		if (log.isDebugEnabled()) {
			log.debug("SFTP connecting to " + authority.getHost() + " using the following private keys: " + keys);
		}
		if (keys != null) {
			for (String key: keys) {
				try {
					log.debug("Adding new identity from " + key);
					jsch.addIdentity(key);
				} catch (Exception e) {
					log.warn("Failed to read private key", e);
				}
			}
		}
		String[] user = getUserInfo();
		String username = user[0];
		String password = user.length == 2 ? user[1] : null;
		
		if (log.isWarnEnabled()) {
			if (!StringUtils.isEmpty(username) && StringUtils.isEmpty(password) && (keys == null || keys.isEmpty())) {
				log.warn("No password or private key specified for user " + username);
			}
		}

		Proxy[] proxies = getProxies();
		try {
			if (log.isDebugEnabled()) {
				log.debug("Connecting to " + authority.getHost() + " with password authentication");
			}
			return getSession(jsch, username, new URLUserInfo(password), proxies);
		} catch (Exception e) {
			if (log.isDebugEnabled()) {
				log.debug("Connecting to " + authority.getHost() + " with keyboard-interactive authentication");
			}
			return getSession(jsch, username, new URLUserInfoIteractive(password), proxies);
		}
	}
	
	protected void connect() throws IOException {
		synchronized (lock) { // CLO-2533
			session = getSession();
			session.setConfig("StrictHostKeyChecking", "no");
			try {
				getChannelSftp();
			} catch (JSchException e) {
				throw new IOException(e);
			}
		}
	}

	private Session getSession(JSch jsch, String username, UserInfo password, Proxy[] proxies) throws IOException {
		assert (proxies != null) && (proxies.length > 0);
		
		Session session;
		try {
			if (authority.getPort() == 0) {
				session = jsch.getSession(username, authority.getHost());
			} else {
				session = jsch.getSession(username, authority.getHost(), authority.getPort() == -1 ? DEFAULT_PORT : authority.getPort());
			}

			// password will be given via UserInfo interface.
			session.setUserInfo(password);
			
			Exception exception = null;
			for (Proxy proxy: proxies) {
				if (proxy != null) {
					session.setProxy(proxy);
				}
				try {
					session.connect();
					return session;
				} catch (Exception e) {
					exception = e;
				}
			}
			
			throw exception;
		} catch (Exception e) {
			if (log.isDebugEnabled()) {
				ConnectionPool pool = ConnectionPool.getInstance();
				synchronized (pool) {
					log.debug(MessageFormat.format("Connection pool status: {0} idle, {1} active", pool.getNumIdle(), pool.getNumActive()));
				}
			}
			throw new IOException(e);
		}
	}

	public void disconnect() {
		synchronized (lock) { // CLO-2533
			if ((session != null) && session.isConnected()) {
				session.disconnect();
			}
		}
	}

	@Override
	public boolean isOpen() {
		if (!session.isConnected()) {
			return false;
		}
		try {
			ChannelSftp channel = getChannelSftp();
			if (!channel.isConnected() || channel.isClosed()) {
				return false;
			}
			// test connection
			channel.realpath("."); // pwd() is cached, don't use
			return true;
		} catch (Exception ex) {
			return false;
		}
	}

	public InputStream getInputStream(String file) throws IOException {
		final Thread owner = Thread.currentThread();
		try {
			channel = getChannelSftp();
			InputStream is = new BufferedInputStream(channel.get(file.equals("") ? "/" : file)) {
				
				/**
				 * Helper method for the Callable.
				 * 
				 * @throws IOException
				 */
				private void superClose() throws IOException {
					super.close();
				}
				
				@Override
				public void close() throws IOException {
					try {
						if (Thread.currentThread() == owner) {
							// CLO-2522: hopefully safe; if deadlocks occur, use the approach below even in this case
							superClose();
						} else {
							// CLO-2522:
							// potentially dangerous, JSch is not thread-safe
							// also used by asynchronous java.nio.channels.Channel interruption
							// might block indefinitely, so use a background thread with a timeout
							// setting a socket timeout now will not help, it is already too late to set it
							
							// convert the super.close() call to a Callable
							Callable<Void> callable = new Callable<Void>() {

								@Override
								public Void call() throws Exception {
									superClose();
									return null;
								}
								
							};
							
							// decorate the callable with timeout support
							// this should be a rare case, no need to have the thread running the whole time
							ExecutorService executorService = Executors.newSingleThreadExecutor(THREAD_FACTORY);
							Future<Void> future = executorService.submit(callable);
							executorService.shutdown();
							try {
								future.get(CLOSE_TIMEOUT, TimeUnit.SECONDS);
							} catch (InterruptedException e) {
								throw new IOException("Failed to close input stream", e);
							} catch (ExecutionException e) {
								Throwable cause = e.getCause();
								if (cause instanceof IOException) {
									throw (IOException) cause;
								} else if (cause != null) {
									throw new IOException("Failed to close input stream", cause);
								} else {
									throw new IOException("Failed to close input stream", e);
								}
							} catch (TimeoutException e) {
								// something is wrong with the pooled ChannelSftp, better close it and create a new connection
								log.warn("SFTP close stream timeout", e);
								future.cancel(true); // this should interrupt the ExecutorService thread
								disconnect(); // this should close the socket and lead to termination of all related threads
								throw new IOException("Failed to close input stream", e);
							} finally {
								executorService.shutdownNow(); // fallback
							}
						}
					} finally {
						returnToPool();
					}
				}
			};
			return is;
		} catch (Exception e) {
			returnToPool();
			throw getIOException(e);
		}
	}

	public OutputStream getOutputStream(String file, int mode) throws IOException {
		try {
			channel = getChannelSftp();
			OutputStream os = new BufferedOutputStream(channel.put(file, mode)) {
				@Override
				public void close() throws IOException {
					try {
						super.close();
					} finally {
						returnToPool();
					}
				}
			};
			return os;
		} catch (Exception e) {
			returnToPool();
			throw getIOException(e);
		}
	}
}
