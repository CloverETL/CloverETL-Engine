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
package org.jetel.util.protocols.sftp;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLDecoder;
import java.util.Vector;

import org.jetel.util.protocols.ProxyAuthenticable;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Proxy;
import com.jcraft.jsch.ProxyHTTP;
import com.jcraft.jsch.ProxySOCKS4;
import com.jcraft.jsch.ProxySOCKS5;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpATTRS;
import com.jcraft.jsch.SftpException;
import com.jcraft.jsch.UIKeyboardInteractive;
import com.jcraft.jsch.UserInfo;

/**
 * URL Connection for sftp protocol.
 * 
 * @author Jan Ausperger (jan.ausperger@javlinconsulting.cz) (c) Javlin
 *         Consulting (www.javlinconsulting.cz)
 */
public class SFTPConnection extends URLConnection implements ProxyAuthenticable {

	private static final JSch jsch = new JSch();
	
	protected Session session;
	protected ChannelSftp channel;

	protected int mode;

	private Proxy proxy;
	private ProxySOCKS4 proxy4;
	
	private final SFTPStreamHandler handler;
	private int openedStreams = 0;

	// standard encoding for URLDecoder
	// see http://www.w3.org/TR/html40/appendix/notes.html#non-ascii-chars
	private static final String ENCODING = "UTF-8";

	/**
	 * SFTP constructor.
	 * @param url
	 * @param handler
	 */
	protected SFTPConnection(URL url, SFTPStreamHandler handler) {
		this(url, null, handler);
	}
	
	/**
	 * SFTP constructor.
	 * @param url
	 * @param proxy
	 * @param handler
	 */
	protected SFTPConnection(URL url, java.net.Proxy proxy, SFTPStreamHandler handler) {
		super(url);
		mode = ChannelSftp.OVERWRITE;
		this.handler = handler;
		
		if (proxy == null) return;
		SocketAddress sa = proxy.address();
		if (!(sa instanceof InetSocketAddress)) return;
		String hostName = ((InetSocketAddress) sa).getHostName();
		int port = ((InetSocketAddress) sa).getPort();
		if (proxy.type() == java.net.Proxy.Type.HTTP) {
			this.proxy = port >= 0 ? new ProxyHTTP(hostName, port) : new ProxyHTTP(hostName);
		} 
		else if (proxy.type() == java.net.Proxy.Type.SOCKS) {
			this.proxy = port >= 0 ? new ProxySOCKS5(hostName, port) : new ProxySOCKS5(hostName);
			this.proxy4 = port >= 0 ? new ProxySOCKS4(hostName, port) : new ProxySOCKS4(hostName);
		}
	}

	/**
	 * Changes directory.
	 * 
	 * @return
	 * @throws IOException
	 */
	public void cd(String path) throws IOException {
		connect();
		try {
			channel = getChannelSftp();
			channel.cd(path);
		} catch (JSchException e) {
			throw new IOException(e.getMessage());
		} catch (SftpException e) {
			throw new IOException(e.getMessage());
		} finally {
		}
	}
	
	@Override
	public void connect() throws IOException {
		String[] user = getUserInfo();
		try {
			connect(new URLUserInfo(user.length == 2 ? user[1] : null));
		} catch (Exception e) {
			connect(new URLUserInfoIteractive(user.length == 2 ? user[1] : null));
		}
	}

	private void connect(AUserInfo aUserInfo) throws IOException {
		if (session != null && session.isConnected()) return;
		
		String[] user = getUserInfo();
		try {
			if (url.getPort() == 0) session = jsch.getSession(user[0], url.getHost());
			else session = jsch.getSession(user[0], url.getHost(), url.getPort() == -1 ? 22 : url.getPort());

			// password will be given via UserInfo interface.
			session.setUserInfo(aUserInfo);
			if (proxy != null) session.setProxy(proxy);
			session.connect();
		} catch (Exception e) {
			if (proxy4 != null) {
				try {
					session.connect();
					return;
				} catch (JSchException e1) {}
			}
			throw new IOException(e.getMessage());
		}
	}

	/**
	 * Session disconnect.
	 */
	public void disconnect() {
		if (openedStreams == 0 && session != null && session.isConnected()) {
			session.disconnect();
			handler.removeFromPool(this);
		}
	}

	/**
	 * Gets file from remote host.
	 * 
	 * @param remore -
	 *            remote path
	 * @param os -
	 *            output stream
	 * @throws IOException
	 */
	public void get(String remore, OutputStream os) throws IOException {
		connect();
		try {
			channel = getChannelSftp();
			channel.get(remore, os);
		} catch (JSchException e) {
			throw new IOException(e.getMessage());
		} catch (SftpException e) {
			throw new IOException(e.getMessage());
		} finally {
		}
	}

	@Override
	public InputStream getInputStream() throws IOException {
		connect();
		try {
			channel = getChannelSftp();
			String file = url.getFile();
			InputStream is = new BufferedInputStream(channel.get(file.equals("") ? "/" : file)) {
				@Override
				public void close() throws IOException {
					openedStreams--;
					if (openedStreams >= 0) {
						super.close();
						disconnect();
					}
				}
			};
			openedStreams++;
			return is;
		} catch (SftpException e) {
			throw new IOException(e.getMessage());
		} catch (JSchException e) {
			throw new IOException(e.getMessage());
		}
	}

	@Override
	public OutputStream getOutputStream() throws IOException {
		connect();
		try {
			channel = getChannelSftp();
			OutputStream os = new BufferedOutputStream(channel.put(url.getFile(), mode)) {
				@Override
				public void close() throws IOException {
					openedStreams--;
					if (openedStreams >= 0) {
						super.close();
						disconnect();
					}
				}
			};
			openedStreams++;
			return os;
		} catch (SftpException e) {
			throw new IOException(e.getMessage());
		} catch (JSchException e) {
			throw new IOException(e.getMessage());
		}
	}

	/**
	 * Lists path.
	 * 
	 * @return
	 * @throws IOException
	 */
	public Vector<?> ls(String path) throws IOException {
		connect();
		try {
			channel = getChannelSftp();
			return channel.ls(path);
		} catch (JSchException e) {
			throw new IOException(e.getMessage());
		} catch (SftpException e) {
			throw new IOException(e.getMessage());
		} finally {
		}
	}

	/**
	 * Pwd command.
	 * 
	 * @return
	 * @throws IOException
	 */
	public String pwd() throws IOException {
		connect();
		try {
			channel = getChannelSftp();
			return channel.pwd();
		} catch (JSchException e) {
			throw new IOException(e.getMessage());
		} catch (SftpException e) {
			throw new IOException(e.getMessage());
		} finally {
		}
	}

	/**
	 * Supports sftp put mode that can be ChannelSftp.APPEND or
	 * ChannelSftp.OVERWRITE or ChannelSftp.RESUME value.
	 * 
	 * @param mode
	 */
	public void setMode(int mode) {
		this.mode = mode;
	}

	public void setTimeout(int timeout) throws JSchException {
		session.setTimeout(timeout);
	}

	/**
	 * Gets informations for actual path.
	 * 
	 * @return
	 * @throws IOException
	 */
	public SftpATTRS stat(String path) throws IOException {
		connect();
		try {
			channel = getChannelSftp();
			return channel.stat(path);
		} catch (JSchException e) {
			throw new IOException(e.getMessage());
		} catch (SftpException e) {
			throw new IOException(e.getMessage());
		} finally {
		}
	}

	private String[] getUserInfo() {
		String userInfo = url.getUserInfo();
		if (userInfo == null) return new String[] {""};
		return decodeString(userInfo).split(":");
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

	/**
	 * Gets ChannelSftp.
	 * 
	 * @return ChannelSftp
	 * @throws JSchException
	 */
	private ChannelSftp getChannelSftp() throws JSchException {
		if (channel == null || !channel.isConnected()) {
			channel = (ChannelSftp) session.openChannel(url.getProtocol());
			channel.connect();
		}
		return channel;
	}

	public static abstract class AUserInfo implements UserInfo {
		protected String password;

		protected String passphrase = null;

		public AUserInfo(String password) {
			this.password = password;
		}

		@Override
		public void showMessage(String message) {
		}

		@Override
		public boolean promptPassphrase(String message) {
			return true;
		}

		@Override
		public boolean promptYesNo(String str) {
			return true;
		}

		@Override
		public String getPassphrase() {
			return passphrase;
		}
	}

	/**
	 * Class for password supporting.
	 * 
	 * @author Jan Ausperger (jan.ausperger@javlinconsulting.cz) (c) Javlin
	 *         Consulting (www.javlinconsulting.cz)
	 */
	public static class URLUserInfoIteractive extends AUserInfo implements
			UIKeyboardInteractive {

		public URLUserInfoIteractive(String password) {
			super(password);
		}

		@Override
		public String getPassword() {
			return null;
		}

		@Override
		public boolean promptPassword(String message) {
			return true;
		}

		@Override
		public String[] promptKeyboardInteractive(String destination,
				String name, String instruction, String[] prompt, boolean[] echo) {
			return new String[] { password };
		}
	}

	public static class URLUserInfo extends AUserInfo {

		public URLUserInfo(String password) {
			super(password);
		}

		@Override
		public String getPassword() {
			return password;
		}

		@Override
		public boolean promptPassword(String message) {
			return password != null;
		}

	}
	
	void setURL(URL url) {
		super.url = url;
	}

	@Override
	public void setProxyCredentials(org.jetel.util.protocols.UserInfo userInfo) {
		String user = userInfo.getUser();
		String password = userInfo.getPassword();
		if (proxy4 != null) {
			proxy4.setUserPasswd(user, password);
		}
		if (proxy instanceof ProxyHTTP) {
			((ProxyHTTP) proxy).setUserPasswd(user, password);
		} else if (proxy instanceof ProxySOCKS5) {
			((ProxySOCKS5) proxy).setUserPasswd(user, password);
		}
	}
}
