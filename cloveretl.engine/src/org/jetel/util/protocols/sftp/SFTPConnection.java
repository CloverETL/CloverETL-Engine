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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Proxy;
import java.net.URL;
import java.net.URLConnection;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.component.fileoperation.pool.Authority;
import org.jetel.component.fileoperation.pool.ConnectionPool;
import org.jetel.component.fileoperation.pool.PooledSFTPConnection;
import org.jetel.component.fileoperation.pool.SFTPAuthority;
import org.jetel.util.protocols.ProxyAuthenticable;
import org.jetel.util.protocols.UserInfo;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpATTRS;
import com.jcraft.jsch.SftpException;

/**
 * URL Connection for sftp protocol with pooling.
 * 
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created Jan 28, 2013
 */
public class SFTPConnection extends URLConnection implements ProxyAuthenticable {

	private static final ConnectionPool pool = ConnectionPool.getInstance();
	
	private static final Log logger = LogFactory.getLog(SFTPConnection.class);

	private int mode;

	private SFTPAuthority authority;
	
	/**
	 * SFTP constructor.
	 * @param url
	 */
	protected SFTPConnection(URL url) {
		this(url, null);
	}
	
	/**
	 * SFTP constructor.
	 * @param url
	 * @param proxy
	 */
	protected SFTPConnection(URL url, Proxy proxy) {
		super(url);
		this.authority = new SFTPAuthority(url, proxy);
		mode = ChannelSftp.OVERWRITE;
	}

	/**
	 * Changes directory.
	 * 
	 * @return
	 * @throws IOException
	 * 
	 * @deprecated since the connections are pooled,
	 * changing the current directory is a potentially dangerous
	 * operation. Should not be used from URLConnection anyway.
	 */
	@Deprecated
	public void cd(String path) throws IOException {
		PooledSFTPConnection connection = null;
		try {
			connection = connect(authority);
			connection.getChannelSftp().cd(path);
		} catch (JSchException e) {
			throw new IOException(e);
		} catch (SftpException e) {
			throw new IOException(e.getMessage());
		} finally {
			disconnectQuietly(connection);
		}
	}
	
	private PooledSFTPConnection connect(Authority authority) throws IOException {
		try {
			PooledSFTPConnection connection = (PooledSFTPConnection) pool.borrowObject(authority);
			
			return connection;
		} catch (IOException ioe) {
			throw ioe;
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	private void disconnectQuietly(PooledSFTPConnection connection) {
		// make sure the object is returned to the pool
		if (connection != null) {
			try {
				pool.returnObject(connection.getAuthority(), connection);
			} catch (Exception ex) {
				logger.debug("Failed to return the connection to the pool", ex);
			}
		}
	}

	@Override
	public void connect() throws IOException {
		PooledSFTPConnection connection = null;
		try {
			connection = connect(authority);
		} finally {
			disconnectQuietly(connection);
		}
	}

	/**
	 * Gets file from remote host.
	 * 
	 * @param remote -
	 *            remote path
	 * @param os -
	 *            output stream
	 * @throws IOException
	 */
	public void get(String remote, OutputStream os) throws IOException {
		PooledSFTPConnection connection = null;
		try {
			connection = connect(authority);
			connection.getChannelSftp().get(remote, os);
		} catch (JSchException e) {
			throw new IOException(e);
		} catch (SftpException e) {
			throw new IOException(e.getMessage());
		} finally {
			disconnectQuietly(connection);
		}
	}

	@Override
	public InputStream getInputStream() throws IOException {
		PooledSFTPConnection obj = connect(authority);
		String file = url.getFile();
		return obj.getInputStream(file.equals("") ? "/" : file);
	}

	@Override
	public OutputStream getOutputStream() throws IOException {
		PooledSFTPConnection obj = connect(authority);
		return obj.getOutputStream(url.getFile(), mode);
	}

	/**
	 * Lists path.
	 * 
	 * @return
	 * @throws IOException
	 */
	public Vector<?> ls(String path) throws IOException {
		PooledSFTPConnection connection = null;
		try {
			connection = connect(authority);
			return connection.getChannelSftp().ls(path);
		} catch (JSchException e) {
			throw new IOException(e);
		} catch (SftpException e) {
			throw new IOException(e.getMessage());
		} finally {
			disconnectQuietly(connection);
		}
	}

	/**
	 * Pwd command.
	 * 
	 * @return
	 * @throws IOException
	 */
	public String pwd() throws IOException {
		PooledSFTPConnection connection = null;
		try {
			connection = connect(authority);
			return connection.getChannelSftp().pwd();
		} catch (JSchException e) {
			throw new IOException(e);
		} catch (SftpException e) {
			throw new IOException(e.getMessage());
		} finally {
			disconnectQuietly(connection);
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

	/**
	 * Gets informations for actual path.
	 * 
	 * @return
	 * @throws IOException
	 */
	public SftpATTRS stat(String path) throws IOException {
		PooledSFTPConnection connection = null;
		try {
			connection = connect(authority);
			return connection.getChannelSftp().stat(path);
		} catch (JSchException e) {
			throw new IOException(e);
		} catch (SftpException e) {
			throw new IOException(e.getMessage());
		} finally {
			disconnectQuietly(connection);
		}
	}

	@Override
	public void setProxyCredentials(UserInfo userInfo) {
		authority.setProxyCredentials(userInfo);
	}
}
