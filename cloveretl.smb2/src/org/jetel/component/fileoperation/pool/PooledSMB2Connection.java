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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;

import org.jetel.component.fileoperation.SMB2Utils;
import org.jetel.component.fileoperation.URIUtils;
import org.jetel.exception.MissingBouncyCastleException;
import org.jetel.util.file.FileUtils;
import org.jetel.util.protocols.URLValidator;
import org.jetel.util.protocols.Validable;
import org.jetel.util.string.StringUtils;

import com.hierynomus.smbj.SMBClient;
import com.hierynomus.smbj.SmbConfig;
import com.hierynomus.smbj.auth.AuthenticationContext;
import com.hierynomus.smbj.connection.Connection;
import com.hierynomus.smbj.connection.NegotiatedProtocol;
import com.hierynomus.smbj.session.Session;
import com.hierynomus.smbj.share.DiskShare;
import com.hierynomus.smbj.share.Share;

/**
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 18. 3. 2015
 */
public class PooledSMB2Connection extends AbstractPoolableConnection implements Validable, URLValidator {

	private Connection connection;
	private Session session;
	private DiskShare share;
	
	private int writeBufferSize;
	
	public PooledSMB2Connection(Authority authority) {
		super(authority);
	}
	
	public void init() throws IOException {
		connect();
	}
	
	private void connect() throws IOException {
		this.connection = openConnection();

        NegotiatedProtocol negotiatedProtocol = connection.getNegotiatedProtocol();
        SmbConfig config = connection.getConfig();
		writeBufferSize = Math.min(config.getWriteBufferSize(), negotiatedProtocol.getMaxWriteSize());

		try {
			this.session = startSession();
		} catch (NoClassDefFoundError error) {
			throw new MissingBouncyCastleException("SMBv2 requires Bouncy Castle cryptographic library. Please visit http://doc.cloveretl.com/documentation/UserGuide/topic/com.cloveretl.gui.docs/docs/optional-installation-steps.html#smb-support for installation instructions.", error);
		}
		this.share = connectShare();
	}

	public void disconnect() throws IOException {
		FileUtils.closeAll(share, session, connection);
		this.share = null;
		this.session = null;
		this.connection = null;
	}
	
	@Override
	public SMB2Authority getAuthority() {
		return (SMB2Authority) super.getAuthority();
	}

	private Connection openConnection() throws IOException {
		String host = authority.getHost();
		int port = authority.getPort();
		
		SmbConfig config = SmbConfig.createDefaultConfig(); // TODO: SmbConfig - SMB2 dialects, timeouts, proxy
		SMBClient client = new SMBClient(config);
		
		if (port < 0) {
			return client.connect(host);
		} else {
			return client.connect(host, port);
		}
	}
	
	private Session startSession() throws IOException {
		String userInfoString = authority.getUserInfo();
		String[] userInfo = userInfoString.split(":");
		String username = URIUtils.urlDecode(userInfo[0]);
		String password = URIUtils.urlDecode(userInfo[1]);
		String domain = null;
		if (username.contains(";")) {
			String[] user = username.split(";");
			domain = user[0];
			username = user[1];
		}
		
		AuthenticationContext authContext = new AuthenticationContext(username, password.toCharArray(), domain);
		return connection.authenticate(authContext);
	}

	private DiskShare connectShare() throws IOException {
		String shareName = getAuthority().getShare();
		if (StringUtils.isEmpty(shareName) || shareName.equals(URIUtils.CURRENT_DIR_NAME)) {
			throw new IOException("Share name is missing in the URL");
		}
		Share share = session.connectShare(shareName);
		if (share instanceof DiskShare) {
			return (DiskShare) share;
		} else {
			throw new IOException(shareName + " is not a disk share");
		}
	}

	@Override
	public boolean isOpen() {
		return (session != null) && connection.isConnected() && (share != null) && share.isConnected();
	}

	public DiskShare getShare() {
		return share;
	}
	
	/**
	 * Closing the connection returns it to the connection pool.
	 * Use {@link #disconnect()} to actually close the connection.
	 * 
	 * @see #disconnect()
	 * @see #returnToPool()
	 */
	@Override
	public void close() throws IOException {
		returnToPool();
	}

	@Override
	public void validate(URL url) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void validate() throws IOException {
		// TODO Auto-generated method stub
		
	}

	public int getWriteBufferSize() {
		return writeBufferSize;
	}

	public InputStream getInputStream(URL url) throws IOException {
		return SMB2Utils.getInputStream(this, url); // TODO return the connection to pool on exception
	}

	public OutputStream getOutputStream(URL url) throws IOException {
		return getOutputStream(url, false); // TODO return the connection to pool on exception
	}

	public OutputStream getOutputStream(URL url, boolean append) throws IOException {
		return SMB2Utils.getOutputStream(this, url, append); // TODO return the connection to pool on exception
	}
	
}
