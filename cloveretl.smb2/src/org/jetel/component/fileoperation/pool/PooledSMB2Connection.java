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
import org.jetel.util.file.FileUtils;
import org.jetel.util.protocols.URLValidator;
import org.jetel.util.protocols.Validable;
import org.jetel.util.string.StringUtils;

import com.hierynomus.smbj.SMBClient;
import com.hierynomus.smbj.SmbConfig;
import com.hierynomus.smbj.auth.AuthenticationContext;
import com.hierynomus.smbj.connection.Connection;
import com.hierynomus.smbj.event.SMBEvent;
import com.hierynomus.smbj.event.SMBEventBus;
import com.hierynomus.smbj.session.Session;
import com.hierynomus.smbj.share.DiskShare;

import net.engio.mbassy.bus.SyncMessageBus;
import net.engio.mbassy.bus.error.IPublicationErrorHandler;

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

	public PooledSMB2Connection(Authority authority) {
		super(authority);
	}
	
	public void init() throws IOException {
		connect();
	}
	
	private void connect() throws IOException {
		this.connection = openConnection();
		this.session = startSession();
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
		
		/*
		 * We do not use SmbClient, because we don't need its built-in connection pooling
		 * and it logs the following message for each connection:
		 * INFO: No error handler has been configured to handle exceptions during publication. 
		 */
		SmbConfig config = SmbConfig.createDefaultConfig(); // TODO: SmbConfig - SMB2 dialects, timeouts, proxy
		IPublicationErrorHandler.ConsoleLogger errorHandler = new IPublicationErrorHandler.ConsoleLogger();
		SMBEventBus bus = new SMBEventBus(new SyncMessageBus<SMBEvent>(errorHandler));
		
		Connection connection = new Connection(config, bus);
		if (port > -1) {
			connection.connect(host, port);
		} else {
			connection.connect(host, SMBClient.DEFAULT_PORT);
		}
		return connection;
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
		return (DiskShare) session.connectShare(shareName);
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
