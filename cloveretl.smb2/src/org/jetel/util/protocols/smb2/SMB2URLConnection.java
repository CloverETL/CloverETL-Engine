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
package org.jetel.util.protocols.smb2;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;

import org.jetel.component.fileoperation.pool.Authority;
import org.jetel.component.fileoperation.pool.PooledSMB2Connection;
import org.jetel.component.fileoperation.pool.SMB2Authority;
import org.jetel.util.protocols.AbstractURLConnection;

public class SMB2URLConnection extends AbstractURLConnection<SMB2Authority> {
	
	/**
	 * @param url
	 */
	public SMB2URLConnection(URL url) throws IOException {
		super(url, new SMB2Authority(url));
	}

	@Override
	protected PooledSMB2Connection connect(Authority authority) throws IOException {
		return (PooledSMB2Connection) super.connect(authority);
	}

	@Override
	public InputStream getInputStream() throws IOException {
		PooledSMB2Connection pooledConnection = connect(authority);
		return pooledConnection.getInputStream(url);
	}

	@Override
	public OutputStream getOutputStream() throws IOException {
		return getOutputStream(false);
	}
	
	public OutputStream getOutputStream(boolean append) throws IOException {
		PooledSMB2Connection pooledConnection = connect(authority);
		return pooledConnection.getOutputStream(url, append);
	}

}
