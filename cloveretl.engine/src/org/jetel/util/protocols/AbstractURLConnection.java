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
import java.net.URL;
import java.net.URLConnection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.component.fileoperation.pool.Authority;
import org.jetel.component.fileoperation.pool.ConnectionPool;
import org.jetel.component.fileoperation.pool.PoolableConnection;

/**
 * An abstract implementation of {@link URLConnection}
 * for protocols based on the {@link ConnectionPool}.
 * 
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 20. 3. 2015
 */
public abstract class AbstractURLConnection extends URLConnection implements Validable {

	private static final ConnectionPool pool = ConnectionPool.getInstance();

	private static final Log logger = LogFactory.getLog(AbstractURLConnection.class);
	
	protected final Authority authority;

	protected AbstractURLConnection(URL url, Authority authority) {
		super(url);
		this.authority = authority;
	}

	/**
	 * Creates a connection and returns it to the pool for later use.
	 */
	@Override
	public void connect() throws IOException {
		PoolableConnection connection = null;
		try {
			// obtain a pooled connection or perform a connection attempt
			connection = connect(authority);
		} finally {
			// return the connection to the pool, subsequent operations will retrieve it from there
			disconnectQuietly(connection);
		}
	}

	/**
	 * Borrows a connection from the pool.
	 * 
	 * @param authority
	 * @return
	 * @throws IOException
	 */
	protected PoolableConnection connect(Authority authority) throws IOException {
		try {
			PoolableConnection connection = pool.borrowObject(authority);
			
			return connection;
		} catch (IOException ioe) {
			throw ioe;
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	/**
	 * Returns a connection to the pool.
	 * 
	 * @param connection
	 */
	protected void disconnectQuietly(PoolableConnection connection) {
		// make sure the object is returned to the pool
		if (connection != null) {
			try {
				pool.returnObject(connection.getAuthority(), connection);
			} catch (Exception ex) {
				logger.debug("Failed to return the connection to the pool", ex);
			}
		}
	}
	
	protected Authority getAuthority() {
		return authority;
	}

	@Override
	public void validate() throws IOException {
		PoolableConnection pooledConnection = null;
		try {
			pooledConnection = connect(authority);
			if (pooledConnection instanceof Validable) {
				Validable validable = (Validable) pooledConnection;
				validable.validate();
			}
		} finally {
			if (pooledConnection != null) {
				pooledConnection.returnToPool();
			}
		}
	}

}
