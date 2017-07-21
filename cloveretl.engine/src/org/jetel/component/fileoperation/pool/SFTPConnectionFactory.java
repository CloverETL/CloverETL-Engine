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

public class SFTPConnectionFactory implements ConnectionFactory {

	private static final String SFTP_SCHEME = "sftp";
	
	@Override
	public PoolableConnection makeObject(Authority authority) throws Exception {
		PooledSFTPConnection connection = new PooledSFTPConnection(authority);
		connection.connect();
		return connection;
	}

	@Override
	public void destroyObject(Authority key, PoolableConnection obj)
			throws Exception {
		((PooledSFTPConnection) obj).disconnect();
	}

	@Override
	public boolean validateObject(Authority key, PoolableConnection obj) {
		return ((PooledSFTPConnection) obj).isOpen();
	}

	@Override
	public void activateObject(Authority key, PoolableConnection obj)
			throws Exception {
	}

	@Override
	public void passivateObject(Authority key, PoolableConnection obj)
			throws Exception {
	}

	@Override
	public boolean supports(Authority authority) {
		return authority.getProtocol().equalsIgnoreCase(SFTP_SCHEME);
	}

}
