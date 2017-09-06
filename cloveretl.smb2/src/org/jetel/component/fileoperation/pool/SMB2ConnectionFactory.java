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

public class SMB2ConnectionFactory implements ConnectionFactory {
	
	@Override
	public PoolableConnection makeObject(Authority authority) throws Exception {
		PooledSMB2Connection connection = new PooledSMB2Connection(authority);
		connection.init();
		return connection;
	}

	@Override
	public void destroyObject(Authority authority, PoolableConnection obj) throws Exception {
		PooledSMB2Connection connection = (PooledSMB2Connection) obj;
		connection.disconnect();
	}

	@Override
	public boolean validateObject(Authority authority, PoolableConnection obj) {
		return obj.isOpen();
	}
	@Override
	public boolean supports(Authority authority) {
		return authority.getProtocol().equalsIgnoreCase("smb2");
	}

	@Override
	public void activateObject(Authority arg0, PoolableConnection arg1) throws Exception {
	}

	@Override
	public void passivateObject(Authority arg0, PoolableConnection arg1) throws Exception {
	}

}
