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

import org.jetel.component.fileoperation.S3OperationHandler;

/**
 * A factory for pooled S3 connections.
 * 
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 23. 3. 2015
 */
public class S3ConnectionFactory implements ConnectionFactory {

	@Override
	public PoolableConnection makeObject(Authority authority) throws Exception {
		PooledS3Connection connection = new PooledS3Connection((S3Authority) authority);
		connection.init();
		return connection;
	}

	@Override
	public void destroyObject(Authority authority, PoolableConnection obj) throws Exception {
		obj.close();
	}

	@Override
	public boolean validateObject(Authority authority, PoolableConnection obj) {
		return obj.isOpen();
	}

	@Override
	public boolean supports(Authority authority) {
		return authority.getProtocol().equalsIgnoreCase(S3OperationHandler.S3_SCHEME);
	}

	@Override
	public void activateObject(Authority authority, PoolableConnection obj) throws Exception {
	}

	@Override
	public void passivateObject(Authority authority, PoolableConnection obj) throws Exception {
	}

}
