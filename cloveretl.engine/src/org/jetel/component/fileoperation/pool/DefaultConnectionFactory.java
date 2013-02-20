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

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.pool.KeyedPoolableObjectFactory;

public class DefaultConnectionFactory implements KeyedPoolableObjectFactory<Authority, PoolableConnection> {

	private final Set<ConnectionFactory> factories = new HashSet<ConnectionFactory>();
	
	public DefaultConnectionFactory() {
		factories.add(new SFTPConnectionFactory());
		factories.add(new FTPConnectionFactory());
//		factories.add(new FTPSConnectionFactory());
	}
	
	@Override
	public PoolableConnection makeObject(Authority authority) throws Exception {
		return getFactory(authority).makeObject(authority);
	}
	
	protected ConnectionFactory getFactory(Authority authority) {
		for (ConnectionFactory factory: factories) {
			if (factory.supports(authority)) {
				return factory;
			}
		}
		
		throw new UnsupportedOperationException();
	}
	
	@Override
	public void destroyObject(Authority key, PoolableConnection obj)
			throws Exception {
		getFactory(key).destroyObject(key, obj);
	}

	@Override
	public boolean validateObject(Authority key, PoolableConnection obj) {
		return getFactory(key).validateObject(key, obj);
	}

	@Override
	public void activateObject(Authority key, PoolableConnection obj)
			throws Exception {
		getFactory(key).activateObject(key, obj);
	}

	@Override
	public void passivateObject(Authority key, PoolableConnection obj)
			throws Exception {
		getFactory(key).passivateObject(key, obj);
	}

}