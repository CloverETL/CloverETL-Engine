package org.jetel.component.fileoperation.pool;

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.pool.KeyedPoolableObjectFactory;

public class DefaultConnectionFactory implements KeyedPoolableObjectFactory<Authority, PoolableConnection> {

	private final Set<ConnectionFactory> factories = new HashSet<ConnectionFactory>();
	
	public DefaultConnectionFactory() {
		factories.add(new SFTPConnectionFactory());
//		factories.add(new FTPConnectionFactory());
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