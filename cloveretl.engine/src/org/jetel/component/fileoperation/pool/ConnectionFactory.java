package org.jetel.component.fileoperation.pool;

import org.apache.commons.pool.KeyedPoolableObjectFactory;

public interface ConnectionFactory extends KeyedPoolableObjectFactory<Authority, PoolableConnection> {

	public boolean supports(Authority authority);
	
}
