package org.jetel.component.fileoperation.pool;

import java.io.IOException;

public abstract class AbstractPoolableConnection implements PoolableConnection {

	protected final Authority authority;
	
	public AbstractPoolableConnection(Authority authority) {
		this.authority = authority;
	}

	@Override
	public Authority getAuthority() {
		return authority;
	}
	
	protected static IOException getIOException(Exception e) {
		if (e instanceof IOException) {
			return (IOException) e;
		} else {
			return new IOException(e);
		}
	}

	protected void returnToPool() {
		try {
			ConnectionPool.getInstance().returnObject(getAuthority(), this);
		} catch (Exception e) {
			e.printStackTrace(); // FIXME log
		}
	}

}
