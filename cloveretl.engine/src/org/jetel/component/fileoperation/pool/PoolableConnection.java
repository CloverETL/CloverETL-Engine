package org.jetel.component.fileoperation.pool;

import java.io.Closeable;

public interface PoolableConnection extends Closeable {

	public boolean isOpen();
	
	public Authority getAuthority();
	
}
