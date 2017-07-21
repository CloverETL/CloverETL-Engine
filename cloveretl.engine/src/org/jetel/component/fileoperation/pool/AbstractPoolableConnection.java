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

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public abstract class AbstractPoolableConnection implements PoolableConnection {

	private static final Log log = LogFactory.getLog(AbstractPoolableConnection.class);

	protected final Authority authority;
	
	private AtomicBoolean borrowed = new AtomicBoolean(false);
	
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

	@Override
	public boolean isBorrowed() {
		return borrowed.get();
	}

	@Override
	public boolean setBorrowed(boolean borrowed) {
		return this.borrowed.getAndSet(borrowed);
	}

	@Override
	public boolean returnToPool() {
		try {
			ConnectionPool.getInstance().returnObject(getAuthority(), this);
			return true;
		} catch (Exception e) {
			log.debug("Failed to return connection to the pool", e);
			return false;
		}
	}

	@Override
	public void reset() {
		// default implementation does nothing
	}

	/**
	 * Fallback - should not be used.
	 * 
	 * Instead of garbage collection, 
	 * an attempt should be made to return the object to the pool,
	 * but only if it's not in the pool already!
	 */
	@Override
	protected void finalize() throws Throwable {
		if (isBorrowed()) {
			log.debug("Garbage collection of a borrowed connection, a possible leak");
			if (!returnToPool()) {
				super.finalize();
			}
		} else {
			super.finalize();
		}
	}

}
