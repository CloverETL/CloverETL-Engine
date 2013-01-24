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
