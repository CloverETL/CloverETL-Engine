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

import java.io.Closeable;

public interface PoolableConnection extends Closeable {

	public boolean isOpen();
	
	public Authority getAuthority();
	
	/**
	 * Resets the state of the object, 
	 * as if it were newly created.
	 */
	public void reset();
	
	/**
	 * This method should only be called by the {@link ConnectionPool}.
	 * 
	 * It is not a part of the public interface.
	 */
	public void setBorrowed(boolean borrowed);
	
	/**
	 * Returns <code>true</code> if the object is borrowed.
	 * 
	 * @return
	 */
	public boolean isBorrowed();
	
	/**
	 * Returns the object to the pool, 
	 * but only if it has not been returned before.
	 * It is the object's responsibility to ensure that.
	 * 
	 * Whenever the object is borrowed or returned, {@link #setBorrowed()} 
	 * is called automatically.
	 * 
	 * Returns <code>true</code> if the object is successfully returned.
	 */
	public boolean returnToPool();
	
}
