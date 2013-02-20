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
package org.jetel.hadoop.service;

/**
 * <p>Represents data and settings that are needed by instances of {@link HadoopConnectingService} to connect to Hadoop
 * cluster. This is abstract super class that holds only common data that might be used by all types of connection to
 * Hadoop cluster.</p>
 * 
 * @author Rastislav Mirek &lt;<a href="mailto:rmirek@mail.muni.cz">rmirek@mail.muni.cz</a>&gt</br> &#169; Javlin, a.s
 *         (<a href="http://www.javlin.eu">www.javlin.eu</a>) &lt;<a
 *         href="mailto:info@cloveretl.com">info@cloveretl.com</a>&gt
 * @since rel-3-4-0-M2
 * @created 14.12.2012
 * @see HadoopConnectingService
 */
public class AbstractHadoopConnectionData {
	private String user;

	/**
	 * Creates new instance specifying user.
	 * @param user User that is connecting to Hadoop cluster.
	 */
	protected AbstractHadoopConnectionData(String user) {
		this.user = user;
	}

	/**
	 * Gets user.
	 * @return User name or <code>null</code> if none was specified.
	 */
	public String getUser() {
		return user;
	}
}