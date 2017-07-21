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

import java.io.Closeable;
import java.io.IOException;
import java.util.Properties;

/**
 * <p> Defines service that can connect to Hadoop cluster and communicate with the cluster, optionally performing operations
 * on the cluster. This interface only describes ability to connect to the cluster. It is meant to be inherited to
 * provide definition of specific operations. </p>
 * 
 * <p> This service must be connected before it can provide any useful operation. As that connection may fail due to
 * external factors instances of this interface should not be used if such a failure is not expected. Specifically,
 * instances of this interface are only meant to be used from engine. Separated services that do not extend this
 * interface should be created for use from designer.</p>
 * 
 * @param T Type of object that provides information needed to connect.
 * @author Rastislav Mirek &lt;<a href="mailto:rmirek@mail.muni.cz">rmirek@mail.muni.cz</a>&gt</br> &#169; Javlin, a.s
 *         (<a href="http://www.javlin.eu">www.javlin.eu</a>) &lt;<a
 *         href="mailto:info@cloveretl.com">info@cloveretl.com</a>&gt
 * @since rel-3-4-0-M2
 * @created 17.11.2012
 */
public interface HadoopConnectingService<T extends AbstractHadoopConnectionData> extends Closeable {

	public static final String NOT_CONNECTED_MESSAGE = "Not connected to Hadoop cluster. Either connect() was not "
			+ "called or close() was called already. Call connect() first.";
	public static final String DUPLICATED_CONNECT_CALL_MESSAGE = "Method connect() was called while already"
			+ " connected to Hadoop cluster. Only one call of method connect() per instance is permited.";

	/**
	 * <p> Connects to one or more Hadoop master nodes (typically namenode, jobtracker or both). Must be called before
	 * any other operation that require connection to Hadoop cluster are performed on instance of
	 * {@code HadoopConnectingService}. </p>
	 * 
	 * <p> Implementors should ensure that additional settings specified by {@code additionalSettings} parameter do
	 * <strong>not</strong> take priority to connection settings contained in {@code connectionData} parameter. That is
	 * by default if the same setting is specified by both parameters then value specified by {@code connectionData}
	 * overrides value specified by {@code additionalSettings}. However this behaviour can be changed by implementors if
	 * desired. </p>
	 * 
	 * @param connectionData Non null object containing at least information necessary to establish connection.
	 * @param additionalSettings Non null additional settings of connection to Hadoop cluster.
	 * @throws IOException If communication with master node fails.
	 * @throws IllegalStateException If called twice or more times.
	 */
	public void connect(final T connectionData, final Properties additionalSettings) throws IOException;

	/**
	 * Validates connection to Hadoop cluster established by {@link HadoopConnectingService#connect(Properties)}.
	 * 
	 * @return <code>null</code> if validation was successful, non-null message describing validation failure reason
	 *         otherwise.
	 * @throws IOException If communication with cluster fails. In such case connection should also be considered
	 *         invalid.
	 * @throws IllegalStateException If {@link HadoopConnectingService#isConnected()} returns false.
	 */
	public String validateConnection() throws IOException;

	/**
	 * Gets connection status of this {@code HadoopConnectingService}. Returns <code>true</code> if it is connected to
	 * some Hadoop cluster.
	 * 
	 * @return <code>true</code> if {@link HadoopConnectingService#connect(Properties)} has been called successfully and
	 *         {@link HadoopConnectingService#close()} has not been called yet, <code>false</code> otherwise.
	 */
	public boolean isConnected();
}