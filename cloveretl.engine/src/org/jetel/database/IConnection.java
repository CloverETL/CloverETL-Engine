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
package org.jetel.database;

import java.sql.SQLException;
import java.util.Properties;

import org.jetel.graph.IGraphElement;
import org.jetel.metadata.DataRecordMetadata;

public interface IConnection extends IGraphElement {

//    /**
//     * Method which connects to database and if successful, sets various
//     * connection parameters. If as a property "transactionIsolation" is defined, then
//     * following options are allowed:<br>
//     * <ul>
//     * <li>READ_UNCOMMITTED</li>
//     * <li>READ_COMMITTED</li>
//     * <li>REPEATABLE_READ</li>
//     * <li>SERIALIZABLE</li>
//     * </ul>
//     * 
//     * @see java.sql.Connection#setTransactionIsolation(int)
//     */
//    public void connect();

    /**
     * Creates clover metadata from this connection. Used by DataRecordMetadataStub
     * for definition metadata from connection.
     * For example - in JDBC connection is expected parameter sqlQuery for definition metadata.
     * @param parameters
     * @return
     * 
     * @throws SQLException if creating metadata fails on SQL querying
     * @throws UnsupportedOperationException if operation is not defined for a given connection
     */
    public DataRecordMetadata createMetadata(Properties parameters) throws SQLException;

}
