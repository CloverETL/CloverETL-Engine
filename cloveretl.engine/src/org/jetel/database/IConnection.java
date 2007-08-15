/*
 * Copyright (c) 2004-2005 Javlin Consulting s.r.o. All rights reserved.
 * 
 * $Header$
 */
package org.jetel.database;

import java.util.Properties;

import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationStatus;
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
     */
    public DataRecordMetadata createMetadata(Properties parameters) throws Exception;

}
