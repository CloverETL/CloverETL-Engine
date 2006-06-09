/*
 * Copyright (c) 2004-2005 OpenTech s.r.o. All rights reserved.
 * 
 * $Header$
 */
package org.jetel.database;

import org.jetel.exception.ComponentNotReadyException;

public interface IConnection {

    /**
     * Method which connects to database and if successful, sets various
     * connection parameters. If as a property "transactionIsolation" is defined, then
     * following options are allowed:<br>
     * <ul>
     * <li>READ_UNCOMMITTED</li>
     * <li>READ_COMMITTED</li>
     * <li>REPEATABLE_READ</li>
     * <li>SERIALIZABLE</li>
     * </ul>
     * 
     * @see java.sql.Connection#setTransactionIsolation(int)
     */
    public void connect();

    /**
     * NOTE: copy from GraphElement
     */
    public abstract boolean checkConfig();

    /**
     * NOTE: copy from GraphElement
     */
    public abstract void init() throws ComponentNotReadyException;

    /**
     * Closes the sequence (current instance). All internal resources should be freed in
     * this method.
     * NOTE: copy from GraphElement
     */
    public abstract void free();


}
