/*
 * Copyright (c) 2004-2005 OpenTech s.r.o. All rights reserved.
 * 
 * $Header$
 */
package org.jetel.database;

public interface IConnection {

    public void connect();

    public void close() throws Exception;

}
