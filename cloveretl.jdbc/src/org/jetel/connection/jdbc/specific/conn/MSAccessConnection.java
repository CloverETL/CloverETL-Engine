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
package org.jetel.connection.jdbc.specific.conn;

import java.sql.Connection;

import org.jetel.database.sql.DBConnection;
import org.jetel.database.sql.JdbcSpecific.OperationType;
import org.jetel.exception.JetelException;

/**
 * Adapter for MS Access java.sql.Connection.
 * 
 * @author Martin Slama (martin.slama@javlin.eu) (c) Javlin, a.s (http://www.javlin.eu)
 *
 * @created January 27, 2012
 */
public class MSAccessConnection extends GenericODBCConnection {

	/**
	 * Constructor.
	 * @param dbConnection
	 * @param operationType
	 * @throws JetelException
	 */
	public MSAccessConnection(DBConnection dbConnection, OperationType operationType) throws JetelException {
		super(dbConnection, operationType);
	}

	/* (non-Javadoc) We had to ommit holdability settings.
	 * @see org.jetel.connection.jdbc.specific.conn.DefaultConnection#optimizeConnection(org.jetel.connection.jdbc.specific.JdbcSpecific.OperationType)
	 */
	@Override
	protected void optimizeConnection(OperationType operationType) throws Exception {
		switch (operationType) {
		case READ:
			connection.setAutoCommit(false);
			connection.setReadOnly(true);
			break;
		case WRITE:
		case CALL:
			connection.setAutoCommit(false);
			connection.setReadOnly(false);
			break;

		case TRANSACTION:
			connection.setAutoCommit(true);
			connection.setReadOnly(false);
			break;
		}
		
		//it's not possible to set transaction isolation level
		//log the message about it
		String transactionIsolationLevel = "";
		switch (connection.getTransactionIsolation()) {
		case Connection.TRANSACTION_NONE:
			transactionIsolationLevel = "TRANSACTION_NONE";
			break;
		case Connection.TRANSACTION_READ_COMMITTED:
			transactionIsolationLevel = "TRANSACTION_READ_COMMITTED";
			break;
		case Connection.TRANSACTION_READ_UNCOMMITTED:
			transactionIsolationLevel = "TRANSACTION_READ_UNCOMMITTED";
			break;
		case Connection.TRANSACTION_REPEATABLE_READ:
			transactionIsolationLevel = "TRANSACTION_REPEATABLE_READ";
			break;
		case Connection.TRANSACTION_SERIALIZABLE:
			transactionIsolationLevel = "TRANSACTION_SERIALIZABLE";
			break;
		}
		logger.warn("Transaction isolation level is set to " + transactionIsolationLevel + " and cannot be changed.");
	}
}
