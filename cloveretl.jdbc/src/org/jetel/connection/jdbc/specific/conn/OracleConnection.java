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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.jetel.data.Defaults;
import org.jetel.database.sql.DBConnection;
import org.jetel.database.sql.JdbcSpecific.OperationType;
import org.jetel.exception.JetelException;

public class OracleConnection extends BasicSqlConnection {
	
	private final static int ROW_PREFETCH_UNUSED = -1;

	public OracleConnection(DBConnection dbConnection, Connection connection, OperationType operationType) throws JetelException {
		super(dbConnection, connection, operationType);
	}

	@Override
	public Statement createStatement() throws SQLException {
		Statement stmt = super.createStatement();
		return prefetchRows(stmt);
	}

	@Override
	public Statement createStatement(int resultSetType,
			int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		Statement stmt = super.createStatement(resultSetType, resultSetConcurrency,
				resultSetHoldability);
		return prefetchRows(stmt);
	}

	@Override
	public Statement createStatement(int resultSetType, int resultSetConcurrency)
			throws SQLException {
		Statement stmt = super.createStatement(resultSetType, resultSetConcurrency);
		return prefetchRows(stmt); 
	}

	/**
	 * Casting the Statement prepare to OracleStatement
     * and setting the Row Pre-fetch value as prefetchValue
	 */
	private Statement prefetchRows(Statement stmt) {
		int prefetchValue = Defaults.OracleConnection.ROW_PREFETCH;
		boolean prefetchRows = (prefetchValue != ROW_PREFETCH_UNUSED);
		
		if (prefetchRows) {
			try {
				stmt.setFetchSize(prefetchValue);
			} catch (SQLException e) {
				logger.warn(e.getMessage());
			} catch (UnsupportedOperationException e) {
				logger.warn(e.getMessage());
			}
		}
		return stmt;
	}
	
	@Override
	protected void optimizeConnection(OperationType operationType) throws Exception {
		switch (operationType) {
		case READ:
			connection.setAutoCommit(false);
			connection.setReadOnly(true);
			connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
			connection.setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT);
			break;
		case WRITE:
		case CALL:
			connection.setAutoCommit(false);
			connection.setReadOnly(false);
			connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
			connection.setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT);
			break;

		case TRANSACTION:
			connection.setAutoCommit(true);
			connection.setReadOnly(false);
			connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
			connection.setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT);
			break;
		}
	}
}
