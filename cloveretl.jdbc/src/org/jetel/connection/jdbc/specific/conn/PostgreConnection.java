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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.jetel.database.sql.DBConnection;
import org.jetel.database.sql.JdbcSpecific.OperationType;
import org.jetel.exception.JetelException;

/**
 * Adapter for Postgre java.sql.Connection.
 * 
 * @author Martin Zatopek (martin.zatopek@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created Jun 3, 2008
 */
public class PostgreConnection extends BasicSqlConnection {

	/**
	 * PostgreSQL Error State code of org.postgresql.util.PSQLException with message
	 * "ERROR: current transaction is aborted, commands ignored until end of transaction block"
	 * See issue #5711.
	 * 
	 * PostreSQL gets into this state if some statement of a transaction fails. The transaction needs to be rolled back,
	 * or "healed" using SQL SAVEPOINT to get into some previous OK state of the transaction.
	 */
	public static final String TRANSACTION_ABORTED_SQL_STATE = "25P02";
	
	public PostgreConnection(DBConnection dbConnection, Connection connection, OperationType operationType) throws JetelException {
		super(dbConnection, connection, operationType);
	}

	/* (non-Javadoc)
	 * @see org.jetel.connection.jdbc.specific.conn.DefaultConnection#prepareStatement(java.lang.String, int[])
	 */
	@Override
	public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
		return super.prepareStatement(sql, columnIndexes);
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.connection.jdbc.specific.conn.DefaultConnection#prepareStatement(java.lang.String, java.lang.String[])
	 */
	@Override
	public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
		return super.prepareStatement(sql, columnNames);
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.connection.jdbc.specific.conn.DefaultConnection#optimizeStatement(java.sql.Statement)
	 */
	@Override
	protected Statement optimizeStatement(Statement statement) throws SQLException {
		super.optimizeStatement(statement);
		
		switch (operationType) {
		case READ:
			statement.setFetchSize(DEFAULT_FETCH_SIZE);
			break;
		}

		return statement;
	}

	@Override
	public boolean isTransactionsSupported() {
		try {
			return connection.getTransactionIsolation() != Connection.TRANSACTION_NONE;
		} catch (SQLException e) {
			// Fix of issue #5711
			// If exception occurs in a transaction, connection.getTransactionIsolation() throws an exception.
			// We need to return true so that commit/rollback is called.
			return TRANSACTION_ABORTED_SQL_STATE.equals(e.getSQLState());
		}
	}
	
	@Override
	public List<String> getSchemas() throws SQLException {
		List<String> tmp;
		List<String> schemas = new ArrayList<String>();

		// add schemas
		tmp = getMetaSchemas();
		if (tmp != null) {
			schemas.addAll(tmp);
		}

		// catalogs not added - postgresql allows only catalog specified in connection url, cannot get matadata from
		// other catalogs
		// add catalogs

		return schemas;
	}	

	@Override
	public ResultSet getTables(String schema) throws SQLException {
		return getTablesAsSchema(schema);
	}

}
