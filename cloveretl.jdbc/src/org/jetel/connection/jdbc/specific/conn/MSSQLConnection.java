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
 * Adapter for MS SQL java.sql.Connection.
 * 
 * NOTE: this connection is used by MS SQL and Informix specifics.
 * 
 * @author Martin Zatopek (martin.zatopek@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created May 29, 2008
 */
public class MSSQLConnection extends BasicSqlConnection {

	public MSSQLConnection(DBConnection dbConnection, Connection connection, OperationType operationType) throws JetelException {
		super(dbConnection, connection, operationType);
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.connection.jdbc.specific.conn.DefaultConnection#createStatement()
	 */
	@Override
	public Statement createStatement() throws SQLException {
		Statement statement;
		
		switch (operationType) {
		case READ:
			statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			break;
		default:
			statement = connection.createStatement();
		}
		
		return optimizeStatement(statement);
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.connection.jdbc.specific.conn.DefaultConnection#prepareStatement(java.lang.String)
	 */
	@Override
	public PreparedStatement prepareStatement(String sql) throws SQLException {
		switch (operationType) {
		case READ:
			return connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		default:
			return connection.prepareStatement(sql);
		}
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
			connection.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
			break;
		case WRITE:
		case CALL:
			connection.setAutoCommit(false);
			connection.setReadOnly(false);
			connection.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
			break;

		case TRANSACTION:
			connection.setAutoCommit(true);
			connection.setReadOnly(false);
			connection.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
			break;
		}
	}

	@Override
	public List<String> getSchemas() throws SQLException {
		ArrayList<String> currentCatalog = new ArrayList<String>();
		currentCatalog.add(connection.getCatalog());
		return currentCatalog;
	}

}
