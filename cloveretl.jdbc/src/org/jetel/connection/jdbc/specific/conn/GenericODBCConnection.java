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

import org.jetel.database.sql.DBConnection;
import org.jetel.database.sql.JdbcSpecific.OperationType;
import org.jetel.exception.JetelException;

/**
 * Adapter for general ODBC java.sql.Connection.
 * 
 * @author Martin Slama (martin.slama@javlin.eu) (c) Javlin, a.s (http://www.javlin.eu)
 *
 * @created January 19, 2012
 */
public class GenericODBCConnection extends BasicSqlConnection {

	public GenericODBCConnection(DBConnection dbConnection, Connection connection, OperationType operationType) throws JetelException {
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
		logger.warn("Cannot set transaction isolation level.");
	}

	@Override
	public void setAutoCommit(boolean autoCommit) throws SQLException {
		//driver does not support setting of auto commit
		logger.warn("AutoCommit is set to " + connection.getAutoCommit() + " and cannot be changed.");
	}
}
