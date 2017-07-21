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
package org.jetel.connection.jdbc.specific;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;

import org.jetel.connection.jdbc.DBConnection;
import org.jetel.connection.jdbc.specific.JdbcSpecific.OperationType;

/**
 * This class is a result value of DBConnection.getConnection(...).
 * Simple java.sql.Connection is not already returned. This class comprises
 * from instance of java.sql.Connection and backward link to DBConnection
 * as a source of this DBConnectionInstance.
 * 
 * @author Martin Zatopek (martin.zatopek@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created Jun 3, 2008
 */
public class DBConnectionInstance {

	private Connection sqlConnection;
	
	private DBConnection dbConnection;
	
	private OperationType operationType;
	
	/**
	 * Constructor.
	 * @param dbConnection
	 * @param sqlConnection
	 * @param operationType
	 */
	public DBConnectionInstance(DBConnection dbConnection, Connection sqlConnection, OperationType operationType) {
		this.sqlConnection = sqlConnection;
		this.dbConnection = dbConnection;
		this.operationType = operationType;
	}

	/**
	 * @return a comprised java.sql.Connection instance
	 */
	public Connection getSqlConnection() {
		return sqlConnection;
	}

	/**
	 * @return a source DBConnection of this connection
	 */
	public DBConnection getDbConnection() {
		return dbConnection;
	}

	/**
	 * @return operation type which was used to create this connection instance
	 */
	public OperationType getOperationType() {
		return operationType;
	}

	/**
	 * @return appropriate jdbc specific object associated with this connection instance
	 */
	public JdbcSpecific getJdbcSpecific() {
		return getDbConnection().getJdbcSpecific();
	}
	
	/**
	 * Returns a ResultSet representing schemas
	 * @param dbMeta
	 * @return ArrayList<String[]> Returns arraylist of rows, each contains a pair of strings CATALOG, SCHEMA
	 * @throws SQLException
	 */
	public ArrayList<String> getSchemas() throws SQLException {
		return getJdbcSpecific().getSchemas(getSqlConnection());
	}
	
	/**
	 * Returns a ResultSet representing tables in given database
	 * It has to extract it from dbMeta object
	 * 
	 * @param schema
	 * @return
	 */
	public ResultSet getTables(String schema) throws SQLException {
		return getJdbcSpecific().getTables(getSqlConnection(), schema);
	}

	/**
	 * Returns columns metadata for the given table.
	 * @param schema
	 * @param table
	 * @return
	 * @throws SQLException 
	 */
	public ResultSetMetaData getColumns(String schema, String owner, String table) throws SQLException {
		return getJdbcSpecific().getColumns(getSqlConnection(), schema, owner, table);
	}
	
}
