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

import org.jetel.database.sql.DBConnection;
import org.jetel.database.sql.JdbcSpecific.OperationType;
import org.jetel.exception.JetelException;

/**
 * Adapter for MySQL java.sql.Connection.
 * 
 * @author Martin Zatopek (martin.zatopek@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created Jun 3, 2008
 */
public class MySQLConnection extends BasicSqlConnection {
	
	public MySQLConnection(DBConnection dbConnection, Connection connection, OperationType operationType) throws JetelException {
		super(dbConnection, connection, operationType);
	}

	/* (non-Javadoc)
	 * @see org.jetel.connection.jdbc.specific.conn.DefaultConnection#optimizeStatement(java.sql.Statement)
	 */
	@Override
	protected Statement optimizeStatement(Statement statement) throws SQLException {
		super.optimizeStatement(statement);
		
		switch (operationType) {
		case READ:
			statement.setFetchSize(Integer.MIN_VALUE);
			break;
		}

		return statement;
	}
	
	/**
	 * for MySQL a database is a catalog AND a schema
	 */
	@Override
	public ResultSet getTables(String dbName) throws SQLException {
		return connection.getMetaData().getTables(dbName, dbName, "%", new String[] {"TABLE", "VIEW" });
	}

}