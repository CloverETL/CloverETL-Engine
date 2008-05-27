/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2002-04  David Pavlis <david_pavlis@hotmail.com>
*    
*    This library is free software; you can redistribute it and/or
*    modify it under the terms of the GNU Lesser General Public
*    License as published by the Free Software Foundation; either
*    version 2.1 of the License, or (at your option) any later version.
*    
*    This library is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU    
*    Lesser General Public License for more details.
*    
*    You should have received a copy of the GNU Lesser General Public
*    License along with this library; if not, write to the Free Software
*    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*
*/
package org.jetel.connection.jdbc.specific;

import java.sql.Connection;

import org.jetel.connection.jdbc.DBConnection;
import org.jetel.connection.jdbc.specific.JDBCSpecific.OperationType;

public class DBConnectionInstance {

	private Connection sqlConnection;
	
	private DBConnection dbConnection;
	
	private OperationType operationType;
	
	public DBConnectionInstance(DBConnection dbConnection, Connection sqlConnection, OperationType operationType) {
		this.sqlConnection = sqlConnection;
		this.dbConnection = dbConnection;
		this.operationType = operationType;
	}

	public Connection getSqlConnection() {
		return sqlConnection;
	}

	public DBConnection getDbConnection() {
		return dbConnection;
	}

	public OperationType getOperationType() {
		return operationType;
	}

	public JDBCSpecific getJdbcSpecific() {
		return getDbConnection().getJdbcSpecific();
	}
	
}
