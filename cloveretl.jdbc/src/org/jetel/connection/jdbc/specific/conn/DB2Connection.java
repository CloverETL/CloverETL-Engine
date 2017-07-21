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

import org.apache.log4j.Logger;
import org.jetel.database.sql.DBConnection;
import org.jetel.database.sql.JdbcSpecific.OperationType;
import org.jetel.exception.JetelException;

/**
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 1.2.2013
 */
public class DB2Connection extends BasicSqlConnection {

	private static final Logger log = Logger.getLogger(DB2Connection.class);
	
	public DB2Connection(DBConnection dbConnection, Connection connection, OperationType operationType) throws JetelException {
		super(dbConnection, connection, operationType);
	}
	
	@Override
	public ResultSet getTables(String schema) throws SQLException {
		return getTablesAsSchema(schema);
	}
	
	@Override
	public void close() throws SQLException {
		/*
		 * DB2 does not allow to close connection if there is an ongoing transaction -
		 * so if not in auto commit mode, the user is expected to take care for the transaction demarcation.
		 * In order to prevent leaking open connections, rollback is attempted here.
		 * 
		 * See CLO-4963
		 */
		if (!connection.getAutoCommit()) {
			try {
				connection.rollback();
			} catch (SQLException e) {
				log.warn("Error while rolling back transaction before connection close.", e);
			}
		}
		connection.close();
	}
}
