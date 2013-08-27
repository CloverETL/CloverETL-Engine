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
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.jetel.database.sql.DBConnection;
import org.jetel.database.sql.JdbcSpecific.OperationType;
import org.jetel.exception.JetelException;

/**
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created Aug 19, 2013
 */
public class ExasolConnection extends BasicSqlConnection {

	/**
	 * @param dbConnection
	 * @param connection
	 * @param operationType
	 * @throws JetelException
	 */
	public ExasolConnection(DBConnection dbConnection, Connection connection, OperationType operationType)
			throws JetelException {
		super(dbConnection, connection, operationType);
	}

	/*
	 * Overridden - do not add catalog name as a prefix to the schemas.
	 */
	@Override
	protected List<String> getMetaSchemas() throws SQLException {
		DatabaseMetaData dbMeta = connection.getMetaData();
		List<String> ret = new ArrayList<String>();
		ResultSet result = dbMeta.getSchemas();
		
		while (result.next()) {
			ret.add(result.getString(1));
		}
		
		result.close();
		return ret;
	}

	@Override
	public List<String> getSchemas() throws SQLException {
		return getMetaSchemas();
	}

	/*
	 * Also returns system tables - allows to expand the schema "SYS".
	 */
	@Override
	protected ResultSet getTablesAsSchema(String schema) throws SQLException {
		return connection.getMetaData().getTables(null, schema, "%", new String[] {"TABLE", "VIEW", "SYSTEM TABLE" });
	}

	@Override
	public ResultSet getTables(String schema) throws SQLException {
		return getTablesAsSchema(schema);
	}

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

	/*
	 * The connection.isValid(int) throws AbstractMethodError
	 */
	@Override
	public boolean isValid(int timeout) throws SQLException {
		try {
			return connection.isValid(timeout);
		} catch (AbstractMethodError ame) {
			if (timeout < 0) {
				throw new SQLException("negative timeout");
			}
			Statement statement = null;
			ResultSet rs = null;
			try {
				statement = connection.createStatement();
				statement.setQueryTimeout(timeout);
				rs = statement.executeQuery("select * from sys.dual");
				return true;
			} catch (SQLException sqle) {
				return false;
			} finally {
				if (rs != null) {
					try {
						rs.close();
					} catch (SQLException sqle) {}
				}
				if (statement != null) {
					try {
						statement.close();
					} catch (SQLException sqle) {}
				}
			}
		}
	}

}
