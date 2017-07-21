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
import java.sql.SQLWarning;
import java.sql.Statement;

import org.apache.log4j.Logger;
import org.jetel.database.sql.DBConnection;
import org.jetel.database.sql.JdbcSpecific.OperationType;
import org.jetel.exception.JetelException;

/**
 * Connection adapter to Apache Hive -- the data warehouse system for Hadoop.
 * 
 * Created and tested on version 0.8.1 of the Hive JDBC driver.
 *
 * @author tkramolis (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 26.10.2012
 */
public class HiveConnection extends BasicSqlConnection {
	
	private Logger logger = Logger.getLogger(HiveConnection.class);

	public HiveConnection(DBConnection dbConnection, Connection connection, OperationType operationType) throws JetelException {
		super(dbConnection, connection, operationType);
	}
	
	@Override
	public Statement createStatement() throws SQLException {
		return new HiveStatementBugWorkaround(optimizeStatement(connection.createStatement()));
	}

	@Override
	protected void optimizeConnection(OperationType operationType) throws Exception {
		// Don't do anything; The driver's HiveConnection isn't much customizable, it just throws SQLException("Method not supported") all the time
	}

	@Override
	public void setAutoCommit(boolean autoCommit) throws SQLException {
		// Method not supported by the Hive JDBC driver
		if (!autoCommit) {
			throw new SQLException("Cannot disable Auto-commit on Hive connection");
		}
	}
	
	@Override
	public void setReadOnly(boolean readOnly) throws SQLException {
		// Method not supported by the Hive JDBC driver
		if (!readOnly) {
			logger.debug("Ignoring setReadOnly(false) method call. Hive connection cannot be set to read-only mode.");
		}
	}
	
	
	/**
	 * Workaround for bug in HiveStatement.executeUpdate(String) method which throws SQLException("Method not supported") 
	 * even if the update statement was executed successfully -- https://issues.apache.org/jira/browse/HIVE-1450
	 * 
	 * @author tkramolis (info@cloveretl.com)
	 *         (c) Javlin, a.s. (www.cloveretl.com)
	 *
	 * @created 31.10.2012
	 */
	private static class HiveStatementBugWorkaround implements Statement {
		
		private Statement delegate;
		
		public HiveStatementBugWorkaround(Statement delegate) {
			this.delegate = delegate;
		}

		@Override
		public int executeUpdate(String sql) throws SQLException {
			try {
				return delegate.executeUpdate(sql);
			} catch (SQLException e) {
				if ("Method not supported".equals(e.getMessage())) {
					return 0; // well, this number is not always correct, but what can we do...
				}
				throw e;
			}
		}

		@Override
		public ResultSet executeQuery(String sql) throws SQLException {
			return delegate.executeQuery(sql);
		}

		@Override
		public <T> T unwrap(Class<T> iface) throws SQLException {
			return delegate.unwrap(iface);
		}

		@Override
		public boolean isWrapperFor(Class<?> iface) throws SQLException {
			return delegate.isWrapperFor(iface);
		}

		@Override
		public void close() throws SQLException {
			delegate.close();
		}

		@Override
		public int getMaxFieldSize() throws SQLException {
			return delegate.getMaxFieldSize();
		}

		@Override
		public void setMaxFieldSize(int max) throws SQLException {
			delegate.setMaxFieldSize(max);
		}

		@Override
		public int getMaxRows() throws SQLException {
			return delegate.getMaxRows();
		}

		@Override
		public void setMaxRows(int max) throws SQLException {
			delegate.setMaxRows(max);
		}

		@Override
		public void setEscapeProcessing(boolean enable) throws SQLException {
			delegate.setEscapeProcessing(enable);
		}

		@Override
		public int getQueryTimeout() throws SQLException {
			return delegate.getQueryTimeout();
		}

		@Override
		public void setQueryTimeout(int seconds) throws SQLException {
			delegate.setQueryTimeout(seconds);
		}

		@Override
		public void cancel() throws SQLException {
			delegate.cancel();
		}

		@Override
		public SQLWarning getWarnings() throws SQLException {
			return delegate.getWarnings();
		}

		@Override
		public void clearWarnings() throws SQLException {
			delegate.clearWarnings();
		}

		@Override
		public void setCursorName(String name) throws SQLException {
			delegate.setCursorName(name);
		}

		@Override
		public boolean execute(String sql) throws SQLException {
			return delegate.execute(sql);
		}

		@Override
		public ResultSet getResultSet() throws SQLException {
			return delegate.getResultSet();
		}

		@Override
		public int getUpdateCount() throws SQLException {
			return delegate.getUpdateCount();
		}

		@Override
		public boolean getMoreResults() throws SQLException {
			return delegate.getMoreResults();
		}

		@Override
		public void setFetchDirection(int direction) throws SQLException {
			delegate.setFetchDirection(direction);
		}

		@Override
		public int getFetchDirection() throws SQLException {
			return delegate.getFetchDirection();
		}

		@Override
		public void setFetchSize(int rows) throws SQLException {
			delegate.setFetchSize(rows);
		}

		@Override
		public int getFetchSize() throws SQLException {
			return delegate.getFetchSize();
		}

		@Override
		public int getResultSetConcurrency() throws SQLException {
			return delegate.getResultSetConcurrency();
		}

		@Override
		public int getResultSetType() throws SQLException {
			return delegate.getResultSetType();
		}

		@Override
		public void addBatch(String sql) throws SQLException {
			delegate.addBatch(sql);
		}

		@Override
		public void clearBatch() throws SQLException {
			delegate.clearBatch();
		}

		@Override
		public int[] executeBatch() throws SQLException {
			return delegate.executeBatch();
		}

		@Override
		public Connection getConnection() throws SQLException {
			return delegate.getConnection();
		}

		@Override
		public boolean getMoreResults(int current) throws SQLException {
			return delegate.getMoreResults(current);
		}

		@Override
		public ResultSet getGeneratedKeys() throws SQLException {
			return delegate.getGeneratedKeys();
		}

		@Override
		public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
			return delegate.executeUpdate(sql, autoGeneratedKeys);
		}

		@Override
		public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
			return delegate.executeUpdate(sql, columnIndexes);
		}

		@Override
		public int executeUpdate(String sql, String[] columnNames) throws SQLException {
			return delegate.executeUpdate(sql, columnNames);
		}

		@Override
		public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
			return delegate.execute(sql, autoGeneratedKeys);
		}

		@Override
		public boolean execute(String sql, int[] columnIndexes) throws SQLException {
			return delegate.execute(sql, columnIndexes);
		}

		@Override
		public boolean execute(String sql, String[] columnNames) throws SQLException {
			return delegate.execute(sql, columnNames);
		}

		@Override
		public int getResultSetHoldability() throws SQLException {
			return delegate.getResultSetHoldability();
		}

		@Override
		public boolean isClosed() throws SQLException {
			return delegate.isClosed();
		}

		@Override
		public void setPoolable(boolean poolable) throws SQLException {
			delegate.setPoolable(poolable);
		}

		@Override
		public boolean isPoolable() throws SQLException {
			return delegate.isPoolable();
		}
		
		
	}
}
