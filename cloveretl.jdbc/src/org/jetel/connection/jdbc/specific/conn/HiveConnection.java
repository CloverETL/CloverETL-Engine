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
import org.jetel.connection.jdbc.DBConnection;
import org.jetel.connection.jdbc.specific.JdbcSpecific.AutoGeneratedKeysType;
import org.jetel.connection.jdbc.specific.JdbcSpecific.OperationType;
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
public class HiveConnection extends DefaultConnection {
	
	private Logger logger = Logger.getLogger(HiveConnection.class);

	public HiveConnection(DBConnection dbConnection, OperationType operationType) throws JetelException {
		super(dbConnection, operationType, AutoGeneratedKeysType.NONE);
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

		public ResultSet executeQuery(String sql) throws SQLException {
			return delegate.executeQuery(sql);
		}

		public <T> T unwrap(Class<T> iface) throws SQLException {
			return delegate.unwrap(iface);
		}

		public boolean isWrapperFor(Class<?> iface) throws SQLException {
			return delegate.isWrapperFor(iface);
		}

		public void close() throws SQLException {
			delegate.close();
		}

		public int getMaxFieldSize() throws SQLException {
			return delegate.getMaxFieldSize();
		}

		public void setMaxFieldSize(int max) throws SQLException {
			delegate.setMaxFieldSize(max);
		}

		public int getMaxRows() throws SQLException {
			return delegate.getMaxRows();
		}

		public void setMaxRows(int max) throws SQLException {
			delegate.setMaxRows(max);
		}

		public void setEscapeProcessing(boolean enable) throws SQLException {
			delegate.setEscapeProcessing(enable);
		}

		public int getQueryTimeout() throws SQLException {
			return delegate.getQueryTimeout();
		}

		public void setQueryTimeout(int seconds) throws SQLException {
			delegate.setQueryTimeout(seconds);
		}

		public void cancel() throws SQLException {
			delegate.cancel();
		}

		public SQLWarning getWarnings() throws SQLException {
			return delegate.getWarnings();
		}

		public void clearWarnings() throws SQLException {
			delegate.clearWarnings();
		}

		public void setCursorName(String name) throws SQLException {
			delegate.setCursorName(name);
		}

		public boolean execute(String sql) throws SQLException {
			return delegate.execute(sql);
		}

		public ResultSet getResultSet() throws SQLException {
			return delegate.getResultSet();
		}

		public int getUpdateCount() throws SQLException {
			return delegate.getUpdateCount();
		}

		public boolean getMoreResults() throws SQLException {
			return delegate.getMoreResults();
		}

		public void setFetchDirection(int direction) throws SQLException {
			delegate.setFetchDirection(direction);
		}

		public int getFetchDirection() throws SQLException {
			return delegate.getFetchDirection();
		}

		public void setFetchSize(int rows) throws SQLException {
			delegate.setFetchSize(rows);
		}

		public int getFetchSize() throws SQLException {
			return delegate.getFetchSize();
		}

		public int getResultSetConcurrency() throws SQLException {
			return delegate.getResultSetConcurrency();
		}

		public int getResultSetType() throws SQLException {
			return delegate.getResultSetType();
		}

		public void addBatch(String sql) throws SQLException {
			delegate.addBatch(sql);
		}

		public void clearBatch() throws SQLException {
			delegate.clearBatch();
		}

		public int[] executeBatch() throws SQLException {
			return delegate.executeBatch();
		}

		public Connection getConnection() throws SQLException {
			return delegate.getConnection();
		}

		public boolean getMoreResults(int current) throws SQLException {
			return delegate.getMoreResults(current);
		}

		public ResultSet getGeneratedKeys() throws SQLException {
			return delegate.getGeneratedKeys();
		}

		public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
			return delegate.executeUpdate(sql, autoGeneratedKeys);
		}

		public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
			return delegate.executeUpdate(sql, columnIndexes);
		}

		public int executeUpdate(String sql, String[] columnNames) throws SQLException {
			return delegate.executeUpdate(sql, columnNames);
		}

		public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
			return delegate.execute(sql, autoGeneratedKeys);
		}

		public boolean execute(String sql, int[] columnIndexes) throws SQLException {
			return delegate.execute(sql, columnIndexes);
		}

		public boolean execute(String sql, String[] columnNames) throws SQLException {
			return delegate.execute(sql, columnNames);
		}

		public int getResultSetHoldability() throws SQLException {
			return delegate.getResultSetHoldability();
		}

		public boolean isClosed() throws SQLException {
			return delegate.isClosed();
		}

		public void setPoolable(boolean poolable) throws SQLException {
			delegate.setPoolable(poolable);
		}

		public boolean isPoolable() throws SQLException {
			return delegate.isPoolable();
		}
		
		
	}
}