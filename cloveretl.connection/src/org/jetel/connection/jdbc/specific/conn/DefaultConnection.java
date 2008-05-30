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
package org.jetel.connection.jdbc.specific.conn;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.connection.jdbc.DBConnection;
import org.jetel.connection.jdbc.driver.JdbcDriver;
import org.jetel.connection.jdbc.specific.JdbcSpecific.AutoGeneratedKeysType;
import org.jetel.connection.jdbc.specific.JdbcSpecific.OperationType;
import org.jetel.exception.JetelException;

/**
 * 
 * Default adapter for common java.sql.Connection class used in DefaultJdbcSpecific.
 * 
 * @author Martin Zatopek (martin.zatopek@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created May 29, 2008
 */
public class DefaultConnection implements Connection {

	protected final static Log logger = LogFactory.getLog(DefaultConnection.class);

	protected final static int DEFAULT_FETCH_SIZE = 50;

	protected Connection connection;
	
	protected OperationType operationType;
	
	protected AutoGeneratedKeysType autoGeneratedKeysType;
	
	public DefaultConnection(DBConnection dbConnection, OperationType operationType, AutoGeneratedKeysType autoGeneratedKeysType) throws JetelException {
		this.operationType = operationType;
		this.autoGeneratedKeysType = autoGeneratedKeysType;
		this.connection = connect(dbConnection);
		
		optimizeConnection();
	}

	public void clearWarnings() throws SQLException {
		connection.clearWarnings();
	}

	public void close() throws SQLException {
		connection.close();
	}

	public void commit() throws SQLException {
		connection.commit();
	}

	public Statement createStatement() throws SQLException {
		Statement statement;

		switch (operationType) {
		case READ:
			try {
				statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT);
			} catch (SQLException e) {
				logger.warn(e.getMessage());
				logger.info("Result set hold ability ignored");
				statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			}
			break;
		default:
			statement = connection.createStatement();
		}
		
		return optimizeStatement(statement);
	}

	public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
		return optimizeStatement(connection.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability));
	}

	public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
		return optimizeStatement(connection.createStatement(resultSetType, resultSetConcurrency));
	}

	public boolean getAutoCommit() throws SQLException {
		return connection.getAutoCommit();
	}

	public String getCatalog() throws SQLException {
		return connection.getCatalog();
	}

	public int getHoldability() throws SQLException {
		return connection.getHoldability();
	}

	public DatabaseMetaData getMetaData() throws SQLException {
		return connection.getMetaData();
	}

	public int getTransactionIsolation() throws SQLException {
		return connection.getTransactionIsolation();
	}

	public Map<String, Class<?>> getTypeMap() throws SQLException {
		return connection.getTypeMap();
	}

	public SQLWarning getWarnings() throws SQLException {
		return connection.getWarnings();
	}

	public boolean isClosed() throws SQLException {
		return connection.isClosed();
	}

	public boolean isReadOnly() throws SQLException {
		return connection.isReadOnly();
	}

	public String nativeSQL(String sql) throws SQLException {
		return connection.nativeSQL(sql);
	}

	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		return connection.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
	}

	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
		return connection.prepareCall(sql, resultSetType, resultSetConcurrency);
	}

	public CallableStatement prepareCall(String sql) throws SQLException {
		return connection.prepareCall(sql);
	}

	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		return connection.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
	}

	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
		return connection.prepareStatement(sql, resultSetType, resultSetConcurrency);
	}

	public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
		return connection.prepareStatement(sql, autoGeneratedKeys);
	}

	public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
		if(autoGeneratedKeysType == AutoGeneratedKeysType.SINGLE) {
			if (columnIndexes != null) {
				return connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
			}
			logger.warn("Columns are null");
			logger.info("Getting generated keys switched off !");
			return connection.prepareStatement(sql);
		}
		return connection.prepareStatement(sql, columnIndexes);
	}

	public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
		if(autoGeneratedKeysType == AutoGeneratedKeysType.SINGLE) {
			if (columnNames != null) {
				return connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
			}
			logger.warn("Columns are null");
			logger.info("Getting generated keys switched off !");
			return connection.prepareStatement(sql);
		}
		return connection.prepareStatement(sql, columnNames);
	}

	public PreparedStatement prepareStatement(String sql) throws SQLException {
		switch (operationType) {
		case READ:
			try {
				return connection.prepareStatement(sql,ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT);
			} catch (SQLException e) {
				logger.warn(e.getMessage());
				logger.info("Result set hold ability ignored");
				return connection.prepareStatement(sql,ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			}
		default:
			return connection.prepareStatement(sql);
		}
	}

	public void releaseSavepoint(Savepoint savepoint) throws SQLException {
		connection.releaseSavepoint(savepoint);
	}

	public void rollback() throws SQLException {
		connection.rollback();
	}

	public void rollback(Savepoint savepoint) throws SQLException {
		connection.rollback(savepoint);
	}

	public void setAutoCommit(boolean autoCommit) throws SQLException {
		connection.setAutoCommit(autoCommit);
	}

	public void setCatalog(String catalog) throws SQLException {
		connection.setCatalog(catalog);
	}

	public void setHoldability(int holdability) throws SQLException {
		connection.setHoldability(holdability);
	}

	public void setReadOnly(boolean readOnly) throws SQLException {
		connection.setReadOnly(readOnly);
	}

	public Savepoint setSavepoint() throws SQLException {
		return connection.setSavepoint();
	}

	public Savepoint setSavepoint(String name) throws SQLException {
		return connection.setSavepoint(name);
	}

	public void setTransactionIsolation(int level) throws SQLException {
		connection.setTransactionIsolation(level);
	}

	public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
		connection.setTypeMap(map);
	}

	//*************** END of java.sql.Connection INTERFACE ******************//
	
	protected Statement optimizeStatement(Statement statement) throws SQLException {
		switch (operationType) {
			case READ:
				try{
					statement.setFetchDirection(ResultSet.FETCH_FORWARD);
				}catch(SQLException ex){
					//TODO: for now, do nothing;
				}
				break;
		}
		
		return statement;
	}

	protected Connection connect(DBConnection dbConnection) throws JetelException {
		JdbcDriver jdbcDriver = dbConnection.getJdbcDriver();
		Driver driver = jdbcDriver.getDriver();
		Connection connection;
		
        try {
            connection = driver.connect(dbConnection.getDbUrl(), dbConnection.createConnectionProperties());

//TODO move this code to the initiate's project
//            // unlock initiatesystems driver
//            try {
//                Class embeddedConClass;
//                if (classLoader == null) {
//                    embeddedConClass = Class.forName(EMBEDDED_UNLOCK_CLASS);
//                } else {
//                    embeddedConClass = Class.forName(EMBEDDED_UNLOCK_CLASS, true, classLoader);
//                }
//                if (embeddedConClass != null) {
//                    if(embeddedConClass.isInstance(dbConnection)) {
//                            java.lang.reflect.Method unlockMethod = 
//                                embeddedConClass.getMethod("unlock", new Class[] { String.class});
//                            unlockMethod.invoke(dbConnection, new Object[] { "INITIATESYSTEMSINCJDBCPW" });
//                    }
//                }
//            } catch (Exception ex) {
//            }
        } catch (SQLException ex) {
            throw new JetelException("Can't connect to DB.", ex);
        }
        if (dbConnection == null) {
            throw new JetelException("Not suitable driver for specified DB URL (" + driver + " / " + dbConnection.getDbUrl());
        }
//        // try to set Transaction isolation level, it it was specified
//        if (config.containsKey(TRANSACTION_ISOLATION_PROPERTY_NAME)) {
//            int trLevel;
//            String isolationLevel = config.getProperty(TRANSACTION_ISOLATION_PROPERTY_NAME);
//            if (isolationLevel.equalsIgnoreCase("READ_UNCOMMITTED")) {
//                trLevel = Connection.TRANSACTION_READ_UNCOMMITTED;
//            } else if (isolationLevel.equalsIgnoreCase("READ_COMMITTED")) {
//                trLevel = Connection.TRANSACTION_READ_COMMITTED;
//            } else if (isolationLevel.equalsIgnoreCase("REPEATABLE_READ")) {
//                trLevel = Connection.TRANSACTION_REPEATABLE_READ;
//            } else if (isolationLevel.equalsIgnoreCase("SERIALIZABLE")) {
//                trLevel = Connection.TRANSACTION_SERIALIZABLE;
//            } else {
//                trLevel = Connection.TRANSACTION_NONE;
//            }
//            try {
//                connection.setTransactionIsolation(trLevel);
//            } catch (SQLException ex) {
//                // we do nothing, if anything goes wrong, we just
//                // leave whatever was the default
//            }
//        }
        // DEBUG logger.debug("DBConenction (" + getId() +") finishes connect function to the database at " + simpleDateFormat.format(new Date()));
        
        return connection;
	}

	/**
	 * @param connection
	 * @param operationType
	 */
	protected void optimizeConnection() {
		switch (operationType) {
		case READ:
			try {
				connection.setAutoCommit(false);
				connection.setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT);
				connection.setReadOnly(true);
				connection.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
			} catch (SQLException ex) {
				// TODO: for now, do nothing
			}
			break;
		case WRITE:
		case CALL:
			try {
				connection.setAutoCommit(false);
				connection.setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT);
				connection.setReadOnly(false);
				connection.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
			} catch (SQLException ex) {
				// TODO: for now, do nothing
			}
			break;

		case TRANSACTION:
			try {
				connection.setAutoCommit(true);
				connection.setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT);
				connection.setReadOnly(false);
				connection.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
			} catch (SQLException ex) {
				// TODO: for now, do nothing
			}
			break;
		}
	}
	
}
