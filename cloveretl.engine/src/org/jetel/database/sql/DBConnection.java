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
package org.jetel.database.sql;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Properties;

import org.jetel.database.IConnection;
import org.jetel.database.sql.JdbcSpecific.OperationType;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelException;
import org.jetel.util.primitive.TypedProperties;


/**
 * This is graph element which represents connection to a SQL database.
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 29.1.2013
 */
public interface DBConnection extends IConnection {

	public final static String XML_JDBC_SPECIFIC_ATTRIBUTE = "jdbcSpecific";
    public final static String XML_DRIVER_LIBRARY_ATTRIBUTE = "driverLibrary";
    public static final String XML_JNDI_NAME_ATTRIBUTE = "jndiName";
    public static final String XML_DBURL_ATTRIBUTE = "dbURL";
    public static final String XML_DBDRIVER_ATTRIBUTE = "dbDriver";
    public static final String XML_DBCONFIG_ATTRIBUTE = "dbConfig";
    public static final String XML_DATABASE_ATTRIBUTE = "database"; // database type - used to lookup in build-in JDBC drivers
    public static final String XML_PASSWORD_ATTRIBUTE = "password";
    public static final String XML_USER_ATTRIBUTE = "user";
    public static final String XML_THREAD_SAFE_CONNECTIONS = "threadSafeConnection";
    public static final String XML_IS_PASSWORD_ENCRYPTED = "passwordEncrypted";
    public static final String XML_HOLDABILITY  = "holdability";
    public static final String XML_TRANSACTION_ISOLATION = "transactionIsolation";
	
	/**
	 * Prepares properties needed to establish connection.
	 * Resulted properties collection contains all extra properties 
	 * (properties with prefix 'jdbc.') and user name and password.
	 * @return
	 */
	public Properties createConnectionProperties();
	
    /**
     * @param elementId
     * @return
     * @throws JetelException
     */
    public SqlConnection getConnection(String elementId) throws JetelException;

    /**
     * Returns connection instance for the given elementId and operation type. If this db connection
     * is threads safe, all connection are cached, and elementId and operation type 
     * servers as key to hash map.
     * This is main method for using DBConnection class. Each graph element can claim its respective db connection.  
     * @param elementId
     * @param operationType
     * @return
     * @throws JetelException
     */
    public SqlConnection getConnection(String elementId, OperationType operationType) throws JetelException;

    /**
     * Closes connection stored in cache under key specified by elementId and OperationType.UNKNOWN.
     * Closed connection is also removed from cache.
     */
    public void closeConnection(String elementId);
    
    /**
     * Closes connection stored in cache under key specified by elementId and operationType.
     * Connection is closed only if DBConnection is thread-safe.
     * Closed connection is also removed from cache.
     */
    public void closeConnection(String elementId, OperationType operationType);

    /**
     * Saves to the given output stream all DBConnection properties.
     * @param outStream
     * @throws IOException
     */
    public void saveConfiguration(OutputStream outStream) throws IOException;
    
    /**
     * Saves to the given output stream all DBConnection properties.
     * @param outStream
     * @throws IOException
     */
    public void saveConfiguration(OutputStream outStream, Properties moreProperties) throws IOException;    

    public boolean isThreadSafeConnections();
    
    public boolean isPasswordEncrypted();

    public String getJndiName();

	public JdbcDriver getJdbcDriver();

	public String getDbUrl();

	public String getUser();

	public String getPassword();

	public String getDatabase();

	public String getDbDriver();

	public String getDriverLibrary();

	public JdbcSpecific getJdbcSpecific();
	
    /**
     * @return type of associated result set
     * @throws ComponentNotReadyException
     */
    public int getResultSetType() throws ComponentNotReadyException;
	
	public TypedProperties getExtraProperties();
	
	/**
	 * @return the holdability
	 */
	public Integer getHoldability();

	/**
	 * @return the transactionIsolation
	 */
	public Integer getTransactionIsolation();

	/**
	 * @param transactionIsolation the transactionIsolation to set
	 */
	public void setTransactionIsolation(Integer transactionIsolation);
	
    /**
     * Enum for the transaction isolation property values.
     * 
     * @author Jaroslav Urban (jaroslav.urban@javlin.eu)
     *         (c) Javlin a.s. (www.javlin.eu)
     *
     * @since Oct 2, 2009
     */
    public static enum TransactionIsolation {
    	TRANSACTION_NONE(Connection.TRANSACTION_NONE),
    	READ_UNCOMMITTED(Connection.TRANSACTION_READ_UNCOMMITTED),
    	READ_COMMITTED(Connection.TRANSACTION_READ_COMMITTED),
    	REPEATABLE_READ(Connection.TRANSACTION_REPEATABLE_READ),
    	SERIALIZABLE(Connection.TRANSACTION_SERIALIZABLE);
    	
    	private int code;
    	
    	// map of transaction isolation codes to their enum values
    	private static HashMap<Integer, TransactionIsolation> codeMap = 
    		new HashMap<Integer, TransactionIsolation>();
    	
    	static {
    		TransactionIsolation[] values = TransactionIsolation.values();
    		for (TransactionIsolation transationIsolation : values) {
				codeMap.put(transationIsolation.getCode(), transationIsolation);
			}
    	}
    	
    	/**
    	 * @param code
    	 * @return enum value for the transaction isolation code.
    	 */
    	public static TransactionIsolation fromCode(int code) {
    		return codeMap.get(code);
    	}
    	
    	/**
    	 * Allocates a new <tt>TransactionIsolation</tt> object.
    	 *
    	 * @param code
    	 */
    	private TransactionIsolation(int code) {
    		this.code = code;
    	}
    	
    	/**
    	 * @return the number code for the transaction isolation.
    	 */
    	public int getCode() {
    		return this.code;
    	}
    }
    
    /**
     * Enum for the holdability property values.
     * 
     * @author Jaroslav Urban (jaroslav.urban@javlin.eu)
     *         (c) Javlin a.s. (www.javlin.eu)
     *
     * @since Oct 2, 2009
     */
    public static enum Holdability {
    	HOLD_CURSORS(ResultSet.HOLD_CURSORS_OVER_COMMIT),
    	CLOSE_CURSORS(ResultSet.CLOSE_CURSORS_AT_COMMIT);
    	
    	private int code;
    	
    	// map of holdability codes to their enum values
    	private static HashMap<Integer, Holdability> codeMap = 
    		new HashMap<Integer, Holdability>();
    	
    	static {
    		Holdability[] values = Holdability.values();
    		for (Holdability holdability : values) {
				codeMap.put(holdability.getCode(), holdability);
			}
    	}
    	
    	/**
    	 * @param code
    	 * @return enum value for the holdability code.
    	 */
    	public static Holdability fromCode(int code) {
    		return codeMap.get(code);
    	}


    	/**
    	 * Allocates a new <tt>Holdability</tt> object.
    	 *
    	 * @param code
    	 */
    	private Holdability(int code) {
    		this.code = code;
    	}
    	
    	/**
    	 * @return the number code for the holdability.
    	 */
    	public int getCode() {
    		return this.code;
    	}
    }

}

