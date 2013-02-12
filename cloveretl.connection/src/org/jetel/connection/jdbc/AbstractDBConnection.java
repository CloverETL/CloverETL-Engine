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
package org.jetel.connection.jdbc;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Enumeration;
import java.util.Properties;
import java.util.Set;

import org.jetel.database.sql.DBConnection;
import org.jetel.database.sql.SqlConnection;
import org.jetel.database.sql.JdbcSpecific.OperationType;
import org.jetel.exception.JetelException;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.graph.GraphElement;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.primitive.TypedProperties;
import org.jetel.util.string.StringUtils;

/**
 * Ad hoc base class with metadata generation logic.
 * @author jan.michalica (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 8.2.2013
 */
public abstract class AbstractDBConnection extends GraphElement implements DBConnection {

	public final static String SQL_QUERY_PROPERTY = "sqlQuery";

    public final static String XML_JDBC_SPECIFIC_ATTRIBUTE = "jdbcSpecific";
    public final static String XML_DRIVER_LIBRARY_ATTRIBUTE = "driverLibrary";
    public static final String XML_JNDI_NAME_ATTRIBUTE = "jndiName";
    public static final String XML_DBURL_ATTRIBUTE = "dbURL";
    public static final String XML_DBDRIVER_ATTRIBUTE = "dbDriver";
    public static final String XML_DBCONFIG_ATTRIBUTE = "dbConfig";
    public static final String XML_DATABASE_ATTRIBUTE = "database"; // database type - used to lookup in build-in JDBC drivers
    public static final String XML_PASSWORD_ATTRIBUTE = "password";
    public static final String XML_USER_ATTRIBUTE = "user";
    public static final String XML_THREAD_SAFE_CONNECTIONS="threadSafeConnection";
    public static final String XML_IS_PASSWORD_ENCRYPTED = "passwordEncrypted";
    public static final String XML_HOLDABILITY  = "holdability";
    public static final String XML_TRANSACTION_ISOLATION = "transactionIsolation";
	
    public static final String XML_JDBC_PROPERTIES_PREFIX = "jdbc.";
    
	/**
	 * @param id
	 * @param name
	 */
	public AbstractDBConnection(String id, String name) {
		super(id, name);
	}

	/**
	 * @param id
	 * @param graph
	 * @param name
	 */
	public AbstractDBConnection(String id, TransformationGraph graph, String name) {
		super(id, graph, name);
	}

	/**
	 * @param id
	 * @param graph
	 */
	public AbstractDBConnection(String id, TransformationGraph graph) {
		super(id, graph);
	}

	/**
	 * @param id
	 */
	public AbstractDBConnection(String id) {
		super(id);
	}
	
	/* (non-Javadoc)
     * @see org.jetel.database.DBConnection#getConnection(java.lang.String)
     */
	@Override
	public SqlConnection getConnection(String elementId) throws JetelException {
		return getConnection(elementId, OperationType.UNKNOWN);
	}
	
	/* (non-Javadoc)
     * @see org.jetel.database.DBConnection#closeConnection(java.lang.String)
     */
	@Override
	public void closeConnection(String elementId) {
		closeConnection(elementId, OperationType.UNKNOWN);
	}

	/* (non-Javadoc)
     * @see org.jetel.database.IConnection#createMetadata(java.util.Properties)
     */
    @Override
	public DataRecordMetadata createMetadata(Properties parameters) throws SQLException {
    	if (!isInitialized()) {
    		throw new IllegalStateException("DBConnection has to be initialized to be able to create metadata.");
    	}
    	
        Statement statement = null;
        ResultSet resultSet = null;

        String sqlQuery = parameters.getProperty(SQL_QUERY_PROPERTY);
        if(StringUtils.isEmpty(sqlQuery)) {
            throw new IllegalArgumentException("JDBC stub for clover metadata can't find sqlQuery parameter.");
        }

        int index = sqlQuery.toUpperCase().indexOf("WHERE");

		if (index >= 0) {
			sqlQuery = sqlQuery.substring(0, index).concat("WHERE 0=1");
		} else {
			sqlQuery = sqlQuery.concat(" WHERE 0=1");
		}

        Connection connection;
		try {
			connection = connect(OperationType.UNKNOWN);
		} catch (JetelException e) {
			throw new SQLException(e.getMessage());
		} catch (JetelRuntimeException e) {
			throw new SQLException(e.getMessage());
		}
        
        try {
            statement = connection.createStatement();
            resultSet = statement.executeQuery(sqlQuery);
            DataRecordMetadata drMetaData = SQLUtil.dbMetadata2jetel(resultSet.getMetaData(), getJdbcSpecific());
            return drMetaData;
        } finally {
            // make sure we close all connection resources
        	SQLUtil.closeConnection(resultSet, statement, connection);
        }
    }
    
    protected abstract SqlConnection connect(OperationType opType) throws JetelException;
    
    /**
     * Saves to the given output stream all DBConnection properties.
     * @param outStream
     * @throws IOException
     */
    @Override
	public void saveConfiguration(OutputStream outStream) throws IOException {
    	saveConfiguration(outStream, null);
    }
    
    /**
     * Saves to the given output stream all DBConnection properties.
     * @param outStream
     * @throws IOException
     */
    @Override
	public void saveConfiguration(OutputStream outStream, Properties moreProperties) throws IOException {
        Properties propsToStore = new Properties();

        TypedProperties extraProperties = getExtraProperties();
        Set<Object> jdbcProps = extraProperties.keySet();
        for (Object key : jdbcProps) {
        	String propName = (String) key; 
			propsToStore.setProperty(XML_JDBC_PROPERTIES_PREFIX + propName, extraProperties.getProperty(propName));
		}

        if (moreProperties != null) {
        	for (Enumeration<?> enu = moreProperties.propertyNames(); enu.hasMoreElements(); ) {
        		String key = (String) enu.nextElement();
        		propsToStore.setProperty(key, moreProperties.getProperty(key));
        	}
        }
        
        
        if(getUser() != null) {
        	propsToStore.setProperty(XML_USER_ATTRIBUTE, getUser());
        }
        if(getPassword() != null) {
        	propsToStore.setProperty(XML_PASSWORD_ATTRIBUTE, getPassword());
        }
        if(getDbUrl() != null) {
        	propsToStore.setProperty(XML_DBURL_ATTRIBUTE, getDbUrl());
        }
        if(getDbDriver() != null) {
        	propsToStore.setProperty(XML_DBDRIVER_ATTRIBUTE, getDbDriver());
        }
        if(getDatabase() != null) {
        	propsToStore.setProperty(XML_DATABASE_ATTRIBUTE, getDatabase());
        }
        if(getDriverLibrary() != null) {
        	propsToStore.setProperty(XML_DRIVER_LIBRARY_ATTRIBUTE, getDriverLibrary());
        }
        if(getJdbcSpecificId() != null) {
        	propsToStore.setProperty(XML_JDBC_SPECIFIC_ATTRIBUTE, getJdbcSpecificId());
        }
        if(getJndiName() != null) {
        	propsToStore.setProperty(XML_JNDI_NAME_ATTRIBUTE, getJndiName());
        }
        if (getHoldability() != null) {
        	propsToStore.setProperty(XML_HOLDABILITY, Integer.toString(getHoldability()));
        }
        if (getTransactionIsolation() != null) {
        	propsToStore.setProperty(XML_TRANSACTION_ISOLATION, Integer.toString(getTransactionIsolation()));
        }
        propsToStore.setProperty(XML_THREAD_SAFE_CONNECTIONS, Boolean.toString(isThreadSafeConnections()));
        propsToStore.setProperty(XML_IS_PASSWORD_ENCRYPTED, Boolean.toString(isPasswordEncrypted()));

        propsToStore.store(outStream, null);
    }
    
    public abstract String getJdbcSpecificId();
}
