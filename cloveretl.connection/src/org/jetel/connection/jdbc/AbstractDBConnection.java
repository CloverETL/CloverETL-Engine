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
import org.jetel.database.sql.JdbcSpecific.OperationType;
import org.jetel.database.sql.SqlConnection;
import org.jetel.exception.JetelException;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.graph.GraphElement;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.TransformationGraphXMLReaderWriter;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataFieldType;
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
	
	public final static String OPTIMIZE_QUERY_PROPERTY = "sqlOptimization";

	public final static String UNKNOWN_JDBC_TYPES_AS_STRING_PROPERTY = "unknownJdbcTypesAsString";
	
    public static final String XML_JDBC_PROPERTIES_PREFIX = "jdbc.";
    
    /** CLO-4510 */
    public enum SqlQueryOptimizeOption {
    	TRUE, FALSE, NAIVE, BASIC_TRAVERSE
    }
    
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
        try {
        	// CLO-4238:
        	SQLScriptParser sqlParser = new SQLScriptParser();
        	sqlParser.setBackslashQuoteEscaping(getJdbcSpecific().isBackslashEscaping());
        	sqlParser.setRequireLastDelimiter(false);
        	sqlParser.setStringInput(sqlQuery);
			sqlQuery = sqlParser.getNextStatement();
		} catch (IOException e1) {
			logger.warn("Failed to parse SQL query", e1);
		}
        
        String optimizeProperty = parameters.getProperty(OPTIMIZE_QUERY_PROPERTY);
        SqlQueryOptimizeOption optimize;
        if (optimizeProperty == null) {
        	optimize = SqlQueryOptimizeOption.FALSE;
        } else {
        	try {
        		optimize = SqlQueryOptimizeOption.valueOf(optimizeProperty.toUpperCase());
        	} catch (Exception e) {
        		optimize = SqlQueryOptimizeOption.FALSE;
        	}
        }

        if (optimize != SqlQueryOptimizeOption.FALSE) {
        	logger.debug("Optimizing sql query for dynamic metadata. Original query: " + sqlQuery);
	        switch (optimize) {
	        case TRUE:
        		sqlQuery = SQLUtil.encloseInQptimizingQuery(sqlQuery);
        		break;
	        case NAIVE:
        		sqlQuery = SQLUtil.appendOptimizingWhereClause(sqlQuery);
	        	break;
	        case BASIC_TRAVERSE: {
	    		sqlQuery = SQLUtil.modifyQueryUsingBasicTraversal(sqlQuery);
	        	break;
	        }
	        default:
	        	//empty
	        }
        	logger.debug("Optimizing sql query for dynamic metadata. Optimized query: " + sqlQuery);
        }

        //process 'unknownJdbcTypesAsString' parameter, which allows to process even directly unsupported data types, see CLO-11740
        String sUnknownJdbcTypesAsString = parameters.getProperty(UNKNOWN_JDBC_TYPES_AS_STRING_PROPERTY, "false");
        boolean unknownJdbcTypesAsString = Boolean.valueOf(sUnknownJdbcTypesAsString);
        
        Connection connection;
		try {
			connection = connect(OperationType.UNKNOWN);
		} catch (JetelException | JetelRuntimeException e) {
			throw new SQLException(e);
		}
        
        try {
            statement = connection.createStatement();
            resultSet = statement.executeQuery(sqlQuery);
            // CLO-11167: warn when query returns records
            if (resultSet.next()) {
            	String id = parameters.getProperty(TransformationGraphXMLReaderWriter.ID_ATTRIBUTE);
            	StringBuilder sb = new StringBuilder();
            	sb.append("SQL query for dynamic metadata ");
            	if (!StringUtils.isEmpty(id)) {
            		sb.append("(id=");
            		sb.append(id);
            		sb.append(") ");
            	}
            	sb.append("should return zero records. Consider using TOP, LIMIT or ROWNUM clause in your query, or adding metadata attribute sqlOptimization=\"true\".");
            	logger.warn(sb.toString());
            }
            DataRecordMetadata drMetaData = SQLUtil.dbMetadata2jetel(resultSet.getMetaData(), getJdbcSpecific(), !unknownJdbcTypesAsString);
            if (unknownJdbcTypesAsString) {
            	//convert unknown data types to string
            	for (DataFieldMetadata fieldMetadata : drMetaData) {
            		if (fieldMetadata.getDataType() == DataFieldType.UNKNOWN) {
            			fieldMetadata.setDataType(DataFieldType.STRING);
            		}
            	}
            }
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
