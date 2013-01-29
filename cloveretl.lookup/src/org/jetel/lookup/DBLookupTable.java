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
package org.jetel.lookup;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.jetel.connection.jdbc.AbstractCopySQLData;
import org.jetel.connection.jdbc.SQLCloverStatement;
import org.jetel.connection.jdbc.SQLUtil;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.Defaults;
import org.jetel.data.HashKey;
import org.jetel.data.RecordKey;
import org.jetel.data.lookup.Lookup;
import org.jetel.data.lookup.LookupTable;
import org.jetel.database.sql.CopySQLData;
import org.jetel.database.sql.DBConnection;
import org.jetel.database.sql.JdbcSpecific;
import org.jetel.database.sql.JdbcSpecific.OperationType;
import org.jetel.database.sql.SqlConnection;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.ConfigurationStatus.Priority;
import org.jetel.exception.ConfigurationStatus.Severity;
import org.jetel.exception.GraphConfigurationException;
import org.jetel.exception.JetelException;
import org.jetel.exception.NotInitializedException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.GraphElement;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.metadata.DataRecordParsingType;
import org.jetel.util.primitive.TypedProperties;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.string.StringUtils;
import org.w3c.dom.Element;

/**
 *  Database table/SQLquery based lookup table which gets data by performing SQL
 *  query. Caching of found values can be provided - if the constructor with
 *  <code>numCached</code> parameter is used. 
 * 
 * The XML DTD describing the internal structure is as follows:
 * 
 *  * &lt;!ATTLIST LookupTable
 *              id ID #REQUIRED
 *              type NMTOKEN (DBLookup) #REQUIRED
 *              metadata CDATA #REQUIRED
 *              sqlQuery CDATA #REQUIRED
 *              dbConnection CDATA #REQUIRED
 *              maxCached CDATA #IMPLIED&gt;
 *              storeNulls CDATA #IMPLIED&gt;
 * 
 *
 *@author     dpavlis
 *@since      May 22, 2003
 */
public class DBLookupTable extends GraphElement implements LookupTable {

    private static final String XML_LOOKUP_TYPE_DB_LOOKUP = "DBLookup"; 
    private static final String XML_SQL_QUERY = "sqlQuery";
    private static final String XML_LOOKUP_MAX_CACHE_SIZE = "maxCached";
    private static final String XML_STORE_NULL_RESPOND = "storeNulls";
    
    private final static String[] REQUESTED_ATTRIBUTE = {XML_ID_ATTRIBUTE, XML_TYPE_ATTRIBUTE, XML_DBCONNECTION,
    	XML_SQL_QUERY
    };
    
    protected String metadataId;
    protected String connectionId;

	protected DataRecordMetadata dbMetadata;
	protected DBConnection connection;
	protected SqlConnection sqlConnection;
	protected String sqlQuery;//this query can contain $field
	
	protected int maxCached = 0;
	protected boolean storeNulls = false;
	
	private List<DBLookup> activeLookups = new ArrayList<DBLookup>();
	
	public DBLookupTable(String id, String connectionId, String metadataId, String sqlQuery){
		super(id);
		this.connectionId = connectionId;
		this.metadataId = metadataId;
		this.sqlQuery = sqlQuery;
	}
	/**
	 *  Constructor for the DBLookupTable object
	 *
	 *@param  dbConnection      Description of the Parameter
	 *@param  dbRecordMetadata  Description of the Parameter
	 *@param  sqlQuery          Description of the Parameter
	 */
	public DBLookupTable(String id, DBConnection connection,
			DataRecordMetadata dbRecordMetadata, String sqlQuery) {
		super(id);
		this.connection = connection;
		this.dbMetadata = dbRecordMetadata;
		this.sqlQuery = sqlQuery;
	}

	public DBLookupTable(String id, DBConnection connection,
			DataRecordMetadata dbRecordMetadata, String sqlQuery, int numCached) {
		super(id);
		this.connection = connection;
		this.dbMetadata = dbRecordMetadata;
		this.sqlQuery = sqlQuery;
		this.maxCached = numCached;
	}
  
	/**
	 *  Gets the dbMetadata attribute of the DBLookupTable object.<br>
	 *  <i>init() should be called prior to calling this method (unless dbMetadata
	 * was passed in using appropriate constructor.</i>
	 *
	 *@return    The dbMetadata value
	 */
	@Override
	public DataRecordMetadata getMetadata() {
		if (dbMetadata == null){
			for (DBLookup activeLookup : activeLookups) {
				if (activeLookup.getMetadata() != null) {
					dbMetadata = activeLookup.getMetadata();
					break;
				}
			}
		}
		return dbMetadata;
	}
	
    /**
	 *  Initialization of lookup table - loading all data into it.
	 *
	 *@exception  JetelException  Description of the Exception
	 *@since                      May 2, 2002
	 */
    @Override
	synchronized public void init() throws ComponentNotReadyException {
        if (isInitialized()) {
//            throw new IllegalStateException("The lookup table has already been initialized!");
        	return;
        }

		super.init();
		
		if (connection == null) {
			connection = (DBConnection) getGraph().getConnection(connectionId);
			if (connection == null) {
				throw new ComponentNotReadyException("Connection " + StringUtils.quote(connectionId) + " does not exist!!!");
			}
			connection.init();
		}
		if (metadataId != null) {
			dbMetadata = getGraph().getDataRecordMetadata(metadataId, true);
		}
		
    }
    
	@Override
	public synchronized void preExecute() throws ComponentNotReadyException {
		super.preExecute();
		if (firstRun()) {// a phase-dependent part of initialization
			try {
				sqlConnection = connection.getConnection(getId(), OperationType.READ);
			} catch (JetelException e) {
				throw new ComponentNotReadyException("Can't connect to database: " + e.getMessage(), e);
			}
		} else {
			if (getGraph() != null && getGraph().getRuntimeContext().isBatchMode() && connection.isThreadSafeConnections()) {
				try {
					sqlConnection = connection.getConnection(getId(), OperationType.READ);
				} catch (JetelException e) {
					throw new ComponentNotReadyException("Can't connect to database: " + e.getMessage(), e);
				}
			}
		}
	}
    
    @Override
	public void postExecute() throws ComponentNotReadyException {
		super.postExecute();
		try {
			for (DBLookup activeLookup : activeLookups) {
				activeLookup.close();
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} finally {
			activeLookups.clear();
		}
		if (getGraph() != null && getGraph().getRuntimeContext().isBatchMode()) {
			connection.closeConnection(getId(), OperationType.READ);
		}
	}
	
    public static DBLookupTable fromProperties(TypedProperties properties) 
    throws AttributeNotFoundException, GraphConfigurationException{

    	for (String property : REQUESTED_ATTRIBUTE) {
			if (!properties.containsKey(property)) {
				throw new AttributeNotFoundException(property);
			}
		}
        
    	String type = properties.getStringProperty(XML_TYPE_ATTRIBUTE);
    	if (!type.equalsIgnoreCase(XML_LOOKUP_TYPE_DB_LOOKUP)){
    		throw new GraphConfigurationException("Can't create db lookup table from type " + type);
    	}
    	
        DBLookupTable lookupTable = new DBLookupTable(properties.getProperty(XML_ID_ATTRIBUTE), 
        		properties.getStringProperty(XML_DBCONNECTION), properties.getStringProperty(XML_METADATA_ID), 
        		properties.getStringProperty(XML_SQL_QUERY));
        
        if (properties.containsKey(XML_NAME_ATTRIBUTE)){
        	lookupTable.setName(properties.getStringProperty(XML_NAME_ATTRIBUTE));
        }
        
        if(properties.containsKey(XML_LOOKUP_MAX_CACHE_SIZE)) {
            lookupTable.setNumCached(properties.getIntProperty(XML_LOOKUP_MAX_CACHE_SIZE));
        }
        if (properties.containsKey(XML_STORE_NULL_RESPOND)){
        	lookupTable.setStoreNulls(properties.getBooleanProperty(XML_STORE_NULL_RESPOND));
        }
        
        return lookupTable;
    }
    
    public static DBLookupTable fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException {
        ComponentXMLAttributes xattribs = new ComponentXMLAttributes(xmlElement, graph);
        DBLookupTable lookupTable = null;
        String id;
        String type;
        
        //reading obligatory attributes
        try {
            id = xattribs.getString(XML_ID_ATTRIBUTE);
            type = xattribs.getString(XML_TYPE_ATTRIBUTE);
        } catch(AttributeNotFoundException ex) {
            throw new XMLConfigurationException("Can't create lookup table - " + ex.getMessage(), ex);
        }
        
        //check type
        if (!type.equalsIgnoreCase(XML_LOOKUP_TYPE_DB_LOOKUP)) {
            throw new XMLConfigurationException("Can't create db lookup table from type " + type);
        }
        
        //create db lookup table
        //String[] keys = xattribs.getString(XML_LOOKUP_KEY).split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);
        
        try {
            lookupTable = new DBLookupTable(id, xattribs.getString(XML_DBCONNECTION),
                    xattribs.exists(XML_METADATA_ID) ? xattribs.getString(XML_METADATA_ID) : null, xattribs.getString(XML_SQL_QUERY));
            
            if (xattribs.exists(XML_NAME_ATTRIBUTE)){
            	lookupTable.setName(xattribs.getString(XML_NAME_ATTRIBUTE));
            }
            
            if(xattribs.exists(XML_LOOKUP_MAX_CACHE_SIZE)) {
                lookupTable.setNumCached(xattribs.getInteger(XML_LOOKUP_MAX_CACHE_SIZE));
            }
            if (xattribs.exists(XML_STORE_NULL_RESPOND)) {
            	lookupTable.setStoreNulls(xattribs.getBoolean(XML_STORE_NULL_RESPOND));
            }
            
            return lookupTable;
        } catch (AttributeNotFoundException ex) {
            throw new XMLConfigurationException(ex);
        }
    }

    @Override
    public synchronized void free() {
        if (isInitialized()) {
            super.free();
            
            try {
                for (DBLookup activeLookup : activeLookups) {
                    activeLookup.close();
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            } finally {
                activeLookups.clear();
            }
        }
    }
	
	/**
	 * Set max number of records stored in cache
	 * 
	 * @param numCached
	 */
	public void setNumCached(int numCached){
        this.maxCached=numCached;
	}

	/**
	 * Set max number of records stored in cache
	 * 
	 * @param numCached max number of stored records
	 * @param storeNulls inicates if store key for which there aren't records in db
	 */
	public void setStoreNulls(boolean storeNulls){
		this.storeNulls = storeNulls;
	}

    @Override
    public ConfigurationStatus checkConfig(ConfigurationStatus status) {
        super.checkConfig(status);

		if (connection == null) {
			DBConnection tmp = (DBConnection)getGraph().getConnection(connectionId);
			if (tmp == null) {
				status.add(new ConfigurationProblem("Connection " + StringUtils.quote(connectionId) + 
						" does not exist!!!", Severity.ERROR, this, Priority.NORMAL, XML_DBCONNECTION));
			}
		}

		if (metadataId != null) {
			dbMetadata = getGraph().getDataRecordMetadata(metadataId, false);
			if (dbMetadata == null) {
				status.add(new ConfigurationProblem("Metadata " + StringUtils.quote(metadataId) + 
						" does not exist. DB metadata will be created from sql query.", Severity.WARNING, this, 
						Priority.LOW, XML_METADATA_ID));
			}
		}
        return status;
    }

    @Override
	public Iterator<DataRecord> iterator() {
        if (!isInitialized()) {
            throw new NotInitializedException(this);
        } else if (sqlConnection == null) {
        	throw new NotInitializedException("No DB connection! (pre-execute initialization not performed?)", this);
        }
        
    	//remove WHERE condidion from sql query
    	StringBuilder query = new StringBuilder(sqlQuery);
    	int whereIndex = query.toString().toLowerCase().indexOf("where");
    	int groupIndex = query.toString().toLowerCase().indexOf("group");
    	int orderIndex = query.toString().toLowerCase().indexOf("order");
    	if (whereIndex > -1){
    		if (groupIndex > -1 || orderIndex > -1){
    			query.delete(whereIndex, groupIndex);
    		}else{
    			query.setLength(whereIndex);
    		}
    	}
    	synchronized (sqlConnection) {
    		return iteratorImpl(query.toString());
    	}
   }
    
    private Iterator<DataRecord> iteratorImpl(String query) {
    	
    	ResultSet resultSet = null;
        
       	try {
        	SQLCloverStatement st = new SQLCloverStatement(sqlConnection, query, null);
        	st.init();

        	resultSet = st.executeQuery();
			sqlConnection.getJdbcSpecific().optimizeResultSet(resultSet, OperationType.READ);
		   
			if (dbMetadata == null) {
	            if (st.getCloverOutputFields() == null) {
					dbMetadata = SQLUtil.dbMetadata2jetel(resultSet	.getMetaData(), sqlConnection.getJdbcSpecific());
				}else{
					ResultSetMetaData dbMeta = resultSet.getMetaData();
					JdbcSpecific jdbcSpecific = sqlConnection.getJdbcSpecific();
					String[] fieldName = st.getCloverOutputFields();
					DataFieldMetadata fieldMetadata;
					String tableName = dbMeta.getTableName(1);
					dbMetadata = new DataRecordMetadata(DataRecordMetadata.EMPTY_NAME, DataRecordParsingType.DELIMITED);
					dbMetadata.setLabel(tableName);
					dbMetadata.setFieldDelimiter(Defaults.Component.KEY_FIELDS_DELIMITER);
					dbMetadata.setRecordDelimiter("\n");
					for (int i = 1; i <= dbMeta.getColumnCount(); i++) {
						fieldMetadata = SQLUtil.dbMetadata2jetel(fieldName[i], dbMeta, i, jdbcSpecific);
						dbMetadata.addField(fieldMetadata);
					}
					dbMetadata.normalize();
				}
		   }
		   DataRecord record = DataRecordFactory.newRecord(dbMetadata);
		   record.init();
			CopySQLData[] transMap = AbstractCopySQLData.sql2JetelTransMap(SQLUtil.getFieldTypes(dbMetadata, sqlConnection.getJdbcSpecific()), 
					dbMetadata, record, sqlConnection.getJdbcSpecific());
			ArrayList<DataRecord> records = new ArrayList<DataRecord>();
			while (resultSet.next()){
				for (int i = 0; i < transMap.length; i++) {
					transMap[i].sql2jetel(resultSet);
				}
				records.add(record.duplicate());
			}
			return records.iterator();
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			try {
				if (resultSet != null)
					resultSet.close();
			} catch (SQLException e) {
				// we ignore this, as we are only trying to close the stream after exception was thrown before 
				// - the orignal exception is the one the user is interested in, not this unsuccessful attempt
				// to clean up
			}
		}
    }
    
	@Override
	public Lookup createLookup(RecordKey key) throws ComponentNotReadyException {
		return createLookup(key, null);
	}
	
	@Override
	public Lookup createLookup(RecordKey key, DataRecord keyRecord) throws ComponentNotReadyException {
        if (!isInitialized()) {
            throw new NotInitializedException(this);
        } else if (sqlConnection == null) {
        	throw new NotInitializedException("No DB connection! (pre-execute initialization not performed?)", this);
        }

        DBLookup lookup;
        key.init();

        try {
        	lookup = new DBLookup(new SQLCloverStatement(sqlConnection, sqlQuery, keyRecord, key.getKeyFieldNames()),
    				key, keyRecord);
        } catch (SQLException e) {
            throw new ComponentNotReadyException(this, e);
        }
        lookup.setLookupTable(this);
        activeLookups.add(lookup);

        return lookup;
	}
	
	@Override
	public DataRecordMetadata getKeyMetadata() throws ComponentNotReadyException {
		throw new UnsupportedOperationException("DBLookupTable does not provide key metadata.");
	}
	
	@Override
	public boolean isPutSupported() {
		return false;
	}
	
    @Override
	public boolean isRemoveSupported() {
        return false;
    }
    
	@Override
	public boolean put(DataRecord dataRecord) {
		throw new UnsupportedOperationException(); 
	}
	
	@Override
	public boolean remove(DataRecord dataRecord) {
		throw new UnsupportedOperationException(); 
	}
	
	@Override
	public boolean remove(HashKey key) {
		throw new UnsupportedOperationException(); 
	}
	@Override
	public void setCurrentPhase(int phase) {
		//isn't required by the lookup table
	}
}
