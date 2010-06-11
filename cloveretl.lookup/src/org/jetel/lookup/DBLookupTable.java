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

import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.commons.logging.LogFactory;
import org.jetel.connection.jdbc.CopySQLData;
import org.jetel.connection.jdbc.DBConnection;
import org.jetel.connection.jdbc.SQLCloverStatement;
import org.jetel.connection.jdbc.SQLUtil;
import org.jetel.connection.jdbc.specific.DBConnectionInstance;
import org.jetel.connection.jdbc.specific.JdbcSpecific;
import org.jetel.connection.jdbc.specific.JdbcSpecific.OperationType;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.HashKey;
import org.jetel.data.NullRecord;
import org.jetel.data.RecordKey;
import org.jetel.data.lookup.Lookup;
import org.jetel.data.lookup.LookupTable;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.GraphConfigurationException;
import org.jetel.exception.JetelException;
import org.jetel.exception.NotInitializedException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.exception.ConfigurationStatus.Priority;
import org.jetel.exception.ConfigurationStatus.Severity;
import org.jetel.graph.GraphElement;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.primitive.SimpleCache;
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
 *              type NMTOKEN (dbLookup) #REQUIRED
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

    private static final String XML_LOOKUP_TYPE_DB_LOOKUP = "dbLookup"; 
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
	protected DBConnectionInstance dbConnection;
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
	public DataRecordMetadata getMetadata() {
		if (dbMetadata == null){
			metadataSearching:
			for (DBLookup activeLookup : activeLookups) {
				if (activeLookup.getMetadata() != null) {
					dbMetadata = activeLookup.getMetadata();
					break metadataSearching;
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
				dbConnection = connection.getConnection(getId(), OperationType.READ);
			} catch (JetelException e) {
				throw new ComponentNotReadyException("Can't connect to database: " + e.getMessage(), e);
			}
		} else {
			if (getGraph() != null && getGraph().getRuntimeContext().isBatchMode() && connection.isThreadSafeConnections()) {
				try {
					dbConnection = connection.getConnection(getId(), OperationType.READ);
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
	
	@Override
    public synchronized void reset() throws ComponentNotReadyException {
    	super.reset();
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
			dbMetadata = getGraph().getDataRecordMetadata(metadataId);
			if (dbMetadata == null) {
				status.add(new ConfigurationProblem("Metadata " + StringUtils.quote(metadataId) + 
						" does not exist. DB metadata will be created from sql query.", Severity.WARNING, this, 
						Priority.LOW, XML_METADATA_ID));
			}
		}
        return status;
    }

	public void toXML(Element xmlElement) {
		// TODO Auto-generated method stub
		
	}
    
    public Iterator<DataRecord> iterator() {
        if (!isInitialized()) {
            throw new NotInitializedException(this);
        }

        try {
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
        	SQLCloverStatement st = new SQLCloverStatement(dbConnection, query.toString(), null);
        	st.init();
			ResultSet resultSet = st.executeQuery();
			dbConnection.getJdbcSpecific().optimizeResultSet(resultSet, OperationType.READ);
		   if (dbMetadata == null) {
	            if (st.getCloverOutputFields() == null) {
					dbMetadata = SQLUtil.dbMetadata2jetel(resultSet	.getMetaData(), dbConnection.getJdbcSpecific());
				}else{
					ResultSetMetaData dbMeta = resultSet.getMetaData();
					JdbcSpecific jdbcSpecific = dbConnection.getJdbcSpecific();
					String[] fieldName = st.getCloverOutputFields();
					DataFieldMetadata fieldMetadata;
					String tableName = dbMeta.getTableName(1);
					if (!StringUtils.isValidObjectName(tableName)) {
						tableName = StringUtils.normalizeName(tableName);
					}
					dbMetadata = new DataRecordMetadata(tableName, DataRecordMetadata.DELIMITED_RECORD);
					dbMetadata.setFieldDelimiter(Defaults.Component.KEY_FIELDS_DELIMITER);
					dbMetadata.setRecordDelimiter("\n");
					for (int i = 1; i <= dbMeta.getColumnCount(); i++) {
						fieldMetadata = SQLUtil.dbMetadata2jetel(fieldName[i], dbMeta, i, jdbcSpecific);
						dbMetadata.addField(fieldMetadata);
					}
				}
		   }
		   DataRecord record = new DataRecord(dbMetadata);
		   record.init();
			CopySQLData[] transMap = CopySQLData.sql2JetelTransMap(SQLUtil.getFieldTypes(dbMetadata, dbConnection.getJdbcSpecific()), 
					dbMetadata, record);
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
		}
   }
    
	public Lookup createLookup(RecordKey key) throws ComponentNotReadyException {
		return createLookup(key, null);
	}
	
	public Lookup createLookup(RecordKey key, DataRecord keyRecord) throws ComponentNotReadyException {
        if (!isInitialized()) {
            throw new NotInitializedException(this);
        }

        DBLookup lookup;
        key.init();

        try {
            lookup = new DBLookup(new SQLCloverStatement(dbConnection, sqlQuery, keyRecord, key.getKeyFieldNames()), key, keyRecord);
        } catch (SQLException e) {
            throw new ComponentNotReadyException(this, e);
        }

        lookup.setLookupTable(this);
        lookup.setCacheSize(maxCached);
        lookup.setStoreNulls(storeNulls);
        activeLookups.add(lookup);

        return lookup;
	}
	
	public DataRecordMetadata getKeyMetadata() throws ComponentNotReadyException {
        if (!isInitialized()) {
            throw new NotInitializedException(this);
        }

        DataRecordMetadata dbMetadata = getMetadata();

        if (dbMetadata != null) {
            return dbMetadata;
        }

        DataRecord tmpRecord = new DataRecord(new DataRecordMetadata("_tmp_"));
        tmpRecord.init();

        SQLCloverStatement statement = new SQLCloverStatement(dbConnection, sqlQuery, tmpRecord);

        try {
            statement.init();
        } catch (SQLException e) {
            throw new ComponentNotReadyException(this, e);
        }

        try {
            ParameterMetaData sqlMetadata = ((PreparedStatement) statement.getStatement()).getParameterMetaData();

            return SQLUtil.dbMetadata2jetel(sqlMetadata, "_dbLookupTable_" + getName(), dbConnection.getJdbcSpecific());
        } catch (SQLException e) {
            throw new RuntimeException("Can't get metadata from database", e);
        }
	}
	
	public boolean isPutSupported() {
		return false;
	}
	
    public boolean isRemoveSupported() {
        return false;
    }
    
	public boolean put(DataRecord dataRecord) {
		throw new UnsupportedOperationException(); 
	}
	
	public boolean remove(DataRecord dataRecord) {
		throw new UnsupportedOperationException(); 
	}
	
	public boolean remove(HashKey key) {
		throw new UnsupportedOperationException(); 
	}
    
}
/**
 * Implementation of lookup for DBLookupTable. 
 * 
 * @author Agata Vackova (agata.vackova@javlinconsulting.cz) ; 
 * (c) JavlinConsulting s.r.o.
 *  www.javlinconsulting.cz
 *
 * @since Nov 6, 2008
 */
class DBLookup implements Lookup{

	private DBLookupTable lookupTable;
	private DataRecord inRecord;
	private RecordKey recordKey;
	private HashKey key;
	
	private boolean storeNulls;
	private int cacheSize;

	private SimpleCache resultCache;
	private List<DataRecord> result;
	private List<DataRecord> resultList = new ArrayList<DataRecord>();
	private DataRecord currentResult;
	private int no;
	private boolean hasNext;
	
	private int cacheNumber = 0;
	private int totalNumber = 0;

	private DataRecordMetadata dbMetadata;
	private SQLCloverStatement statement;
	private ResultSet resultSet;
	private CopySQLData[] transMap;
	
	DBLookup(SQLCloverStatement statement, RecordKey key, DataRecord record) throws ComponentNotReadyException, SQLException {
		this.recordKey = key;
		this.inRecord = record;
		this.key = new HashKey(recordKey, inRecord);
		this.statement = statement;
		statement.init();
	}
	
	public RecordKey getKey() {
		return recordKey;
	}

	void setLookupTable(DBLookupTable lookupTable){
		this.lookupTable = lookupTable;
		dbMetadata = lookupTable.getMetadata();
	}
	
	public LookupTable getLookupTable() {
		return lookupTable;
	}

	public int getNumFound() {
    	if (resultCache!=null){
    		return result != null ? result.size() : 0;
    	}
        if (resultSet != null) {
            try {
                int curRow=resultSet.getRow();
                resultSet.last();
                int count=resultSet.getRow();
                resultSet.first();
                resultSet.absolute(curRow);
                return count;
            } catch (SQLException ex) {
                return -1;
            }
        }
       throw new IllegalStateException("Looking up has been never performed. Call seek method first");
	}

	public void seek() {
		if (resultCache == null && resultSet == null) {//first seek
			if (cacheSize>0){
		        this.resultCache= new SimpleCache(cacheSize);
		        resultCache.enableDuplicity();
		    }
		}
		try {
			if (resultCache != null) {
				seekInCache();
			}else {
				seekInDB();
				hasNext = fetch();
			}
			totalNumber++;
			no = 0;
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	private void seekInDB() throws SQLException {
		//execute query
	   resultSet = statement.executeQuery();
	   
	   if (dbMetadata == null) {
		   if (statement.getCloverOutputFields() == null) {
			dbMetadata = SQLUtil.dbMetadata2jetel(resultSet.getMetaData(), lookupTable.dbConnection.getJdbcSpecific());
		   }else{
				ResultSetMetaData dbMeta = resultSet.getMetaData();
				JdbcSpecific jdbcSpecific = lookupTable.dbConnection.getJdbcSpecific();
				String[] fieldName = statement.getCloverOutputFields();
				DataFieldMetadata fieldMetadata;
				String tableName = dbMeta.getTableName(1);
				if (!StringUtils.isValidObjectName(tableName)) {
					tableName = StringUtils.normalizeName(tableName);
				}
				dbMetadata = new DataRecordMetadata(tableName, DataRecordMetadata.DELIMITED_RECORD);
				dbMetadata.setFieldDelimiter(Defaults.Component.KEY_FIELDS_DELIMITER);
				dbMetadata.setRecordDelimiter("\n");
				for (int i = 1; i <= dbMeta.getColumnCount(); i++) {
					fieldMetadata = SQLUtil.dbMetadata2jetel(fieldName[i], dbMeta, i, jdbcSpecific);
					dbMetadata.addField(fieldMetadata);
				}
		   }
	   }
	}
	
	private void seekInCache() throws SQLException {
		if (!resultCache.containsKey(key)) {
			if (inRecord == null) throw new IllegalStateException("No key data for performing lookup");
            result = resultList;
            result.clear();

			seekInDB();
		    HashKey hashKey = new HashKey(recordKey, inRecord.duplicate());

            if (fetch()) {
		        boolean resultCached = true;

		        do {
		            DataRecord dataRecord = currentResult.duplicate();
					resultCached &= resultCache.put(hashKey, dataRecord);
                    result.add(dataRecord);
				} while (fetch());

	            if (!resultCached) {
                    LogFactory.getLog(getClass()).warn("Too many data records for a single key! Enlarge the cache " +
                    		"size to accomodate more data records...");
                    resultCache.clear();
		        } 
		    } else if (storeNulls) {
                resultCache.put(hashKey, NullRecord.NULL_RECORD);
                result.add(NullRecord.NULL_RECORD);
            }
		} else {
            result = resultCache.getAll(key);
            cacheNumber++;
        }
	}
	
	private boolean fetch() throws SQLException {
		if (!resultSet.next()) {
			return false;
		}
		if (transMap == null) {
			currentResult = new DataRecord(dbMetadata);
			currentResult.init();
			transMap =  CopySQLData.sql2JetelTransMap(SQLUtil.getFieldTypes(dbMetadata, lookupTable.dbConnection.getJdbcSpecific()), 
					dbMetadata, currentResult);
		}
		//get data from results
		for (int i = 0; i < transMap.length; i++) {
			transMap[i].sql2jetel(resultSet);
		}
		return true;
	}

	public void seek(DataRecord keyRecord) {
		key.setDataRecord(keyRecord);
		try {
			statement.setInRecord(keyRecord);
		} catch (ComponentNotReadyException e) {
			throw new IllegalStateException(e);
		}
		seek();
	}

	public boolean hasNext() {
		if (resultCache == null && resultSet == null) {
			throw new IllegalStateException("Looking up has been never performed. Call seek method first");
		}
		return resultCache == null ? hasNext :
			result != null && no < result.size() && result.get(no) != NullRecord.NULL_RECORD;
	}

	public DataRecord next() {
		if (resultCache == null && resultSet == null) {
			throw new IllegalStateException("Looking up has been never performed. Call seek method first");
		}
		if (resultCache != null) {
			if (no >= result.size()) throw new NoSuchElementException();
			return result.get(no++);
		}
		DataRecord tmp = currentResult.duplicate();
		try {
			hasNext = fetch();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
		return tmp;
	}

	public void close() throws SQLException{
		statement.close();
	}
	
	public void remove() {
		throw new UnsupportedOperationException();
	}
	
	public void setCacheSize(int cacheSize) {
		this.cacheSize = cacheSize;
	}
	
	public int getCacheSize(){
		return cacheSize;
	}

	public void setStoreNulls(boolean storeNulls){
		this.storeNulls = storeNulls;
	}

	public boolean isStoreNulls() {
		return storeNulls;
	}
 
	public int getCacheNumber() {
		return cacheNumber;
	}

	public int getTotalNumber() {
		return totalNumber;
	}
	
	public DataRecordMetadata getMetadata(){
		return dbMetadata;
	}
}
