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
package org.jetel.lookup;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;

import org.jetel.connection.jdbc.CopySQLData;
import org.jetel.connection.jdbc.DBConnection;
import org.jetel.connection.jdbc.SQLUtil;
import org.jetel.connection.jdbc.specific.DBConnectionInstance;
import org.jetel.connection.jdbc.specific.JdbcSpecific.OperationType;
import org.jetel.data.DataRecord;
import org.jetel.data.HashKey;
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
    //private static final String XML_LOOKUP_KEY = "key";
    private static final String XML_SQL_QUERY = "sqlQuery";
    private static final String XML_LOOKUP_MAX_CACHE_SIZE = "maxCached";
    private static final String XML_STORE_NULL_RESPOND = "storeNulls";
    
    private final static String[] REQUESTED_ATTRIBUTE = {XML_ID_ATTRIBUTE, XML_TYPE_ATTRIBUTE, XML_DBCONNECTION,
    	XML_SQL_QUERY
    };
    
    protected String metadataId;
    protected String connectionId;

	protected DataRecordMetadata dbMetadata;
	protected DBConnectionInstance dbConnection;
	protected PreparedStatement pStatement;
//	protected RecordKey lookupKey;
	protected int[] keyFields;
	protected String[] keys;
	protected String sqlQuery;
//	protected ResultSet resultSet;
//	protected CopySQLData[] transMap;
	protected CopySQLData[] keyTransMap;
//	protected DataRecord dbDataRecord;
	protected DataRecord keyDataRecord = null;
//	protected SimpleCache resultCache;
	
	protected int maxCached = 0;
	protected HashKey cacheKey;
	protected boolean storeNulls = false;
	
//	protected int cacheNumber = 0;
//	protected int totalNumber = 0;
  
	
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
	public DBLookupTable(String id, DBConnectionInstance dbConnection,
			DataRecordMetadata dbRecordMetadata, String sqlQuery) {
		super(id);
		this.dbConnection = dbConnection;
		this.dbMetadata = dbRecordMetadata;
		this.sqlQuery = sqlQuery;
	}

	public DBLookupTable(String id, DBConnectionInstance dbConnection,
			DataRecordMetadata dbRecordMetadata, String sqlQuery, int numCached) {
		super(id);
		this.dbConnection = dbConnection;
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
		return dbMetadata;
	}

	ResultSet seek(HashKey key){
		if (!isInitialized()) {
			throw new NotInitializedException(this);
		}
        try {
			pStatement.clearParameters();
			// initialization of trans map if it was not already done
			if (keyTransMap==null){
			    
			    try {
			        keyTransMap = CopySQLData.jetel2sqlTransMap(
			                SQLUtil.getFieldTypes(pStatement.getParameterMetaData()),  key.getDataRecord(),key.getKeyFields());
			    } catch (JetelException ex){
			        throw new RuntimeException("Can't create keyRecord transmap: "+ex.getMessage());
			    }catch (Throwable ex) {
			        // PreparedStatement parameterMetadata probably not implemented - use work-around
			        // we only guess the correct data types on JDBC side
			        try{
			            keyTransMap = CopySQLData.jetel2sqlTransMap(key.getDataRecord(),key.getKeyFields(), dbConnection.getJdbcSpecific());
			        }catch(JetelException ex1){
			            throw new RuntimeException("Can't create keyRecord transmap: "+ex1.getMessage());
			        }catch(Exception ex1){
			            // some serious problem
			            throw new RuntimeException("Can't create keyRecord transmap: "+ex1.getClass().getName()+":"+ex1.getMessage());
			        }
			    }
			}
			// set prepared statement parameters
			for (int i = 0; i < keyTransMap.length; i++) {
			    keyTransMap[i].jetel2sql(pStatement);
			}
			
		   //execute query
		   ResultSet result = pStatement.executeQuery();
   
		   if (dbMetadata == null) {
	            try {
	                dbMetadata = SQLUtil.dbMetadata2jetel(result.getMetaData(), dbConnection.getJdbcSpecific());
	            } catch (SQLException ex) {
	                throw new RuntimeException(
	                        "Can't automatically obtain dbMetadata (use other constructor and provide metadat for output record): "
	                        + ex.getMessage());
	            }
		   }
		   
		   return result;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new RuntimeException(e.getMessage());
		}
	}
	
	CopySQLData[] createTransMap(ResultSet resultSet, DataRecord outRecord){
        
        // create trans map which will be used for fetching data
        try {
            return CopySQLData.sql2JetelTransMap(
                    SQLUtil.getFieldTypes(outRecord.getMetadata(), dbConnection.getJdbcSpecific()), outRecord.getMetadata(), outRecord);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Can't automatically obtain dbMetadata/create transMap : "
                    + ex.getMessage());
        }
	}
	

    /**
	 *  Initializtaion of lookup table - loading all data into it.
	 *
	 *@exception  JetelException  Description of the Exception
	 *@since                      May 2, 2002
	 */
    synchronized public void init() throws ComponentNotReadyException {
        if(isInitialized()) return;
		super.init();
		
		if (dbConnection == null) {
			DBConnection tmp = (DBConnection)getGraph().getConnection(connectionId);
			if (tmp == null) {
				throw new ComponentNotReadyException("Connection " + StringUtils.quote(connectionId) + 
						" does not exist!!!");
			}
			tmp.init();
			try {
				dbConnection = tmp.getConnection(getId(), OperationType.READ);
			} catch (JetelException e) {
				throw new ComponentNotReadyException("Can't connect to database: " + e.getMessage(), e);
			}
		}
		
		if (metadataId != null) {
			dbMetadata = getGraph().getDataRecordMetadata(metadataId);
		}
		
        try {
            pStatement = dbConnection.getSqlConnection().prepareStatement(sqlQuery);
        } catch (SQLException ex) {
            throw new ComponentNotReadyException("Can't create SQL statement: " + ex.getMessage());
        }
    }
    
    @Override
    public synchronized void reset() throws ComponentNotReadyException {
    	super.reset();
        keyTransMap = null;
    }
    
    public static DBLookupTable fromProperties(TypedProperties properties) 
    throws AttributeNotFoundException, GraphConfigurationException{

    	for (String property : REQUESTED_ATTRIBUTE) {
			if (!properties.containsKey(property)) {
				throw new AttributeNotFoundException(property);
			}
		}
        
    	String type = properties.getProperty(XML_TYPE_ATTRIBUTE);
    	if (!type.equalsIgnoreCase(XML_LOOKUP_TYPE_DB_LOOKUP)){
    		throw new GraphConfigurationException("Can't create db lookup table from type " + type);
    	}
    	
        DBLookupTable lookupTable = new DBLookupTable(properties.getProperty(XML_ID_ATTRIBUTE), 
        		properties.getProperty(XML_DBCONNECTION), properties.getProperty(XML_METADATA_ID), 
        		properties.getProperty(XML_SQL_QUERY));
        
        if (properties.containsKey(XML_NAME_ATTRIBUTE)){
        	lookupTable.setName(properties.getProperty(XML_NAME_ATTRIBUTE));
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
                    xattribs.getString(XML_METADATA_ID), xattribs.getString(XML_SQL_QUERY));
            
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

	/**
	 *  Deallocates resources
	 */
    synchronized public void free() {
        if(!isInitialized()) return;
        super.free();
        
        try {
            if(pStatement != null) {
                pStatement.close();
            }
        }
        catch (SQLException ex) {
            throw new RuntimeException(ex.getMessage());
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
    /* (non-Javadoc)
     * @see org.jetel.graph.GraphElement#checkConfig()
     */
    @Override
    public ConfigurationStatus checkConfig(ConfigurationStatus status) {
        super.checkConfig(status);

		if (dbConnection == null) {
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

//	public int getCacheNumber() {
//		return cacheNumber;
//	}
//
//	public int getTotalNumber() {
//		return totalNumber;
//	}

	public void toXML(Element xmlElement) {
		// TODO Auto-generated method stub
		
	}
    
    /* (non-Javadoc)
     * @see java.lang.Iterable#iterator()
     */
    public Iterator<DataRecord> iterator() {
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
        	Statement statement = dbConnection.getSqlConnection().createStatement();
			ResultSet resultSet = statement.executeQuery(query.toString());
			dbConnection.getJdbcSpecific().optimizeResultSet(resultSet, OperationType.READ);
		   if (dbMetadata == null) {
	            try {
	                dbMetadata = SQLUtil.dbMetadata2jetel(resultSet.getMetaData(), dbConnection.getJdbcSpecific());
	            } catch (SQLException ex) {
	                throw new RuntimeException(
	                        "Can't automatically obtain dbMetadata (use other constructor and provide metadat for output record): "
	                        + ex.getMessage());
	            }
		   }
			DataRecord dbDataRecord = new DataRecord(dbMetadata);
			CopySQLData[] transMap = createTransMap(resultSet, dbDataRecord);
			ArrayList<DataRecord> records = new ArrayList<DataRecord>();
			while (resultSet.next()){
				for (int i = 0; i < transMap.length; i++) {
					transMap[i].sql2jetel(resultSet);
				}
				records.add(dbDataRecord.duplicate());
			}
			return records.iterator();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
   }
	public Lookup createLookup(RecordKey key) {
		return createLookup(key, null);
	}
	public Lookup createLookup(RecordKey key, DataRecord keyRecord) {
		DBLookup lookup = new DBLookup(this, key, keyRecord);
		lookup.setCacheSize(maxCached);
		lookup.setStoreNulls(storeNulls);
		return lookup;
	}
	public boolean isReadOnly() {
		return true;
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

class DBLookup implements Lookup{

	protected ResultSet resultSet;
	protected SimpleCache resultCache;
	protected DBLookupTable lookupTable;
	protected DataRecord currentResult;
	protected HashKey key;
	protected CopySQLData[] transMap;
	protected DataRecord inRecord;
	protected RecordKey recordKey;
	private boolean storeNulls;
	
	DBLookup(DBLookupTable lookupTable, RecordKey key, DataRecord record){
		this.lookupTable = lookupTable;
		this.recordKey = key;
		this.inRecord = record;
		this.key = new HashKey(recordKey, inRecord);
	}
	
	public RecordKey getKey() {
		return recordKey;
	}

	public LookupTable getLookupTable() {
		return lookupTable;
	}

	public int getNumFound() {
    	if (resultCache!=null){
    		return resultCache.getNumFound();
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
        return -1;
	}

	public void seek() {
		if (resultCache != null) {
			try {
				seekInCache();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	private void seekInCache() throws SQLException {
		if (!resultCache.containsKey(key)) {
			resultSet = lookupTable.seek(key);
		    HashKey hashKey = new HashKey(recordKey, inRecord.duplicate());
		    if (fetch()) {
				do {
					DataRecord storeRecord = currentResult.duplicate();
					resultCache.put(hashKey, storeRecord);
				} while (fetch());		    	
		    }else{
				if (storeNulls) {
					resultCache.put(hashKey, (DataRecord) null);
				}		    	
		    }
		}
		currentResult = (DataRecord) resultCache.get(key);		
	}
	
	private boolean fetch() throws SQLException {
		if (!resultSet.next()) {
			currentResult = null;
			return false;
		}
		if (transMap == null) {
			currentResult = new DataRecord(lookupTable.getMetadata());
			currentResult.init();
			transMap = lookupTable.createTransMap(resultSet, currentResult);
		}
		//get data from results
		for (int i = 0; i < transMap.length; i++) {
			transMap[i].sql2jetel(resultSet);
		}
		return true;
	}

	public void seek(DataRecord keyRecord) {
		key.setDataRecord(keyRecord);
		seek();
	}

	public boolean hasNext() {
		return currentResult != null;
	}

	public DataRecord next() {
		DataRecord result = currentResult;
		if (resultCache != null){
			currentResult = (DataRecord) resultCache.getNext();
		}else {
			try {
				fetch();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return result;
	}

	public void remove() {
		// TODO Auto-generated method stub
		
	}
	
	public void setCacheSize(int cacheSize) {
		if (cacheSize>0){
	        this.resultCache= new SimpleCache(cacheSize);
	        resultCache.enableDuplicity();
	    }
	}

	public void setStoreNulls(boolean storeNulls){
		this.storeNulls = storeNulls;
	}
}
