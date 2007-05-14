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

import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;

import org.jetel.connection.CopySQLData;
import org.jetel.connection.DBConnection;
import org.jetel.connection.SQLUtil;
import org.jetel.data.DataRecord;
import org.jetel.data.HashKey;
import org.jetel.data.RecordKey;
import org.jetel.data.lookup.LookupTable;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.JetelException;
import org.jetel.exception.TransformException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.GraphElement;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.ComponentXMLAttributes;
import org.jetel.util.SimpleCache;
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
 * 
 *
 *@author     dpavlis
 *@since      May 22, 2003
 */
public class DBLookupTable extends GraphElement implements LookupTable {

    private static final String XML_LOOKUP_TYPE_DB_LOOKUP = "dbLookup"; 
    //private static final String XML_LOOKUP_KEY = "key";
    private static final String XML_METADATA_ID ="metadata";
    private static final String XML_SQL_QUERY = "sqlQuery";
    private static final String XML_DBCONNECTION = "dbConnection";
    private static final String XML_LOOKUP_MAX_CACHE_SIZE = "maxCached";

	protected DataRecordMetadata dbMetadata;
	protected DBConnection dbConnection;
	protected PreparedStatement pStatement;
	protected RecordKey lookupKey;
	protected int[] keyFields;
	protected String[] keys;
	protected String sqlQuery;
	protected ResultSet resultSet;
	protected CopySQLData[] transMap;
	protected CopySQLData[] keyTransMap;
	protected DataRecord dbDataRecord;
	protected DataRecord keyDataRecord = null;
	protected SimpleCache resultCache;
	
	protected int maxCached;
	protected HashKey cacheKey;
	
	protected int cacheNumber = 0;
	protected int totalNumber = 0;
  
	/**
	 *  Constructor for the DBLookupTable object
	 *
	 *@param  dbConnection      Description of the Parameter
	 *@param  dbRecordMetadata  Description of the Parameter
	 *@param  sqlQuery          Description of the Parameter
	 */
  public DBLookupTable(String id, DBConnection dbConnection, 
          DataRecordMetadata dbRecordMetadata, String sqlQuery) {
      super(id);
		this.dbConnection = dbConnection;
		this.dbMetadata = dbRecordMetadata;
		this.sqlQuery = sqlQuery;
		this.maxCached = 0;
	}

  /**
   * Constructor for the DBLookupTable object
   *
   *@param  dbConnection      Description of the Parameter
   *@param  dbRecordMetadata  Description of the Parameter
   *@param  sqlQuery          Description of the Parameter
   * @param dbFieldTypes      List containing the types of the final record
   */
  public DBLookupTable(String id, DBConnection dbConnection, 
          DataRecordMetadata dbRecordMetadata, java.lang.String sqlQuery, java.util.List dbFieldTypes) {
      this(id, dbConnection,dbRecordMetadata,sqlQuery);
  }
  
  public DBLookupTable(String id, DBConnection dbConnection, 
          DataRecordMetadata dbRecordMetadata, String sqlQuery, int numCached) {
      super(id);
      this.dbConnection = dbConnection;
      this.dbMetadata = dbRecordMetadata;
      this.sqlQuery = sqlQuery;
      this.maxCached=numCached;
      
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

	/**
	 *  Looks-up data based on speficied key.<br>
	 *  The key value is taken from passed-in data record. If caching is enabled, the
	 * internal cache is searched first, then the DB is queried.
	 *
	 *@return                     found DataRecord or NULL
	 *@since                      May 2, 2002
	 */
	public DataRecord get(DataRecord keyRecord) {
		totalNumber++;
	    // if cached, then try to query cache first
	    if (resultCache!=null){
	        cacheKey.setDataRecord(keyRecord);
	        DataRecord data=(DataRecord)resultCache.get(cacheKey);
	        if (data!=null){
	        	cacheNumber++;
	            return data;
	        }
	    }
	    
	    try {
	        pStatement.clearParameters();
	        
	        // initialization of trans map if it was not already done
	        if (keyTransMap==null){
	            if (lookupKey == null) {
	                throw new RuntimeException("RecordKey was not defined for lookup !");
	            }
	            
	            try {
	                keyTransMap = CopySQLData.jetel2sqlTransMap(
	                        SQLUtil.getFieldTypes(pStatement.getParameterMetaData()),
	                        keyRecord,lookupKey.getKeyFields());
	            } catch (JetelException ex){
	                throw new RuntimeException("Can't create keyRecord transmap: "+ex.getMessage());
	            }catch (Throwable ex) {
	                // PreparedStatement parameterMetadata probably not implemented - use work-around
	                // we only guess the correct data types on JDBC side
	                try{
	                    keyTransMap = CopySQLData.jetel2sqlTransMap(keyRecord,lookupKey.getKeyFields());
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
	    resultSet = pStatement.executeQuery();
	    //put found records to cache
	    if (resultCache!=null){
		    HashKey hashKey = new HashKey(lookupKey, keyRecord.duplicate());
		    while (fetch()) {
		    	DataRecord storeRecord=dbDataRecord.duplicate();
			    resultCache.put(hashKey, storeRecord);
		    }
	    }else {
	    	return getNext();
	    }
	} catch (SQLException ex) {
	    throw new RuntimeException(ex.getMessage());
	}

	return (DataRecord)resultCache.get(cacheKey) ;
}

	/**
	 *  Looks-up record/data based on specified array of parameters(values).
	 * No caching is performed.
	 *
	 *@param  keys                Description of the Parameter
	 *@return                     found DataRecord or NULL
	 */
	public DataRecord get(Object keys[]) {
		totalNumber++;
		try {
			// set up parameters for query
			// statement uses indexing from 1
			pStatement.clearParameters();
			for (int i = 0; i < keys.length; i++) {
				pStatement.setObject(i + 1, keys[i]);
			}
			//execute query
			resultSet = pStatement.executeQuery();
			if (!fetch()) {
				return null;
			}
    }
    catch (SQLException ex) {
			throw new RuntimeException(ex.getMessage());
		}
		return dbDataRecord;
	}

	/**
	 *  Looks-up data based on specified key-string.<br>
	 * If caching is enabled, the
	 * internal cache is searched first, then the DB is queried.<br>
	 * <b>Warning:</b>it is not recommended to  mix this call with call to <code>get(DataRecord keyRecord)</code> method.
	 *
	 *@param  keyStr              string representation of the key-value
	 *@return                     found DataRecord or NULL
	 */
	public DataRecord get(String keyStr) {
		totalNumber++;
	    // if cached, then try to query cache first
	    if (resultCache!=null){
	        DataRecord data=(DataRecord)resultCache.get(keyStr);
	        if (data!=null){
	        	cacheNumber++;
	            return data;
	        }
	    }
	    
	    try {
	        // set up parameters for query
	        // statement uses indexing from 1
	        pStatement.clearParameters();
	        pStatement.setString(1, keyStr);
	        //execute query
	        resultSet = pStatement.executeQuery();
		    //put found records to cache
		    if (resultCache!=null){
			    while (fetch()) {
			    	DataRecord storeRecord=dbDataRecord.duplicate();
				    resultCache.put(keyStr, storeRecord);
			    }
		    }else {
		    	return getNext();
		    }
	    }
	    catch (SQLException ex) {
	        throw new RuntimeException(ex.getMessage());
	    }
	    return (DataRecord)resultCache.get(cacheKey) ;
	}

	/**
	 *  Executes query and returns data record (statement must be initialized with
	 *  parameters prior to calling this function
	 *
	 *@return                   DataRecord obtained from DB or null if not found
	 *@exception  SQLException  Description of the Exception
	 */
	private boolean fetch() throws SQLException {
		if (!resultSet.next()) {
			return false;
		}
		// initialize trans map if needed
		if (transMap==null){
		    initInternal();
			//check Clover and db metadata
			ResultSetMetaData dbMeta = resultSet.getMetaData();
			if (transMap.length != dbMeta.getColumnCount()){
				StringBuilder message = new StringBuilder("Different number of fields " +
						"in defined DB metadata and metadata obtained from database!!!\n" +
						"Clover metadata:\n");
				for (int i=0;i<dbMetadata.getNumFields();i++){
					message.append(dbMetadata.getField(i).getName() + " - " + 
							dbMetadata.getFieldTypeAsString(i) + "\n");
				}
				message.append("Database metadata:\n");
				for (int i=1;i<=dbMeta.getColumnCount();i++){
					message.append(dbMeta.getColumnLabel(i) + " - " + 
							dbMeta.getColumnTypeName(i) + "\n");
				}
				throw new RuntimeException(message.toString());
			}
		}
		//get data from results
		for (int i = 0; i < transMap.length; i++) {
			transMap[i].sql2jetel(resultSet);
		}
		return true;
	}

	/**
	 *  Returns the next found record if previous get() method succeeded.<br>
	 *  If no more records, returns NULL
	 *
	 *@return                     The next found record
	 *@exception  JetelException  Description of the Exception
	 */
	public DataRecord getNext() {
		if (resultCache!=null){
			return (DataRecord)resultCache.getNext();
		}else {
	        try {
	            if (!fetch()) {
	                return null;
	            } else {
	                return dbDataRecord;
	            }
	        } catch (SQLException ex) {
	            throw new RuntimeException(ex.getMessage());
	        }
		}
    }

    /* (non-Javadoc)
     * @see org.jetel.data.lookup.LookupTable#getNumFound()
     * 
     * Using this method on this implementation of LookupTable
     * can be time consuming as it requires sequential scan through
     * the whole result set returned from DB (on some DBMSs).
     */
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

    
    /* (non-Javadoc)
     * @see org.jetel.data.lookup.LookupTable#setLookupKey(java.lang.Object)
     */
    public void setLookupKey(Object obj){
        this.keyTransMap=null; // invalidate current transmap -if it exists
        if (obj instanceof RecordKey){
	        this.lookupKey=((RecordKey)obj);
	        this.cacheKey=new HashKey(lookupKey,null);
	    }else{
            this.lookupKey=null;
            this.cacheKey=null;
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
   	// if caching is required, crate map to store records
    	if (maxCached>0){
            this.resultCache= new SimpleCache(maxCached);
            resultCache.enableDuplicity();
             
        }
        // first try to connect to db
        try {
            dbConnection.init();
            pStatement = dbConnection.prepareStatement(sqlQuery);
            /*ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY,
                    ResultSet.CLOSE_CURSORS_AT_COMMIT);*/
            
        } catch (SQLException ex) {
            throw new ComponentNotReadyException("Can't create SQL statement: " + ex.getMessage());
        }
    }
    
    /**
     * We assume that query has already been executed and
     * we have resultSet available to get metadata.
     * 
     * 
     * @throws JetelException
     */
    private void initInternal()  {
        // obtain dbMetadata info if needed
        if (dbMetadata == null) {
            try {
                dbMetadata = SQLUtil.dbMetadata2jetel(resultSet.getMetaData());
            } catch (SQLException ex) {
                throw new RuntimeException(
                        "Can't automatically obtain dbMetadata (use other constructor and provide metadat for output record): "
                        + ex.getMessage());
            }
        }
        // create data record for fetching data from DB
        dbDataRecord = new DataRecord(dbMetadata);
        dbDataRecord.init();
        
        // create trans map which will be used for fetching data
        try {
            transMap = CopySQLData.sql2JetelTransMap(
                    SQLUtil.getFieldTypes(dbMetadata), dbMetadata,
                    dbDataRecord);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Can't automatically obtain dbMetadata/create transMap : "
                    + ex.getMessage());
        }

    }

    public static LookupTable fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException {
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
            DataRecordMetadata metadata = graph.getDataRecordMetadata(xattribs.getString(XML_METADATA_ID));

            lookupTable = new DBLookupTable(id, (DBConnection) graph
                    .getConnection(xattribs.getString(XML_DBCONNECTION)),
                    metadata, xattribs.getString(XML_SQL_QUERY));
            
            if(xattribs.exists(XML_LOOKUP_MAX_CACHE_SIZE)) {
                lookupTable.setNumCached(xattribs.getInteger(XML_LOOKUP_MAX_CACHE_SIZE));
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
        super.free();
        
        try {
            if(pStatement != null) {
                pStatement.close();
            }
            resultCache = null;
            transMap = null;
        }
        catch (SQLException ex) {
            throw new RuntimeException(ex.getMessage());
        }
    }
	
	
	public void setNumCached(int numCached){
	    if (numCached>0){
	          this.resultCache= new SimpleCache(numCached);
	          this.maxCached=numCached;
	      }
	}

    /* (non-Javadoc)
     * @see org.jetel.graph.GraphElement#checkConfig()
     */
    @Override
    public ConfigurationStatus checkConfig(ConfigurationStatus status) {
        super.checkConfig(status);
        //TODO
        return status;
    }

	public int getCacheNumber() {
		return cacheNumber;
	}

	public int getTotalNumber() {
		return totalNumber;
	}

	public void toXML(Element xmlElement) {
		// TODO Auto-generated method stub
		
	}
    
    public boolean put(Object key,DataRecord value){
        throw new UnsupportedOperationException("Operation put() not supported");
    }
    
    public boolean remove(Object key){
        throw new UnsupportedOperationException("Operation remove() not supported");
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
        	Statement statement = dbConnection.getStatement(); 
			resultSet = statement.executeQuery(query.toString());
			ArrayList<DataRecord> records = new ArrayList<DataRecord>();
			while (fetch()){
				records.add(dbDataRecord.duplicate());
			}
			return records.iterator();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
   }
}
