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
package org.jetel.database;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.jetel.data.DataRecord;
import org.jetel.data.RecordKey;
import org.jetel.data.lookup.LookupTable;
import org.jetel.exception.JetelException;
import org.jetel.metadata.DataRecordMetadata;

/**
 *  Database table/SQLquery based lookup table which gets data by performing SQL
 *  query. No caching is performed, except the one done on DB side.
 * 
 *  Example using DBLookupTable:
 * 
 *
 *@author     dpavlis
 *@created    25. kvìten 2003
 *@since      May 22, 2003
 */
public class DBLookupTable implements LookupTable {

	private DataRecordMetadata dbMetadata;
	private DBConnection dbConnection;
	private PreparedStatement pStatement;
	private RecordKey key;
	private int[] keyFields;
	private String[] keys;
	private String sqlQuery;
	private ResultSet resultSet;
	private CopySQLData[] transMap;
	private CopySQLData[] keyTransMap;
	private DataRecord dbDataRecord;
	private DataRecord keyDataRecord = null;
	private List dbFieldTypes;

	/**
	 *  Constructor for the DBLookupTable object.<br>
	 *
	 *
	 *@param  keys           Names of fields which comprise key to lookup table
	 *@param  sqlQuery       Parametrized SQL query which will be executed against DB to obtain sought data
	 *@param  keyDataRecord  Data record from which the key-field values will be
	 *      taken
	 *@param  dbConnection   Database connection object which will be used for communicating with DB
	 *@since                 May 2, 2002
	 */
  public DBLookupTable(DBConnection dbConnection, DataRecord keyDataRecord,
                       String[] keys, String sqlQuery) {
		this(dbConnection,null,keyDataRecord,keys,sqlQuery);
	}

  
	/**
	 *  Constructor for the DBLookupTable object
	 *
	 *@param  dbConnection      Database connection object which will be used for communicating with DB
	 *@param  dbRecordMetadata  Metadata describing structure of data returned by DB when executing SQL query
	 *@param  keyDataRecord     Data record from which the key-field values will be
	 *      taken
	 *@param  keys              Names of fields which comprise key to lookup table
	 *@param  sqlQuery          Parametrized SQL query which will be executed against DB to obtain sought data
	 */
  public DBLookupTable(DBConnection dbConnection,
                       DataRecordMetadata dbRecordMetadata,
                       DataRecord keyDataRecord, String[] keys, String sqlQuery) {
		this.dbConnection = dbConnection;
		this.sqlQuery = sqlQuery;
		this.keyDataRecord = keyDataRecord;
		this.dbMetadata = dbRecordMetadata;
		this.keys=keys;
		if (keyDataRecord!=null){
		    key = new RecordKey(keys, keyDataRecord.getMetadata());
		    key.init();
		    keyFields = key.getKeyFields();
		}
	}

  public DBLookupTable(DBConnection dbConnection,
          DataRecordMetadata dbRecordMetadata,
          String[] keys, String sqlQuery) {
      this(dbConnection,dbRecordMetadata,null,keys,sqlQuery);
  }
  
	/**
	 *  Constructor for the DBLookupTable object
	 *
	 *@param  sqlQuery      SQL query which returns data record based on specified
	 *      condition
	 *@param  dbConnection  Database connection object which will be used for communicating with DB
	 */
	public DBLookupTable(DBConnection dbConnection, String sqlQuery) {
		this.dbConnection = dbConnection;
		this.sqlQuery = sqlQuery;
	}

	/**
	 *  Constructor for the DBLookupTable object
	 *
	 *@param  dbConnection      Description of the Parameter
	 *@param  dbRecordMetadata  Description of the Parameter
	 *@param  sqlQuery          Description of the Parameter
   *@param dbFieldTypes      List containing the types of the final record
	 */
  public DBLookupTable(DBConnection dbConnection,
                       DataRecordMetadata dbRecordMetadata, String sqlQuery,
                       List dbFieldTypes) {
		this.dbConnection = dbConnection;
		this.dbMetadata = dbRecordMetadata;
		this.sqlQuery = sqlQuery;
		this.dbFieldTypes = dbFieldTypes;
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
	 *  The key value is taken from data record
	 *
	 *@return                     Associated DataRecord or NULL if not found
	 *@since                      May 2, 2002
	 */
	public DataRecord get(DataRecord keyRecord) {
		try {
            pStatement.clearParameters();

            if (keyDataRecord != null) {

                for (int i = 0; i < transMap.length; i++) {
                    keyTransMap[i].jetel2sql(pStatement);
                }
            } else {
                if (keyFields==null){
                    key = new RecordKey(keys, keyRecord.getMetadata());
            		key.init();
            		keyFields = key.getKeyFields();
                }
                for (int i = 0; i < keyFields.length; i++) {
                    pStatement.setObject(i + 1, keyRecord
                            .getField(keyFields[i]).getValue());
                }
            }
            //execute query
            resultSet = pStatement.executeQuery();
            fetch();
        } catch (SQLException ex) {
            throw new RuntimeException(ex.getMessage());
        }
        return dbDataRecord;
    }

	/**
	 *  Looks-up record/data based on specified array of parameters
	 *
	 *@param  keys                Description of the Parameter
	 *@return                     Description of the Return Value
	 */
	public DataRecord get(Object keys[]) {
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
	 *  Looks-up data based on specified key-string
	 *
	 *@param  keyStr              Description of the Parameter
	 *@return                     Description of the Return Value
	 */
	public DataRecord get(String keyStr) {
		try {
			// set up parameters for query
			// statement uses indexing from 1
			pStatement.clearParameters();
			pStatement.setString(1, keyStr);
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

    /* (non-Javadoc)
     * @see org.jetel.data.lookup.LookupTable#getNumFound()
     * 
     * Using this method on this implementation of LookupTable
     * can be time consuming as it requires sequential scan through
     * the whole result set returned from DB (on some DBMSs).
     * Also, it resets the position in result set. So subsequent
     * calls to getNext() will start reading the data found from
     * the first record.
     */
    public int getNumFound() {
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
        // do nothing
    }
    
	/**
	 *  Initializtaion of lookup table - loading all data into it.
	 *
	 *@exception  JetelException  Description of the Exception
	 *@since                      May 2, 2002
	 */
    public void init() throws JetelException {
        // first try to connect to db
        try {
            //dbConnection.connect();
            pStatement = dbConnection.prepareStatement(sqlQuery);
            /*ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY,
                    ResultSet.CLOSE_CURSORS_AT_COMMIT);*/
            
        } catch (SQLException ex) {
            throw new JetelException("Can't establish DB connection: "
                    + ex.getMessage());
        }
        // obtain dbMetadata info if needed
        if (dbMetadata == null) {
            try {
                dbMetadata = SQLUtil.dbMetadata2jetel(pStatement.getMetaData());
            } catch (SQLException ex) {
                throw new JetelException(
                        "Can't automatically obtain dbMetadata (use other constructor): "
                        + ex.getMessage());
            }
        }
        // create data record for fetching data from DB
        dbDataRecord = new DataRecord(dbMetadata);
        dbDataRecord.init();
        
        // create trans map which will be used for fetching data
        if (dbFieldTypes != null) {
            
            transMap = CopySQLData.sql2JetelTransMap(dbFieldTypes, dbMetadata,
                    dbDataRecord);
            
        } else {
            
           try {
                transMap = CopySQLData.sql2JetelTransMap(
                        SQLUtil.getFieldTypes(dbMetadata), dbMetadata,
                        dbDataRecord);
            } catch (Exception ex) {
                throw new JetelException(
                        "Can't automatically obtain dbMetadata/create transMap (use other constructor): "
                        + ex.getMessage());
            }
            
        }
        
        // create keyTransMap if key data record defined (passed)
        if (keyDataRecord != null) {
            try {
                keyTransMap = CopySQLData.jetel2sqlTransMap(SQLUtil
                        .getFieldTypes(pStatement.getParameterMetaData()),
                        keyDataRecord);
            } catch (SQLException ex) {
                keyTransMap = null;
            }
        }
    }

	/**
	 *  Deallocates resources
	 */
	public void close() {
		try {
			pStatement.close();
    }
    catch (SQLException ex) {
			throw new RuntimeException(ex.getMessage());
		}
	}
	
	public String getName(){
	    return dbMetadata.getName();
	}
}
