/*
 *  jETeL/Clover - Java based ETL application framework.
 *  Copyright (C) 2002  David Pavlis
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 2 of the License, or
 *  (at your option) any later version.
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
package org.jetel.database;

import java.util.*;
import java.io.*;
import java.sql.*;
import org.jetel.data.*;
import org.jetel.metadata.*;

import org.jetel.exception.JetelException;
import org.jetel.metadata.DataRecordMetadata;

/**
 *  Database table/SQLquery based lookup table which gets data by performing SQL
 *  query. No caching is performed, except the one done on DB side
 *
 *@author     dpavlis
 *@created    25. kvìten 2003
 *@since      May 22, 2003
 */
public class DBLookupTable {

	private DataRecordMetadata metadata;
	private DBConnection dbConnection;
	private PreparedStatement pStatement;
	private RecordKey key;
	private int[] keyFields;
	private String sqlQuery;
	private ResultSet resultSet;
	private CopySQLData[] transMap;
	private CopySQLData[] keyTransMap;
	private DataRecord dataRecord;
	private DataRecord keyDataRecord = null;
  private List dbFieldTypes;

	/**
	 *  Constructor for the DBLookupTable object.<br>
	 *
	 *
	 *@param  keys           Names of fields which comprise key to lookup table
	 *@param  sqlQuery       Description of the Parameter
	 *@param  keyDataRecord  data record from which the key-field values will be
	 *      taken
	 *@param  dbConnection   Description of the Parameter
	 *@since                 May 2, 2002
	 */
  public DBLookupTable(DBConnection dbConnection, DataRecord keyDataRecord,
                       String[] keys, String sqlQuery) {
		this.dbConnection = dbConnection;
		this.sqlQuery = sqlQuery;
		this.keyDataRecord = keyDataRecord;
		key = new RecordKey(keys, keyDataRecord.getMetadata());
		key.init();
		keyFields = key.getKeyFields();
	}

	/**
	 *  Constructor for the DBLookupTable object
	 *
	 *@param  dbConnection      Description of the Parameter
	 *@param  dbRecordMetadata  Description of the Parameter
	 *@param  keyDataRecord     Description of the Parameter
	 *@param  keys              Description of the Parameter
	 *@param  sqlQuery          Description of the Parameter
	 */
  public DBLookupTable(DBConnection dbConnection,
                       DataRecordMetadata dbRecordMetadata,
                       DataRecord keyDataRecord, String[] keys, String sqlQuery) {
		this.dbConnection = dbConnection;
		this.sqlQuery = sqlQuery;
		this.keyDataRecord = keyDataRecord;
		this.metadata = dbRecordMetadata;
		key = new RecordKey(keys, keyDataRecord.getMetadata());
		key.init();
		keyFields = key.getKeyFields();
	}

	/**
	 *  Constructor for the DBLookupTable object
	 *
	 *@param  sqlQuery      SQL query which returns data record based on specified
	 *      condition
	 *@param  dbConnection  Description of the Parameter
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
		this.metadata = dbRecordMetadata;
		this.sqlQuery = sqlQuery;
    this.dbFieldTypes = dbFieldTypes;
	}

	/**
	 *  Gets the metadata attribute of the DBLookupTable object.<br>
	 *  <i>init() must be called prior to calling this method</i>
	 *
	 *@return    The metadata value
	 */
	public DataRecordMetadata getMetadata() {
		return metadata;
	}

	/**
	 *  Looks-up data based on speficied key.<br>
	 *  The key value is taken from data record
	 *
	 *@return                     Associated DataRecord or NULL if not found
	 *@exception  JetelException  Description of the Exception
	 *@since                      May 2, 2002
	 */
	public DataRecord get() throws JetelException {
		if (keyDataRecord == null) {
      throw new JetelException(
          "Wrong constructor used. No metadata defined for record used as a key!");
		}
		try {
			pStatement.clearParameters();
			for (int i = 0; i < transMap.length; i++) {
				keyTransMap[i].jetel2sql(pStatement);
			}
			//execute query
			resultSet = pStatement.executeQuery();
			fetch();
    }
    catch (SQLException ex) {
			throw new JetelException(ex.getMessage());
		}
		return dataRecord;
	}

	/**
	 *  Looks-up record/data based on specified array of parameters
	 *
	 *@param  keys                Description of the Parameter
	 *@return                     Description of the Return Value
	 *@exception  JetelException  Description of the Exception
	 */
	public DataRecord get(Object keys[]) throws JetelException {
		int i;
		try {
			// set up parameters for query
			// statement uses indexing from 1
			pStatement.clearParameters();
			for (i = 0; i < keys.length; i++) {
				pStatement.setObject(i + 1, keys[i]);
			}
			//execute query
			resultSet = pStatement.executeQuery();
			if (!fetch()) {
				return null;
			}
    }
    catch (SQLException ex) {
			throw new JetelException(ex.getMessage());
		}
		return dataRecord;
	}

	/**
	 *  Looks-up data based on specified key-string
	 *
	 *@param  keyStr              Description of the Parameter
	 *@return                     Description of the Return Value
	 *@exception  JetelException  Description of the Exception
	 */
	public DataRecord get(String keyStr) throws JetelException {
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
			throw new JetelException(ex.getMessage());
		}
		return dataRecord;
	}

	/**
	 *  Executes query and returns data record (statement must be initialized with
	 *  parameters prior to calling this function
	 *
	 *@return                   DataRecord obtained from DB or null if not found
	 *@exception  SQLException  Description of the Exception
	 */
	private boolean fetch() throws SQLException {
		// go to first result
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
	public DataRecord getNext() throws JetelException {
		try {
			if (!fetch()) {
				return null;
      }
      else {
				return dataRecord;
			}
    }
    catch (SQLException ex) {
			throw new JetelException(ex.getMessage());
		}
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
			dbConnection.connect();
			pStatement = dbConnection.prepareStatement(sqlQuery);

    }
    catch (SQLException ex) {
      throw new JetelException("Can't establish DB connection: " +
                               ex.getMessage());
		}
		// obtain metadata info if needed
		if (metadata == null) {
			try {
				metadata = SQLUtil.dbMetadata2jetel(pStatement.getMetaData());
      }
      catch (SQLException ex) {
        throw new JetelException(
            "Can't automatically obtain metadata (use other constructor): " +
            ex.getMessage());
			}
		}
		// create data record
		dataRecord = new DataRecord(metadata);
		dataRecord.init();

		// create trans map
    if (dbFieldTypes!=null) {

      transMap = CopySQLData.sql2JetelTransMap(dbFieldTypes, metadata, dataRecord);

    } else {

      try {
        transMap = CopySQLData.sql2JetelTransMap(SQLUtil.getFieldTypes(pStatement.
            getMetaData()), metadata, dataRecord);
      }
      catch (SQLException ex) {
        throw new JetelException(
            "Can't automatically obtain metadata (use other constructor): " +
            ex.getMessage());
			}

    }

		// create keyTransMap if key data record defined (passed)	
		if (keyDataRecord != null) {
      try {
        keyTransMap = CopySQLData.jetel2sqlTransMap(SQLUtil.getFieldTypes(
            pStatement.getParameterMetaData()),
						keyDataRecord);
      }
      catch (SQLException ex) {
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
}
