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
 *  Simple lookup table which reads data from flat file and creates Map
 *  structure.
 *
 *@author     dpavlis
 *@created    22. kvìten 2003
 *@since      May 2, 2002
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
	private DataRecord keyDataRecord=null;


	/**
	 *  Constructor for the SimpleLookupTable object.<br>
	 *  It uses HashMap class to store key->data pairs in it.
	 *
	 *@param  keys              Names of fields which comprise key to lookup table
	 *@param  connection        Description of the Parameter
	 *@param  dbRecordMetadata  Description of the Parameter
	 *@param  sqlQuery          Description of the Parameter
	 *@param  keyDataRecord     Description of the Parameter
	 *@since                    May 2, 2002
	 */
	public DBLookupTable(DBConnection dbConnection, DataRecordMetadata dbRecordMetadata, DataRecord keyDataRecord, String[] keys, String sqlQuery) {
		this.dbConnection = dbConnection;
		this.metadata = dbRecordMetadata;
		this.sqlQuery = sqlQuery;
		this.keyDataRecord = keyDataRecord;
		key = new RecordKey(keys, keyDataRecord.getMetadata());
		key.init();
		keyFields = key.getKeyFields();
	}


	/**
	 *  Constructor for the DBLookupTable object
	 *
	 *@param  connection        Description of the Parameter
	 *@param  dbRecordMetadata  Description of the Parameter
	 *@param  sqlQuery          Description of the Parameter
	 */
	public DBLookupTable(DBConnection dbConnection, DataRecordMetadata dbRecordMetadata, String sqlQuery) {
		this.dbConnection = dbConnection;
		this.metadata = dbRecordMetadata;
		this.sqlQuery = sqlQuery;
	}


	/**
	 *  Looks-up data based on speficied key.<br>
	 *  The key should be result of calling RecordKey.getKeyString()
	 *
	 *@return                     Associated DataRecord or NULL if not found
	 *@exception  JetelException  Description of the Exception
	 *@since                      May 2, 2002
	 */
	public DataRecord get() throws JetelException {
		if (keyDataRecord == null) {
			throw new JetelException("Wrong constructor used. No metadata defined for record used as a key!");
		}
		try {
			pStatement.clearParameters();
			for (int i = 0; i < transMap.length; i++) {
				keyTransMap[i].jetel2sql(pStatement);
			}
			fetch();
		} catch (SQLException ex) {
			throw new JetelException(ex.getMessage());
		}
		return dataRecord;
	}


	/**
	 *  Description of the Method
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
			if (!fetch()) {
				return null;
			}
		} catch (SQLException ex) {
			throw new JetelException(ex.getMessage());
		}
		return dataRecord;
	}



	/**
	 *  Description of the Method
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
			if (!fetch()) {
				return null;
			}
		} catch (SQLException ex) {
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
		//execute query
		resultSet = pStatement.executeQuery();
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
	 *  Initializtaion of lookup table - loading all data into it.
	 *
	 *@exception  JetelException  Description of the Exception
	 *@since                      May 2, 2002
	 */
	public void init() throws JetelException {

		dataRecord = new DataRecord(metadata);
		dataRecord.init();

		// first try to connect to db
		try {
			System.out.println("going to connect");
			dbConnection.connect();
			System.out.println("connected - preparing statement");
			pStatement = dbConnection.prepareStatement(sqlQuery);
			System.out.println("prepared");

			transMap = CopySQLData.sql2JetelTransMap(metadata, dataRecord);
			System.out.println("created trans map");
			if (keyDataRecord != null) {
				keyTransMap = CopySQLData.jetel2sqlTransMap(SQLUtil.getFieldTypes(pStatement.getParameterMetaData()),
						keyDataRecord);
			}
		} catch (SQLException ex) {
			throw new JetelException("Can't establish DB connection: " + ex.getMessage());
		}

	}


	/**
	 *  Deallocates resources
	 */
	public void close() {
		try {
			pStatement.close();
		} catch (SQLException ex) {
			throw new RuntimeException(ex.getMessage());
		}
	}
}

