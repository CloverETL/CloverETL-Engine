/*
 *  jETeL/Clover - Java based ETL application framework.
 *  Created on Apr 26, 2003
 *  Copyright (C) 2003, 2002  David Pavlis, Wes Maciorowski
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

package org.jetel.data;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.jetel.database.CopySQLData;
import org.jetel.database.DBConnection;
import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.BadDataFormatExceptionHandler;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelException;
import org.jetel.metadata.DataRecordMetadata;

/**
 * @author maciorowski
 *
 */
public class SQLDataParser implements DataParser {
	private final static int SQL_FETCH_SIZE_ROWS = 100;

	private BadDataFormatExceptionHandler handlerBDFE;
	private DataRecordMetadata metadata;
	private int recordCounter;
	private int fieldCount;

	private DBConnection dbConnection;
	private String dbConnectionName;
	private String sqlQuery;
	private Statement statement;

	private ResultSet resultSet = null;
	private CopySQLData[] transMap;

	/**
	 * @param sqlQuery
	 */
	public SQLDataParser(String dbConnectionName,String sqlQuery) {
		this.dbConnectionName = dbConnectionName;
		this.sqlQuery = sqlQuery;
	}


	/**
	 *  Returs next data record parsed from input stream or NULL if no more data
	 *  available The specified DataRecord's fields are altered to contain new
	 *  values.
	 *
	 *@param  record           Description of Parameter
	 *@return                  The Next value
	 *@exception  IOException  Description of Exception
	 *@since                   May 2, 2002
	 */

	public DataRecord getNext(DataRecord record) throws SQLException {
		record = parseNext(record);
		if(handlerBDFE != null ) {  //use handler only if configured
			while(handlerBDFE.isThrowException()) {
				handlerBDFE.handleException(record);
				//record.init();  redundant
				record = parseNext(record);
			}
		}
		return record;
	}



		/**
		 *  Assembles error message when exception occures during parsing
		 *
		 * @param  exceptionMessage  message from exception getMessage() call
		 * @param  recNo             recordNumber
		 * @param  fieldNo           fieldNumber
		 * @return                   error message
		 * @since                    September 19, 2002
		 */
		private String getErrorMessage(String exceptionMessage, int recNo, int fieldNo) {
			StringBuffer message = new StringBuffer();
			message.append(exceptionMessage);
			message.append(" when parsing record #");
			message.append(recordCounter);
			message.append(" field ");
			message.append(metadata.getField(fieldNo).getName());
			return message.toString();
		}


	/**
	 *  Gets the Next attribute of the FixLenDataParser object
	 *
	 * @return                  The Next value
	 * @exception  IOException  Description of Exception
	 * @since                   August 21, 2002
	 */

	public DataRecord getNext() throws JetelException {
		// create a new data record
		DataRecord record = new DataRecord(metadata);

		record.init();

		try {
			return getNext(record);
		} catch (SQLException e) {
			e.printStackTrace();
			throw new JetelException(e.getMessage());
		}
	}


	/**
	 * @param record
	 * @return
	 */
	private DataRecord parseNext(DataRecord record) throws SQLException {
		if(resultSet.next() == false)
			return null;
			
			for (int i = 1; i <= fieldCount; i++) {
				populateField(record, i);
			}
		
		return record;
	}

	/**
	 *  Description of the Method
	 *
	 *@param  record    Description of Parameter
	 *@param  fieldNum  Description of Parameter
	 *@param  data      Description of Parameter
	 *@since            March 28, 2002
	 */

	protected void populateField(DataRecord record, int fieldNum) {
		String data = null;
		
		try {
			//transMap[fieldNum].sql2jetel(resultSet);
			data = resultSet.getString(fieldNum);
			record.getField(fieldNum-1).fromString( data );

		} catch (BadDataFormatException bdfe) {
			if(handlerBDFE != null ) {  //use handler only if configured
			handlerBDFE.populateFieldFailure(record,fieldNum-1,data);
			} else {
				throw new RuntimeException(getErrorMessage(bdfe.getMessage(), recordCounter, fieldNum));
			}
		} catch (Exception ex) {
			throw new RuntimeException(ex.getMessage());
		}
	}


	/* (non-Javadoc)
	 * @see org.jetel.data.DataParser#open(java.lang.Object, org.jetel.metadata.DataRecordMetadata)
	 */
	public void open(Object inputDataSource, DataRecordMetadata _metadata) throws ComponentNotReadyException {
		DataRecord outRecord = new DataRecord(_metadata);
		fieldCount = _metadata.getNumFields();
		int i;

		outRecord.init();
		// get dbConnection from graph
		dbConnection= (DBConnection) inputDataSource;
		if (dbConnection==null){
			throw new ComponentNotReadyException("Can't find DBConnection ID: "+dbConnectionName);
		}
		try {
			dbConnection.connect();
			statement = dbConnection.getStatement();
			// following calls are not always supported (as it seems)
			//statement.setFetchDirection(ResultSet.FETCH_FORWARD); 
			//statement.setFetchSize(SQL_FETCH_SIZE_ROWS);
			resultSet = statement.executeQuery(sqlQuery);
		} catch (SQLException e) {
			e.printStackTrace();
			throw new ComponentNotReadyException(e.getMessage());
		}
		
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.DataParser#close()
	 */
	public void close() {
		try {
			if (resultSet != null) {
				resultSet.close();
			}
			statement.close();
		}
		catch (SQLException ex) {
			ex.printStackTrace();
		}
//	}
			}



	/**
	 * @param handler
	 */
	public void addBDFHandler(BadDataFormatExceptionHandler handler) {
		this.handlerBDFE = handler;
	}



}
