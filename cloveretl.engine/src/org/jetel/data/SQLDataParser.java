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

import java.sql.SQLException;
import java.sql.Statement;

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

	private DBConnection dbConnection;
	private String dbConnectionName;
	private String sqlQuery;
	private Statement statement;

	/**
	 * @param sqlQuery
	 */
	public SQLDataParser(String sqlQuery) {
		
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
	 *  Description of the Method
	 *
	 *@param  record    Description of Parameter
	 *@param  fieldNum  Description of Parameter
	 *@param  data      Description of Parameter
	 *@since            March 28, 2002
	 */

	protected void populateField(DataRecord record, int fieldNum, String data) {

		try {
			record.getField(fieldNum).fromString( data );

		} catch (BadDataFormatException bdfe) {
			if(handlerBDFE != null ) {  //use handler only if configured
			handlerBDFE.populateFieldFailure(record,fieldNum,data);
			} else {
				throw new RuntimeException(getErrorMessage(bdfe.getMessage(), recordCounter, fieldNum));
			}
		} catch (Exception ex) {
			throw new RuntimeException(ex.getMessage());
		}

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
			return parseNext(record);
		} catch (SQLException e) {
			e.printStackTrace();
			throw new JetelException(e.getMessage());
		}
	}


	/**
	 * @param record
	 * @return
	 */
	private DataRecord parseNext(DataRecord record) {
//
//		ResultSet resultSet = null;
//		OutputPort outPort = getOutputPort(WRITE_TO_PORT);
//		DataRecord outRecord = new DataRecord(outPort.getMetadata());
//		CopySQLData[] transMap;
//		int i;
//
//		outRecord.init();
//		transMap = CopySQLData.sql2JetelTransMap(outPort.getMetadata(), outRecord);
//		// run sql query
//		try {
//			resultSet = statement.executeQuery(sqlQuery);
///				while (resultSet.next() && runIt) {
//					for (i = 0; i < transMap.length; i++) {
//						transMap[i].sql2jetel(resultSet);
//					}
//					// send the record through output port
//					writeRecord(WRITE_TO_PORT, outRecord);
//				}
//
//		}
		return null;
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.DataParser#open(java.lang.Object, org.jetel.metadata.DataRecordMetadata)
	 */
	public void open(Object inputDataSource, DataRecordMetadata _metadata) {
		// get dbConnection from graph
		dbConnection= (DBConnection) inputDataSource;
		if (dbConnection==null){
			throw new ComponentNotReadyException("Can't find DBConnection ID: "+dbConnectionName);
		}
		statement = dbConnection.getStatement();
		// following calls are not always supported (as it seems)
		//statement.setFetchDirection(ResultSet.FETCH_FORWARD); 
		//statement.setFetchSize(SQL_FETCH_SIZE_ROWS);
		
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
