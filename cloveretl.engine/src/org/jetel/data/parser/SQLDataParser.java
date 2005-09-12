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

package org.jetel.data.parser;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.database.CopySQLData;
import org.jetel.database.SQLUtil;
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
public class SQLDataParser implements Parser {
	private final static int SQL_FETCH_SIZE_ROWS = 20;

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
	private DataRecord outRecord = null;

	private int fetchSize = SQL_FETCH_SIZE_ROWS;
	
	static Log logger = LogFactory.getLog(SQLDataParser.class);
	
	/**
	 * @param sqlQuery
	 */
	public SQLDataParser(String dbConnectionName,String sqlQuery) {
		this.dbConnectionName = dbConnectionName;
		this.sqlQuery = sqlQuery;
		this.recordCounter = 1;
	}


	/**
	 *  Returs next data record parsed from input data sorce or NULL if no more data
	 *  available The specified DataRecord's fields are altered to contain new
	 *  values.
	 *
	 *@param  record           Description of Parameter
	 *@return                  The Next value
	 *@exception  SQLException  Description of Exception
	 *@since                   May 2, 2002
	 */

	public DataRecord getNext(DataRecord record) throws JetelException {
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
	 *  Gets the Next attribute of the SQLDataParser object
	 *
	 * @return                  The Next value
	 * @exception  JetelException  Description of Exception
	 * @since                   August 21, 2002
	 */

	public DataRecord getNext() throws JetelException {
		if (outRecord==null){
			outRecord = new DataRecord(metadata);
			outRecord.init();
		}

		return getNext(outRecord);
	}


	/**
	 * @param record
	 * @return
	 */
	private DataRecord parseNext(DataRecord record) throws JetelException {
		try {
			if(resultSet.next() == false)
				return null;
		} catch (SQLException e) {
			e.printStackTrace();
			throw new JetelException(e.getMessage());
		}
			
			for (int i = 1; i <= fieldCount; i++) {
				populateField(record, i);
			}
		
        recordCounter++;
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
		//String data = null;
		
		try {
			transMap[fieldNum-1].sql2jetel(resultSet);
			//data = resultSet.getString(fieldNum);
			//record.getField(fieldNum-1).fromString( data );

		} catch (BadDataFormatException bdfe) {
			if(handlerBDFE != null ) {  //use handler only if configured
				handlerBDFE.populateFieldFailure(getErrorMessage(bdfe.getMessage(), recordCounter, fieldNum), record,fieldNum-1,bdfe.getOffendingFormat());
			} else {
				throw new RuntimeException(getErrorMessage(bdfe.getMessage(), recordCounter, fieldNum));
			}
		} catch (Exception ex) {
			ex.printStackTrace();
			throw new RuntimeException(ex.getMessage());
		}
	}

	public void initSQLDataMap(DataRecord record){
		try{
			transMap = CopySQLData.sql2JetelTransMap( SQLUtil.getFieldTypes(resultSet.getMetaData()),metadata, record);
		}catch (Exception ex) {
			ex.printStackTrace();
			throw new RuntimeException(ex.getMessage());
		}
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.DataParser#open(java.lang.Object, org.jetel.metadata.DataRecordMetadata)
	 */
	public void open(Object inputDataSource, DataRecordMetadata _metadata) throws ComponentNotReadyException {
		metadata = _metadata;
		fieldCount = _metadata.getNumFields();

		//outRecord.init();
		// get dbConnection from graph
		dbConnection= (DBConnection) inputDataSource;
		if (dbConnection==null){
			throw new ComponentNotReadyException("Can't find DBConnection ID: "+dbConnectionName);
		}
		try {
			// connection is created up front
			//dbConnection.connect();
			statement = dbConnection.getStatement();
			/*ResultSet.TYPE_FORWARD_ONLY,
			        ResultSet.CONCUR_READ_ONLY,ResultSet.CLOSE_CURSORS_AT_COMMIT);*/
		} catch (SQLException e) {
			e.printStackTrace();
			throw new ComponentNotReadyException(e.getMessage());
		}
		
		// !!! POTENTIALLY DANGEROUS - SOME DBs produce fatal error - Abstract method call !!
		// this needs some detecting of supported features first (may-be which version of JDBC is implemented or so
		try {
			// following calls are not always supported (as it seems)
			// if error occures, we just ignore it
			statement.setFetchDirection(ResultSet.TYPE_FORWARD_ONLY); 
			statement.setFetchSize(fetchSize);
		
		} catch (Exception e) {
			logger.warn(e);
		}
		
		try{
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
	}



	/**
	 * @param handler
	 */
	public void addBDFHandler(BadDataFormatExceptionHandler handler) {
		this.handlerBDFE = handler;
	}

	public void setFetchSize(int fetchSize){
	    this.fetchSize=fetchSize;
	}

}
