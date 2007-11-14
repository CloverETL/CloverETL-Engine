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

package org.jetel.connection;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.parser.Parser;
import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.IParserExceptionHandler;
import org.jetel.exception.JetelException;
import org.jetel.exception.PolicyType;
import org.jetel.metadata.DataRecordMetadata;

/**
 * @author David Pavlis
 *
 */
public class SQLDataParser implements Parser {
	private final static int DEFAULT_SQL_FETCH_SIZE_ROWS = 20;

	protected IParserExceptionHandler exceptionHandler;
	protected DataRecordMetadata metadata;
	protected int recordCounter;
	protected int fieldCount = 0;

	protected DBConnection dbConnection;
	protected String sqlQuery;
	protected Statement statement;
	protected QueryAnalyzer analyzer;

	protected ResultSet resultSet = null;
	protected CopySQLData[] transMap=null;
	protected DataRecord outRecord = null;

	protected int fetchSize = DEFAULT_SQL_FETCH_SIZE_ROWS;
	
	static Log logger = LogFactory.getLog(SQLDataParser.class);
	
	/**
	 * @param sqlQuery
	 */
	public SQLDataParser(String dbConnectionName,String sqlQuery) {
		this(sqlQuery);
	}

	/**
	 * Creates SQLDataParser object
	 * 
	 * @param sqlQuery query to be executed against DB
	 */
	public SQLDataParser(String sqlQuery) {
		analyzer = new QueryAnalyzer(sqlQuery);
		this.sqlQuery = analyzer.getNotInsertQuery();
	}
	

	/**
	 *  Returns next data record parsed from input data sorce or NULL if no more data
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
		if(exceptionHandler != null ) {  //use handler only if configured
			while(exceptionHandler.isExceptionThrowed()) {
                exceptionHandler.handleException();
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
		DataRecord localOutRecord=new DataRecord(metadata);
		localOutRecord.init();

		return getNext(localOutRecord);
	}


	/**
	 * @param record
	 * @return
	 */
	protected DataRecord parseNext(DataRecord record) throws JetelException {
		try {
			if(resultSet.next() == false)
				return null;
		} catch (SQLException e) {
            logger.debug("SQLException when reading resultSet: "+e.getMessage(),e);
			throw new JetelException("SQLException when reading resultSet: "+e.getMessage(),e);
		}
		// init transMap if null
		if (transMap==null){
		    initSQLMap(record);
		}else if (record!=outRecord){
		    CopySQLData.resetDataRecord(transMap,record);
		    outRecord=record;
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
		
		try {
			transMap[fieldNum-1].sql2jetel(resultSet);

		} catch (BadDataFormatException bdfe) {
			if(exceptionHandler != null ) {  //use handler only if configured
                exceptionHandler.populateHandler(getErrorMessage(
                		bdfe.getMessage(), recordCounter, fieldNum), record, -1, 
                		fieldNum-1, bdfe.getOffendingValue().toString(), bdfe);
			} else {
				throw bdfe;
			}
		} catch (Exception ex) {
            logger.debug(ex.getMessage(),ex);
			throw new RuntimeException(ex.getMessage(),ex);
		}
	}

	public void initSQLDataMap(DataRecord record){
	    initSQLMap(record);
	}
	
	protected void initSQLMap(DataRecord record){
		try{
			HashMap<String, String> cloverDbMap = analyzer.getCloverDbFieldMap();
			if (cloverDbMap.size() > 0 ) {
				transMap = CopySQLData.sql2JetelTransMap(SQLUtil.getFieldTypes(resultSet.getMetaData()), metadata, 
						record, cloverDbMap.keySet().toArray(new String[0]));
			}else{
				transMap = CopySQLData.sql2JetelTransMap( SQLUtil.getFieldTypes(resultSet.getMetaData()),metadata, 
						record);
			}
			fieldCount = transMap.length;
		}catch (Exception ex) {
            logger.debug(ex.getMessage(),ex);
			throw new RuntimeException(ex.getMessage(),ex);
		}
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.parser.Parser#init(org.jetel.metadata.DataRecordMetadata)
	 */
	public void init(DataRecordMetadata _metadata) throws ComponentNotReadyException {
		metadata = _metadata;
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.parser.Parser#setDataSource(java.lang.Object)
	 */
	public void setReleaseDataSource(boolean releaseInputSource)  {
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.parser.Parser#setDataSource(java.lang.Object)
	 */
	public void setDataSource(Object inputDataSource) throws ComponentNotReadyException {
		if (dbConnection != null) close();
		
		//outRecord.init();
        // get dbConnection from graph
        if (! (inputDataSource instanceof DBConnection)){
            throw new RuntimeException("Need DBConnection object !");
        }
        dbConnection= (DBConnection) inputDataSource;
        
        try{
            // try to set autocommit to false
            dbConnection.getConnection().setAutoCommit(false);
        }catch (Exception e) {
            logger.warn(e);
        }
        try {
            // connection is created up front
            //dbConnection.connect();
            statement = dbConnection.getStatement();
            /*ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY,ResultSet.CLOSE_CURSORS_AT_COMMIT);*/
        } catch (SQLException e) {
            throw new ComponentNotReadyException(e);
        }
        
        // !!! POTENTIALLY DANGEROUS - SOME DBs produce fatal error - Abstract method call !!
        // this needs some detecting of supported features first (may-be which version of JDBC is implemented or so
        try {
            // following calls are not always supported (as it seems)
            // if error occures, we just ignore it
            statement.setFetchDirection(ResultSet.FETCH_FORWARD); 
            statement.setFetchSize(fetchSize);
        
        } catch (Exception e) {
            logger.warn(e);
        }
        
        try{
            resultSet = statement.executeQuery(sqlQuery);
        } catch (SQLException e) {
            logger.debug(e);
            throw new ComponentNotReadyException(e);
        }
        // try to set up some cursor parameters (fetchSize, reading type)
        try{
            resultSet.setFetchDirection(ResultSet.TYPE_FORWARD_ONLY);
            resultSet.setFetchSize(fetchSize);
        }catch (SQLException e){
            // do nothing - just attempt
            logger.debug("unable to set FetchDirection & FetchSize for DB connection ["+dbConnection.getId()+"]");
        }
		this.recordCounter = 1;
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.DataParser#close()
	 */
	public void close() {
		try {
			if (resultSet != null) {
				resultSet.close();
			}
			// try to commit (as some DBs apparently need commit even when data is read only
			if (!dbConnection.getConnection().getAutoCommit()) {
				dbConnection.getConnection().commit();
			}            
			// close statement
			statement.close();
		}
		catch (SQLException ex) {
            logger.warn("SQLException when closing statement",ex);
		}
	}

	public void setFetchSize(int fetchSize){
	    this.fetchSize=fetchSize;
	}

    public void setExceptionHandler(IParserExceptionHandler handler) {
        this.exceptionHandler = handler;
    }

    public IParserExceptionHandler getExceptionHandler() {
        return exceptionHandler;
    }

    public PolicyType getPolicyType() {
        if(exceptionHandler != null) {
            return exceptionHandler.getType();
        }
        return null;
    }

	public int skip(int nRec) throws JetelException {
		throw new UnsupportedOperationException("Not yet implemented");
	}

}
