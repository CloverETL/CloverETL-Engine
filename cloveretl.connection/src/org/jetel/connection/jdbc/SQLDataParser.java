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

package org.jetel.connection.jdbc;

import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.TimeZone;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.connection.jdbc.specific.DBConnectionInstance;
import org.jetel.connection.jdbc.specific.JdbcSpecific.OperationType;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.parser.Parser;
import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.IParserExceptionHandler;
import org.jetel.exception.JetelException;
import org.jetel.exception.PolicyType;
import org.jetel.graph.GraphElement;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.string.StringUtils;

/**
 * Gets records from database. Query can be set in clover format with mapping between clover and db fields, eg:
 * <i>select $f1:=db1, $f2:=db2, ... from myTable</i>.<br>
 * Supports incremental reading with given key:
 * <ul><li>query, eg. <i>select $f1:=db1, $f2:=db2, ... from myTable where dbX > #myKey1 and dbY <=#myKey2</i></li>
 * <li>keyDefinition - properties where <i>key</i> is <i>keyName</i> and <i>value</i> is <i>keyDefinition</i>; key definition defines
 *   which value from result set is stored (<b>last</b>, <b>first</b>, <b>min</b> or <b>max</b>) and on which db field is defined, eg:
 *   <i>myKey1=first(dbX);myKey2=min(dbY)</i> (see query above).</li>
 * <li>incrementalFile - url to file where key values are stored. Values have to be set by user for 1st reading, then are set to 
 *   requested value (see above) automatically, eg. <i>myKey1=0;myKey2=1990-01-01</i>. Dates, times and timestamps have be written
 *   in format defined in @see Defaults.DEFAULT_DATE_FORMAT, Defaults.DEFAULT_TIME_FORMAT, Defaults.DEFAULT_DATETIME_FORMAT</li>
 * </ul> 
 * 
 * @author David Pavlis
 * @author Agata Vackova (agata.vackova@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @since Jul 21, 2008
 */
public class SQLDataParser implements Parser {
	protected IParserExceptionHandler exceptionHandler;
	protected DataRecordMetadata metadata;
	protected int recordCounter;

	protected DBConnectionInstance dbConnection;
	protected String sqlQuery;

	protected ResultSet resultSet = null;
	protected CopySQLData[] transMap=null;
	protected DataRecord outRecord = null;

	private GraphElement parentNode;

	protected int fetchSize = -1;
	
	private String incrementalFile;
	private Properties incrementalKey;
	private SQLIncremental incremental;
	private SQLCloverStatement sqlCloverStatement;
	
	static Log logger = LogFactory.getLog(SQLDataParser.class);

	/**
	 * @param sqlQuery
	 */
	@Deprecated
	public SQLDataParser(String dbConnectionName,String sqlQuery) {
		this(sqlQuery);
	}

	/**
	 * Creates SQLDataParser object
	 * 
	 * @param sqlQuery query to be executed against DB
	 */
	public SQLDataParser(String sqlQuery) {
		this.sqlQuery = sqlQuery;
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
			message.append(metadata.getField(fieldNo-1).getName());
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
		    try {
				initSQLMap(record);
			} catch (SQLException ex) {
	            logger.debug(ex.getMessage(),ex);
				throw new JetelException(ex.getMessage(),ex);
			}
		}else if (record!=outRecord){
		    CopySQLData.resetDataRecord(transMap,record);
		    outRecord=record;
		}
			
		for (int i = 1; i <= transMap.length; i++) {
			populateField(record, i);
		}
		try {
			if (incremental != null){
				for(int i = 0; i < incrementalKey.size(); i++) {
					incremental.updatePosition(resultSet, i);
				}
			}
		} catch (SQLException e) {
			throw new JetelException("Problem when updating incremental position", e);
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

	protected void initSQLMap(DataRecord record) throws SQLException{
		if (sqlCloverStatement.getCloverOutputFields() == null) {
			transMap = CopySQLData.sql2JetelTransMap(SQLUtil.getFieldTypes(resultSet.getMetaData()) ,metadata, record);
		}else{
			transMap = CopySQLData.sql2JetelTransMap(SQLUtil.getFieldTypes(resultSet.getMetaData()) , metadata, record, 
					sqlCloverStatement.getCloverOutputFields());
		}
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.parser.Parser#init(org.jetel.metadata.DataRecordMetadata)
	 */
	public void init(DataRecordMetadata _metadata) throws ComponentNotReadyException {
		if (_metadata == null) {
			throw new ComponentNotReadyException("Metadata are null");
		}
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
        if (! (inputDataSource instanceof DBConnectionInstance)){
            throw new RuntimeException("Need org.jetel.data.connection.jdbc.specific.DBConnectionInstance object !");
        }
        dbConnection = (DBConnectionInstance) inputDataSource;
        
        long startTime;
        sqlCloverStatement = new SQLCloverStatement(dbConnection, sqlQuery, null);
        sqlCloverStatement.setLogger(logger);
        try {
			if (incrementalKey != null && sqlQuery.contains(SQLIncremental.INCREMENTAL_KEY_INDICATOR)) {
				if (incremental == null) {
					incremental = new SQLIncremental(incrementalKey, sqlQuery, incrementalFile);
				}
				sqlCloverStatement.setIncremental(incremental);
			}
			sqlCloverStatement.init();
	        logger.debug((parentNode != null ? (parentNode.getId() + ": ") : "") + "Sending query " + 
	        		StringUtils.quote(sqlCloverStatement.getQuery()));
			startTime = System.currentTimeMillis();
			resultSet = sqlCloverStatement.executeQuery();
            long executionTime = System.currentTimeMillis() - startTime;
            SimpleDateFormat formatter = new SimpleDateFormat("HH:mm:ss.SSS");
            formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
            logger.debug((parentNode != null ? (parentNode.getId() + ": ") : "") + "Query execution time: " + 
            		formatter.format(new Date(executionTime)));
		} catch (Exception e1) {
            throw new ComponentNotReadyException(e1);
		}
        
        dbConnection.getJdbcSpecific().optimizeResultSet(resultSet, OperationType.READ);
        if (fetchSize > -1) {
        	try {
				resultSet.setFetchSize(fetchSize);
			} catch (SQLException e) {
				logger.warn("Can't set fetch size to " + fetchSize);
				logger.info("Using default value of fetch size");
			}
        }
        
		this.recordCounter = 1;
	}
	
	public boolean checkIncremental() throws ComponentNotReadyException{
    	if (incrementalKey != null && sqlQuery.contains(SQLIncremental.INCREMENTAL_KEY_INDICATOR)) {
			try {
				new SQLIncremental(incrementalKey, sqlQuery, incrementalFile);
			} catch (Exception e) {
				throw new ComponentNotReadyException(e);
			}
		}
		return true;
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.DataParser#close()
	 */
	public void close() {
		if (dbConnection == null) return;//not initialized yet
		try {
			if (resultSet != null) {
				resultSet.close();
			}
			// try to commit (as some DBs apparently need commit even when data is read only
			Connection conn = dbConnection.getSqlConnection();
			if (!conn.isClosed() && !conn.getAutoCommit()) {
				conn.commit();
			}            
			// close statement
			sqlCloverStatement.close();
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

	/*
	 * (non-Javadoc)
	 * @see org.jetel.data.parser.Parser#reset()
	 */
	public void reset() {
		try {
			if (resultSet != null) {
				resultSet.close();
			}
			// close statement
			sqlCloverStatement.close();
		}
		catch (SQLException ex) {
            logger.warn("SQLException when closing statement",ex);
		}
		transMap = null;
		recordCounter = 1;
	}

	public GraphElement getParentNode() {
		return parentNode;
	}

	public void setParentNode(GraphElement parentNode) {
		this.parentNode = parentNode;
	}

	public Object getPosition() {
		return incremental != null ? incremental.getPosition() : null;
	}

	public void movePosition(Object position) {
		if (incremental != null) {
			incremental.setValues((Properties)position);
		}
	}
	
	public void setIncrementalFile(String incrementalFile){
		this.incrementalFile = incrementalFile;
	}
	
	public void setIncrementalKey(Properties incrementalKeys){
		this.incrementalKey = incrementalKeys;
	}
	
	public String getIncrementalFile() {
		return incrementalFile;
	}

	public Properties getIncrementalKey() {
		return incrementalKey;
	}
	
	public void storeIncrementalReading() throws IOException {
		if (incremental == null || incrementalFile == null) return;
		Properties incVal = (Properties)incremental.getPosition();
		incVal.store(new FileOutputStream(incrementalFile), null);
	}

}
