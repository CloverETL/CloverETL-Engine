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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.TimeZone;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.connection.jdbc.config.JdbcBaseConfig;
import org.jetel.connection.jdbc.config.JdbcConfigFactory;
import org.jetel.connection.jdbc.config.JdbcBaseConfig.OperationType;
import org.jetel.data.DataRecord;
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
 * @author David Pavlis
 *
 */
public class SQLDataParser implements Parser {
	protected IParserExceptionHandler exceptionHandler;
	protected DataRecordMetadata metadata;
	protected int recordCounter;
	protected int fieldCount = 0;

	protected Connection dbConnection;
	protected JdbcBaseConfig connectionConfig;
	protected String sqlQuery;
	protected Statement statement;
	protected QueryAnalyzer analyzer;

	protected ResultSet resultSet = null;
	protected CopySQLData[] transMap=null;
	protected DataRecord outRecord = null;

	private GraphElement parentNode;

	protected int fetchSize = -1;
	
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
        if (! (inputDataSource instanceof Connection)){
            throw new RuntimeException("Need java.sql.Connection object !");
        }
        dbConnection= (Connection) inputDataSource;

        if (connectionConfig == null) {
        	try {
				//TODO
				connectionConfig = JdbcConfigFactory.createConfig(dbConnection
						.getMetaData().getDatabaseProductName());
			} catch (Exception e) {
				// TODO: handle exception
			}
        }
        connectionConfig.optimizeConnection(dbConnection, OperationType.READ);
        
        try {
        	statement = connectionConfig.createStatement(dbConnection, OperationType.READ);
        } catch (SQLException e) {
            throw new ComponentNotReadyException(e);
        }
        
        connectionConfig.optimizeStatement(statement, OperationType.READ);
        
        logger.debug((parentNode != null ? (parentNode.getId() + ": ") : "") + "Sending query " + 
        		StringUtils.quote(sqlQuery));
        long startTime = System.currentTimeMillis();
        try{
            resultSet = statement.executeQuery(sqlQuery);
            long executionTime = System.currentTimeMillis() - startTime;
            SimpleDateFormat formatter = new SimpleDateFormat("HH:mm:ss.SSS");
            formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
            logger.debug((parentNode != null ? (parentNode.getId() + ": ") : "") + "Query execution time: " + 
            		formatter.format(new Date(executionTime)));
        } catch (SQLException e) {
            logger.debug(e);
            throw new ComponentNotReadyException(e);
        }

        connectionConfig.optimizeResultSet(resultSet, OperationType.READ);
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

	/* (non-Javadoc)
	 * @see org.jetel.data.DataParser#close()
	 */
	public void close() {
		try {
			if (resultSet != null) {
				resultSet.close();
			}
			// try to commit (as some DBs apparently need commit even when data is read only
			if (!dbConnection.getAutoCommit()) {
				dbConnection.commit();
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
			statement.close();
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

	public JdbcBaseConfig getConnectionConfig() {
		return connectionConfig;
	}

	public void setConnectionConfig(JdbcBaseConfig connectionConfig) {
		this.connectionConfig = connectionConfig;
	}
		
	public Object getPosition() {
		// TODO Auto-generated method stub
		return null;
	}

	public void movePosition(Object position) {
		// TODO Auto-generated method stub
		
	}
}
