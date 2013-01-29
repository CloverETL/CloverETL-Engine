/*
 * jETeL/CloverETL - Java based ETL application framework.
 * Copyright (c) Javlin, a.s. (info@cloveretl.com)
 *  
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */
package org.jetel.connection.jdbc;

import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.parser.AbstractParser;
import org.jetel.database.sql.CopySQLData;
import org.jetel.database.sql.JdbcSpecific;
import org.jetel.database.sql.JdbcSpecific.OperationType;
import org.jetel.database.sql.SqlConnection;
import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.IParserExceptionHandler;
import org.jetel.exception.JetelException;
import org.jetel.exception.PolicyType;
import org.jetel.graph.GraphElement;
import org.jetel.metadata.DataFieldMetadata;
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
public class SQLDataParser extends AbstractParser {
	protected IParserExceptionHandler exceptionHandler;
	protected DataRecordMetadata metadata;
	protected int recordCounter;

	protected SqlConnection dbConnection;
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
	
	private boolean autoCommit = true;
	
	static Log logger = LogFactory.getLog(SQLDataParser.class);

	/**
	 * @param sqlQuery
	 */
	@Deprecated
	public SQLDataParser(DataRecordMetadata metadata, String dbConnectionName,String sqlQuery) {
		this(metadata, sqlQuery);
	}

	/**
	 * Creates SQLDataParser object
	 * 
	 * @param sqlQuery query to be executed against DB
	 */
	public SQLDataParser(DataRecordMetadata metadata, String sqlQuery) {
		this.metadata = metadata;
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

	@Override
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

	@Override
	public DataRecord getNext() throws JetelException {
		DataRecord localOutRecord=DataRecordFactory.newRecord(metadata);
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
			AbstractCopySQLData.resetDataRecord(transMap,record);
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

	private String getErrorMessage(Exception ex, DataRecord record, int fieldNum) {
		String fieldName = null;
		String fieldType = null;
		String metadataName = null;
		
		if (record != null && record.getMetadata() != null) {
			DataRecordMetadata metadata = record.getMetadata();
			metadataName = metadata.getName();

			if (metadata.getField(fieldNum-1)  != null) {
				DataFieldMetadata fieldMetadata = metadata.getField(fieldNum-1);
				
				fieldType = fieldMetadata.getDataType().toString(fieldMetadata.getContainerType());
				fieldName = fieldMetadata.getName();
			}
		}
		
		StringBuilder builder = new StringBuilder();
		builder.append(fieldName).append(" (").append(fieldType).append(") ");
		builder.append("- ").append(ex.getMessage()).append("; in field ").append(fieldNum).append(" (\"").append(fieldName).append("\")").append(", metadata ").append(metadataName);
		
		return builder.toString();
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
                		fieldNum-1, "" + bdfe.getOffendingValue(), bdfe);
			} else {
				throw bdfe;
			}
		} catch (Exception ex) {
			
            logger.debug(getErrorMessage(ex, record, fieldNum) ,ex);
			throw new RuntimeException(getErrorMessage(ex, record, fieldNum), ex);
		}
	}

	protected void initSQLMap(DataRecord record) throws SQLException{
		List<Integer> fieldTypes = (this.dbConnection!=null && this.dbConnection.getJdbcSpecific()!=null)?
				this.dbConnection.getJdbcSpecific().getFieldTypes(resultSet.getMetaData(), metadata):
				SQLUtil.getFieldTypes(resultSet.getMetaData());	
		if (sqlCloverStatement.getCloverOutputFields() == null) {
			transMap = AbstractCopySQLData.sql2JetelTransMap(fieldTypes ,metadata, record, dbConnection.getJdbcSpecific());
		}else{
			transMap = AbstractCopySQLData.sql2JetelTransMap(fieldTypes ,metadata, record, 
					sqlCloverStatement.getCloverOutputFields(), dbConnection.getJdbcSpecific());
		}
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.parser.Parser#init(org.jetel.metadata.DataRecordMetadata)
	 */
	@Override
	public void init() throws ComponentNotReadyException {
		if (metadata == null) {
			throw new ComponentNotReadyException("Metadata are null");
		}
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.parser.Parser#setDataSource(java.lang.Object)
	 */
	@Override
	public void setReleaseDataSource(boolean releaseInputSource)  {
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.parser.Parser#setDataSource(java.lang.Object)
	 */
	@Override
	public void setDataSource(Object inputDataSource) throws ComponentNotReadyException {
		if (dbConnection != null) close();
		
		//outRecord.init();
        // get dbConnection from graph
        if (! (inputDataSource instanceof SqlConnection)){
            throw new RuntimeException("Need org.jetel.data.connection.jdbc.specific.DBConnectionInstance object !");
        }
        dbConnection = (SqlConnection) inputDataSource;
        
        long startTime;
        sqlCloverStatement = new SQLCloverStatement(dbConnection, sqlQuery, null);
        sqlCloverStatement.setLogger(logger);
        try {
			if (incrementalKey != null && sqlQuery.contains(SQLIncremental.INCREMENTAL_KEY_INDICATOR)) {
				if (incremental == null) {
					incremental = new SQLIncremental(incrementalKey, sqlQuery, incrementalFile, 
							dbConnection.getJdbcSpecific());
				}
				sqlCloverStatement.setIncremental(incremental);
			}
			sqlCloverStatement.init();
	        logger.debug((parentNode != null ? (parentNode.getId() + ": ") : "") + "Sending query " + 
	        		StringUtils.quote(sqlCloverStatement.getQuery()));
			startTime = System.currentTimeMillis();
			resultSet = sqlCloverStatement.executeQuery();
			if (logger.isDebugEnabled()) {
	            long executionTime = System.currentTimeMillis() - startTime;
	            SimpleDateFormat formatter = new SimpleDateFormat("HH:mm:ss.SSS");
	            formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
	            logger.debug((parentNode != null ? (parentNode.getId() + ": ") : "") + "Query execution time: " + 
	            		formatter.format(new Date(executionTime)));
			}
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
	
	public boolean checkIncremental(JdbcSpecific jdbcSpecific) throws ComponentNotReadyException{
    	if (incrementalKey != null && sqlQuery != null && sqlQuery.contains(SQLIncremental.INCREMENTAL_KEY_INDICATOR)) {
			try {
				SQLIncremental sqlIncremental = new SQLIncremental(incrementalKey, sqlQuery, incrementalFile, 
						jdbcSpecific);
				sqlIncremental.checkConfig();
			} catch (Exception e) {
				throw new ComponentNotReadyException(e);
			}
		}
		return true;
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.DataParser#close()
	 */
	@Override
	public void close() {
		if (dbConnection == null) return;//not initialized yet
		try {
			if (resultSet != null) {
				resultSet.close();
			}
			// try to commit (as some DBs apparently need commit even when data is read only
			if (!dbConnection.isClosed() && !dbConnection.getAutoCommit() && autoCommit) {
				dbConnection.commit();
			}            
			// close statement
			sqlCloverStatement.close();
		}
		catch (SQLException ex) {
            logger.warn("SQLException when closing statement",ex);
		}
	}

	public void setFetchSize(int fetchSize){
	    this.fetchSize = fetchSize;
	}

    @Override
	public void setExceptionHandler(IParserExceptionHandler handler) {
        this.exceptionHandler = handler;
    }

    @Override
	public IParserExceptionHandler getExceptionHandler() {
        return exceptionHandler;
    }

    @Override
	public PolicyType getPolicyType() {
        if(exceptionHandler != null) {
            return exceptionHandler.getType();
        }
        return null;
    }

	@Override
	public int skip(int nRec) throws JetelException {
		throw new UnsupportedOperationException("Not yet implemented");
	}

	/*
	 * (non-Javadoc)
	 * @see org.jetel.data.parser.Parser#reset()
	 */
	@Override
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

	@Override
	public Object getPosition() {
		return incremental != null ? incremental.getPosition() : null;
	}

	@Override
	public void movePosition(Object position) {
		if (incremental != null) {
			incremental.setValues((Properties)position);
		}
	}
	
	/**
	 * Updates <code>position</code> with incremental key values with incremental key values
	 * this parser currently holds.
	 * @param position
	 */
	public void megrePosition(Object position) {
		if (incremental != null) {
			incremental.mergePosition((Properties)position);
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
		storeIncrementalReading(incremental.getPosition());
	}
	
	public void storeIncrementalReading(Object position) throws IOException {
		if (incrementalKey == null || incrementalFile == null) return;
		((Properties) position).store(new FileOutputStream(incrementalFile), null);
	}

	@Override
	public void preExecute() throws ComponentNotReadyException {
	}

	@Override
	public void postExecute() throws ComponentNotReadyException {
		reset();
	}

	@Override
	public void free() throws ComponentNotReadyException, IOException {
		close();
	}

	@Override
	public boolean nextL3Source() {
		return false;
	}

	public boolean isAutoCommit() {
		return autoCommit;
	}

	public void setAutoCommit(boolean autoCommit) {
		this.autoCommit = autoCommit;
	}
}
