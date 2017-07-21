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
package org.jetel.component;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.connection.jdbc.SQLCloverCallableStatement;
import org.jetel.connection.jdbc.SQLScriptParser;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.Defaults;
import org.jetel.database.IConnection;
import org.jetel.database.sql.DBConnection;
import org.jetel.database.sql.JdbcSpecific.OperationType;
import org.jetel.database.sql.SqlConnection;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.JetelException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPort;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.runtime.tracker.ComponentTokenTracker;
import org.jetel.graph.runtime.tracker.ReformatComponentTokenTracker;
import org.jetel.util.AutoFilling;
import org.jetel.util.ExceptionUtils;
import org.jetel.util.ReadableChannelIterator;
import org.jetel.util.file.FileUtils;
import org.jetel.util.joinKey.JoinKeyUtils;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.property.RefResFlag;
import org.jetel.util.string.StringUtils;
import org.w3c.dom.Element;

/**
 *  <h3>DatabaseExecute Component</h3>
 * <!-- This component executes specified command (SQL/DML) against specified DB. -->
 *
 * <table border="1">
 * <th>Component:</th>
 * <tr><td><h4><i>Name:</i></h4></td>
 * <td>DBExecute</td></tr>
 * <tr><td><h4><i>Category:</i></h4></td>
 * <td></td></tr>
 * <tr><td><h4><i>Description:</i></h4></td>
 * <td>This component executes specified command(s) (SQL/DML) against specified DB</td></tr>
 * <tr><td><h4><i>Inputs:</i></h4></td>
 * <td>[0] (<i>optional</i>) - stored procedure input parameters or sql statements</td></tr>
 * <tr><td><h4><i>Outputs:</i></h4></td>
 * <td>[0] (<i>optional</i>) - stored procedure output parameters and/or query result set<br>
 * [1] (<i>optional</i>) - errors: field with <i>ErrCode</i> autofilling is filled by error code,field with <i>ErrText</i>
 *    autofilling is field by error message </td></tr>
 * <tr><td><h4><i>Comment:</i></h4></td>
 * <td></td></tr>
 * </table>
 *  <br>
 *  <table border="1">
 *  <th>XML attributes:</th>
 *  <tr><td><b>type</b></td><td>"DB_EXECUTE"</td></tr>
 *  <tr><td><b>id</b></td><td>component identification</td>
 *  <tr><td><b>dbConnection</b></td><td>id of the Database Connection object to be used to access the database</td>
 *  <tr><td><b>sqlQuery</b></td><td>SQL/DML/DDL statement(s) which has to be executed on database. In case you want to
 *  call stored procedure or function with parameters or producing output data set, it has to be in form:
 *  <i>{[? = ]call procedreName([?[,?...]])}</i> (Note: remember to close statement in curly brackets), when input/output parameters
 *  has to be set in proper attribute. If several statements should be executed, separate them by [;] (semicolon - default value; see sqlStatementDelimiter). They will be executed one by one.</td>
 *  </tr>
 *  <tr><td><b>inParameters</b></td><td>when calling stored procedure/function with input parameters. Maps out 
 *  which input fields would be treated as proper input parameters. Parameters are counted from 1. Form:<i>
 *  1:=$inField1;...n:=$infieldN<i></td>
 *  <tr><td><b>outParameters</b></td><td>when calling stored procedure/function with output parameters or returning value. Maps out 
 *  which output fields would be treated as proper output parameters. Parameters are counted from 1. If function return 
 *  a value, this is the first parameter Form:<i> 1:=$outField1;...n:=$outfieldN<i></td>
 *  <tr><td><b>outputFields</b></td><td>when stored procedure/function returns set of data its output will be parsed
 *  to given output fields. This is list of output fields delimited by semicolon.<i></td>
 *  <tr><td><b>sqlStatementDelimiter</b><br><i>optional</i></td><td>delimiter of sql statement in sqlQuery attribute</td>
 *  <tr><td><b>url</b><br><i>optional</i></td><td>url location of the query<br>the query will be loaded from file referenced by the url or 
 *  read from input port (see {@link DataReader} component)</td>
 *  <tr><td><b>charset </b><i>optional</i></td><td>encoding of extern query</td></tr>
 *  <tr><td><b>inTransaction<br><i>optional</i></b></td><td>one of: <i>ONE,SET,ALL</i> specifying whether statement(s) should be executed
 * in transaction. For <ul><li>ONE - commit is perform after each query execution</li>
 * <li>SET - for each input record there are executed all statements. After set of statements there is called commit, so if 
 * error occurred during execution of any statement, all statements for this record would be rolled back</li>
 * <li>ALL - commit is called only after all statements, so if error occurred all operations would be rolled back</li></ul>
 * Default is <i>SET</i>.<br>
 * <i>Works only if database supports transactions.</i></td></tr>
 *  <tr><td><b>printStatements</b><br><i>optional</i></td><td>Specifies whether SQL commands are outputted to stdout. Default - No</td></tr>
 *  <tr><td><b>callStatement</b><br><i>optional</i></td><td>boolean value (Y/N) - specifies whether SQL commands should be treated as stored procedure calls - using JDBC CallableStatement. Default - "N"</td></tr>
 *  <tr><td>&lt;SQLCode&gt;<br><i><small>!!XML tag!!</small></i></td><td>This tag allows for specifying more than one statement. See example below.</td></tr>
 *  <tr><td><b>errorActions </b><i>optional</i></td><td>defines if graph is to stop, when sql statement throws Sql Exception.
 *  Available actions are: STOP or CONTINUE. For CONTINUE action, error message is logged to console or file (if errorLog attribute
 *  is specified) and for STOP exception is populated and graph execution is stopped. <br>
 *  Error action can be set for each exception error code (value1=action1;value2=action2;...) or for all values the same action (STOP 
 *  or CONTINUE). It is possible to define error actions for some values and for all other values (MIN_INT=myAction).
 *  Default value is <i>STOP</i></td></tr>
 *  <tr><td><b>errorLog</b><br><i>optional</i></td><td>path to the error log file. Each error (after which graph continues) is logged in 
 *  following way: slqQuery;errorCode;errorMessage - fields are delimited by Defaults.Component.KEY_FIELDS_DELIMITER.</td></tr>
 *  </table>
 *
 *  <h4>Example:</h4>
 *  <pre>&lt;Node id="DATABASE_RUN" type="DB_EXECUTE" dbConnection="NorthwindDB" sqlQuery="drop table employee_z"/&gt;</pre>
 *  <pre>&lt;Node id="DATABASE_RUN" type="DB_EXECUTE" dbConnection="NorthwindDB" inTransaction="Y"&gt;
 *  &lt;SQLCode&gt;
 *	create table testTab (
 *		name varchar(20)
 *	);
 *
 *	insert into testTab ('nobody');
 *	insert into testTab ('somebody'); 
 *  &lt;/SQLCode&gt;
 *  &lt;/Node&gt; </pre>
 *  <pre>
 *  &lt;Node dbConnection="Connection1" id="DB_EXECUTE1" type="DB_EXECUTE"&gt;
 *    &lt;attr name="sqlQuery">create table proc_table (
 *  	id INTEGER,
 * 	    string VARCHAR(80),
 * 	    date DATETIME
 *    );
 *    CREATE PROCEDURE SPDownload
 *      &#64;last_dl_ts DATETIME
 *    AS
 *    BEGIN
 *      SELECT id, string, date
 *        FROM proc_table
 *           WHERE date >=  &#64;last_dl_ts
 *    END;&gt;&lt;/attr&gt;
 * &lt;/Node&gt;</pre>
 * <pre>
 * &lt;Node callStatement="true" dbConnection="Connection1" id="DB_EXECUTE2" inParameters="1:=$date" 
 * 			outputFields="id;string;date" type="DB_EXECUTE" sqlQuery="{call SPDownload(?)}" &gt;
 * 
 * &lt;Node dbConnection="Connection0" errorActions="MIN_INT=CONTINUE;" id="DB_EXECUTE0" printStatements="true" 
 * 			type="DB_EXECUTE" url="port:$0.field1:discrete"/>
 *
 * @author      dpavlis, avackova (avackova@javlinconsulting.cz)
 * @since       Jan 17 2004
 * @created     22. ?ervenec 2003
 * @see         org.jetel.database.AnalyzeDB
 */
public class DBExecute extends Node {

	public static final String XML_PRINTSTATEMENTS_ATTRIBUTE = "printStatements";
	public static final String XML_INTRANSACTION_ATTRIBUTE = "inTransaction";
	public static final String XML_SQLCODE_ELEMENT = "SQLCode";
	public static final String XML_DBCONNECTION_ATTRIBUTE = "dbConnection";
	public static final String XML_SQLQUERY_ATTRIBUTE = "sqlQuery";
    public static final String XML_DBSQL_ATTRIBUTE = "dbSQL";
	public static final String XML_URL_ATTRIBUTE = "url";
    public static final String XML_PROCEDURE_CALL_ATTRIBUTE = "callStatement";
    public static final String XML_STATEMENT_DELIMITER = "sqlStatementDelimiter";
    public static final String XML_CHARSET_ATTRIBUTE = "charset";
    public static final String XML_IN_PARAMETERS = "inParameters";
    public static final String XML_OUT_PARAMETERS = "outParameters";
    public static final String XML_OUTPUT_FIELDS = "outputFields";
	private static final String XML_ERROR_ACTIONS_ATTRIBUTE = "errorActions";
    private static final String XML_ERROR_LOG_ATTRIBUTE = "errorLog";
	
    private enum InTransaction {
    	ONE,
    	SET,
    	ALL,
    	NEVER_COMMIT;
    }
    
	private DBConnection dbConnection;
	private SqlConnection connection;
	private String dbConnectionName;
	private String sqlQuery;
    private String[] dbSQL;
	private InTransaction transaction = InTransaction.SET;
	private boolean printStatements = false;
	private boolean procedureCall = false;
    private String sqlStatementDelimiter;
    
	/**  Description of the Field */
	public final static String COMPONENT_TYPE = "DB_EXECUTE";
	private final static String DEFAULT_SQL_STATEMENT_DELIMITER = ";";
	private final static String PARAMETERS_SET_DELIMITER = "#";
	
	private final static int READ_FROM_PORT = 0;
	private final static int WRITE_TO_PORT = 0;
	private final static int ERROR_PORT = 1;
	
	static Log logger = LogFactory.getLog(DBExecute.class);
	private Statement sqlStatement;
	private SQLCloverCallableStatement[] callableStatement;
	
	private DataRecord inRecord, outRecord;
	private Map<Integer, String>[] inParams, outParams;
	private String[] outputFields;
	private String fileUrl;
	private String charset;

	private String errorActionsString;
	private Map<Integer, ErrorAction> errorActions = new HashMap<Integer, ErrorAction>();
	private String errorLogURL;
	private FileWriter errorLog;
	private int errorCodeFieldNum;
	private int errMessFieldNum;
	private DataRecord errRecord;
	private OutputPort errPort;
	private ReadableChannelIterator channelIterator;
	private OutputPort outPort;
	private SQLScriptParser sqlScriptParser;

	/**
	 *  Constructor for the DBExecute object
	 *
	 * @param  id                Description of Parameter
	 * @param  dbConnectionName  Description of Parameter
	 * @param  dbSQL             Description of the Parameter
	 * @since                    September 27, 2002
	 */
	public DBExecute(String id, String dbConnectionName, String dbSQL) {
        super(id);
        this.dbConnectionName=dbConnectionName;
	    this.sqlQuery=dbSQL;

	}


	/**
	 *Constructor for the DBExecute object
	 *
	 * @param  id                Description of the Parameter
	 * @param  dbConnectionName  Description of the Parameter
	 * @param  dbSQL             Description of the Parameter
	 */
	public DBExecute(String id, String dbConnectionName, String[] dbSQL) {
		super(id);
		this.dbConnectionName = dbConnectionName;
		this.dbSQL = dbSQL;
		// default

	}
	
	public DBExecute(String id, DBConnection dbConnection, String dbSQL){
        super(id);
        this.dbConnection=dbConnection;
        this.sqlQuery=dbSQL;
	}

	public DBExecute(String id, DBConnection dbConnection, String dbSQL[]){
	    super(id);
	    this.dbSQL=dbSQL;
	    this.dbConnection=dbConnection;
	}

	/**
	 *  Description of the Method
	 *
	 * @exception  ComponentNotReadyException  Description of Exception
	 * @since                                  September 27, 2002
	 */
	@Override
	public void init() throws ComponentNotReadyException {
        if(isInitialized()) return;
		super.init();
		
		// get dbConnection from graph
	    if (dbConnection == null){
	        IConnection conn = getGraph().getConnection(dbConnectionName);
            if(conn == null) {
                throw new ComponentNotReadyException("Can't find DBConnection ID: " + dbConnectionName);
            }
            if(!(conn instanceof DBConnection)) {
                throw new ComponentNotReadyException("Connection with ID: " + dbConnectionName + " isn't instance of the DBConnection class.");
            }
            dbConnection = (DBConnection) conn;
	    }
		if (!dbConnection.isInitialized()) {
			dbConnection.init();
		}        
		if (dbSQL==null){
            String delimiter = sqlStatementDelimiter !=null ? sqlStatementDelimiter : DEFAULT_SQL_STATEMENT_DELIMITER;
            sqlScriptParser = new SQLScriptParser();
            sqlScriptParser.setDelimiter(delimiter);
            sqlScriptParser.setBackslashQuoteEscaping(dbConnection.getJdbcSpecific().isBackslashEscaping());
            sqlScriptParser.setRequireLastDelimiter(false);
            if (sqlQuery != null) {
            	sqlScriptParser.setStringInput(sqlQuery);
            	List<String> dbSQLList = new ArrayList<String>();
            	String sqlQuery;
            	try {
					while ((sqlQuery = sqlScriptParser.getNextStatement()) != null) {
						dbSQLList.add(sqlQuery);
					}
				} catch (IOException e) {
					throw new ComponentNotReadyException("Cannot parse SQL statements", e);
				}
            	dbSQL = dbSQLList.toArray(new String[dbSQLList.size()]);
			}else{//read statements from file or input port
				channelIterator = new ReadableChannelIterator(getInputPort(READ_FROM_PORT), getGraph().getRuntimeContext().getContextURL(),
						fileUrl);
				channelIterator.setCharset(charset);
				channelIterator.setDictionary(getGraph().getDictionary());
				channelIterator.init();
			}
        }
		if ((outPort = getOutputPort(WRITE_TO_PORT)) != null) {
			outRecord = DataRecordFactory.newRecord(outPort.getMetadata());
			outRecord.init();
		}
		errPort = getOutputPort(ERROR_PORT);
		if (errPort != null){
			errRecord = DataRecordFactory.newRecord(errPort.getMetadata());
			errRecord.init();
			errorCodeFieldNum = errRecord.getMetadata().findAutoFilledField(AutoFilling.ERROR_CODE);
			errMessFieldNum = errRecord.getMetadata().findAutoFilledField(AutoFilling.ERROR_MESSAGE);
		}
		errorActions = new HashMap<Integer, ErrorAction>();
		if (errorActionsString != null){
        	String[] actions = StringUtils.split(errorActionsString);
        	if (actions.length == 1 && !actions[0].contains("=")){
        		errorActions.put(Integer.MIN_VALUE, ErrorAction.valueOf(actions[0].trim().toUpperCase()));
        	}else{
        	String[] action;
	        	for (String string : actions) {
					action = JoinKeyUtils.getMappingItemsFromMappingString(string);
					try {
						errorActions.put(Integer.parseInt(action[0]), ErrorAction.valueOf(action[1].toUpperCase()));
					} catch (NumberFormatException e) {
						if (action[0].equals(ComponentXMLAttributes.STR_MIN_INT)) {
							errorActions.put(Integer.MIN_VALUE, ErrorAction.valueOf(action[1].toUpperCase()));
						}
					}
				}
        	}
        }else{
        	errorActions.put(Integer.MIN_VALUE, ErrorAction.DEFAULT_ERROR_ACTION);
        }
	}

	@Override
	public synchronized void reset() throws ComponentNotReadyException {
		super.reset();
				
	}

	@Override
	public void preExecute() throws ComponentNotReadyException {
		super.preExecute();
		acquireConnection();
		if (outRecord != null) {
			outRecord.reset();
		}
		if (errRecord != null) {
			errRecord.reset();
		}
		if (getInPorts().size() > 0) {
			inRecord = DataRecordFactory.newRecord(getInputPort(READ_FROM_PORT).getMetadata());
			inRecord.init();
		}
		initStatements();
		if (errorLogURL != null) {
			try {
				errorLog = new FileWriter(FileUtils.getFile(getGraph().getRuntimeContext().getContextURL(), errorLogURL));
			} catch (IOException e) {
				throw new ComponentNotReadyException(this, XML_ERROR_LOG_ATTRIBUTE, e);
			}
		}
	}

	@Override
	public void postExecute() throws ComponentNotReadyException {
		super.postExecute();
		if (errorLog != null){
			try {
				errorLog.flush();
			} catch (IOException e) {
				throw new ComponentNotReadyException(this, XML_ERROR_LOG_ATTRIBUTE, e);
			}
			try {
				errorLog.close();
			} catch (IOException e) {
				throw new ComponentNotReadyException(this, XML_ERROR_LOG_ATTRIBUTE, e);
			}
		}
		try {
			if (callableStatement != null) {
				for (SQLCloverCallableStatement statement : callableStatement) {
					statement.close(); 
				}
			}
			if (sqlStatement != null) {
				sqlStatement.close();
			}
		} catch (SQLException e) {
			logger.warn("SQLException when closing statement", e);
		}
		dbConnection.closeConnection(getId(), procedureCall ? OperationType.CALL : OperationType.WRITE);
	}

	private void acquireConnection() throws ComponentNotReadyException {
		try {
			if (procedureCall) {
				connection = dbConnection.getConnection(getId(), OperationType.CALL);
			} else {
				connection = dbConnection.getConnection(getId(), OperationType.WRITE);
			}
		} catch (JetelException e) {
			throw new ComponentNotReadyException(e);
		}
	}
	
	private void initStatements() throws ComponentNotReadyException {
		try {
			// prepare statements if are not read from file or port
			if (procedureCall) {
				int resultSetType = dbConnection.getResultSetType();

				if (dbSQL != null) {
					callableStatement = new SQLCloverCallableStatement[dbSQL.length];
					for (int i = 0; i < callableStatement.length; i++) {
						callableStatement[i] = new SQLCloverCallableStatement(
								connection, dbSQL[i], inRecord, outRecord, resultSetType);
						if (inParams != null) {
							callableStatement[i].setInParameters(inParams[i]);
						}
						if (outParams != null) {
							callableStatement[i].setOutParameters(outParams[i]);
						}
						callableStatement[i].setOutputFields(outputFields);
						callableStatement[i].prepareCall();
					}
				} else if (inParams != null) {
					throw new ComponentNotReadyException(this, XML_SQLQUERY_ATTRIBUTE,
							"Can't read statement and parameters from input port");
				} else {
					callableStatement = new SQLCloverCallableStatement[1];
				}
			} else {
				sqlStatement = connection.createStatement();
			}
			// this does not work for some drivers
			try {
				// -pnajvar
				// This was bugfix #2207 but seems to cause more trouble than good
				// Rather, set transaction to default empty
				// if (DefaultConnection.isTransactionsSupported(connectionInstance.getSqlConnection())) {
				// connectionInstance.getSqlConnection().setAutoCommit(false);
				// }
				// Autocommit should be disabled only if multiple queries are executed within a single transaction.
				// Otherwise some queries might fail.
				connection.setAutoCommit(transaction == InTransaction.ONE);
			} catch (SQLException ex) {
				if (transaction != InTransaction.ONE) {
					throw new ComponentNotReadyException("Can't disable AutoCommit mode (required by current \"Transaction set\" setting) for DB: " + dbConnection + " !", ex);
				}
			}
		} catch (SQLException e) {
			throw new ComponentNotReadyException(this, XML_SQLCODE_ELEMENT, e);
		} catch (Exception e) {
			throw new ComponentNotReadyException(e);
		}
	}

	/**
	 *  Sets the transaction attribute of the DBExecute object
	 *
	 * @param  transaction  The new transaction value
	 */
	public void setTransaction(String transaction){
		try {
			this.transaction = InTransaction.valueOf(transaction.toUpperCase());
		} catch (IllegalArgumentException e) {
			if (Boolean.parseBoolean(transaction)) {
				this.transaction = InTransaction.ALL;
			}
		}
	}

	public void setPrintStatements(boolean printStatements){
		this.printStatements=printStatements;
	}

	private void handleException(SQLException e, DataRecord inRecord, int queryIndex) 
	throws IOException, InterruptedException, SQLException{
		ErrorAction action = errorActions.get(e.getErrorCode());
		if (action == null) {
			action = errorActions.get(Integer.MIN_VALUE);
			if (action == null) {
				action = ErrorAction.DEFAULT_ERROR_ACTION;
			}
		}
		if (action == ErrorAction.CONTINUE) {
			if (errRecord != null) {
				if (inRecord != null) {
					errRecord.copyFieldsByName(inRecord);
				}
				if (errorCodeFieldNum != -1) {
					errRecord.getField(errorCodeFieldNum).setValue(e.getErrorCode());
				}
				if (errMessFieldNum != -1) {
					errRecord.getField(errMessFieldNum).setValue(ExceptionUtils.getMessage(e));
				}
				errPort.writeRecord(errRecord);
			}else if (errorLog != null){
				errorLog.write(queryIndex > -1 ? dbSQL[queryIndex] : "commit");
				errorLog.write(Defaults.Component.KEY_FIELDS_DELIMITER);
				errorLog.write(String.valueOf(e.getErrorCode()));
				errorLog.write(Defaults.Component.KEY_FIELDS_DELIMITER);
				errorLog.write(ExceptionUtils.getMessage(e));
				errorLog.write("\n");
			}else{
				logger.warn(ExceptionUtils.getMessage(e));
			}
		}else{
			if (errorLog != null){
				errorLog.flush();
				errorLog.close();
			}
			try {
				connection.rollback();
			} catch (SQLException e1) {
				logger.warn("Can't rollback!!", e);
			}
			throw e;
		}
	}
	
	private void dbCommit() throws IOException, InterruptedException, SQLException{
		if (!connection.getAutoCommit()) {
    		try {
    			connection.commit();
    		} catch (SQLException e) {
    			handleException(e, inRecord, -1);
    		}
		}
	}
	
	@Override
	public Result execute() throws Exception {
		try {
    		if (channelIterator != null) {
    			Object readableByteChannel;
    			Charset nioCharset = this.charset != null ? Charset.forName(this.charset) : Charset.defaultCharset();
    			while (channelIterator.hasNext()) {
    				readableByteChannel = channelIterator.next();
    				if (readableByteChannel == null) break;
    				sqlScriptParser.setInput(readableByteChannel, nioCharset);
    				String statement;
    				int index = 0;
    				while ((statement = sqlScriptParser.getNextStatement()) != null) {
    					if (printStatements) {
							logger.info("Executing statement: " + statement);
    					}
    					try {
    						if (procedureCall) {
    							callableStatement[0] = new SQLCloverCallableStatement(connection, 
    									statement, null, outRecord, dbConnection.getResultSetType());
    							callableStatement[0].prepareCall();
    							executeCall(callableStatement[0], index);
    						}else{
    							sqlStatement.executeUpdate(statement);
    						}
    					} catch (SQLException e) {
    						handleException(e, null, index);
    					}
    					index++;
    					if (transaction == InTransaction.ONE){
    						dbCommit();
    					}
    				}
    				if (transaction == InTransaction.SET){
    					dbCommit();
    				}
    			}
    		}else{//sql statements are "solid" (set as sql query)
    			InputPort inPort = getInputPort(READ_FROM_PORT);
    			if (inPort != null) {
    				inRecord = inPort.readRecord(inRecord);
    			}
    			if (inPort == null || inRecord != null) do {
    				for (int i = 0; i < dbSQL.length; i++){
    					if (printStatements) {
    						logger.info("Executing statement: " + dbSQL[i]);
    					}
    					try {
    						if (procedureCall) {
    							executeCall(callableStatement[i], i);
    						}else{
    							sqlStatement.executeUpdate(dbSQL[i]);
    						}
    					} catch (SQLException e) {
    						handleException(e, inRecord, i);
    					}
    					if (transaction == InTransaction.ONE){
    						dbCommit();
    					}
    				}
    				if (transaction == InTransaction.SET){
    					dbCommit();
    				}
    				if (inPort != null) {
    					inRecord = inPort.readRecord(inRecord);
    				}
    			} while (runIt && inRecord != null);
    		}
    		if (runIt && transaction == InTransaction.ALL){
    			dbCommit();
    		}
    		if (!runIt) {
    			connection.rollback();
    		}
		} finally {
    		broadcastEOF();
		}
        return runIt ? Result.FINISHED_OK : Result.ABORTED;
	}
	
	/**
	 * Executes call and sends results to output port
	 * 
	 * @param callableStatement callable sql clover statement
	 * @param i number of statement (for proper setting output parameters)
	 * @throws SQLException
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private void executeCall(SQLCloverCallableStatement callableStatement, int i) throws SQLException, IOException, InterruptedException{
		boolean sendOut = outParams != null && i < outParams.length && outParams[i] != null;

		
		/*
		 * pnajvar-
		 * sendOut only if outParams is different than "result_set"
		 * This is a workaround which should be reviewed by the one who implemented this method
		 * as I don't have any knowledge why "send out if any output parameters even when isNext()==false" behavior
		 */
		if (sendOut && outParams[i].containsValue(SQLCloverCallableStatement.RESULT_SET_OUTPARAMETER_NAME)){
			sendOut = false;
		}
		
		
		callableStatement.executeCall();
		if (outPort != null) {
			
//			do {
//				if (sendOut) {
//					outPort.writeRecord(callableStatement.getOutRecord());
//				}
//				sendOut = callableStatement.isNext();
//			} while (sendOut);
			
			// order in this is important - isNext() THEN sendOut
			while(callableStatement.isNext() || sendOut) {
				outPort.writeRecord(callableStatement.getOutRecord());
				sendOut = false;
			}
			
		}
	}

	/**
	 *  Description of the Method
	 *
	 * @param  nodeXML  Description of Parameter
	 * @return          Description of the Returned Value
	 * @throws AttributeNotFoundException 
	 * @since           September 27, 2002
	 */
    public static Node fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException, AttributeNotFoundException {
        ComponentXMLAttributes xattribs = new ComponentXMLAttributes(
                xmlElement, graph);
        org.w3c.dom.Node childNode;
        ComponentXMLAttributes xattribsChild;
        DBExecute executeSQL;
        String query = null, fileURL = null;

    	if (xattribs.exists(XML_URL_ATTRIBUTE)) {
            fileURL = xattribs.getStringEx(XML_URL_ATTRIBUTE, RefResFlag.URL);
        } else if (xattribs.exists(XML_SQLQUERY_ATTRIBUTE)) {
            query = xattribs.getString(XML_SQLQUERY_ATTRIBUTE);
        } else if (xattribs.exists(XML_DBSQL_ATTRIBUTE)) {
            query = xattribs.getString(XML_DBSQL_ATTRIBUTE);
        } else if (xattribs.exists(XML_SQLCODE_ELEMENT)) {
            query = xattribs.getString(XML_SQLCODE_ELEMENT);
        } else {// we try to get it from child text node - slightly obsolete
                // now
            childNode = xattribs.getChildNode(xmlElement,
                    XML_SQLCODE_ELEMENT);
            if (childNode == null) {
                throw new RuntimeException("Can't find <SQLCode> node !");
            }
            xattribsChild = new ComponentXMLAttributes((Element)childNode, graph);
            query = xattribsChild.getText(childNode);
        }
        executeSQL = new DBExecute(xattribs
                .getString(XML_ID_ATTRIBUTE), xattribs
                .getString(XML_DBCONNECTION_ATTRIBUTE), 
                query);
        if (fileURL != null) {
        	executeSQL.setFileURL(fileURL);
        	if (xattribs.exists(XML_CHARSET_ATTRIBUTE)) {
        		executeSQL.setCharset(xattribs.getString(XML_CHARSET_ATTRIBUTE));
        	}
        }
        if (xattribs.exists(XML_INTRANSACTION_ATTRIBUTE)) {
            executeSQL.setTransaction(xattribs
                    .getString(XML_INTRANSACTION_ATTRIBUTE));
        }

        if (xattribs.exists(XML_PRINTSTATEMENTS_ATTRIBUTE)) {
            executeSQL.setPrintStatements(xattribs
                    .getBoolean(XML_PRINTSTATEMENTS_ATTRIBUTE));
        }

        if (xattribs.exists(XML_PROCEDURE_CALL_ATTRIBUTE)){
            executeSQL.setProcedureCall(xattribs.getBoolean(XML_PROCEDURE_CALL_ATTRIBUTE));
        }
        if (xattribs.exists(XML_IN_PARAMETERS)){
        	executeSQL.setInParameters(xattribs.getString(XML_IN_PARAMETERS));
        }
        if (xattribs.exists(XML_OUT_PARAMETERS)){
        	executeSQL.setOutParameters(xattribs.getString(XML_OUT_PARAMETERS));
        }
        if (xattribs.exists(XML_OUTPUT_FIELDS)){
        	executeSQL.setOutputFields(xattribs.getString(XML_OUTPUT_FIELDS).split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX));
        }
        if (xattribs.exists(XML_STATEMENT_DELIMITER)){
            executeSQL.setSqlStatementDelimiter(xattribs.getString(XML_STATEMENT_DELIMITER));
        }
		if (xattribs.exists(XML_ERROR_ACTIONS_ATTRIBUTE)){
			executeSQL.setErrorActions(xattribs.getString(XML_ERROR_ACTIONS_ATTRIBUTE));
		}
		if (xattribs.exists(XML_ERROR_LOG_ATTRIBUTE)){
			executeSQL.setErrorLog(xattribs.getString(XML_ERROR_LOG_ATTRIBUTE));
		}

        return executeSQL;
    }

	public void setCharset(String charset) {
		this.charset = charset;
	}


	public void setFileURL(String fileURL) {
		this.fileUrl = fileURL;
	}


	public void setErrorLog(String errorLog) {
		this.errorLogURL = errorLog;
	}

	public void setErrorActions(String string) {
		this.errorActionsString = string;		
	}

	public void setInParameters(String string) {
		String[] inParameters = string.split(PARAMETERS_SET_DELIMITER);
		inParams = new HashMap[inParameters.length];
		for (int i = 0; i < inParameters.length; i++) {
			inParams[i] = convertMappingToMap(inParameters[i]);
		}
	}

	public void setOutParameters(String string) {
		String[] outParameters = string.split(PARAMETERS_SET_DELIMITER);
		outParams = new HashMap[outParameters.length];
		for (int i = 0; i < outParameters.length; i++) {
			outParams[i] = convertMappingToMap(outParameters[i]);
		}
	}

	public static Map<Integer, String> convertMappingToMap(String mapping){
		if (StringUtils.isEmpty(mapping)) return null;
		String[] mappings = mapping.split(Defaults.Component.KEY_FIELDS_DELIMITER);
		HashMap<Integer, String> result = new HashMap<Integer, String>();
		int assignIndex;
		boolean isFieldInicator = mapping.indexOf(Defaults.CLOVER_FIELD_INDICATOR) > -1;
		int assignSignLength = Defaults.ASSIGN_SIGN.length();
		for (int i = 0; i < mappings.length; i++) {
			assignIndex = mappings[i].indexOf(Defaults.ASSIGN_SIGN);
			if (assignIndex > -1) {
				if (mappings[i].startsWith(Defaults.CLOVER_FIELD_INDICATOR)) {
					result.put(Integer.parseInt(mappings[i].substring(assignIndex + assignSignLength).trim()), 
							isFieldInicator ? 
									mappings[i].substring(Defaults.CLOVER_FIELD_INDICATOR.length(), assignIndex).trim() :
									mappings[i].substring(0, assignIndex).trim());
				} else {
					result.put(Integer.parseInt(mappings[i].substring(0, assignIndex).trim()), 
							isFieldInicator ?
									mappings[i].substring(assignIndex + assignSignLength).trim().substring(Defaults.CLOVER_FIELD_INDICATOR.length()):
									mappings[i].substring(assignIndex + assignSignLength).trim());
				}
			}else{
				result.put(i+1, isFieldInicator ?
						mappings[i].trim().substring(Defaults.CLOVER_FIELD_INDICATOR.length()) :
						mappings[i].trim());
			}
		}
		return result.size() > 0 ? result : null;
	}
	/**
	 *  Description of the Method
	 *
	 * @return    Description of the Return Value
	 */
    @Override
    public ConfigurationStatus checkConfig(ConfigurationStatus status) {
        super.checkConfig(status);
        
        if(!checkInputPorts(status, 0, 1)
        		|| !checkOutputPorts(status, 0, 2, false)) {
        	return status;
        }
        
        if (charset != null && !Charset.isSupported(charset)) {
        	status.add(new ConfigurationProblem(
            		"Charset "+charset+" not supported!", 
            		ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL));
        }

		try {
		    if (dbConnection == null){
		        IConnection conn = getGraph().getConnection(dbConnectionName);
	            if(conn == null) {
	                throw new ComponentNotReadyException("Can't find DBConnection ID: " + dbConnectionName);
	            }
	            if(!(conn instanceof DBConnection)) {
	                throw new ComponentNotReadyException("Connection with ID: " + dbConnectionName + " isn't instance of the DBConnection class.");
	            }
		    }
            if (errorActionsString != null){
				ErrorAction.checkActions(errorActionsString);
            }
            
            if (errorLog != null){
 				FileUtils.canWrite(getGraph().getRuntimeContext().getContextURL(), errorLogURL);
            }
            if (getOutputPort(WRITE_TO_PORT) == null && procedureCall && (dbSQL != null || sqlQuery != null) && outParams != null) {
            	status.add(new ConfigurationProblem("Output port must be defined when output parameters are set.", ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL));
            }
        } catch (ComponentNotReadyException e) {
            ConfigurationProblem problem = new ConfigurationProblem(ExceptionUtils.getMessage(e), ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL);
            if(!StringUtils.isEmpty(e.getAttributeName())) {
                problem.setAttributeName(e.getAttributeName());
            }
            status.add(problem);
            
        }
        
        return status;
   }
	
    public boolean isProcedureCall() {
        return procedureCall;
    }


    public void setProcedureCall(boolean procedureCall) {
        this.procedureCall = procedureCall;
    }


    /**
     * @return the sqlStatementDelimiter
     */
    public String getSqlStatementDelimiter() {
        return sqlStatementDelimiter;
    }


    /**
     * @param sqlStatementDelimiter the sqlStatementDelimiter to set
     */
    public void setSqlStatementDelimiter(String sqlStatementDelimiter) {
        this.sqlStatementDelimiter = sqlStatementDelimiter;
    }


	public void setOutputFields(String[] outputFields) {
		this.outputFields = outputFields;
	}

	@Override
	protected ComponentTokenTracker createComponentTokenTracker() {
		return new ReformatComponentTokenTracker(this);
	}

}

