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
package org.jetel.component;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.connection.jdbc.DBConnection;
import org.jetel.connection.jdbc.SQLCloverCallableStatement;
import org.jetel.connection.jdbc.specific.DBConnectionInstance;
import org.jetel.connection.jdbc.specific.JDBCSpecific.OperationType;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.database.IConnection;
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
import org.jetel.util.file.FileUtils;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.string.StringUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Text;

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
 * <td>[0]- stored procedure input parameters</td></tr>
 * <tr><td><h4><i>Outputs:</i></h4></td>
 * <td>[0]- stored procedure output parameters and/or query result set</td></tr>
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
 *  <tr><td><b>url</b><br><i>optional</i></td><td>url location of the query<br>the query will be loaded from file referenced by the url</td>
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
 * @author      dpavlis, avackova (avackova@javlinconsulting.cz)
 * @since       Jan 17 2004
 * @revision    $Revision$
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
	
    private enum InTransaction {
    	ONE,
    	SET,
    	ALL;
    }
    
	private DBConnection dbConnection;
	private DBConnectionInstance connectionInstance;
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
	
	static Log logger = LogFactory.getLog(DBExecute.class);
	private PreparedStatement[] sqlStatement;
	private SQLCloverCallableStatement[] callableStatement;
	
	private DataRecord inRecord, outRecord;
	private Map<Integer, String>[] inParams, outParams;
	private String[] outputFields;

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
            dbSQL=sqlQuery.split(delimiter);
        }
		if (printStatements){
			for (int i = 0; i < dbSQL.length; i++) {
				logger.info(dbSQL[i]);
			}
		}
		if (getInPorts().size() > 0) {
			inRecord = new DataRecord(getInputPort(READ_FROM_PORT).getMetadata());
			inRecord.init();
		}
		if (getOutPorts().size() > 0) {
			outRecord = new DataRecord(getOutputPort(WRITE_TO_PORT).getMetadata());
			outRecord.init();
		}
		try {
			if (procedureCall) {
				connectionInstance = dbConnection.getConnection(getId(), OperationType.CALL);
				callableStatement = new SQLCloverCallableStatement[dbSQL.length];
				for (int i = 0; i < callableStatement.length; i++){
					callableStatement[i] = new SQLCloverCallableStatement(connectionInstance, dbSQL[i], inRecord, outRecord);
					if (inParams != null) {
						callableStatement[i].setInParameters(inParams[i]);
					}
					if (outParams != null) {
						callableStatement[i].setOutParameters(outParams[i]);
					}
					callableStatement[i].setOutputFields(outputFields);
					callableStatement[i].prepareCall();
				}
			}else{
				connectionInstance = dbConnection.getConnection(getId(), OperationType.WRITE);
				sqlStatement = new PreparedStatement[dbSQL.length];
				for (int i = 0; i < sqlStatement.length; i++){
					sqlStatement[i] = connectionInstance.getSqlConnection().prepareStatement(dbSQL[i]);
				}
			}
		} catch (SQLException e) {
			throw new ComponentNotReadyException(this, XML_SQLCODE_ELEMENT, e.getMessage());
		} catch (JetelException e) {
			throw new ComponentNotReadyException(e);
		}
	}

	@Override
	public synchronized void reset() throws ComponentNotReadyException {
		super.reset();
		if (procedureCall && getInPorts().size() > 0) {
			inRecord = new DataRecord(getInputPort(READ_FROM_PORT).getMetadata());
			inRecord.init();
			for (SQLCloverCallableStatement statement : callableStatement) {
				statement.setInRecord(inRecord);
			}
		}
		if (outRecord != null){
			outRecord.reset();
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

	
	@Override
	public Result execute() throws Exception {
		// this does not work for some drivers
		try {
			connectionInstance.getSqlConnection().setAutoCommit(false);
		} catch (SQLException ex) {
			if (transaction == InTransaction.ONE) {
				logger.error("Can't disable AutoCommit mode for DB: " + dbConnection + " !");
				throw new JetelException("Can't disable AutoCommit mode for DB: " + dbConnection + " !");
			}
		}

		InputPort inPort = getInputPort(READ_FROM_PORT);
		OutputPort outPort = getOutputPort(WRITE_TO_PORT);
		try {
			if (inPort != null) {
				inRecord = inPort.readRecord(inRecord);
			}
			boolean sendOut;
			do {
				for (int i = 0; i < dbSQL.length; i++){
					sendOut = outParams != null && outParams[i] != null;
					if (procedureCall) {
						callableStatement[i].executeCall();
						if (outPort != null) {
							sendOut = sendOut || callableStatement[i].isNext();
							do {
								if (sendOut) {
									outPort.writeRecord(callableStatement[i].getOutRecord());
								}
								sendOut = callableStatement[i].isNext();
							}while (sendOut);
						}
					}else{
						sqlStatement[i].executeUpdate();
					}
					if (transaction == InTransaction.ONE){
						connectionInstance.getSqlConnection().commit();
					}
				}
				if (transaction == InTransaction.SET){
					connectionInstance.getSqlConnection().commit();
				}
				if (inPort != null) {
					inRecord = inPort.readRecord(inRecord);
				}
			} while (runIt && inRecord != null);
			if (transaction == InTransaction.ALL){
				connectionInstance.getSqlConnection().commit();
			}
			broadcastEOF();
		} catch (Exception ex) {
			connectionInstance.getSqlConnection().rollback();
			throw ex;
		}	
		if (!runIt) {
			connectionInstance.getSqlConnection().rollback();
		}
        return runIt ? Result.FINISHED_OK : Result.ABORTED;
	}
	
	@Override
	public synchronized void free() {
		super.free();
		dbConnection.free();
	}

	/**
	 *  Description of the Method
	 *
	 * @return    Description of the Returned Value
	 * @since     September 27, 2002
	 */
	@Override public void toXML(Element xmlElement) {
		
		// set attributes of DBExecute
		super.toXML(xmlElement);
		xmlElement.setAttribute(XML_DBCONNECTION_ATTRIBUTE, this.dbConnectionName);
		xmlElement.setAttribute(XML_PRINTSTATEMENTS_ATTRIBUTE, String.valueOf(this.printStatements));
		xmlElement.setAttribute(XML_INTRANSACTION_ATTRIBUTE, String.valueOf(this.transaction));
		xmlElement.setAttribute(XML_PROCEDURE_CALL_ATTRIBUTE,String.valueOf(procedureCall));
        
        if (sqlStatementDelimiter!=null){
            xmlElement.setAttribute(XML_STATEMENT_DELIMITER, sqlStatementDelimiter);
        }
        
        if (outputFields != null){
        	xmlElement.setAttribute(XML_OUTPUT_FIELDS, StringUtils.stringArraytoString(outputFields, Defaults.Component.KEY_FIELDS_DELIMITER));
        }
        
        StringBuilder attr = new StringBuilder();
        if (inParams != null) {
        	for (int i = 0; i < inParams.length; i++) {
				attr.append(StringUtils.mapToString(inParams[i], Defaults.ASSIGN_SIGN, Defaults.Component.KEY_FIELDS_DELIMITER));
				attr.append(PARAMETERS_SET_DELIMITER);
			}
        	xmlElement.setAttribute(XML_IN_PARAMETERS, attr.toString());
        }
        
        attr.setLength(0);
        if (outParams != null) {
        	for (int i = 0; i < outParams.length; i++) {
				attr.append(StringUtils.mapToString(outParams[i], Defaults.ASSIGN_SIGN, Defaults.Component.KEY_FIELDS_DELIMITER));
				attr.append(PARAMETERS_SET_DELIMITER);
			}
        	xmlElement.setAttribute(XML_OUT_PARAMETERS, attr.toString());
        }

        // use attribute for single SQL command, SQLCode element for multiple
		if (this.dbSQL.length == 1) {
			xmlElement.setAttribute(XML_SQLQUERY_ATTRIBUTE, this.dbSQL[0]);
		} else {
			Document doc = xmlElement.getOwnerDocument();
			Element childElement = doc.createElement(ComponentXMLAttributes.XML_ATTRIBUTE_NODE_NAME);
            childElement.setAttribute(ComponentXMLAttributes.XML_ATTRIBUTE_NODE_NAME_ATTRIBUTE, XML_SQLCODE_ELEMENT);
			// join given SQL commands
			StringBuffer buf = new StringBuffer(dbSQL[0]);
            String delimiter = sqlStatementDelimiter !=null ? sqlStatementDelimiter : DEFAULT_SQL_STATEMENT_DELIMITER;
			for (int i=1; i<dbSQL.length; i++) {
				buf.append(delimiter + dbSQL[i] + "\n");
			}
			Text textElement = doc.createTextNode(buf.toString());
			childElement.appendChild(textElement);
			xmlElement.appendChild(childElement);
		}
	}

	/**
	 *  Description of the Method
	 *
	 * @param  nodeXML  Description of Parameter
	 * @return          Description of the Returned Value
	 * @since           September 27, 2002
	 */
    public static Node fromXML(TransformationGraph graph, Element xmlElement)
            throws XMLConfigurationException {
        ComponentXMLAttributes xattribs = new ComponentXMLAttributes(
                xmlElement, graph);
        org.w3c.dom.Node childNode;
        ComponentXMLAttributes xattribsChild;
        DBExecute executeSQL;
        String query = null;

        try {
            if (xattribs.exists(XML_SQLQUERY_ATTRIBUTE)) {
                query = xattribs.getString(XML_SQLQUERY_ATTRIBUTE);
            } else if (xattribs.exists(XML_DBSQL_ATTRIBUTE)) {
                query = xattribs.getString(XML_DBSQL_ATTRIBUTE);
            } else if (xattribs.exists(XML_URL_ATTRIBUTE)) {
                query = xattribs.resolveReferences(FileUtils.getStringFromURL(
						graph.getProjectURL(), xattribs.getString(XML_URL_ATTRIBUTE), 
						xattribs.getString(XML_CHARSET_ATTRIBUTE, null)));
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
            
        } catch (Exception ex) {
            throw new XMLConfigurationException(COMPONENT_TYPE + ":" + xattribs.getString(XML_ID_ATTRIBUTE," unknown ID ") + ":" + ex.getMessage(),ex);
        }

        return executeSQL;
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
        		|| !checkOutputPorts(status, 0, 1)) {
        	return status;
        }

        try {
            init();
        } catch (ComponentNotReadyException e) {
            ConfigurationProblem problem = new ConfigurationProblem(e.getMessage(), ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL);
            if(!StringUtils.isEmpty(e.getAttributeName())) {
                problem.setAttributeName(e.getAttributeName());
            }
            status.add(problem);
            
        }finally{
        	free();
        }
        
        return status;
   }
	
	public String getType(){
		return COMPONENT_TYPE;
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

}

