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

import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.connection.DBConnection;
import org.jetel.database.IConnection;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.JetelException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.Node;
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
 * <td></td></tr>
 * <tr><td><h4><i>Outputs:</i></h4></td>
 * <td></td></tr>
 * <tr><td><h4><i>Comment:</i></h4></td>
 * <td></td></tr>
 * </table>
 *  <br>
 *  <table border="1">
 *  <th>XML attributes:</th>
 *  <tr><td><b>type</b></td><td>"DB_EXECUTE"</td></tr>
 *  <tr><td><b>id</b></td><td>component identification</td>
 *  <tr><td><b>dbConnection</b></td><td>id of the Database Connection object to be used to access the database</td>
 *  <tr><td><b>sqlQuery</b></td><td>SQL/DML/DDL statement(s) which has to be executed on database.
 *  If several statements should be executed, separate them by [;] (semicolon - default value; see sqlStatementDelimiter). They will be executed one by one.</td>
 *  </tr>
 *  <tr><td><b>sqlStatementDelimiter</b><br><i>optional</i></td><td>delimiter of sql statement in sqlQuery attribute</td>
 *  <tr><td><b>url</b><br><i>optional</i></td><td>url location of the query<br>the query will be loaded from file referenced by the url</td>
 *  <tr><td><b>charset </b><i>optional</i></td><td>encoding of extern query</td></tr>
 *  <tr><td><b>inTransaction</b></td><td>boolean value (Y/N) specifying whether statement(s) should be executed
 * in transaction. If Yes, then failure of one statement means that all changes will be rolled back by database.<br>
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
 *  &lt;/Node&gt;
 *  </pre>
 *  <br>
 *
 * @author      dpavlis
 * @since       Jan 17 2004
 * @revision    $Revision$
 * @created     22. ?ervenec 2003
 * @see         org.jetel.database.AnalyzeDB
 */
public class DBExecute extends Node {

	private static final String XML_PRINTSTATEMENTS_ATTRIBUTE = "printStatements";
	private static final String XML_INTRANSACTION_ATTRIBUTE = "inTransaction";
	private static final String XML_SQLCODE_ELEMENT = "SQLCode";
	private static final String XML_DBCONNECTION_ATTRIBUTE = "dbConnection";
	private static final String XML_SQLQUERY_ATTRIBUTE = "sqlQuery";
    private static final String XML_DBSQL_ATTRIBUTE = "dbSQL";
	private static final String XML_URL_ATTRIBUTE = "url";
    private static final String XML_PROCEDURE_CALL_ATTRIBUTE = "callStatement";
    private static final String XML_STATEMENT_DELIMITER = "sqlStatementDelimiter";
    private static final String XML_CHARSET_ATTRIBUTE = "charset";
	
	private DBConnection dbConnection;
	private String dbConnectionName;
	private String sqlQuery;
    private String[] dbSQL;
	private boolean oneTransaction = false;
	private boolean printStatements = false;
	private boolean procedureCall = false;
    private String sqlStatementDelimiter;
    
	/**  Description of the Field */
	public final static String COMPONENT_TYPE = "DB_EXECUTE";
	private final static String DEFAULT_SQL_STATEMENT_DELIMITER = ";";

	static Log logger = LogFactory.getLog(DBExecute.class);
	private String url = null;


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
	}




	/**
	 *  Sets the transaction attribute of the DBExecute object
	 *
	 * @param  transaction  The new transaction value
	 */
	public void setTransaction(boolean transaction) {
		oneTransaction = transaction;
	}

	public void setPrintStatements(boolean printStatements){
		this.printStatements=printStatements;
	}

	
	public void setURL(String url) {
		this.url = url;
	}
	
	public String getURL() {
		return(this.url = null);
	}
	
	@Override
	public Result execute() throws Exception {
		Statement sqlStatement=null;
		// this does not work for some drivers
		try {
			dbConnection.getConnection(getId()).setAutoCommit(false);
		} catch (SQLException ex) {
			if (oneTransaction) {
				logger.fatal("Can't disable AutoCommit mode for DB: " + dbConnection + " !");
				throw new JetelException("Can't disable AutoCommit mode for DB: " + dbConnection + " !");
			}
		}

		try {
			// let's create statement - based on what do we execute
            if (!procedureCall){
                sqlStatement = dbConnection.getConnection(getId()).createStatement();
            }
            
			for (int i = 0; i < dbSQL.length; i++) {
				// empty strings are skipped
				if (dbSQL[i].trim().length() == 0) {
					continue;
				}
				// shall we print what is sent to DB ?
				if (printStatements){
					logger.info(dbSQL[i]);
				}
                if (sqlStatement==null){
                        sqlStatement=dbConnection.getConnection(getId()).prepareCall(dbSQL[i]);
                }
				sqlStatement.executeUpdate(dbSQL[i]);
				// shall we commit each statemetn ?
				if (!oneTransaction) {
					dbConnection.getConnection(getId()).commit();
				}
                if (procedureCall){
                    sqlStatement.close();
                    sqlStatement=null;
                }
			}
			// let's commit what remains
			dbConnection.getConnection(getId()).commit();
			if(sqlStatement!=null) { sqlStatement.close(); }

		} catch (Exception ex) {
			performRollback();
			logger.fatal(ex);
			throw new JetelException(ex.getMessage(),ex);
		}	
        return runIt ? Result.FINISHED_OK : Result.ABORTED;
	}


	/**  Description of the Method */
	private void performRollback() {
		try {
			dbConnection.getConnection(getId()).rollback();
		} catch (Exception ex) {
		}
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
		xmlElement.setAttribute(XML_INTRANSACTION_ATTRIBUTE, String.valueOf(this.oneTransaction));
		xmlElement.setAttribute(XML_PROCEDURE_CALL_ATTRIBUTE,String.valueOf(procedureCall));
        
        if (sqlStatementDelimiter!=null){
            xmlElement.setAttribute(XML_STATEMENT_DELIMITER, sqlStatementDelimiter);
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
						graph.getRuntimeParameters().getProjectURL(), xattribs.getString(XML_URL_ATTRIBUTE), 
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
                        .getBoolean(XML_INTRANSACTION_ATTRIBUTE));
            }

            if (xattribs.exists(XML_PRINTSTATEMENTS_ATTRIBUTE)) {
                executeSQL.setPrintStatements(xattribs
                        .getBoolean(XML_PRINTSTATEMENTS_ATTRIBUTE));
            }

            if (xattribs.exists(XML_URL_ATTRIBUTE)) {
                executeSQL.setURL(xattribs.getString(XML_URL_ATTRIBUTE));
            }
            if (xattribs.exists(XML_PROCEDURE_CALL_ATTRIBUTE)){
                executeSQL.setProcedureCall(xattribs.getBoolean(XML_PROCEDURE_CALL_ATTRIBUTE));
            }
            if (xattribs.exists(XML_STATEMENT_DELIMITER)){
                executeSQL.setSqlStatementDelimiter(xattribs.getString(XML_STATEMENT_DELIMITER));
            }
            
        } catch (Exception ex) {
            throw new XMLConfigurationException(COMPONENT_TYPE + ":" + xattribs.getString(XML_ID_ATTRIBUTE," unknown ID ") + ":" + ex.getMessage(),ex);
        }

        return executeSQL;
    }


	/**
	 *  Description of the Method
	 *
	 * @return    Description of the Return Value
	 */
    @Override
    public ConfigurationStatus checkConfig(ConfigurationStatus status) {
        super.checkConfig(status);
        
        checkInputPorts(status, 0, 0);
        checkOutputPorts(status, 0, 0);

        try {
            init();
            free();
        } catch (ComponentNotReadyException e) {
            ConfigurationProblem problem = new ConfigurationProblem(e.getMessage(), ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL);
            if(!StringUtils.isEmpty(e.getAttributeName())) {
                problem.setAttributeName(e.getAttributeName());
            }
            status.add(problem);
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

}

