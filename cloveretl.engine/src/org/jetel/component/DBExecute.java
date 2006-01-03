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

import java.sql.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.database.*;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.*;
import org.jetel.util.ComponentXMLAttributes;
import org.jetel.util.FileUtils;
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
 *  <tr><td><b>dbSQL</b></td><td>SQL/DML/DDL statement(s) which has to be executed on database.
 *  If several statements should be executed, separate them by [;] (semicolon). They will be executed one by one.</td>
 *  </tr>
 *  <tr><td><b>url</b><br><i>optional</i></td><td>url location of the query<br>the query will be loaded from file referenced by the url</td>
 *  <tr><td><b>inTransaction</b></td><td>boolean value (Y/N) specifying whether statement(s) should be executed
 * in transaction. If Yes, then failure of one statement means that all changes will be rolled back by database.<br>
 * <i>Works only if database supports transactions.</i></td></tr>
 *  <tr><td><b>printStatements</b><br><i>optional</i></td><td>Specifies whether SQL commands are outputted to stdout. Default - No</td></tr>
 *  <tr><td>&lt;SQLCode&gt;<br><i><small>!!XML tag!!</small></i></td><td>This tag allows for specifying more than one statement. See example below.</td></tr>
 *  </table>
 *
 *  <h4>Example:</h4>
 *  <pre>&lt;Node id="DATABASE_RUN" type="DB_EXECUTE" dbConnection="NorthwindDB" dbSQL="drop table employee_z"/&gt;</pre>
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
	private static final String XML_DBSQL_ATTRIBUTE = "dbSQL";
	private static final String XML_URL_ATTRIBUTE = "url";
	
	private DBConnection dbConnection;
	private String dbConnectionName;
	private String[] dbSQL;
	private boolean oneTransaction = false;
	private boolean printStatements = false;

	/**  Description of the Field */
	public final static String COMPONENT_TYPE = "DB_EXECUTE";
	private final static String SQL_STATEMENT_DELIMITER = ";";

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
	    this(id,dbConnectionName,new String[]{dbSQL});

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
	    this(id,dbConnection,new String[]{dbSQL});
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
		//if (inPorts.size() >0 || outPorts.size >0) {
		//	throw new ComponentNotReadyException("This is independent component. No INPUT or OUTPUT connectins may exist !");
		//}
		// get dbConnection from graph
	    if (dbConnection == null){
	        dbConnection = this.graph.getDBConnection(dbConnectionName);
	    }
		if (dbConnection == null) {
			throw new ComponentNotReadyException("Can't find DBConnection ID: " + dbConnectionName);
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
	/**
	 *  Main processing method for the DBInputTable object
	 *
	 * @since    September 27, 2002
	 */
	public void run() {
		Statement sqlStatement;
		// this does not work for some drivers
		try {
			dbConnection.getConnection().setAutoCommit(false);
		} catch (SQLException ex) {
			if (oneTransaction) {
				logger.fatal("Can't disable AutoCommit mode for DB: " + dbConnection + " !");
				resultMsg = ex.getMessage();
				resultCode = Node.RESULT_ERROR;
				return;
			}
		}

		try {
			// let's create statement
			sqlStatement = dbConnection.getStatement();
			for (int i = 0; i < dbSQL.length; i++) {
				// empty strings are skipped
				if (dbSQL[i].trim().length() == 0) {
					continue;
				}
				// shall we print what is sent to DB ?
				if (printStatements){
					logger.info(dbSQL[i]);
				}
				sqlStatement.executeUpdate(dbSQL[i]);
				// shall we commit each statemetn ?
				if (!oneTransaction) {
					dbConnection.getConnection().commit();
				}
			}
			// let's commit what remains
			dbConnection.getConnection().commit();
			sqlStatement.close();

		} catch (SQLException ex) {
			performRollback();
			logger.fatal(ex);
			resultMsg = ex.getMessage();
			resultCode = Node.RESULT_ERROR;
			return;
		} catch (Exception ex) {
			performRollback();
			ex.printStackTrace();
			resultMsg = ex.getClass().getName()+" : "+ ex.getMessage();
			resultCode = Node.RESULT_FATAL_ERROR;
			//closeAllOutputPorts();
			return;
		}
		if (runIt) {
			resultMsg = "OK";
		} else {
			resultMsg = "STOPPED";
		}
		resultCode = Node.RESULT_OK;
	}


	/**  Description of the Method */
	private void performRollback() {
		try {
			dbConnection.getConnection().rollback();
		} catch (Exception ex) {
		}
	}

	/**
	 *  Description of the Method
	 *
	 * @return    Description of the Returned Value
	 * @since     September 27, 2002
	 */
	public void toXML(Element xmlElement) {
		
		// set attributes of DBExecute
		super.toXML(xmlElement);
		xmlElement.setAttribute(XML_DBCONNECTION_ATTRIBUTE, this.dbConnectionName);
		if (this.printStatements) {
			xmlElement.setAttribute(XML_PRINTSTATEMENTS_ATTRIBUTE, String.valueOf(this.printStatements));
		}
		
		if (this.oneTransaction) {
			xmlElement.setAttribute(XML_INTRANSACTION_ATTRIBUTE, String.valueOf(this.oneTransaction));
		}
		
		// use attribute for single SQL command, SQLCode element for multiple
		if (this.dbSQL.length == 1) {
			xmlElement.setAttribute(XML_DBSQL_ATTRIBUTE, this.dbSQL[0]);
		} else {
			Document doc = TransformationGraphXMLReaderWriter.getReference().getOutputXMLDocumentReference();
			Element childElement = doc.createElement(XML_SQLCODE_ELEMENT);
			// join given SQL commands
			StringBuffer buf = new StringBuffer(dbSQL[0]);
			for (int i=1; i<dbSQL.length; i++) {
				buf.append(SQL_STATEMENT_DELIMITER + dbSQL[i] + "\n");
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
	public static Node fromXML(org.w3c.dom.Node nodeXML) {
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(nodeXML);
		org.w3c.dom.Node childNode;
		ComponentXMLAttributes xattribsChild;
		DBExecute executeSQL;
		String query=null;	
		
		try {
		    if (xattribs.exists(XML_DBSQL_ATTRIBUTE)) {
			    query=xattribs.getString(XML_DBSQL_ATTRIBUTE);
			}else if (xattribs.exists(XML_URL_ATTRIBUTE)){
			    query=xattribs.resloveReferences(FileUtils.getStringFromURL(xattribs.getString(XML_URL_ATTRIBUTE)));
			}else {//we try to get it from child text node
				childNode = xattribs.getChildNode(nodeXML, XML_SQLCODE_ELEMENT);
				if (childNode == null) {
					throw new RuntimeException("Can't find <SQLCode> node !");
				}
				xattribsChild = new ComponentXMLAttributes(childNode);
				query=xattribsChild.getText(childNode);
			}   
			executeSQL = new DBExecute(xattribs.getString(Node.XML_ID_ATTRIBUTE),
						xattribs.getString(XML_DBCONNECTION_ATTRIBUTE),
						query.split(SQL_STATEMENT_DELIMITER));

			} catch (Exception ex) {
				System.err.println(COMPONENT_TYPE + ":" + ((xattribs.exists(XML_ID_ATTRIBUTE)) ? xattribs.getString(Node.XML_ID_ATTRIBUTE) : " unknown ID ") + ":" + ex.getMessage());
				return null;
			}

		if (xattribs.exists(XML_INTRANSACTION_ATTRIBUTE)) {
			executeSQL.setTransaction(xattribs.getBoolean(XML_INTRANSACTION_ATTRIBUTE));
		}

		if (xattribs.exists(XML_PRINTSTATEMENTS_ATTRIBUTE)) {
			executeSQL.setPrintStatements(xattribs.getBoolean(XML_PRINTSTATEMENTS_ATTRIBUTE));
		}
		
		if (xattribs.exists(XML_URL_ATTRIBUTE)) {
			executeSQL.setURL(xattribs.getString(XML_URL_ATTRIBUTE));
		}
		
		
		return executeSQL;
	}


	/**
	 *  Description of the Method
	 *
	 * @return    Description of the Return Value
	 */
	public boolean checkConfig() {
		return true;
	}
	
	public String getType(){
		return COMPONENT_TYPE;
	}

}

