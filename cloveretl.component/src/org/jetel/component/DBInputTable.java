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

import java.io.IOException;

import org.jetel.connection.SQLDataParser;
import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.IParserExceptionHandler;
import org.jetel.exception.ParserExceptionHandlerFactory;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.Node;
import org.jetel.graph.TransformationGraph;
import org.jetel.util.ComponentXMLAttributes;
import org.jetel.util.FileUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Text;

/**
 *  <h3>DatabaseInputTable Component</h3>
 *
 * <!-- This component reads data from DB. It first executes specified query on DB and then
 *  extracts all the rows returned.The metadata provided throuh output port/edge must precisely
 *  describe the structure of read rows. Use DBAnalyze utilitity to analyze DB structures and
 *  create Jetel/Clover metadata. -->
 *
 * <table border="1">
 * <th>Component:</th>
 * <tr><td><h4><i>Name:</i></h4></td>
 * <td>DBInputTable</td></tr>
 * <tr><td><h4><i>Category:</i></h4></td>
 * <td></td></tr>
 * <tr><td><h4><i>Description:</i></h4></td>
 * <td>This component reads data from DB. It first executes specified query on DB and then
 *  extracts all the rows returned.<br>
 *  The metadata provided throuh output port/edge must precisely describe the structure of
 *  read rows.<br>
 *  Use DBAnalyze utilitity to analyze DB structures and create Jetel/Clover metadata.</td></tr>
 * <tr><td><h4><i>Inputs:</i></h4></td>
 * <td></td></tr>
 * <tr><td><h4><i>Outputs:</i></h4></td>
 * <td>[0]- output records</td></tr>
 * <tr><td><h4><i>Comment:</i></h4></td>
 * <td></td></tr>
 * </table>
 *  <br>
 *  <table border="1">
 *  <th>XML attributes:</th>
 *  <tr><td><b>type</b></td><td>"DB_INPUT_TABLE"</td></tr>
 *  <tr><td><b>id</b></td><td>component identification</td>
 *  <tr><td><b>sqlQuery</b><br><i>optional</i></td><td>query to be sent to database<br><i><code>sqlQuery</code> or <code>url</code> must be defined</i></td>
 *  <tr><td><b>url</b><br><i>optional</i></td><td>url location of the query<br>the query will be loaded from file referenced by url</td>
 *  <tr><td><b>dbConnection</b></td><td>id of the Database Connection object to be used to access the database</td>
 *  <tr><td><b>fetchSize</b><br><i>optional</i></td><td>how many records should be fetched from db at once. <i>See JDBC's java.sql.Statement.setFetchSize()</i><br><b><code>MIN_INT</code></b> constant
 * is implemented - is resolved to Integer.MIN_INT value <i>(good for MySQL JDBC driver)</i></td>
 *  <tr><td>&lt;SQLCode&gt;<br><i>optional<small>!!XML tag!!</small></i></td><td>This tag allows for embedding large SQL statement directly into graph.. See example below.</td></tr>
 *  <tr><td><b>DataPolicy</b></td><td>specifies how to handle misformatted or incorrect data.  'Strict' (default value) aborts processing, 'Controlled' logs the entire record while processing continues, and 'Lenient' attempts to set incorrect data to default values while processing continues.</td></tr>
 *  </table>
 *
 *  <br>sqlQuery and url are mutually exclusive.  url is the primary and if found the sqlQuery will not be used.<br>
 *
 *  <h4>Example:</h4>
 *  <pre>&lt;Node id="INPUT" type="DB_INPUT_TABLE" dbConnection="NorthwindDB" sqlQuery="select * from employee_z"/&gt;</pre>
 *
 *  <h4>Example:</h4>
 *  <pre>&lt;Node id="INPUT" type="DB_INPUT_TABLE" dbConnection="NorthwindDB" url="c:/temp/test.sql"/&gt;</pre>
 *	
 *  <h4>Example:</h4>
 *  <pre>&lt;Node id="INPUT" type="DB_INPUT_TABLE" dbConnection="NorthwindDB" &gt;
 *  &lt;SQLCode&gt;
 *	select * from employee_z
 *  &lt;/SQLCode&gt;
 *  &lt;/Node&gt;
 *  </pre>
 *
 * @author      dpavlis
 * @since       September 27, 2002
 * @revision    $Revision$
 * @see         org.jetel.database.AnalyzeDB
 */
public class DBInputTable extends Node {
	private static final String XML_DATAPOLICY_ATTRIBUTE = "DataPolicy";
	private static final String XML_DBCONNECTION_ATTRIBUTE = "dbConnection";
	private static final String XML_SQLQUERY_ATTRIBUTE = "sqlQuery";
	private static final String XML_URL_ATTRIBUTE = "url";
	private static final String XML_FETCHSIZE_ATTRIBUTE = "fetchSize";
	private static final String XML_SQLCODE_ELEMENT = "SQLCode";
	
	private SQLDataParser parser;

	private String dbConnectionName;
	private String sqlQuery;
	
	private int fetchSize=0;

	/**  Description of the Field */
	public final static String COMPONENT_TYPE = "DB_INPUT_TABLE";
	private final static int WRITE_TO_PORT = 0;
	private String url = null;


	/**
	 *Constructor for the DBInputTable object
	 *
	 * @param  id                Description of Parameter
	 * @param  dbConnectionName  Description of Parameter
	 * @param  sqlQuery          Description of Parameter
	 * @since                    September 27, 2002
	 */
	public DBInputTable(String id, String dbConnectionName, String sqlQuery) {
		super(id);
		this.dbConnectionName = dbConnectionName;
		this.sqlQuery = sqlQuery;

		parser = new SQLDataParser(dbConnectionName, sqlQuery);
	}


	/**
	 *  Description of the Method
	 *
	 * @exception  ComponentNotReadyException  Description of Exception
	 * @since                                  September 27, 2002
	 */
	public void init() throws ComponentNotReadyException {
		if (outPorts.size() < 1) {
			throw new ComponentNotReadyException("At least one output port has to be defined!");
		}
		//set fetch size (if defined)
		if (fetchSize!=0) parser.setFetchSize(fetchSize);
		// try to open file & initialize data parser
        Object connection=getGraph().getConnection(dbConnectionName);
        if (connection==null){
            throw new ComponentNotReadyException("Can't obtain DBConnection object: \""+dbConnectionName+"\"");
        }
		parser.init(getOutputPort(WRITE_TO_PORT).getMetadata());
		parser.setDataSource(connection);
	}




	/**
	 *  Main processing method for the DBInputTable object
	 *
	 * @since    September 27, 2002
	 */
	public void run() {

		// we need to create data record - take the metadata from first output port
		DataRecord record = new DataRecord(getOutputPort(WRITE_TO_PORT).getMetadata());
		record.init();
		parser.initSQLDataMap(record);

		try {
			// till it reaches end of data or it is stopped from outside
			while (((record = parser.getNext(record)) != null) && runIt) {
				//broadcast the record to all connected Edges
				writeRecordBroadcast(record);
			}

		} catch (IOException ex) {
			resultMsg = ex.getMessage();
			resultCode = Node.RESULT_ERROR;
			closeAllOutputPorts();
			return;
		} catch (Exception ex) {
			resultMsg = ex.getClass().getName()+" : "+ ex.getMessage();
			resultCode = Node.RESULT_FATAL_ERROR;
			return;
		}

		// we are done, close all connected output ports to indicate end of stream

		parser.close();

		broadcastEOF();

		if (runIt) {
			resultMsg = "OK";
		} else {
			resultMsg = "STOPPED";
		}

		resultCode = Node.RESULT_OK;

	}

	/**
	 *  Description of the Method
	 *
	 * @return    Description of the Returned Value
	 * @since     September 27, 2002
	 */
	public void toXML(Element xmlElement) {
		super.toXML(xmlElement);
		
		if (this.url != null) {
			// query specified in a file
			xmlElement.setAttribute(XML_URL_ATTRIBUTE, this.url);
		} else {
			Document doc = xmlElement.getOwnerDocument();
			Element childElement = doc.createElement("attr");
			childElement.setAttribute("name", XML_SQLQUERY_ATTRIBUTE);
			// join given SQL commands
			Text textElement = doc.createTextNode(sqlQuery);
			childElement.appendChild(textElement);
			xmlElement.appendChild(childElement);
		}
		
		if (fetchSize != 0) {
			xmlElement.setAttribute(XML_FETCHSIZE_ATTRIBUTE, String.valueOf(fetchSize));
		}
		
		xmlElement.setAttribute(XML_DBCONNECTION_ATTRIBUTE, this.dbConnectionName);
		
	}

	/**
	 *  Description of the Method
	 *
	 * @param  nodeXML  Description of Parameter
	 * @return          Description of the Returned Value
	 * @since           September 27, 2002
	 */
    public static Node fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException {
            ComponentXMLAttributes xattribs = new ComponentXMLAttributes(xmlElement, graph);
            ComponentXMLAttributes xattribsChild;
            DBInputTable aDBInputTable = null;
            org.w3c.dom.Node childNode;

            try 
            {
                String query = null;
                if (xattribs.exists(XML_URL_ATTRIBUTE))
                {
                   query=xattribs.resolveReferences(FileUtils.getStringFromURL(xattribs.getString(XML_URL_ATTRIBUTE)));
                }else if (xattribs.exists(XML_SQLQUERY_ATTRIBUTE)){
                    query = xattribs.getString(XML_SQLQUERY_ATTRIBUTE);
                }else if (xattribs.exists(XML_SQLCODE_ELEMENT)){
                    query = xattribs.getString(XML_SQLCODE_ELEMENT);
                }else{
                    
                    childNode = xattribs.getChildNode(xmlElement, XML_SQLCODE_ELEMENT);
                    if (childNode == null) {
                        throw new XMLConfigurationException(COMPONENT_TYPE + ":" + xattribs.getString(XML_ID_ATTRIBUTE," unknown ID ") + ": Can't find <SQLCode> node !");
                    }
                    xattribsChild = new ComponentXMLAttributes(xmlElement, graph);
                    query=xattribsChild.getText(childNode);

        			
                }

                aDBInputTable = new DBInputTable(xattribs.getString(XML_ID_ATTRIBUTE),
                        xattribs.getString(XML_DBCONNECTION_ATTRIBUTE),
                        query);
                
                if (xattribs.exists(XML_DATAPOLICY_ATTRIBUTE)) 
                {
                    aDBInputTable.setExceptionHandler(ParserExceptionHandlerFactory.getHandler(
                                                xattribs.getString(XML_DATAPOLICY_ATTRIBUTE)));
                }
                
                if (xattribs.exists(XML_FETCHSIZE_ATTRIBUTE)){
                        aDBInputTable.setFetchSize(xattribs.getInteger(XML_FETCHSIZE_ATTRIBUTE));
                }
                
                if (xattribs.exists(XML_URL_ATTRIBUTE)) {
                	aDBInputTable.setURL(XML_URL_ATTRIBUTE);
                }
                
            }catch (Exception ex) {
                throw new XMLConfigurationException(COMPONENT_TYPE + ":" + xattribs.getString(XML_ID_ATTRIBUTE," unknown ID ") + ":" + ex.getMessage(),ex);
            }

            return aDBInputTable;
	}


	/**
	 * @param xml_url_attribute2
	 */
	private void setURL(String url){
		this.url = url;
	}


	/**
	 * @param  handler
	 */
	private void setExceptionHandler(IParserExceptionHandler handler) {
		parser.setExceptionHandler(handler);
	}


	/**  Description of the Method */
	public boolean checkConfig() {
		return true;
	}
	
	public String getType(){
		return COMPONENT_TYPE;
	}

	public void setFetchSize(int fetchSize){
	    this.fetchSize=fetchSize;
	}
	
 }
