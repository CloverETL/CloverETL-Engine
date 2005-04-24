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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Properties;

import org.jetel.data.DataRecord;
import org.jetel.data.parser.SQLDataParser;
import org.jetel.database.DBConnection;
import org.jetel.exception.BadDataFormatExceptionHandler;
import org.jetel.exception.BadDataFormatExceptionHandlerFactory;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.Node;
import org.jetel.util.ComponentXMLAttributes;
import org.jetel.util.PropertyRefResolver;

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
 *  <tr><td><b>url</b><br><i>optional</i></td><td>url location of the query<br>the query will be loaded from file referenced by utl</td>
 *  <tr><td><b>dbConnection</b></td><td>id of the Database Connection object to be used to access the database</td>
 *  <tr><td>&lt;SQLCode&gt;<br><i>optional<small>!!XML tag!!</small></i></td><td>This tag allows for embedding large SQL statement directly into graph.. See example below.</td></tr>
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
	private SQLDataParser parser;

	private DBConnection dbConnection;
	private String dbConnectionName;
	private String sqlQuery;

	/**  Description of the Field */
	public final static String COMPONENT_TYPE = "DB_INPUT_TABLE";
	private final static int WRITE_TO_PORT = 0;


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

		// try to open file & initialize data parser
		parser.open(this.graph.getDBConnection(dbConnectionName), getOutputPort(WRITE_TO_PORT).getMetadata());
	}


	/**
	 *  Description of the Method
	 *
	 * @return    Description of the Returned Value
	 * @since     September 27, 2002
	 */
	public org.w3c.dom.Node toXML() {
		// TODO implement toXML()
		return null;
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
	 * @param  nodeXML  Description of Parameter
	 * @return          Description of the Returned Value
	 * @since           September 27, 2002
	 */
	public static Node fromXML(org.w3c.dom.Node nodeXML) 
        {
            ComponentXMLAttributes xattribs = new ComponentXMLAttributes(nodeXML);
            ComponentXMLAttributes xattribsChild;
            DBInputTable aDBInputTable = null;
            org.w3c.dom.Node childNode;

            try 
            {
                String query = null;
                if (xattribs.exists("url"))
                {
                    query = fromFile(xattribs.getString("url"),xattribs.attributes2Properties(null));
                }
                else if (xattribs.exists("sqlQuery"))
                {
                    query = xattribs.getString("sqlQuery");
                }else{
                    
                    childNode = xattribs.getChildNode(nodeXML, "SQLCode");
                    if (childNode == null) {
                        throw new RuntimeException("Can't find <SQLCode> node !");
                    }
                    xattribsChild = new ComponentXMLAttributes(childNode);
                    query=xattribsChild.getText(childNode);

        			
                }

                aDBInputTable = new DBInputTable(xattribs.getString("id"),
                        xattribs.getString("dbConnection"),
                        query);

                if (xattribs.exists("DataPolicy")) 
                {
                    aDBInputTable.addBDFHandler(BadDataFormatExceptionHandlerFactory.getHandler(
                                                xattribs.getString("DataPolicy")));
                }
            } 
            catch (Exception ex) 
            {
                System.err.println(ex.getMessage());
                ex.printStackTrace();
                return null;
            }

            return aDBInputTable;
	}


	/**
	 * @param  handler
	 */
	private void addBDFHandler(BadDataFormatExceptionHandler handler) {
		parser.addBDFHandler(handler);
	}


	/**  Description of the Method */
	public boolean checkConfig() {
		return true;
	}
	
	public String getType(){
		return COMPONENT_TYPE;
	}

    /**
     * fromFile will allow the etl process to use a file to store the SQL statement
     * @param fileURL the string value that represents the location of the file
     * @return String the SQL Statement pulled from the file
     * @throws IOException when there are problems working with the file.
     */
    public static String fromFile(String fileURL,Properties properties) throws IOException 
    {
        String query = null;
        URL url;
		try{
			url = new URL(fileURL); 
		}catch(MalformedURLException e){
			// try to patch the url
			try {
				url=new URL("file:"+fileURL);
			}catch(MalformedURLException ex){
				throw new RuntimeException("Wrong URL of file specified: "+ex.getMessage());
			}
		}

		StringBuffer sb = new StringBuffer(512);
        
		try
        {
            char[] charBuf=new char[64];
            BufferedReader in=new BufferedReader(new InputStreamReader(url.openStream()));
            int readNum;
            
            while ((readNum=in.read(charBuf,0,charBuf.length)) != -1)
            {
                sb.append(charBuf,0,readNum);
            }
            
            PropertyRefResolver refResolver=new PropertyRefResolver(properties);
                        
            refResolver.resolveRef(sb);
        }
        catch(IOException ex)
        {
            ex.printStackTrace();
            throw new RuntimeException("Can't get SQL command from file " + fileURL + " - " + ex.getClass().getName() + " : " + ex.getMessage());
        }
        return sb.toString();
    }
}
