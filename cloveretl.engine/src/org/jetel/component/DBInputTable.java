/*
 *  jETeL/Clover - Java based ETL application framework.
 *  Copyright (C) 2002  David Pavlis
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 2 of the License, or
 *  (at your option) any later version.
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
package org.jetel.component;

import java.io.*;
import java.sql.*;
import org.w3c.dom.NamedNodeMap;
import org.jetel.graph.*;
import org.jetel.data.DataRecord;
import org.jetel.database.*;
import org.jetel.exception.ComponentNotReadyException;

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
 *  <tr><td><b>type</b></td><td>"DB_INTPUT_TABLE"</td></tr>
 *  <tr><td><b>id</b></td><td>component identification</td>
 *  <tr><td><b>sqlQuery</b></td><td>query to be sent to database</td>
 *  <tr><td><b>dbConnection</b></td><td>id of the Database Connection object to be used to access the database</td>
 *  </tr>
 *  </table>  
 *
 *  <h4>Example:</h4> 
 *  <pre>&lt;Node id="INPUT" type="DB_INTPUT_TABLE" dbConnection="NorthwindDB" sqlQuery="select * from employee_z"/&gt;</pre>
 * 
 * @author     dpavlis
 * @since    September 27, 2002
 * @see		org.jetel.database.AnalyzeDB
 * @revision   $Revision$
 */
public class DBInputTable extends Node {

	private DBConnection dbConnection;
	private String dbConnectionName;
	private String sqlQuery;
	private Statement statement;

	public final static String COMPONENT_TYPE = "DB_INTPUT_TABLE";
	private final static int SQL_FETCH_SIZE_ROWS = 100;
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

	}


	/**
	 *  Gets the Type attribute of the DBInputTable object
	 *
	 * @return    The Type value
	 * @since     September 27, 2002
	 */
	public String getType() {
		return COMPONENT_TYPE;
	}


	/**
	 *  Description of the Method
	 *
	 * @exception  ComponentNotReadyException  Description of Exception
	 * @since                                  September 27, 2002
	 */
	public void init() throws ComponentNotReadyException {
		if (outPorts.size()<1){
			throw new ComponentNotReadyException("At least one output port has to be defined!");
		}
		// get dbConnection from graph
		dbConnection=TransformationGraph.getReference().getDBConnection(dbConnectionName);
		if (dbConnection==null){
			throw new ComponentNotReadyException("Can't find DBConnection ID: "+dbConnectionName);
		}
		try {
			statement = dbConnection.getStatement();
			// following calls are not always supported (as it seems)
			//statement.setFetchDirection(ResultSet.FETCH_FORWARD); 
			//statement.setFetchSize(SQL_FETCH_SIZE_ROWS);
		}
		catch (SQLException ex) {
			throw new ComponentNotReadyException(ex.getMessage());
		}
	}


	/**
	 *  Description of the Method
	 *
	 * @return    Description of the Returned Value
	 * @since     September 27, 2002
	 */
	public org.w3c.dom.Node toXML() {
		// TODO
		return null;
	}


	/**
	 *  Main processing method for the DBInputTable object
	 *
	 * @since    September 27, 2002
	 */
	public void run() {
		ResultSet resultSet = null;
		OutputPort outPort = getOutputPort(WRITE_TO_PORT);
		DataRecord outRecord = new DataRecord(outPort.getMetadata());
		CopySQLData[] transMap;
		int i;

		outRecord.init();
		transMap = CopySQLData.sql2JetelTransMap(outPort.getMetadata(), outRecord);
		// run sql query
		try {
			resultSet = statement.executeQuery(sqlQuery);

			while (resultSet.next() && runIt) {
				for (i = 0; i < transMap.length; i++) {
					transMap[i].sql2jetel(resultSet);
				}
				// send the record through output port
				writeRecord(WRITE_TO_PORT, outRecord);
			}
		}
		catch (IOException ex) {
			resultMsg = ex.getMessage();
			resultCode = Node.RESULT_ERROR;
			closeAllOutputPorts();
			return;
		}
		catch (SQLException ex) {
			resultMsg = ex.getMessage();
			resultCode = Node.RESULT_ERROR;
			closeAllOutputPorts();
			return;
		}
		catch (Exception ex) {
			resultMsg = ex.getMessage();
			resultCode = Node.RESULT_FATAL_ERROR;
			//closeAllOutputPorts();
			return;
		}
		finally {
			try {
				if (resultSet != null) {
					resultSet.close();
				}
				statement.close();
				broadcastEOF();
				if (resultMsg==null){
					if (runIt) {
						resultMsg = "OK";
					} else {
						resultMsg = "STOPPED";
					}
					resultCode = Node.RESULT_OK;
				}
			}
			catch (SQLException ex) {
				resultMsg = ex.getMessage();
				resultCode = Node.RESULT_ERROR;
			}
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
		NamedNodeMap attribs = nodeXML.getAttributes();

		if (attribs != null) {
			String id = attribs.getNamedItem("id").getNodeValue();
			String sqlQuery = attribs.getNamedItem("sqlQuery").getNodeValue();
			String dbConnectionName = attribs.getNamedItem("dbConnection").getNodeValue();
			if (id != null && sqlQuery != null && dbConnectionName != null) {
				return new DBInputTable(id, dbConnectionName, sqlQuery);
			}
		}
		return null;
	}

}

