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
import org.w3c.dom.NamedNodeMap;
import org.jetel.graph.*;
import org.jetel.data.DataRecord;
import org.jetel.data.SQLDataParser;
import org.jetel.database.*;
import org.jetel.exception.BadDataFormatExceptionHandler;
import org.jetel.exception.BadDataFormatExceptionHandlerFactory;
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
	private SQLDataParser parser;

	private DBConnection dbConnection;
	private String dbConnectionName;
	private String sqlQuery;

	public final static String COMPONENT_TYPE = "DB_INTPUT_TABLE";
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

		parser=new SQLDataParser(dbConnectionName, sqlQuery);
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


		// try to open file & initialize data parser
		parser.open(TransformationGraph.getReference().getDBConnection(dbConnectionName), getOutputPort(OUTPUT_PORT).getMetadata());
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

		DataRecord record=new DataRecord(getOutputPort(OUTPUT_PORT).getMetadata());

		record.init();

		

		try{

			// till it reaches end of data or it is stopped from outside

			while(((record=parser.getNext(record))!=null) && runIt){

				//broadcast the record to all connected Edges

				writeRecordBroadcast(record);

			}

		}

		catch(IOException ex){

			resultMsg=ex.getMessage();

			resultCode=Node.RESULT_ERROR;

			closeAllOutputPorts();

			return;

		}catch(Exception ex){

			 resultMsg=ex.getMessage();

			 resultCode=Node.RESULT_FATAL_ERROR;

			 return;

		}

		// we are done, close all connected output ports to indicate end of stream

		parser.close();

		broadcastEOF();

		if (runIt) resultMsg="OK"; else resultMsg="STOPPED";

		resultCode=Node.RESULT_OK;

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
		DBInputTable aDBInputTable = null;

		if (attribs != null) {
			String id = attribs.getNamedItem("id").getNodeValue();
			String sqlQuery = attribs.getNamedItem("sqlQuery").getNodeValue();
			String dbConnectionName = attribs.getNamedItem("dbConnection").getNodeValue();
			String aDataPolicy = attribs.getNamedItem("DataPolicy").getNodeValue();
			if (id != null && sqlQuery != null && dbConnectionName != null) {
				aDBInputTable = new DBInputTable(id, dbConnectionName, sqlQuery);

				if(aDataPolicy != null) {
					aDBInputTable.addBDFHandler(BadDataFormatExceptionHandlerFactory.getHandler(aDataPolicy));
				}
			}
		}
		return aDBInputTable;
	}


	/**
	 * @param handler
	 */
	private void addBDFHandler(BadDataFormatExceptionHandler handler) {
		parser.addBDFHandler(handler);
	}


}

