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
import java.util.logging.*;
import org.jetel.graph.*;
import org.jetel.database.*;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.util.ComponentXMLAttributes;

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
 * <td>This component executes specified command (SQL/DML) against specified DB</td></tr>
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
 *  <tr><td><b>dbSQL</b></td><td>SQL/DML/DDL statement which has to be executed on database</td>
 *  </tr>
 *  </table>
 *
 *  <h4>Example:</h4>
 *  <pre>&lt;Node id="DATABASE_RUN" type="DB_EXECUTE" dbConnection="NorthwindDB" dbSQL="drop table employee_z"/&gt;</pre>
 *  <br>
 *
 * @author      dpavlis
 * @since       Jan 17 2004
 * @revision    $Revision$
 * @created     22. èervenec 2003
 * @see         org.jetel.database.AnalyzeDB
 */
public class DBExecute extends Node {

	private DBConnection dbConnection;
	private String dbConnectionName;
	private String dbSQL;
	
	/**  Description of the Field */
	public final static String COMPONENT_TYPE = "DB_EXECUTE";
	
	static Logger logger = Logger.getLogger("org.jetel");

	/**
	 *  Constructor for the DBExecute object
	 *
	 * @param  id                Description of Parameter
	 * @param  dbConnectionName  Description of Parameter
	 * @param  dbTableName       Description of the Parameter
	 * @since                    September 27, 2002
	 */
	public DBExecute(String id, String dbConnectionName, String dbSQL) {
		super(id);
		this.dbConnectionName = dbConnectionName;
		this.dbSQL = dbSQL;
		// default

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
		dbConnection = TransformationGraph.getReference().getDBConnection(dbConnectionName);
		if (dbConnection == null) {
			throw new ComponentNotReadyException("Can't find DBConnection ID: " + dbConnectionName);
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
		Statement sqlStatement;
		
		try {
			// let's create statement
			sqlStatement = dbConnection.getStatement();
			sqlStatement.executeUpdate(dbSQL);
			dbConnection.getConnection().commit();
			sqlStatement.close();
			
		} catch (SQLException ex) {
			logger.severe(ex.getMessage());
			resultMsg = ex.getMessage();
			resultCode = Node.RESULT_ERROR;
			return;
		} catch (Exception ex) {
			ex.printStackTrace();
			resultMsg = ex.getMessage();
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


	/**
	 *  Description of the Method
	 *
	 * @param  nodeXML  Description of Parameter
	 * @return          Description of the Returned Value
	 * @since           September 27, 2002
	 */
	public static Node fromXML(org.w3c.dom.Node nodeXML) {
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(nodeXML);
		DBExecute outputTable;

		try {
			outputTable = new DBExecute(xattribs.getString("id"),
					xattribs.getString("dbConnection"),
					xattribs.getString("dbSQL"));

			return outputTable;
		} catch (Exception ex) {
			System.err.println(COMPONENT_TYPE+" : "+ex.getMessage());
			return null;
		}
	}


	/**  Description of the Method */
	public boolean checkConfig() {
		return true;
	}

}

