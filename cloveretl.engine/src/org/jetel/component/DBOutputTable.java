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
import java.util.List;
import java.util.logging.*;
import org.jetel.graph.*;
import org.jetel.database.*;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.util.ComponentXMLAttributes;

/**
 *  <h3>DatabaseOutputTable Component</h3>
 * <!-- his component performs append (so far) operation on specified database table.
 *  The metadata describing data comming in through input port[0] must be in the same
 *  structure as the target table. -->
 *
 * <table border="1">
 * <th>Component:</th>
 * <tr><td><h4><i>Name:</i></h4></td>
 * <td>DBOutputTable</td></tr>
 * <tr><td><h4><i>Category:</i></h4></td>
 * <td></td></tr>
 * <tr><td><h4><i>Description:</i></h4></td>
 * <td>This component performs append (so far) operation on specified database table.<br>
 *  The metadata describing data comming in through input port[0] must be in the same
 *  structure as the target table.</td></tr>
 * <tr><td><h4><i>Inputs:</i></h4></td>
 * <td>[0]- input records</td></tr>
 * <tr><td><h4><i>Outputs:</i></h4></td>
 * <td></td></tr>
 * <tr><td><h4><i>Comment:</i></h4></td>
 * <td></td></tr>
 * </table>
 *  <br>
 *  <table border="1">
 *  <th>XML attributes:</th>
 *  <tr><td><b>type</b></td><td>"DB_OUTPUT_TABLE"</td></tr>
 *  <tr><td><b>id</b></td><td>component identification</td>
 *  <tr><td><b>dbTable</b></td><td>name of the DB table to populate data with</td>
 *  <tr><td><b>dbConnection</b></td><td>id of the Database Connection object to be used to access the database</td>
 *  <!-- <tr><td><b>skipList<br><i>optional</i></b></td><td>delimited list of target table's field indices to be skipped (not populated)</td> -->
 *  <tr><td><b>dbFields<br><i>optional</i></b></td><td>delimited list of target table's fields to be populated<br>
 *  Input fields are mappend onto target fields (listed) in the order they are present in Clover's record.</td>
 *  <tr><td><b>commit</b><i>optional</i></td><td>determines how many records are in one db commit. Minimum 1, default is 100</td>
 *  </tr>
 *  </table>
 *
 *  <h4>Example:</h4>
 *  <pre>&lt;Node id="OUTPUT" type="DB_OUTPUT_TABLE" dbConnection="NorthwindDB" dbTable="employee_z"/&gt;</pre>
 *  <br>
 *  <pre>&lt;Node id="OUTPUT" type="DB_OUTPUT_TABLE" dbConnection="NorthwindDB" dbTable="employee_z" dbFields="f_name;l_name;phone"/&gt;</pre>
 *  <br><i>This example shows how to populate only selected fields within target DB table. It can be used for skipping target fields which
 *  are automatically populated by DB (such as autoincremented fields)</i>
 *
 * @author      dpavlis
 * @since       September 27, 2002
 * @revision    $Revision$
 * @created     22. èervenec 2003
 * @see         org.jetel.database.AnalyzeDB
 */
public class DBOutputTable extends Node {

	private DBConnection dbConnection;
	private String dbConnectionName;
	private String dbTableName;
	private PreparedStatement preparedStatement;
	private int[] skipList;
	private String[] dbFields;
	private int recordsInCommit;

	/**  Description of the Field */
	public final static String COMPONENT_TYPE = "DB_OUTPUT_TABLE";
	private final static int SQL_FETCH_SIZE_ROWS = 100;
	private final static int READ_FROM_PORT = 0;
	private final static int RECORDS_IN_COMMIT = 100;

	static Logger logger = Logger.getLogger("org.jetel");

	/**
	 *  Constructor for the DBInputTable object
	 *
	 * @param  id                Description of Parameter
	 * @param  dbConnectionName  Description of Parameter
	 * @param  dbTableName       Description of the Parameter
	 * @since                    September 27, 2002
	 */
	public DBOutputTable(String id, String dbConnectionName, String dbTableName) {
		super(id);
		this.dbConnectionName = dbConnectionName;
		this.dbTableName = dbTableName;
		skipList = null;
		dbFields = null;
		recordsInCommit = RECORDS_IN_COMMIT;
		// default

	}


	/**
	 *  Sets the dBFields attribute of the DBOutputTable object
	 *
	 * @param  dbFields  The new dBFields value
	 */
	public void setDBFields(String[] dbFields) {
		this.dbFields = dbFields;
	}


	/**
	 *  Sets the skipList attribute of the DBOutputTable object
	 *
	 * @param  skipList  The new skipList value
	 */
	public void setSkipList(int[] skipList) {
		this.skipList = skipList;
	}


	/**
	 *  Description of the Method
	 *
	 * @exception  ComponentNotReadyException  Description of Exception
	 * @since                                  September 27, 2002
	 */
	public void init() throws ComponentNotReadyException {
		if (inPorts.size() < 1) {
			throw new ComponentNotReadyException("At least one input port has to be defined!");
		}
		// get dbConnection from graph
		dbConnection = TransformationGraph.getReference().getDBConnection(dbConnectionName);
		if (dbConnection == null) {
			throw new ComponentNotReadyException("Can't find DBConnection ID: " + dbConnectionName);
		}
	}


	/**
	 *  Sets the recordsInCommit attribute of the DBOutputTable object
	 *
	 * @param  nRecs  The new recordsInCommit value
	 */
	public void setRecordsInCommit(int nRecs) {
		if (nRecs > 0) {
			recordsInCommit = nRecs;
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
		InputPort inPort = getInputPort(READ_FROM_PORT);
		DataRecord inRecord = new DataRecord(inPort.getMetadata());
		CopySQLData[] transMap;
		int i;
		int result;
		int recCount = 0;
		List dbFieldTypes;
		String sql;

		inRecord.init();
		try {
			// do we have specified list of fields to populate ?
			if (dbFields != null) {
				sql = SQLUtil.assembleInsertSQLStatement(dbTableName, dbFields);
				dbFieldTypes = SQLUtil.getFieldTypes(dbConnection.getConnection().getMetaData(), dbTableName, dbFields);
			} else {
				// populate all fields
				sql = SQLUtil.assembleInsertSQLStatement(inPort.getMetadata(), dbTableName);
				dbFieldTypes = SQLUtil.getFieldTypes(dbConnection.getConnection().getMetaData(), dbTableName);
			}
			preparedStatement = dbConnection.prepareStatement(sql);

			// this does not work for some drivers
			try {
				dbConnection.getConnection().setAutoCommit(false);
			} catch (SQLException ex) {
				logger.warning("Can't disable AutoCommit mode for DB: "+dbConnection+" > possible slower execution...");
			}

			/*
			 *  this somehow doesn't work (crashes system) at least when tested with Interbase
			 *  ParameterMetaData metaData=preparedStatement.getParameterMetaData();
			 *  if (metaData==null){
			 *  System.err.println("metada data is null!");
			 *  }
			 */
			// do we have skip list defined ? (which fields skip - no to populate)
			if (skipList != null) {
				transMap = CopySQLData.jetel2sqlTransMap(dbFieldTypes, inRecord, skipList);
			} else {
				transMap = CopySQLData.jetel2sqlTransMap(dbFieldTypes, inRecord);
			}

			while (inRecord != null && runIt) {
				inRecord = readRecord(READ_FROM_PORT, inRecord);
				if (inRecord != null) {
					for (i = 0; i < transMap.length; i++) {
						transMap[i].jetel2sql(preparedStatement);
					}
					result = preparedStatement.executeUpdate();
					if (result != 1) {
						throw new SQLException("Error when inserting record");
					}
					preparedStatement.clearParameters();
				}
				if (recCount++ % recordsInCommit == 0) {
					dbConnection.getConnection().commit();
				}
			}
			dbConnection.getConnection().commit();
		} catch (IOException ex) {
			resultMsg = ex.getMessage();
			resultCode = Node.RESULT_ERROR;
			closeAllOutputPorts();
			return;
		} catch (SQLException ex) {
			ex.printStackTrace();
			resultMsg = ex.getMessage();
			resultCode = Node.RESULT_ERROR;
			closeAllOutputPorts();
			return;
		} catch (Exception ex) {
			ex.printStackTrace();
			resultMsg = ex.getMessage();
			resultCode = Node.RESULT_FATAL_ERROR;
			//closeAllOutputPorts();
			return;
		} finally {
			try {
				broadcastEOF();
				if (preparedStatement != null) {
					preparedStatement.close();
				}
				if (resultMsg == null) {
					if (runIt) {
						resultMsg = "OK";
					} else {
						resultMsg = "STOPPED";
					}
					resultCode = Node.RESULT_OK;
				}
			} catch (SQLException ex) {
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
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(nodeXML);
		DBOutputTable outputTable;

		try {
			outputTable = new DBOutputTable(xattribs.getString("id"),
					xattribs.getString("dbConnection"),
					xattribs.getString("dbTable"));

			if (xattribs.exists("dbFields")) {
				outputTable.setDBFields(xattribs.getString("dbFields").split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX));
			}
			// if specified, use skip list which indicates which fields to skip
			if (xattribs.exists("skipFields")) {
				String[] strList = xattribs.getString("skipFields").split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);
				int[] skipList = new int[strList.length];
				for (int i = 0; i < skipList.length; i++) {
					skipList[i] = Integer.parseInt(strList[i]);
				}
				outputTable.setSkipList(skipList);
			}

			if (xattribs.exists("commit")) {
				outputTable.setRecordsInCommit(xattribs.getInteger("commit"));
			}

			return outputTable;
		} catch (Exception ex) {
			System.err.println(ex.getMessage());
			return null;
		}
	}


	/**  Description of the Method */
	public boolean checkConfig() {
		return true;
	}

}

