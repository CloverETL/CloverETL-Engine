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

import java.io.*;
import java.sql.*;
import java.util.List;
import java.util.logging.*;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.database.*;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.*;
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
 * <td>[0]- records rejected by database<br><i>optional</i></td></tr>
 * <tr><td><h4><i>Comment:</i></h4></td>
 * <td></td></tr>
 * </table>
 *  <br>
 *  <table border="1">
 *  <th>XML attributes:</th>
 *  <tr><td><b>type</b></td><td>"DB_OUTPUT_TABLE"</td></tr>
 *  <tr><td><b>id</b></td><td>component identification</td></tr>
 *  <tr><td><b>dbTable</b></td><td>name of the DB table to populate data with</td>
 *  <tr><td><b>dbConnection</b></td><td>id of the Database Connection object to be used to access the database</td>
 *  <tr><td><b>dbFields</b><br><i>optional</i></td><td>delimited list of target table's fields to be populated<br>
 *  Input fields are mappend onto target fields (listed) in the order they are present in Clover's record.</td>
 *  <tr><td><b>commit</b><br><i>optional</i></td><td>determines how many records are in one db commit. Minimum 1, DEFAULT is 100</td>
 *  <tr><td><b>cloverFields</b><br><i>optional</i></td><td>delimited list of input record's fields.<br>Only listed fields (in the order
 *  they appear in the list) will be considered for mapping onto target table's fields. Combined with <b>dbFields</b> option you can
 *  specify mapping from source (Clover's) fields to DB table fields. If no <i>dbFields</i> are specified, then #of <i>cloverFields</i> must
 *  correspond to number of target DB table fields.</td>
 *  <tr><td><b>batchMode</b><br><i>optional</i></td><td>[Yes/No] determines whether to use batch mode for sending statemetns to DB, DEFAULT is No.<br>
 *  <i>Note:If your database/JDBC driver supports this feature, switch it on as it significantly speeds up table population.</i></td>
 *  </tr>
 *  <tr><td><b>sqlQuery</b><br><i>optional</i></td><td>allows specification of SQL query/DML statement to be executed against
 *  database. Questionmarks [?] in the query text are placeholders which are filled with values from input fields specified in <b>cloverFields</b>
 *  attribute. If you use this option/parameter, cloverFields must be specified as well - it determines which input fields will
 *  be used/mapped onto target fields</td></tr>
 *  <tr><td><b>maxErrors</b><br><i>optional</i></td><td>maximum number of allowed SQL errors. Default: 0 (zero). If exceeded, component stops with error.</td></tr>
 * <tr><td>&lt;SQLCode&gt;<br><i>optional<small>!!XML tag!!</small></i></td><td>This tag allows for embedding large SQL statement directly into graph.. See example below.</td></tr>
 *  </table>
 *
 *  <h4>Example:</h4>
 *  <pre>&lt;Node id="OUTPUT" type="DB_OUTPUT_TABLE" dbConnection="NorthwindDB" dbTable="employee_z"/&gt;</pre>
 *  <br>
 *  <pre>&lt;Node id="OUTPUT" type="DB_OUTPUT_TABLE" dbConnection="NorthwindDB" dbTable="employee_z" dbFields="f_name;l_name;phone"/&gt;</pre>
 *  <i>Example above shows how to populate only selected fields within target DB table. It can be used for skipping target fields which
 *  are automatically populated by DB (such as autoincremented fields).</i>
 *  <br>
 *  <pre>&lt;Node id="OUTPUT" type="DB_OUTPUT_TABLE" dbConnection="NorthwindDB" dbTable="employee_z"
 *	   dbFields="f_name;l_name" cloverFields="LastName;FirstName"/&gt;</pre>
 *  <i>Example shows how to simply map Clover's LastName and FirstName fields onto f_name and l_name DB table fields. The order
 *  in which these fields appear in Clover data record is not important.</i>
 *  <br>
 *   <pre>&lt;Node id="OUTPUT" type="DB_OUTPUT_TABLE" dbConnection="NorthwindDB" sqlQuery="insert into myemployee2 (FIRST_NAME,LAST_NAME,DATE,ID) values (?,?,sysdate,123)"
 *	   cloverFields="FirstName;LastName"/&gt;</pre>
 *  <br>
 * <pre>&lt;Node id="OUTPUT" type="DB_OUTPUT_TABLE" dbConnection="NorthwindDB" cloverFields="FirstName;LastName"&gt;
 *  &lt;SQLCode&gt;
 *	insert into myemployee2 (FIRST_NAME,LAST_NAME,DATE,ID) values (?,?,sysdate,123)
 *  &lt;/SQLCode&gt;
 *  &lt;/Node&gt;</pre>
 * <br>
 * @author      dpavlis
 * @since       September 27, 2002
 * @revision    $Revision$
 * @created     22. �ervenec 2003
 * @see         org.jetel.database.AnalyzeDB
 */
public class DBOutputTable extends Node {

	private DBConnection dbConnection;
	private String dbConnectionName;
	private String dbTableName;
	private PreparedStatement preparedStatement;
	private String[] cloverFields;
	private String[] dbFields;
	private String sqlQuery;
	private int recordsInCommit;
	private int maxErrors;
	private boolean useBatch;

	private int countError;
	/**  Description of the Field */
	public final static String COMPONENT_TYPE = "DB_OUTPUT_TABLE";
	private final static int SQL_FETCH_SIZE_ROWS = 100;
	private final static int READ_FROM_PORT = 0;
	private final static int WRITE_REJECTED_TO_PORT = 0;
	private final static int RECORDS_IN_COMMIT = 100;
	private final static int RECORDS_IN_BATCH = 25;
	private final static int MAX_ALLOWED_ERRORS = 0;

	static Logger logger = Logger.getLogger("org.jetel");


	/**
	 *  Constructor for the DBInputTable object
	 *
	 * @param  id                Unique ID of component
	 * @param  dbConnectionName  Name of Clover's database connection to be used for communicationg with DB
	 * @param  dbTableName       Name of target DB table to be populated with data
	 * @since                    September 27, 2002
	 */
	public DBOutputTable(String id, String dbConnectionName, String dbTableName) {
		this(id,dbConnectionName);
		this.dbTableName = dbTableName;
	}

	/**
	 * Constructor for the DBInputTable object
	 * @param id				Unique ID of component
	 * @param dbConnectionName	Name of Clover's database connection to be used for communicationg with DB
	 * @param sqlQuery			SQL query to be executed against DB - can be any DML command (INSERT, UPDATE, DELETE)
	 * @param cloverFields		Array of Clover field names (the input data) which should substitute DML command parameters (i.e. "?")
	 */
	public DBOutputTable(String id, String dbConnectionName, String sqlQuery, String[] cloverFields) {
		this(id,dbConnectionName);
		this.cloverFields = cloverFields;

	}
	
	/**
	 * Constructor for the DBInputTable object
	 */
	DBOutputTable(String id, String dbConnectionName){
		super(id);
		this.dbConnectionName = dbConnectionName;
		this.dbTableName = null;
		cloverFields = null;
		dbFields = null;
		recordsInCommit = RECORDS_IN_COMMIT;
		maxErrors=MAX_ALLOWED_ERRORS;
		useBatch=false;
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
	 *  Sets the useBatch attribute of the DBOutputTable object
	 *
	 * @param  batchMode  The new useBatch value
	 */
	public void setUseBatch(boolean batchMode) {
		this.useBatch = batchMode;
	}


	/**
	 *  Sets the cloverFields attribute of the DBOutputTable object
	 *
	 * @param  cloverFields  The new cloverFields value
	 */
	public void setCloverFields(String[] cloverFields) {
		this.cloverFields = cloverFields;
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
		dbConnection = this.graph.getDBConnection(dbConnectionName);
		if (dbConnection == null) {
			throw new ComponentNotReadyException("Can't find DBConnection ID: " + dbConnectionName);
		}
	}


	/**
	 * @param dbTableName The dbTableName to set.
	 */
	public void setDBTableName(String dbTableName) {
		this.dbTableName = dbTableName;
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
		OutputPort rejectedPort=getOutputPort(WRITE_REJECTED_TO_PORT);
		DataRecord inRecord = new DataRecord(inPort.getMetadata());
		CopySQLData[] transMap;
		int result = 0;
		List dbFieldTypes;
		String sql;
		String errMsg;
		
		inRecord.init();
		try {
			// first check that what we require is supported
			if (useBatch && !dbConnection.getConnection().getMetaData().supportsBatchUpdates()){
				logger.warning("DB indicates no support for batch updates -> switching it off !");
				useBatch=false;
			}
			// it is probably wise to have COMMIT size multiplication of BATCH size
			if (useBatch && (recordsInCommit % RECORDS_IN_BATCH != 0)){
				int multiply= recordsInCommit/RECORDS_IN_BATCH;
				recordsInCommit=(multiply+1) * RECORDS_IN_BATCH;
			}
			
			// if SQL/DML statement is given, then only prepare statement
			if (sqlQuery!=null){
				sql=sqlQuery;
				// if dbFields and dbTableName defined, then
				// get target DB fields metadata from it
				if ((dbFields!=null)&&(dbTableName!=null)){
					dbFieldTypes = SQLUtil.getFieldTypes(dbConnection.getConnection().getMetaData(), dbTableName, dbFields);
				}else{
					// we have to assume that Clover fields types correspond
					// to taget DB table fields types
					dbFieldTypes= SQLUtil.getFieldTypes(inPort.getMetadata(),cloverFields);
				}	
			}else{
				// do we have specified list of fields to populate ?
				if (dbFields != null) {
					sql = SQLUtil.assembleInsertSQLStatement(dbTableName, dbFields);
					dbFieldTypes = SQLUtil.getFieldTypes(dbConnection.getConnection().getMetaData(), dbTableName, dbFields);
				} else {
					// populate all fields
					sql = SQLUtil.assembleInsertSQLStatement(inPort.getMetadata(), dbTableName);
					dbFieldTypes = SQLUtil.getFieldTypes(dbConnection.getConnection().getMetaData(), dbTableName);
				}
			}
			preparedStatement = dbConnection.prepareStatement(sql);
		} catch (SQLException ex) {
			resultMsg = ex.getMessage();
			resultCode = Node.RESULT_ERROR;
			broadcastEOF();
			return;
		}
		// this does not work for some drivers
		try {
			dbConnection.getConnection().setAutoCommit(false);
		} catch (SQLException ex) {
			logger.warning("Can't disable AutoCommit mode for DB: " + dbConnection + " > possible slower execution...");
		}
		
		try{
			// do we have cloverFields list defined ? (which fields from input record to consider)
			if (cloverFields != null) {
				transMap = CopySQLData.jetel2sqlTransMap(dbFieldTypes, inRecord, cloverFields);
			} else {
				transMap = CopySQLData.jetel2sqlTransMap(dbFieldTypes, inRecord);
			}
		} catch (Exception ex) {
			resultMsg = ex.getClass().getName()+" : "+ ex.getMessage();
			resultCode = Node.RESULT_FATAL_ERROR;
			broadcastEOF();
			return;
		}
		/*
		 * Run main processing loop
		 */
		try{
			if (useBatch){
				runInBatchMode(inPort,rejectedPort,inRecord,transMap);
			}else{
				runInNormalMode(inPort,rejectedPort,inRecord,transMap);
			}
		} catch (IOException ex) {
			resultMsg = ex.getMessage();
			resultCode = Node.RESULT_ERROR;
			closeAllOutputPorts();
		} catch (SQLException ex) {
			ex.printStackTrace();
			resultMsg = ex.getMessage();
			resultCode = Node.RESULT_ERROR;
			closeAllOutputPorts();
		} catch (Exception ex) {
			ex.printStackTrace();
			resultMsg = ex.getClass().getName()+" : "+ ex.getMessage();
			resultCode = Node.RESULT_FATAL_ERROR;
			//closeAllOutputPorts();
		} finally {
			broadcastEOF();
			if (preparedStatement != null) {
				try{
					preparedStatement.close();
				} catch (SQLException ex) {
					resultMsg = ex.getMessage();
					resultCode = Node.RESULT_ERROR;
				}
			}
			if (resultMsg == null) {
				if (runIt) {
					resultMsg = "OK";
				} else {
					resultMsg = "STOPPED";
				}
				resultCode = Node.RESULT_OK;
			}
		}
		return;
	}
	

	private void runInNormalMode(InputPort inPort,OutputPort rejectedPort,
			DataRecord inRecord,CopySQLData[] transMap) throws SQLException,InterruptedException,IOException{
		int i;
		int recCount = 0;
		int countError=0;
		while (inRecord != null && runIt) {
			inRecord = readRecord(READ_FROM_PORT, inRecord);
			if (inRecord != null) {
				for (i = 0; i < transMap.length; i++) {
					transMap[i].jetel2sql(preparedStatement);
				}
				
				try {
					preparedStatement.executeUpdate();
				}catch(SQLException ex){
					countError++;
					if (rejectedPort!=null){
						rejectedPort.writeRecord(inRecord);
					}
					if (countError>maxErrors){
						throw new SQLException("Maximum # of errors exceeded when inserting record: "+ex.getMessage());
					}
				}
				preparedStatement.clearParameters();
				if (recCount++ % recordsInCommit == 0) {
					dbConnection.getConnection().commit();
				}
			}
			yield();
		}
		// end of records stream - final commits;
		dbConnection.getConnection().commit();
	}


	private void runInBatchMode(InputPort inPort,OutputPort rejectedPort,
			DataRecord inRecord,CopySQLData[] transMap) throws SQLException,InterruptedException,IOException{
		int i;
		int batchCount=0;
		int recCount = 0;
		int countError=0;
		while (inRecord != null && runIt) {
			inRecord = readRecord(READ_FROM_PORT, inRecord);
			if (inRecord != null) {
				for (i = 0; i < transMap.length; i++) {
					transMap[i].jetel2sql(preparedStatement);
				}
				try{
					preparedStatement.addBatch();
				}catch(SQLException ex){
					countError++;;
					if (rejectedPort!=null){
						rejectedPort.writeRecord(inRecord);
					}
					if (countError>maxErrors){
						throw new SQLException("Maximum # of errors exceeded when inserting record: "+ex.getMessage());
					}
				}
			}
			// shall we commit ?
			if (batchCount++ % RECORDS_IN_BATCH == 0) {
				try {
					preparedStatement.executeBatch();
					preparedStatement.clearBatch();
				} catch (BatchUpdateException ex) {
					throw new SQLException("Batch error:"+ex.getMessage());
				}
				batchCount = 0;
			}
			if (recCount++ % recordsInCommit == 0) {
				if (batchCount!=0){
					try {
						preparedStatement.executeBatch();
						preparedStatement.clearBatch();
					} catch (BatchUpdateException ex) {
						throw new SQLException("Batch error:"+ex.getMessage());
					}
					batchCount = 0;
				}
				dbConnection.getConnection().commit();
			}
			yield();
		}
		
		try{
			preparedStatement.executeBatch();
		}catch (BatchUpdateException ex) {
			throw new SQLException("Batch error:"+ex.getMessage());
		}
		dbConnection.getConnection().commit();
		
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
		ComponentXMLAttributes xattribsChild;
		org.w3c.dom.Node childNode;
		DBOutputTable outputTable;

		try {
			// allows specifying parameterized SQL (with ? - questionmarks)
			if (xattribs.exists("sqlQuery")) {
					outputTable = new DBOutputTable(xattribs.getString("id"),
					xattribs.getString("dbConnection"),
					xattribs.getString("sqlQuery"),	null);
				
			}else if(xattribs.exists("dbTable")){
				outputTable = new DBOutputTable(xattribs.getString("id"),
						xattribs.getString("dbConnection"),
						xattribs.getString("dbTable"));
				
			}else{
			    childNode = xattribs.getChildNode(nodeXML, "SQLCode");
                if (childNode == null) {
                    throw new RuntimeException("Can't find <SQLCode> node !");
                }
                xattribsChild = new ComponentXMLAttributes(childNode);
                outputTable = new DBOutputTable(xattribs.getString("id"),
    					xattribs.getString("dbConnection"),
    					xattribsChild.getText(childNode),
    					null);
			}
			
			if (xattribs.exists("dbFields")) {
				outputTable.setDBFields(xattribs.getString("dbFields").split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX));
			}
			
			if (xattribs.exists("dbTable")) {
				outputTable.setDBTableName(xattribs.getString("dbTable"));
			}
			
			if (xattribs.exists("cloverFields")) {
				outputTable.setCloverFields(xattribs.getString("cloverFields").split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX));
			}
			
			if (xattribs.exists("commit")) {
				outputTable.setRecordsInCommit(xattribs.getInteger("commit"));
			}
			
			if (xattribs.exists("batchMode")) {
				outputTable.setUseBatch(xattribs.getBoolean("batchMode"));
			}
			
			if (xattribs.exists("maxErrors")){
				outputTable.setMaxErrors(xattribs.getInteger("maxErrors"));
			}
			
			return outputTable;
			
		} catch (Exception ex) {
			System.err.println(ex.getMessage());
			return null;
		}
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
	
	/**
	 * @param maxErrors Maximum number of tolerated SQL errors during component run. Default: 0 (zero)
	 */
	public void setMaxErrors(int maxErrors) {
		this.maxErrors = maxErrors;
	}
}

