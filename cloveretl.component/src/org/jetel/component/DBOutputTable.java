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
import java.sql.BatchUpdateException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.connection.CopySQLData;
import org.jetel.connection.DBConnection;
import org.jetel.connection.SQLUtil;
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
import org.jetel.util.ComponentXMLAttributes;
import org.jetel.util.FileUtils;
import org.jetel.util.StringUtils;
import org.jetel.util.SynchronizeUtils;
import org.w3c.dom.Element;

/**
 *  <h3>DatabaseOutputTable Component</h3>
 * <!-- his component performs DML operation on specified database table (inser/update/delete).
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
 * <td>This component performs specified DML operation (insert/update/delete) on specified database table.<br>
 *  The metadata describing data comming in through input port[0] must be in the same
 *  structure as the target table. Parameter placeholder in DML statemet is [?] - questionmark</td></tr>
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
 *  <tr><td><b>dbTable</b><br><i>optional</i></td><td>name of the DB table to populate data with</td>
 *  <tr><td><b>dbConnection</b></td><td>id of the Database Connection object to be used to access the database</td>
 *  <tr><td><b>fieldMap</b><br><i>optional</i></td><td>Pairs of clover fields and db fields (cloverField=dbField) separated by :;| {colon, semicolon, pipe}.<br>
 *  It specifies mapping from source (Clover's) fields to DB table fields. It should be used instead of <i>cloverFields</i> and <i>dbFields</i>
 *  attributes, because it provides more clear mapping. If <i>fieldMap</i> attribute is found <i>cloverFields</i> and <i>dbFields</i> attributes are ignored.
 *  <tr><td><b>dbFields</b><br><i>optional</i></td><td>delimited list of target table's fields to be populated<br>
 *  Input fields are mappend onto target fields (listed) in the order they are present in Clover's record.</td>
 *  <tr><td><b>commit</b><br><i>optional</i></td><td>determines how many records are in one db commit. Minimum 1, DEFAULT is 100.<br>If
 * MAX_INT is specified, it is considered as NEVER COMMIT - i.e. records are send to DB without every issuing commit. It can
 * be called later from withing other component - for example DBExecute.</td>
 *  <tr><td><b>cloverFields</b><br><i>optional</i></td><td>delimited list of input record's fields.<br>Only listed fields (in the order
 *  they appear in the list) will be considered for mapping onto target table's fields. Combined with <b>dbFields</b> option you can
 *  specify mapping from source (Clover's) fields to DB table fields. If no <i>dbFields</i> are specified, then #of <i>cloverFields</i> must
 *  correspond to number of target DB table fields.</td>
 *  <tr><td><b>batchMode</b><br><i>optional</i></td><td>[Yes/No] determines whether to use batch mode for sending statemetns to DB, DEFAULT is No.<br>
 *  <i>Note:If your database/JDBC driver supports this feature, switch it on as it significantly speeds up table population.</i></td>
 *  </tr>
 * <tr><td><b>batchSize</b><br><i>optional</i></td><td>number - determines how many records will be sent to database in one batch update. Default is 25.
 * </td>
 *  </tr> 
 *  <tr><td><b>sqlQuery</b><br><i>optional</i></td><td>allows specification of SQL query/DML statement to be executed against
 *  database. Questionmarks [?] in the query text are placeholders which are filled with values from input fields specified in <b>cloverFields</b>
 *  attribute. If you use this option/parameter, cloverFields must be specified as well - it determines which input fields will
 *  be used/mapped onto target fields</td></tr>
 *  <tr><td><b>url</b><br><i>optional</i></td><td>url location of the query<br>the query will be loaded from file referenced by the url. The same as
 *  for <i>sqlQuery</i> holds for this parameter.</td>
 *   <tr><td><b>maxErrors</b><br><i>optional</i></td><td>maximum number of allowed SQL errors. Default: 0 (zero). If exceeded, component stops with error. If set to <b>-1</b>(minus one) all errors are ignored.</td></tr>
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
 *  <i>Example below shows how to delete records in table using DBOutputTable component</i>
 *  <pre>&lt;Node id="OUTPUT" type="DB_OUTPUT_TABLE" dbConnection="NorthwindDB" cloverFields="FirstName;LastName"&gt;
 *  &lt;SQLCode&gt;
 *  delete from myemployee2 where FIRST_NAME = ? and LAST_NAME = ?
 *  &lt;/SQLCode&gt;
 *  &lt;/Node&gt;</pre>
 * <br>
 *  <i>Example below shows usage of "fieldMap" attribute </i>
 * <pre>&lt;Node dbConnection="DBConnection0" dbTable="employee_tmp" fieldMap=
 * "EMP_NO=emp_no;FIRST_NAME=first_name;LAST_NAME=last_name;PHONE_EXT=phone_ext"
 * id="OUTPUT" type="DB_OUTPUT_TABLE"/&gt;</pre>
 * <br>
 * @author      dpavlis
 * @since       September 27, 2002
 * @revision    $Revision$
 * @created     22. ???ervenec 2003
 * @see         org.jetel.database.AnalyzeDB
 */
public class DBOutputTable extends Node {

	public static final String XML_MAXERRORS_ATRIBUTE = "maxErrors";
	public static final String XML_BATCHMODE_ATTRIBUTE = "batchMode";
	public static final String XML_COMMIT_ATTRIBUTE = "commit";
	public static final String XML_FIELDMAP_ATTRIBUTE = "fieldMap";
	public static final String XML_CLOVERFIELDS_ATTRIBUTE = "cloverFields";
	public static final String XML_DBFIELDS_ATTRIBUTE = "dbFields";
	public static final String XML_SQLCODE_ELEMENT = "SQLCode";
	public static final String XML_DBTABLE_ATTRIBUTE = "dbTable";
	public static final String XML_DBCONNECTION_ATTRIBUTE = "dbConnection";
	public static final String XML_SQLQUERY_ATRIBUTE = "sqlQuery";
	public static final String XML_BATCHSIZE_ATTRIBUTE = "batchSize";
	public static final String XML_URL_ATTRIBUTE = "url";
	
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
	private int batchSize;

	/**  Description of the Field */
	public final static String COMPONENT_TYPE = "DB_OUTPUT_TABLE";
	private final static int SQL_FETCH_SIZE_ROWS = 100;
	private final static int READ_FROM_PORT = 0;
	private final static int WRITE_REJECTED_TO_PORT = 0;
	private final static int RECORDS_IN_COMMIT = 100;
	private final static int RECORDS_IN_BATCH = 25;
	private final static int MAX_ALLOWED_ERRORS = 0;

	static Log logger = LogFactory.getLog(DBOutputTable.class);


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
		this.sqlQuery=sqlQuery;

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
		batchSize=RECORDS_IN_BATCH;
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
	 * Sets batch size - how many records are in batch which is sent
	 * to DB at once.
	 * @param batchSize
	 */
	public void setBatchSize(int batchSize){
	    this.batchSize=batchSize;
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
		super.init();
		// get dbConnection from graph
        IConnection conn = getGraph().getConnection(dbConnectionName);
        if(conn == null) {
            throw new ComponentNotReadyException("Can't find DBConnection ID: " + dbConnectionName);
        }
        if(!(conn instanceof DBConnection)) {
            throw new ComponentNotReadyException("Connection with ID: " + dbConnectionName + " isn't instance of the DBConnection class.");
        }
        dbConnection = (DBConnection) conn;
        dbConnection.init();
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

	@Override
	public Result execute() throws Exception {
		InputPort inPort = getInputPort(READ_FROM_PORT);
		OutputPort rejectedPort=getOutputPort(WRITE_REJECTED_TO_PORT);
		DataRecord inRecord = new DataRecord(inPort.getMetadata());
		CopySQLData[] transMap;
		List dbFieldTypes;
		String sql;
		
		inRecord.init();
		// first check that what we require is supported
		if (useBatch && !dbConnection.getConnection().getMetaData().supportsBatchUpdates()){
			logger.warn("DB indicates no support for batch updates -> switching it off !");
			useBatch=false;
		}
		// it is probably wise to have COMMIT size multiplication of BATCH size
		// except situation when commit size is MAX_INTEGER -> we never commit in this situation;
		if (useBatch && recordsInCommit!=Integer.MAX_VALUE && (recordsInCommit % batchSize != 0)){
			int multiply= recordsInCommit/batchSize;
			recordsInCommit=(multiply+1) * batchSize;
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
		// this does not work for some drivers
		try {
			dbConnection.getConnection().setAutoCommit(false);
		} catch (SQLException ex) {
			logger.warn("Can't disable AutoCommit mode for DB: " + dbConnection + " > possible slower execution...");
		}
		// do we have cloverFields list defined ? (which fields from input record to consider)
		if (cloverFields != null) {
			transMap = CopySQLData.jetel2sqlTransMap(dbFieldTypes, inRecord, cloverFields);
		} else {
			transMap = CopySQLData.jetel2sqlTransMap(dbFieldTypes, inRecord);
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
		} catch (Exception ex) {
			logger.error(ex);
			throw new JetelException(ex.getMessage(),ex);
		} finally {
			broadcastEOF();
			if (preparedStatement != null) {
				preparedStatement.close();
			}
		}
        return runIt ? Result.FINISHED_OK : Result.ABORTED;
	}

	private void runInNormalMode(InputPort inPort,OutputPort rejectedPort,
			DataRecord inRecord,CopySQLData[] transMap) throws SQLException,InterruptedException,IOException{
		int i;
		int recCount = 0;
		int countError=0;
		while (inRecord != null && runIt) {
			inRecord = inPort.readRecord(inRecord);
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
					if (countError>maxErrors && maxErrors!=-1){
						throw new SQLException("Maximum # of errors exceeded when inserting record: "+ex.getMessage());
					}
				}
				preparedStatement.clearParameters();
				if (++recCount % recordsInCommit == 0) {
					dbConnection.getConnection().commit();
				}
			}
			SynchronizeUtils.cloverYield();
		}
 		// end of records stream - final commits;
		 // unless we have option never to commit, commit at the end of processing
	    if (recordsInCommit!=Integer.MAX_VALUE){
	        dbConnection.getConnection().commit();
	    }
	}


	private void runInBatchMode(InputPort inPort,OutputPort rejectedPort,
	        DataRecord inRecord,CopySQLData[] transMap) throws SQLException,InterruptedException,IOException{
	    int i;
	    int batchCount=0;
	    int recCount = 0;
	    int countError=0;
	    DataRecord[] dataRecordHolder;
	    int holderCount=0;
	    
        // first, we set transMap to batchUpdateMode
        CopySQLData.setBatchUpdate(transMap,true);
        
	    // if we have rejected records port connected, we will
	    // store and report erroneous records in batch
	    if (rejectedPort!=null){
	        dataRecordHolder=new DataRecord[batchSize];
	        for (int j=0;j<batchSize;j++){
	            dataRecordHolder[j]=inRecord.duplicate();
	        }
	    }else{
	        dataRecordHolder=null;
	    }
	    
	    while (inRecord != null && runIt) {
	        inRecord = inPort.readRecord(inRecord);
	        if (inRecord != null) {
	            for (i = 0; i < transMap.length; i++) {
	                transMap[i].jetel2sql(preparedStatement);
	            }
	            try{
	                preparedStatement.addBatch();
	                if (dataRecordHolder!=null) dataRecordHolder[holderCount++].copyFrom(inRecord);
	            }catch(SQLException ex){
	                countError++;;
	                if (rejectedPort!=null){
	                    rejectedPort.writeRecord(inRecord);
	                }
	                if (countError>maxErrors && maxErrors!=-1){
	                    throw new SQLException("Maximum # of errors exceeded when inserting record: "+ex.getMessage());
	                }
	            }
	        }
	        // shall we commit ?
	        if (++batchCount % batchSize == 0) {
	            try {
	                preparedStatement.executeBatch();
	                preparedStatement.clearBatch();
	            } catch (BatchUpdateException ex) {
	                preparedStatement.clearBatch();
	                //logger.debug(ex); this might generate a lots of messages
                    flushErrorRecords(dataRecordHolder,holderCount,ex,rejectedPort);
	                if (countError>maxErrors && maxErrors!=-1){
	                    throw new SQLException("Batch error:"+ex.getMessage());
	                }
	            }
	            batchCount = 0;
	            holderCount=0;
	        }
	        if (++recCount % recordsInCommit == 0) {
	            if (batchCount!=0){
	                try {
	                    preparedStatement.executeBatch();
	                    preparedStatement.clearBatch();
	                } catch (BatchUpdateException ex) {
	                    preparedStatement.clearBatch();
	                    //logger.debug(ex); this might generate a lots of messageslogger.debug(ex);
                        flushErrorRecords(dataRecordHolder,holderCount,ex,rejectedPort);
	                    if (countError>maxErrors && maxErrors!=-1){
	                        throw new SQLException("Batch error:"+ex.getMessage());
	                    }
	                }
	                batchCount = 0;
	                holderCount=0;
	            }
	            dbConnection.getConnection().commit();
	        }
	        SynchronizeUtils.cloverYield();
	    }
	    // final commit (if anything is left in batch
	    try{
	        preparedStatement.executeBatch();
	    }catch (BatchUpdateException ex) {
	        //logger.debug(ex); this might generate a lots of messages
            flushErrorRecords(dataRecordHolder,holderCount,ex,rejectedPort);
	        if (dataRecordHolder!=null){
	            Arrays.fill(dataRecordHolder,null);
	        }
	        if (countError>maxErrors && maxErrors!=-1){
	            throw new SQLException("Batch error:"+ex.getMessage());
	        }
	    }
	    // unless we have option never to commit, commit at the end of processing
	    if (recordsInCommit!=Integer.MAX_VALUE){
	        dbConnection.getConnection().commit();
	    }
	    if (dataRecordHolder!=null)
	        Arrays.fill(dataRecordHolder,null);
	}
	
    
    private void flushErrorRecords(DataRecord[] records,int recCount, BatchUpdateException ex,OutputPort port) 
    throws IOException,InterruptedException {
        int[] updateCounts=ex.getUpdateCounts();
        int i=0;

        if (records==null) return;
        
        while(i<updateCounts.length){
            if (updateCounts[i]==Statement.EXECUTE_FAILED){
                port.writeRecord(records[i]);
            }
            i++;
        }
        // flush rest of the records for which we don't have update counts
        while(i<recCount){
            port.writeRecord(records[i++]);
        }
    }
	
	/**
	 *  Description of the Method
	 *
	 * @return    Description of the Returned Value
	 * @since     September 27, 2002
	 */
	@Override public void toXML(Element xmlElement) {
		super.toXML(xmlElement);
		if (dbConnectionName != null) {
			xmlElement.setAttribute(XML_DBCONNECTION_ATTRIBUTE, dbConnectionName);
		}
		if (sqlQuery != null) {
			xmlElement.setAttribute(XML_SQLQUERY_ATRIBUTE, sqlQuery);
		}
		if (dbTableName != null) {
			xmlElement.setAttribute(XML_DBTABLE_ATTRIBUTE, dbTableName);
		}
		
		if (dbFields != null) {
			StringBuffer buf = new StringBuffer(dbFields[0]);
			for (int i=1; i< dbFields.length; i++ ) {
				buf.append(Defaults.Component.KEY_FIELDS_DELIMITER + dbFields[i]);
			}
			xmlElement.setAttribute(XML_DBFIELDS_ATTRIBUTE, buf.toString());
		}
		
		if (cloverFields != null) {
			StringBuffer buf = new StringBuffer(cloverFields[0]);
			for (int i=1; i< cloverFields.length; i++ ) {
				buf.append(Defaults.Component.KEY_FIELDS_DELIMITER + cloverFields[i]);
			}
			xmlElement.setAttribute(XML_DBFIELDS_ATTRIBUTE, buf.toString());
		}
		if (recordsInCommit > 0) {
			xmlElement.setAttribute(XML_COMMIT_ATTRIBUTE,String.valueOf(recordsInCommit));
		}
		
		xmlElement.setAttribute(XML_BATCHMODE_ATTRIBUTE, String.valueOf(useBatch));
		
		xmlElement.setAttribute(XML_BATCHSIZE_ATTRIBUTE, String.valueOf(batchSize));
		
		xmlElement.setAttribute(XML_MAXERRORS_ATRIBUTE, String.valueOf(maxErrors));
		
		
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
		org.w3c.dom.Node childNode;
		DBOutputTable outputTable;

		try {
			// allows specifying parameterized SQL (with ? - questionmarks)
			if (xattribs.exists(XML_SQLQUERY_ATRIBUTE)) {
					outputTable = new DBOutputTable(xattribs.getString(XML_ID_ATTRIBUTE),
					xattribs.getString(XML_DBCONNECTION_ATTRIBUTE),
					xattribs.getString(XML_SQLQUERY_ATRIBUTE),	null);
			}else if(xattribs.exists(XML_URL_ATTRIBUTE)){
				outputTable = new DBOutputTable(xattribs.getString(XML_ID_ATTRIBUTE),
						xattribs.getString(XML_DBCONNECTION_ATTRIBUTE),
						xattribs.resolveReferences(FileUtils.getStringFromURL(graph.getProjectURL(), xattribs.getString(XML_URL_ATTRIBUTE))),	
						null);
			    
			}else if(xattribs.exists(XML_DBTABLE_ATTRIBUTE)){
				outputTable = new DBOutputTable(xattribs.getString(XML_ID_ATTRIBUTE),
						xattribs.getString(XML_DBCONNECTION_ATTRIBUTE),
						xattribs.getString(XML_DBTABLE_ATTRIBUTE));
				
			}else if(xattribs.exists(XML_DBTABLE_ATTRIBUTE)){
				outputTable = new DBOutputTable(xattribs.getString(XML_ID_ATTRIBUTE),
						xattribs.getString(XML_DBCONNECTION_ATTRIBUTE),
						xattribs.getString(XML_DBTABLE_ATTRIBUTE));
				
			}else{
			    childNode = xattribs.getChildNode(xmlElement, XML_SQLCODE_ELEMENT);
                if (childNode == null) {
                    throw new XMLConfigurationException(COMPONENT_TYPE + ":" + xattribs.getString(XML_ID_ATTRIBUTE," unknown ID ") + ": Can't find <SQLCode> node !");
                }
                xattribsChild = new ComponentXMLAttributes((Element)childNode, graph);
                outputTable = new DBOutputTable(xattribs.getString(XML_ID_ATTRIBUTE),
    					xattribs.getString(XML_DBCONNECTION_ATTRIBUTE),
    					xattribsChild.getText(childNode),
    					null);
			}
			
			
			if (xattribs.exists(XML_DBTABLE_ATTRIBUTE)) {
				outputTable.setDBTableName(xattribs.getString(XML_DBTABLE_ATTRIBUTE));
			}
			if (xattribs.exists(XML_FIELDMAP_ATTRIBUTE)){
				String[] pairs = xattribs.getString(XML_FIELDMAP_ATTRIBUTE).split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);
				String[] cloverFields = new String[pairs.length];
				String[] dbFields = new String[pairs.length];
				int equalIndex;
				for (int i=0;i<pairs.length;i++){
					equalIndex = pairs[i].indexOf('=');
					cloverFields[i] = pairs[i].substring(0,equalIndex);
					dbFields[i] = (pairs[i].substring(equalIndex +1));
				}
				outputTable.setCloverFields(cloverFields);
				outputTable.setDBFields(dbFields);
			}else {
				if (xattribs.exists(XML_DBFIELDS_ATTRIBUTE)) {
					outputTable.setDBFields(xattribs.getString(XML_DBFIELDS_ATTRIBUTE).split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX));
				}
	
				if (xattribs.exists(XML_CLOVERFIELDS_ATTRIBUTE)) {
					outputTable.setCloverFields(xattribs.getString(XML_CLOVERFIELDS_ATTRIBUTE).split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX));
				}
			}
			if (xattribs.exists(XML_COMMIT_ATTRIBUTE)) {
				outputTable.setRecordsInCommit(xattribs.getInteger(XML_COMMIT_ATTRIBUTE));
			}
			
			if (xattribs.exists(XML_BATCHMODE_ATTRIBUTE)) {
				outputTable.setUseBatch(xattribs.getBoolean(XML_BATCHMODE_ATTRIBUTE));
			}
			if (xattribs.exists(XML_BATCHSIZE_ATTRIBUTE)) {
				outputTable.setBatchSize(xattribs.getInteger(XML_BATCHSIZE_ATTRIBUTE));
			}
			if (xattribs.exists(XML_MAXERRORS_ATRIBUTE)){
				outputTable.setMaxErrors(xattribs.getInteger(XML_MAXERRORS_ATRIBUTE));
			}
			
			return outputTable;
			
		} catch (Exception ex) {
            throw new XMLConfigurationException(COMPONENT_TYPE + ":" + xattribs.getString(XML_ID_ATTRIBUTE," unknown ID ") + ":" + ex.getMessage(),ex);
        }
	}


	/**
	 *  Description of the Method
	 *
	 * @return    Description of the Return Value
	 */
     @Override
     public ConfigurationStatus checkConfig(ConfigurationStatus status) {
         super.checkConfig(status);
         
         checkInputPorts(status, 1, 1);
         checkOutputPorts(status, 0, 1);

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
	
	/**
	 * @param maxErrors Maximum number of tolerated SQL errors during component run. Default: 0 (zero)
	 */
	public void setMaxErrors(int maxErrors) {
		this.maxErrors = maxErrors;
	}
}

