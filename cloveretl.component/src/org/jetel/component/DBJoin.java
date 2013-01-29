/*
 * jETeL/CloverETL - Java based ETL application framework.
 * Copyright (c) Javlin, a.s. (info@cloveretl.com)
 *  
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */
package org.jetel.component;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.connection.jdbc.DBConnectionImpl;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.Defaults;
import org.jetel.data.NullRecord;
import org.jetel.data.RecordKey;
import org.jetel.data.lookup.Lookup;
import org.jetel.database.IConnection;
import org.jetel.database.sql.DBConnection;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.TransformException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPort;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.lookup.DBLookupTable;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.file.FileUtils;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.property.RefResFlag;
import org.jetel.util.string.StringUtils;
import org.w3c.dom.Element;
/**
 *  <h3>DBJoin Component</h3> <!--  Joins records from input port and database
 *   based on specified key. The flow on port 0 is the driver, record from database
 *   is the slave. For every record from driver flow, corresponding record from
 * slave flow is looked up (if it exists). -->
 *
 * <table border="1">
 *
 *    <th>
 *      Component:
 *    </th>
 *    <tr><td>
 *        <h4><i>Name:</i> </h4></td><td>DBJoin</td>
 *    </tr>
 *    <tr><td><h4><i>Category:</i> </h4></td><td></td>
 *    </tr>
 *    <tr><td><h4><i>Description:</i> </h4></td>
 *      <td>
 *	Joins records on input port and from database. It expects that on port [0],
 *  there is a driver and from database is a slave<br>
 *	For each driver record, slave record is looked up in database.
 *	Pair of driver and slave records is sent to transformation class.<br>
 *	The method <i>transform</i> is called for every pair of driver&slave.<br>
 *	It skips driver records for which there is no corresponding slave (if there is
 *	connected output port 1, thees records are sent to it) - unless 
 *	outer join (leftOuterJoin option) is specified, when only driver record is 
 *	passed to transform method (no records is sent to output port 1). 
 *      </td>
 *    </tr>
 *    <tr><td><h4><i>Inputs:</i> </h4></td>
 *    <td>
 *        [0] - primary records<br>
 *    </td></tr>
 *    <tr><td> <h4><i>Outputs:</i> </h4>
 *      </td>
 *      <td>
 *        [0] - joined records<br>
 *        [1] - (optional) skipped driver records
 *      </td></tr>
 *    <tr><td><h4><i>Comment:</i> </h4>
 *      </td>
 *      <td></td>
 *    </tr>
 *  </table>
 *  <br>
 *  <table border="1">
 *    <th>XML attributes:</th>
 *    <tr><td><b>type</b></td><td>"DBJOIN"</td></tr>
 *    <tr><td><b>id</b></td><td>component identification</td></tr>
 *    <tr><td><b>joinKey</b></td><td>field names separated by Defaults.Component.KEY_FIELDS_DELIMITER_REGEX).
 *    </td></tr>
 *  <tr><td><b>transform</b></td><td>contains definition of transformation as java source, in internal clover format or in Transformation Language</td>
 *    <tr><td><b>transformClass</b><br><i>optional</i></td><td>name of the class to be used for transforming joined data<br>
 *    If no class name is specified then it is expected that the transformation Java source code is embedded in XML 
 *  <tr><td><b>transformURL</b></td><td>path to the file with transformation code</td></tr>
 *  <tr><td><b>charset</b><br><i>optional</i></td><td>encoding of extern source</td></tr>
 *  <tr><td><b>sqlQuery</b><td>query to be sent to database</td>
 *  <tr><td><b>dbConnection</b></td><td>id of the Database Connection object to be used to access the database</td>
 *  <tr><td><b>metadata</b><i>optional</i><td>metadata for data from database</td>
 *  <tr><td><b>maxCached</b><i>optional</i><td>number of sets of records with different key which will be stored in memory</td>
 *  <tr><td><b>leftOuterJoin</b><i>optional</i><td>true/false<I> default: FALSE</I> See description.</td>
 *  <tr><td><b>errorActions </b><i>optional</i></td><td>defines if graph is to stop, when transformation returns negative value.
 *  Available actions are: STOP or CONTINUE. For CONTINUE action, error message is logged to console or file (if errorLog attribute
 *  is specified) and for STOP there is thrown TransformExceptions and graph execution is stopped. <br>
 *  Error action can be set for each negative value (value1=action1;value2=action2;...) or for all values the same action (STOP 
 *  or CONTINUE). It is possible to define error actions for some negative values and for all other values (MIN_INT=myAction).
 *  Default value is <i>-1=CONTINUE;MIN_INT=STOP</i></td></tr>
 *  <tr><td><b>errorLog</b><br><i>optional</i></td><td>path to the error log file. Each error (after which graph continues) is logged in 
 *  following way: recordNumber;errorCode;errorMessage;semiResult - fields are delimited by Defaults.Component.KEY_FIELDS_DELIMITER.</td></tr>
 *    </table>
 *    <h4>Example:</h4> <pre>
 *    &lt;Node id="dbjoin0" type="DBJOIN"&gt;
 *      &lt;attr name="metadata"&gt;Metadata3&lt;/attr&gt;
 *      &lt;attr name="transformClass"&gt;TransformTransformdbjoin0&lt;/attr&gt;
 *      &lt;attr name="sqlQuery"&gt;select * from employee where Employee_ID=?&lt;/attr&gt;
 *      &lt;attr name="joinKey"&gt;EmployeeID&lt;/attr&gt;
 *      &lt;attr name="dbConnection"&gt;DBConnection0&lt;/attr&gt;
 *    &lt;/Node&gt;
</pre>
 *
 * @author avackova <agata.vackova@javlinconsulting.cz> 
 * (c) JavlinConsulting s.r.o.
 *	www.javlinconsulting.cz
 *
 *	@created October 10, 2006
 */
public class DBJoin extends Node {

    public static final String XML_SQL_QUERY_ATTRIBUTE = "sqlQuery"; //$NON-NLS-1$
    public static final String XML_URL_ATTRIBUTE = "url"; //$NON-NLS-1$
    public static final String XML_DBCONNECTION_ATTRIBUTE = "dbConnection"; //$NON-NLS-1$
	public static final String XML_JOIN_KEY_ATTRIBUTE = "joinKey"; //$NON-NLS-1$
	public static final String XML_TRANSFORM_CLASS_ATTRIBUTE = "transformClass"; //$NON-NLS-1$
	public static final String XML_TRANSFORM_ATTRIBUTE = "transform"; //$NON-NLS-1$
	public static final String XML_TRANSFORMURL_ATTRIBUTE = "transformURL"; //$NON-NLS-1$
	public static final String XML_CHARSET_ATTRIBUTE = "charset"; //$NON-NLS-1$
	public static final String XML_DB_METADATA_ATTRIBUTE = "metadata"; //$NON-NLS-1$
	public static final String XML_MAX_CACHED_ATTRIBUTE = "maxCached"; //$NON-NLS-1$
	public static final String XML_LEFTOUTERJOIN_ATTRIBUTE = "leftOuterJoin"; //$NON-NLS-1$
	private static final String XML_ERROR_ACTIONS_ATTRIBUTE = "errorActions"; //$NON-NLS-1$
    private static final String XML_ERROR_LOG_ATTRIBUTE = "errorLog"; //$NON-NLS-1$

	public final static String COMPONENT_TYPE = "DBJOIN"; //$NON-NLS-1$
	
	private final static int WRITE_TO_PORT = 0;
	private final static int REJECTED_PORT = 1;
	private final static int READ_FROM_PORT = 0;
	
	private String transformClassName = null;
	private String transformSource = null;
	private RecordTransform transformation = null;
	private String transformURL = null;
	private String charset = null;
	
	private String[] joinKey;
	private String connectionName;
	private String query;
	private String metadataName;
	private int maxCached;
	private boolean leftOuterJoin = false;

	private String errorActionsString;
	private Map<Integer, ErrorAction> errorActions = new HashMap<Integer, ErrorAction>();
	private String errorLogURL;
	private FileWriter errorLog;

	private Properties transformationParameters=null;
	
	private DBLookupTable lookupTable;
	private Lookup lookup;
	private RecordKey recordKey;
	private DataRecordMetadata dbMetadata;
	private InputPort inPort;
	private DataRecord inRecord;
	
	static Log logger = LogFactory.getLog(DBJoin.class);
	
	/**
	 *Constructor
	 * 
	 * @param id of component
	 * @param connectionName id of connection used for connecting with database
	 * @param query for getting data from database
	 * @param joinKey fields from input port which defines joining records with record from database
	 */
	public DBJoin(String id,String connectionName,String query,String[] joinKey,
			String transform, String transformClass, String transformURL){
		super(id);
		this.connectionName = connectionName;
		this.query = query;
		this.joinKey = joinKey;
		this.transformClassName = transformClass;
		this.transformSource = transform;
		this.transformURL = transformURL;
	}
	
	public DBJoin(String id,String connectionName,String query,String[] joinKey,
			RecordTransform transform){
		this(id, connectionName, query, joinKey, null, null, null);
		this.transformation = transform;
	}

	@Override
	public String getType() {
		return COMPONENT_TYPE;
	}
	
	@Override
	public Result execute() throws Exception {
		//initialize in and out records
		DataRecord[] outRecord = {DataRecordFactory.newRecord(getOutputPort(WRITE_TO_PORT).getMetadata())};
		outRecord[0].init();
		outRecord[0].reset();
		DataRecord[] inRecords = new DataRecord[] {inRecord,null};
		OutputPort rejectedPort = getOutputPort(REJECTED_PORT);

		int counter = 0;
		while (inRecord!=null && runIt) {
				inRecord = inPort.readRecord(inRecord);
				if (inRecord!=null) {
					//find slave record in database
					lookup.seek();
					inRecords[1] = lookup.hasNext() ? lookup.next() : NullRecord.NULL_RECORD;
					do{
						if (transformation != null) {//transform driver and slave
							if ((inRecords[1] != NullRecord.NULL_RECORD || leftOuterJoin)){
								int transformResult = -1;

								try {
									transformResult = transformation.transform(inRecords, outRecord);
								} catch (Exception exception) {
									transformResult = transformation.transformOnError(exception, inRecords, outRecord);
								}

								if (transformResult >= 0) {
									writeRecord(WRITE_TO_PORT, outRecord[0]);
								} else{
									ErrorAction action = errorActions.get(transformResult);
									if (action == null) {
										action = errorActions.get(Integer.MIN_VALUE);
										if (action == null) {
											action = ErrorAction.DEFAULT_ERROR_ACTION;
										}
									}
									String message = "Transformation finished with code: " + transformResult + ". Error message: " +  //$NON-NLS-1$ //$NON-NLS-2$
										transformation.getMessage();
									if (action == ErrorAction.CONTINUE) {
										if (errorLog != null){
											errorLog.write(String.valueOf(counter));
											errorLog.write(Defaults.Component.KEY_FIELDS_DELIMITER);
											errorLog.write(String.valueOf(transformResult));
											errorLog.write(Defaults.Component.KEY_FIELDS_DELIMITER);
											message = transformation.getMessage();
											if (message != null) {
												errorLog.write(message);
											}
											errorLog.write(Defaults.Component.KEY_FIELDS_DELIMITER);
											Object semiResult = transformation.getSemiResult();
											if (semiResult != null) {
												errorLog.write(semiResult.toString());
											}
											errorLog.write("\n"); //$NON-NLS-1$
										} else {
											//CL-2020
											//if no error log is defined, the message is quietly ignored
											//without messy logging in console
											//only in case non empty message given from transformation, the message is printed out
											if (!StringUtils.isEmpty(transformation.getMessage())) {
												logger.warn(message);
											}
										}
									} else {
										throw new TransformException(message);
									}
								}
							}else if (rejectedPort != null){
								writeRecord(REJECTED_PORT, inRecord);
							}
						}else { 
							if (inRecords[1] != NullRecord.NULL_RECORD){//send to output only records from DB
								writeRecord(WRITE_TO_PORT, inRecords[1]);
							}else if (rejectedPort != null){
								writeRecord(REJECTED_PORT, inRecord);
							}
						}
						//get next record from database with the same key
						inRecords[1] = lookup.hasNext() ? lookup.next() : NullRecord.NULL_RECORD;		
					}while (inRecords[1] !=  NullRecord.NULL_RECORD);
				}
				counter++;
		}

		if (errorLog != null){
			errorLog.flush();
		}

		broadcastEOF();
        return runIt ? Result.FINISHED_OK : Result.ABORTED;
	}
	
	@Override
	public void free() {
        if (!isInitialized()) {
            return;
        }

        super.free();

        if (lookup != null) {
            lookup.getLookupTable().free();
        }
	}

    @Override
    public ConfigurationStatus checkConfig(ConfigurationStatus status) {
        super.checkConfig(status);
        
        if(!checkInputPorts(status, 1, 1)
        		|| !checkOutputPorts(status, 1, 2)) {
        	return status;
        }
        
        if (getOutputPort(REJECTED_PORT) != null) {
        	checkMetadata(status, getInputPort(READ_FROM_PORT).getMetadata(), 
        			getOutputPort(REJECTED_PORT).getMetadata());
        }
        
        if (charset != null && !Charset.isSupported(charset)) {
        	status.add(new ConfigurationProblem(
            		"Charset "+charset+" not supported!",  //$NON-NLS-1$ //$NON-NLS-2$
            		ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL));
        }

        dbMetadata = getGraph().getDataRecordMetadata(metadataName, false);

        try {
        	
    		IConnection conn = getGraph().getConnection(connectionName);
            if(conn == null) {
                throw new ComponentNotReadyException("Can't find DBConnection ID: " + connectionName); //$NON-NLS-1$
            }
            if(!(conn instanceof DBConnection)) {
                throw new ComponentNotReadyException("Connection with ID: " + connectionName + " isn't instance of the DBConnection class."); //$NON-NLS-1$ //$NON-NLS-2$
            }

            if (dbMetadata == null) {
	            conn.init();
	            dbMetadata = extractDbMetadata(conn, query);
	            conn.free();
            }

            try {
    			recordKey = new RecordKey(joinKey, getInputPort(READ_FROM_PORT).getMetadata());
    			recordKey.init();
    		} catch (Exception e) {
    			throw new ComponentNotReadyException(this, e);
    		}

    		if (errorActionsString != null) {
				ErrorAction.checkActions(errorActionsString);
			}
    		
            if (errorLog != null){
 				FileUtils.canWrite(getGraph().getRuntimeContext().getContextURL(), errorLogURL);
           	}
        } catch (ComponentNotReadyException e) {
            ConfigurationProblem problem = new ConfigurationProblem(e.getMessage(), ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL);
            if(!StringUtils.isEmpty(e.getAttributeName())) {
                problem.setAttributeName(e.getAttributeName());
            }
            status.add(problem);
        }

        if (dbMetadata != null) {
	        DataRecordMetadata[] inMetadata = new DataRecordMetadata[] {
	        		getInputPort(READ_FROM_PORT).getMetadata(), dbMetadata };
	        DataRecordMetadata[] outMetadata = new DataRecordMetadata[] {
	        		getOutputPort(WRITE_TO_PORT).getMetadata() };

	        //check transformation
	        if (transformation == null) {
	        	TransformFactory<RecordTransform> transformFactory = getTransformFactory(inMetadata, outMetadata);
	        	if (transformFactory.isTransformSpecified()) {
	        		transformFactory.checkConfig(status);
	        	}
	        }
        }

        return status;
    }

	@Override
	public void init() throws ComponentNotReadyException {
        if(isInitialized()) return;
		super.init();
        dbMetadata = getGraph().getDataRecordMetadata(metadataName, true);
		
		// Initializing lookup table
		IConnection conn = getGraph().getConnection(connectionName);
		if (conn == null) {
			throw new ComponentNotReadyException("Can't find DBConnection ID: " + connectionName); //$NON-NLS-1$
		}
		if (!(conn instanceof DBConnection)) {
			throw new ComponentNotReadyException("Connection with ID: " + connectionName + " isn't instance of the DBConnection class."); //$NON-NLS-1$ //$NON-NLS-2$
		}
		conn.init();
		
		if (dbMetadata == null) {
			dbMetadata = extractDbMetadata(conn, query);
		}

		lookupTable = new DBLookupTable("LOOKUP_TABLE_FROM_" + this.getId(), (DBConnection) conn, dbMetadata, query, maxCached); //$NON-NLS-1$
		lookupTable.setGraph(getGraph());
		lookupTable.checkConfig(null);
		lookupTable.init();

		DataRecordMetadata inMetadata[];
		DataRecordMetadata outMetadata[];
		
		InputPort inputPortHandle = getInputPort(READ_FROM_PORT);
		OutputPort outputPortHandle = getOutputPort(WRITE_TO_PORT);

		if (inputPortHandle == null) {
			throw new ComponentNotReadyException(MessageFormat.format(ComponentMessages.getString("DBJoin_InputPortError"), READ_FROM_PORT, this.getId()));  //$NON-NLS-1$
		} else
			inMetadata = new DataRecordMetadata[] { inputPortHandle.getMetadata(), dbMetadata };

		if (outputPortHandle == null) {
			throw new ComponentNotReadyException(MessageFormat.format(ComponentMessages.getString("DBJoin_OutputPortError"), WRITE_TO_PORT, this.getId()));  //$NON-NLS-1$
		} else
			outMetadata = new DataRecordMetadata[] { outputPortHandle.getMetadata() };

		try {
			recordKey = new RecordKey(joinKey, inMetadata[0]);
			recordKey.init();
			
			if (transformation == null) {
				TransformFactory<RecordTransform> transformFactory = getTransformFactory(inMetadata, outMetadata);
	        	if (transformFactory.isTransformSpecified()) {
	        		transformation = transformFactory.createTransform();
	        	}
			}

			// init transformation
	        if (transformation != null && !transformation.init(transformationParameters, inMetadata, outMetadata)) {
	            throw new ComponentNotReadyException("Error when initializing tranformation function."); //$NON-NLS-1$
	        }
		} catch (Exception e) {
			throw new ComponentNotReadyException(this, e);
		}
		inPort=getInputPort(READ_FROM_PORT);
		if (transformation != null && leftOuterJoin && getOutputPort(REJECTED_PORT) != null) {
			logger.info(this.getId() + " info: There will be no skipped records " + //$NON-NLS-1$
					"while left outer join is switched on"); //$NON-NLS-1$
		}
        errorActions = ErrorAction.createMap(errorActionsString);
	}
	
	private TransformFactory<RecordTransform> getTransformFactory(DataRecordMetadata[] inMetadata, DataRecordMetadata[] outMetadata) {
    	TransformFactory<RecordTransform> transformFactory = TransformFactory.createTransformFactory(RecordTransformDescriptor.newInstance());
    	transformFactory.setTransform(transformSource);
    	transformFactory.setTransformClass(transformClassName);
    	transformFactory.setTransformUrl(transformURL);
    	transformFactory.setCharset(charset);
    	transformFactory.setComponent(this);
    	transformFactory.setInMetadata(inMetadata);
    	transformFactory.setOutMetadata(outMetadata);
    	return transformFactory;
	}
	
	private DataRecordMetadata extractDbMetadata(IConnection connection, String sqlQuery)
			throws ComponentNotReadyException {
		Properties parameters = new Properties();
		parameters.setProperty(DBConnectionImpl.SQL_QUERY_PROPERTY, sqlQuery);

		try {
			return connection.createMetadata(parameters);
		} catch (SQLException exception) {
			throw new ComponentNotReadyException("Extraction of DB metadata failed!", exception); //$NON-NLS-1$
		}
	}

	@Override
	public void preExecute() throws ComponentNotReadyException {
		super.preExecute();
		
		if (transformation != null) {
		    transformation.preExecute();
		}

		if (firstRun()) {// a phase-dependent part of initialization
			//all necessary elements have been initialized in init()
		} else {
			if (transformation != null) {
				transformation.reset();
			}
		}
		lookupTable.preExecute();
		
		inRecord = DataRecordFactory.newRecord(inPort.getMetadata());
		inRecord.init();
		lookup = lookupTable.createLookup(recordKey, inRecord);
		if (errorLogURL != null) {
			try {
				errorLog = new FileWriter(FileUtils.getFile(getGraph().getRuntimeContext().getContextURL(), errorLogURL));
			} catch (IOException e) {
				throw new ComponentNotReadyException(this, XML_ERROR_LOG_ATTRIBUTE, e.getMessage());
			}
		}
	}

	@Override
	public synchronized void reset() throws ComponentNotReadyException {
		super.reset();
	}
	
    @Override
	public void postExecute() throws ComponentNotReadyException {
		super.postExecute();
		
		if (transformation != null) {
		    transformation.postExecute();
		    transformation.finished();
		}
		if (lookup != null) {
			lookup.getLookupTable().postExecute();
			lookup = null;
		}
		try {
    	    if (errorLog != null){
    			errorLog.close();
    		}
    	}
    	catch (Exception e) {
    		throw new ComponentNotReadyException(COMPONENT_TYPE + ": " + e.getMessage(),e); //$NON-NLS-1$
    	}
	}

	public static Node fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException {
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(xmlElement, graph);
		DBJoin dbjoin;
		String connectionName;
		String query;
		String[] joinKey;
		//get necessary parameters
		try{
			connectionName = xattribs.getString(XML_DBCONNECTION_ATTRIBUTE);
			if (xattribs.exists(XML_URL_ATTRIBUTE)) {
				query=xattribs.resolveReferences(FileUtils.getStringFromURL(graph.getRuntimeContext().getContextURL(), 
             		   xattribs.getStringEx(XML_URL_ATTRIBUTE,RefResFlag.SPEC_CHARACTERS_OFF), xattribs.getString(XML_CHARSET_ATTRIBUTE, null)));
			} else {
				query = xattribs.getString(XML_SQL_QUERY_ATTRIBUTE);
			}
			joinKey = xattribs.getString(XML_JOIN_KEY_ATTRIBUTE).split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);
		
            dbjoin = new DBJoin(
                    xattribs.getString(XML_ID_ATTRIBUTE),
                    connectionName,query,joinKey,
                    xattribs.getStringEx(XML_TRANSFORM_ATTRIBUTE, null, RefResFlag.SPEC_CHARACTERS_OFF), 
                    xattribs.getString(XML_TRANSFORM_CLASS_ATTRIBUTE, null),
                    xattribs.getStringEx(XML_TRANSFORMURL_ATTRIBUTE,null, RefResFlag.SPEC_CHARACTERS_OFF));
			dbjoin.setCharset(xattribs.getString(XML_CHARSET_ATTRIBUTE, null));
			dbjoin.setTransformationParameters(xattribs.attributes2Properties(new String[]{XML_TRANSFORM_CLASS_ATTRIBUTE}));
			if (xattribs.exists(XML_DB_METADATA_ATTRIBUTE)){
				dbjoin.setDbMetadata(xattribs.getString(XML_DB_METADATA_ATTRIBUTE));
			}
			if (xattribs.exists(XML_LEFTOUTERJOIN_ATTRIBUTE)){
				dbjoin.setLeftOuterJoin(xattribs.getBoolean(XML_LEFTOUTERJOIN_ATTRIBUTE));
			}
			dbjoin.setMaxCached(xattribs.getInteger(XML_MAX_CACHED_ATTRIBUTE,100));
			if (xattribs.exists(XML_ERROR_ACTIONS_ATTRIBUTE)){
				dbjoin.setErrorActions(xattribs.getString(XML_ERROR_ACTIONS_ATTRIBUTE));
			}
			if (xattribs.exists(XML_ERROR_LOG_ATTRIBUTE)){
				dbjoin.setErrorLog(xattribs.getString(XML_ERROR_LOG_ATTRIBUTE));
			}
		} catch (Exception ex) {
            throw new XMLConfigurationException(COMPONENT_TYPE + ":" + xattribs.getString(XML_ID_ATTRIBUTE," unknown ID ") + ":" + ex.getMessage(),ex); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        }
        
		return dbjoin;
	}

	public void setErrorLog(String errorLog) {
		this.errorLogURL = errorLog;
	}

	public void setErrorActions(String string) {
		this.errorActionsString = string;		
	}

	@Override
	public void toXML(Element xmlElement) {
		super.toXML(xmlElement);

		xmlElement.setAttribute(XML_DBCONNECTION_ATTRIBUTE, connectionName);
		xmlElement.setAttribute(XML_SQL_QUERY_ATTRIBUTE, query);
		if (metadataName != null) {
			xmlElement.setAttribute(XML_DB_METADATA_ATTRIBUTE, metadataName);
		}
		if (transformClassName != null) {
			xmlElement.setAttribute(XML_TRANSFORM_CLASS_ATTRIBUTE, transformClassName);
		} 

		if (transformSource!=null){
			xmlElement.setAttribute(XML_TRANSFORM_ATTRIBUTE,transformSource);
		}
		if (transformURL != null) {
			xmlElement.setAttribute(XML_TRANSFORMURL_ATTRIBUTE, transformURL);
		}
		
		if (charset != null){
			xmlElement.setAttribute(XML_CHARSET_ATTRIBUTE, charset);
		}
		if (maxCached >0 ) {
			xmlElement.setAttribute(XML_MAX_CACHED_ATTRIBUTE, String.valueOf(maxCached));
		}
		xmlElement.setAttribute(XML_JOIN_KEY_ATTRIBUTE, StringUtils.stringArraytoString(joinKey, ';'));

		xmlElement.setAttribute(XML_LEFTOUTERJOIN_ATTRIBUTE, String.valueOf(leftOuterJoin));

		if (errorActionsString != null){
			xmlElement.setAttribute(XML_ERROR_ACTIONS_ATTRIBUTE, errorActionsString);
		}
		
		if (errorLogURL != null){
			xmlElement.setAttribute(XML_ERROR_LOG_ATTRIBUTE, errorLogURL);
		}
		Enumeration propertyAtts = transformationParameters.propertyNames();
		while (propertyAtts.hasMoreElements()) {
			String attName = (String)propertyAtts.nextElement();
			xmlElement.setAttribute(attName,transformationParameters.getProperty(attName));
		}
	}

	/**
     * @param transformationParameters The transformationParameters to set.
     */
    public void setTransformationParameters(Properties transformationParameters) {
        this.transformationParameters = transformationParameters;
    }
	/**
	 * @param dbMetadata The dbMetadata to set.
	 */
	public void setDbMetadata(String dbMetadata) {
		this.metadataName = dbMetadata;
	}

	public void setMaxCached(int maxCached) {
		this.maxCached = maxCached;
	}

	public void setLeftOuterJoin(boolean leftOuterJoin) {
		this.leftOuterJoin = leftOuterJoin;
	}

	public String getCharset() {
		return charset;
	}

	public void setCharset(String charset) {
		this.charset = charset;
	}

	
}
