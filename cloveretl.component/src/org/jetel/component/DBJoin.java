
/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2005-06  Javlin Consulting <info@javlinconsulting.cz>
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

import java.util.Enumeration;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.connection.DBConnection;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.RecordKey;
import org.jetel.database.IConnection;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPort;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.lookup.DBLookupTable;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.property.ComponentXMLAttributes;
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

    private static final String XML_SQL_QUERY_ATTRIBUTE = "sqlQuery";
    private static final String XML_DBCONNECTION_ATTRIBUTE = "dbConnection";
	private static final String XML_JOIN_KEY_ATTRIBUTE = "joinKey";
	private static final String XML_TRANSFORM_CLASS_ATTRIBUTE = "transformClass";
	private static final String XML_TRANSFORM_ATTRIBUTE = "transform";
	private static final String XML_TRANSFORMURL_ATTRIBUTE = "transformURL";
	private static final String XML_CHARSET_ATTRIBUTE = "charset";
	private static final String XML_DB_METADATA_ATTRIBUTE = "metadata";
	private static final String XML_MAX_CACHED_ATTRIBUTE = "maxCached";
	private static final String XML_LEFTOUTERJOIN_ATTRIBUTE = "leftOuterJoin";

	public final static String COMPONENT_TYPE = "DBJOIN";
	
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

	private Properties transformationParameters=null;
	
	private DBLookupTable lookupTable;
	private RecordKey recordKey;
	private DataRecordMetadata dbMetadata;
	
	static Log logger = LogFactory.getLog(Reformat.class);
	
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

	/* (non-Javadoc)
	 * @see org.jetel.graph.Node#getType()
	 */
	public String getType() {
		return COMPONENT_TYPE;
	}
	
	@Override
	public Result execute() throws Exception {
		//initialize in and out records
		InputPort inPort=getInputPort(READ_FROM_PORT);
		DataRecord[] outRecord = {new DataRecord(getOutputPort(WRITE_TO_PORT).getMetadata())};
		outRecord[0].init();
		outRecord[0].reset();
		DataRecord inRecord = new DataRecord(inPort.getMetadata());
		inRecord.init();
		DataRecord[] inRecords = new DataRecord[] {inRecord,null};
		OutputPort rejectedPort = getOutputPort(REJECTED_PORT);

		while (inRecord!=null && runIt) {
				inRecord = inPort.readRecord(inRecord);
				if (inRecord!=null) {
					//find slave record in database
					inRecords[1] = lookupTable.get(inRecord);
					do{
						if (transformation != null) {//transform driver and slave
							if ((inRecords[1] != null || leftOuterJoin)){
								if (transformation.transform(inRecords,outRecord)) {
									writeRecord(WRITE_TO_PORT, outRecord[0]);
								}else{
									logger.warn(transformation.getMessage());
								}
							}else if (rejectedPort != null){
								writeRecord(REJECTED_PORT, inRecord);
							}
						}else { 
							if (inRecords[1] != null){//send to output only records from DB
								writeRecord(WRITE_TO_PORT, inRecords[1]);
							}else if (rejectedPort != null){
								writeRecord(REJECTED_PORT, inRecord);
							}
						}
						//get next record from database with the same key
						inRecords[1] = lookupTable.getNext();					
					}while (inRecords[1] != null);
				}
		}
		if (transformation != null) {
			transformation.finished();
		}		
		broadcastEOF();
        return runIt ? Result.FINISHED_OK : Result.ABORTED;
	}
	
	@Override
	public void free() {
        if(!isInitialized()) return;
		super.free();
		
		lookupTable.free();
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.GraphElement#checkConfig()
	 */
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

        try {
        	
    		IConnection conn = getGraph().getConnection(connectionName);
            if(conn == null) {
                throw new ComponentNotReadyException("Can't find DBConnection ID: " + connectionName);
            }
            if(!(conn instanceof DBConnection)) {
                throw new ComponentNotReadyException("Connection with ID: " + connectionName + " isn't instance of the DBConnection class.");
            }
            conn.init();
            
            dbMetadata = getGraph().getDataRecordMetadata(metadataName);
    		DataRecordMetadata inMetadata[]={ getInputPort(READ_FROM_PORT).getMetadata(),dbMetadata};
    		DataRecordMetadata outMetadata[]={getOutputPort(WRITE_TO_PORT).getMetadata()};
            lookupTable = new DBLookupTable("LOOKUP_TABLE_FROM_"+this.getId(),((DBConnection) conn).getConnection(getId()),
            		dbMetadata,query,maxCached);
            lookupTable.checkConfig(status);
//    		lookupTable.init();
    		try {
    			recordKey = new RecordKey(joinKey, inMetadata[0]);
    			recordKey.init();
    			lookupTable.setLookupKey(recordKey);
    		} catch (Exception e) {
    			throw new ComponentNotReadyException(this, e);
    		}
        	
//    		lookupTable.free();
        	
//            init();
//            free();
        } catch (ComponentNotReadyException e) {
            ConfigurationProblem problem = new ConfigurationProblem(e.getMessage(), ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL);
            if(!StringUtils.isEmpty(e.getAttributeName())) {
                problem.setAttributeName(e.getAttributeName());
            }
            status.add(problem);
        }
        
        return status;
    }

	/* (non-Javadoc)
	 * @see org.jetel.graph.GraphElement#init()
	 */
	public void init() throws ComponentNotReadyException {
        if(isInitialized()) return;
		super.init();
		
		//Initializing lookup table
		IConnection conn = getGraph().getConnection(connectionName);
        if(conn == null) {
            throw new ComponentNotReadyException("Can't find DBConnection ID: " + connectionName);
        }
        if(!(conn instanceof DBConnection)) {
            throw new ComponentNotReadyException("Connection with ID: " + connectionName + " isn't instance of the DBConnection class.");
        }
        conn.init();
        
        dbMetadata = getGraph().getDataRecordMetadata(metadataName);
		DataRecordMetadata inMetadata[]={ getInputPort(READ_FROM_PORT).getMetadata(),dbMetadata};
		DataRecordMetadata outMetadata[]={getOutputPort(WRITE_TO_PORT).getMetadata()};
        lookupTable = new DBLookupTable("LOOKUP_TABLE_FROM_"+this.getId(),((DBConnection) conn).getConnection(getId()),
        		dbMetadata,query,maxCached);
        lookupTable.checkConfig(null);
		lookupTable.init();
		try {
			recordKey = new RecordKey(joinKey, inMetadata[0]);
			recordKey.init();
			lookupTable.setLookupKey(recordKey);
			if (transformation != null){
				transformation.init(transformationParameters, inMetadata, outMetadata);
			}
			if (transformSource != null || transformClassName != null) {
				transformation = RecordTransformFactory.createTransform(
						transformSource, transformClassName, transformURL, charset, this, inMetadata, 
						outMetadata, transformationParameters, this.getClass().getClassLoader());
			}			
		} catch (Exception e) {
			throw new ComponentNotReadyException(this, e);
		}
		
		if (transformation != null && leftOuterJoin && getOutputPort(REJECTED_PORT) != null) {
			logger.info(this.getId() + " info: There will be no skipped records " +
					"while left outer join is switched on");
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
			query = xattribs.getString(XML_SQL_QUERY_ATTRIBUTE);
			joinKey = xattribs.getString(XML_JOIN_KEY_ATTRIBUTE).split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);
		
            dbjoin = new DBJoin(
                    xattribs.getString(XML_ID_ATTRIBUTE),
                    connectionName,query,joinKey,
                    xattribs.getString(XML_TRANSFORM_ATTRIBUTE, null), 
                    xattribs.getString(XML_TRANSFORM_CLASS_ATTRIBUTE, null),
                    xattribs.getString(XML_TRANSFORMURL_ATTRIBUTE,null));
			if (xattribs.exists(XML_CHARSET_ATTRIBUTE)) {
				dbjoin.setCharset(xattribs.getString(XML_CHARSET_ATTRIBUTE));
			}
			dbjoin.setTransformationParameters(xattribs.attributes2Properties(new String[]{XML_TRANSFORM_CLASS_ATTRIBUTE}));
			if (xattribs.exists(XML_DB_METADATA_ATTRIBUTE)){
				dbjoin.setDbMetadata(xattribs.getString(XML_DB_METADATA_ATTRIBUTE));
			}
			if (xattribs.exists(XML_LEFTOUTERJOIN_ATTRIBUTE)){
				dbjoin.setLeftOuterJoin(xattribs.getBoolean(XML_LEFTOUTERJOIN_ATTRIBUTE));
			}
			dbjoin.setMaxCached(xattribs.getInteger(XML_MAX_CACHED_ATTRIBUTE,100));
		} catch (Exception ex) {
            throw new XMLConfigurationException(COMPONENT_TYPE + ":" + xattribs.getString(XML_ID_ATTRIBUTE," unknown ID ") + ":" + ex.getMessage(),ex);
        }
        
		return dbjoin;
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.Node#toXML(org.w3c.dom.Element)
	 */
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
