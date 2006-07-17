
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

import java.io.IOException;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.connection.DBConnection;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.RecordKey;
import org.jetel.database.IConnection;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.TransformationGraph;
import org.jetel.lookup.DBLookupTable;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.ComponentXMLAttributes;
import org.jetel.util.DynamicJavaCode;
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
 *	The method <i>transform</i> is called for every pair of driver&amps;slave.<br>
 *	It skips driver records for which there is no corresponding slave.
 *      </td>
 *    </tr>
 *    <tr><td><h4><i>Inputs:</i> </h4></td>
 *    <td>
 *        [0] - primary records<br>
 *    </td></tr>
 *    <tr><td> <h4><i>Outputs:</i> </h4>
 *      </td>
 *      <td>
 *        [0] - one output port
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
 *  <tr><td><b>libraryPath</b><br><i>optional</i></td><td>name of Java library file (.jar,.zip,...) where
 *  to search for class to be used for transforming joined data specified in <tt>transformClass<tt> parameter.</td></tr>
 *  <tr><td><b>transform</b></td><td>contains definition of transformation in internal clover format </td>
 *    <tr><td><b>transformClass</b><br><i>optional</i></td><td>name of the class to be used for transforming joined data<br>
 *    If no class name is specified then it is expected that the transformation Java source code is embedded in XML - <i>see example
 * below</i></td></tr>
 *  <tr><td><b>transform</b></td><td>contains definition of transformation for
 *  	 joined records which has conformity greater then conformity limit in
 *  	 internal clover format</td>
 *  <tr><td><b>sqlQuery</b><td>query to be sent to database</td>
 *  <tr><td><b>dbConnection</b></td><td>id of the Database Connection object to be used to access the database</td>
 *  <tr><td><b>metadata</b><td>metadata for data from database</td>
 *    </table>
 *    <h4>Example:</h4> <pre>
 *    &lt;Node id="dbjoin0" type="DBJOIN"&gt;
&lt;attr name="metadata"&gt;Metadata3&lt;/attr&gt;
&lt;attr name="transformClass"&gt;TransformTransformdbjoin0&lt;/attr&gt;
&lt;attr name="sqlQuery"&gt;select * from employee where Employee_ID=?&lt;/attr&gt;
&lt;attr name="joinKey"&gt;EmployeeID&lt;/attr&gt;
&lt;attr name="dbConnection"&gt;DBConnection0&lt;/attr&gt;
&lt;/Node&gt;
</pre>
 *
 *  @since March 27, 2004
 */

/**
 * @author avackova
 *
 */
public class DBJoin extends Node {

    private static final String XML_SQL_QUERY_ATTRIBUTE = "sqlQuery";
    private static final String XML_DBCONNECTION_ATTRIBUTE = "dbConnection";
	private static final String XML_JOIN_KEY_ATTRIBUTE = "joinKey";
	private static final String XML_TRANSFORM_CLASS_ATTRIBUTE = "transformClass";
	private static final String XML_LIBRARY_PATH_ATTRIBUTE = "libraryPath";
	private static final String XML_JAVA_SOURCE_ATTRIBUTE = "javaSource";
	private static final String XML_TRANSFORM_ATTRIBUTE = "transform";
	private static final String XML_DB_METADATA_ATTRIBUTE = "metadata";

	public final static String COMPONENT_TYPE = "DBJOIN";
	
	private final static int WRITE_TO_PORT = 0;
	private final static int READ_FROM_PORT = 0;
	
	private String transformClassName = null;
	private DynamicJavaCode dynamicTransformCode = null;
	private RecordTransform transformation = null;
	private String libraryPath = null;
	private String transformSource = null;
	
	private String[] joinKey;
	private String connectionName;
	private String query;
	private String metadataName;

	private Properties transformationParameters=null;
	
	private DBLookupTable lookupTable;
	private RecordKey recordKey;
	private DataRecordMetadata dbMetadata;
	
	static Log logger = LogFactory.getLog(Reformat.class);
	
	/**
	 * Basic constructor
	 * 
	 * @param id of component
	 * @param connectionName id of connection used for connecting with database
	 * @param query for getting data from database
	 * @param joinKey fields from input port which defines joing records with record from database
	 */
	public DBJoin(String id,String connectionName,String query,String[] joinKey){
		super(id);
		this.connectionName = connectionName;
		this.query = query;
		this.joinKey = joinKey;
	}
	/**
	 *Constructor for the DBJoin object
	 *
	 * @param  id              unique identification of component
	 * @param  transformClass  Name of transformation CLASS (e.g. org.jetel.test.DemoTrans)
	 */
	
	public DBJoin(String id,String connectionName,String query, String[] joinKey, 
			String transformClass) {
		this(id,connectionName,query,joinKey);
		this.transformClassName = transformClass;
	}

	/**
	 *Constructor for the DBJoin object
	 *
	 * @param  id              unique identification of component
	 * @param  transform       source of transformation in internal format
	 */
	public DBJoin(String id, String connectionName,String query, String[] joinKey,
			String transform, boolean distincter) {
		this(id,connectionName,query,joinKey);
		this.transformSource = transform;
	}

	/**
	 *Constructor for the DBJoin object
	 *
	 * @param  id              unique identification of component
	 * @param  transformClass  Object of class implementing RecordTransform interface
	 */
	public DBJoin(String id,String connectionName,String query, String[] joinKey, 
			RecordTransform transformClass) {
		this(id,connectionName,query,joinKey);
		this.transformation = transformClass;
	}

	/**
	 *Constructor for the DBJoin object
	 *
	 * @param  id           unique identification of component
	 * @param  dynamicCode  DynamicJavaCode object
	 */
	public DBJoin(String id,String connectionName,String query, String[] joinKey, 
			DynamicJavaCode dynamicCode) {
		this(id,connectionName,query,joinKey);
		this.dynamicTransformCode = dynamicCode;
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.graph.Node#getType()
	 */
	public String getType() {
		return COMPONENT_TYPE;
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.graph.Node#run()
	 */
	public void run() {
		//initialize in and out records
		InputPort inPort=getInputPort(WRITE_TO_PORT);
		DataRecord inRecord = new DataRecord(inPort.getMetadata());
		inRecord.init();
		DataRecord[] outRecord = {new DataRecord(getOutputPort(READ_FROM_PORT).getMetadata())};
		outRecord[0].init();
		DataRecord[] inRecords = new DataRecord[] {inRecord,null};
		while (inRecord!=null && runIt) {
			try {
				inRecord = inPort.readRecord(inRecord);
				if (inRecord!=null) {
					//find slave record in database
					inRecords[1] = lookupTable.get(inRecord);
					while (inRecords[1]!=null){
						if (transformation.transform(inRecords, outRecord)) {
							writeRecord(WRITE_TO_PORT,outRecord[0]);
						}
						//get next record from database with the same key
						inRecords[1] = lookupTable.getNext();					}
				}
			} catch (IOException ex) {
				resultMsg = ex.getMessage();
				resultCode = Node.RESULT_ERROR;
				closeAllOutputPorts();
				return;
			} catch (Exception ex) {
				ex.printStackTrace();
				resultMsg = ex.getMessage();
				resultCode = Node.RESULT_FATAL_ERROR;
				closeAllOutputPorts();
				return;
			}
		}
		broadcastEOF();
		if (runIt) {
			resultMsg = "OK";
		} else {
			resultMsg = "STOPPED";
		}
		resultCode = Node.RESULT_OK;
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.GraphElement#checkConfig()
	 */
	public boolean checkConfig() {
		return true;
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.GraphElement#init()
	 */
	public void init() throws ComponentNotReadyException {
		// test that we have one input port and one output
		if (inPorts.size() != 1) {
			throw new ComponentNotReadyException("Exactly one input port has to be defined!");
		} else if (outPorts.size() != 1) {
			throw new ComponentNotReadyException("Exactly one output port has to be defined!");
		}
		// do we have transformation object directly specified or shall we
        // create it ourselves
		if (transformation == null) {
			if (transformClassName != null) {
                transformation=RecordTransformFactory.loadClass(logger,transformClassName,new String[] {libraryPath});
			} else {
                if (transformSource.indexOf(RecordTransformTL.TL_TRANSFORM_CODE_ID)!=-1){
                    transformation=new RecordTransformTL(logger,transformSource);
                }else if(dynamicTransformCode == null) { // transformSource is set
                    transformation=RecordTransformFactory.loadClassDynamic(logger,("Transform"+ getId()),transformSource,
                            (DataRecordMetadata[]) getInMetadata().toArray(new DataRecordMetadata[0]), (DataRecordMetadata[]) getOutMetadata().toArray(new DataRecordMetadata[0]));
			    }else{
			        transformation=RecordTransformFactory.loadClassDynamic(logger,dynamicTransformCode);
                }
			}
		}
        transformation.setGraph(getGraph());
		// init transformation
		DataRecordMetadata inMetadata[]={ getInputPort(READ_FROM_PORT).getMetadata()};
		DataRecordMetadata outMetadata[]={getOutputPort(WRITE_TO_PORT).getMetadata()};
		if (!transformation.init(transformationParameters,inMetadata,outMetadata)) {
			throw new ComponentNotReadyException("Error when initializing reformat function !");
		}
		//Initializing lookup table
		IConnection conn = getGraph().getConnection(connectionName);
        if(conn == null) {
            throw new ComponentNotReadyException("Can't find DBConnection ID: " + connectionName);
        }
        if(!(conn instanceof DBConnection)) {
            throw new ComponentNotReadyException("Connection with ID: " + connectionName + " isn't instance of the DBConnection class.");
        }
        dbMetadata = getGraph().getDataRecordMetadata(metadataName);
        lookupTable = new DBLookupTable("LOOKUP_TABLE_FROM_"+this.getId(),(DBConnection) conn,dbMetadata,query);
		lookupTable.init();
		recordKey = new RecordKey(joinKey,inMetadata[0]);
		recordKey.init();
		lookupTable.setLookupKey(recordKey);
	}
	
	private void setLibraryPath(String libraryPath) {
		this.libraryPath = libraryPath;
	}
	
	
	public static Node fromXML(TransformationGraph graph, org.w3c.dom.Node nodeXML) {
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(nodeXML, graph);
		DynamicJavaCode dynaTransCode = null;
		DBJoin dbjoin;
		String connectionName;
		String query;
		String[] joinKey;
		//get necessary parameters
		try{
			connectionName = xattribs.getString(XML_DBCONNECTION_ATTRIBUTE);
			query = xattribs.getString(XML_SQL_QUERY_ATTRIBUTE);
			joinKey = xattribs.getString(XML_JOIN_KEY_ATTRIBUTE).split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);
		}catch(Exception ex){
			System.err.println(COMPONENT_TYPE + ":" + ex.getMessage());
			return null;
		}

		try {
			//if transform class defined (as an attribute) use it first
			if (xattribs.exists(XML_TRANSFORM_CLASS_ATTRIBUTE)) {
				dbjoin= new DBJoin(xattribs.getString(Node.XML_ID_ATTRIBUTE),
						connectionName,query,joinKey,
						xattribs.getString(XML_TRANSFORM_CLASS_ATTRIBUTE));
				if (xattribs.exists(XML_LIBRARY_PATH_ATTRIBUTE)) {
					dbjoin.setLibraryPath(xattribs.getString(XML_LIBRARY_PATH_ATTRIBUTE));
				}
			} else {
				if (xattribs.exists(XML_JAVA_SOURCE_ATTRIBUTE)){
					dynaTransCode = new DynamicJavaCode(xattribs.getString(XML_JAVA_SOURCE_ATTRIBUTE));
				}else{
					// do we have child node wich Java source code ?
				    try {
				        dynaTransCode = DynamicJavaCode.fromXML(graph, nodeXML);
				    } catch(Exception ex) {
				        //do nothing
				    }
				}
				if (dynaTransCode != null) {
					dbjoin = new DBJoin(xattribs.getString(Node.XML_ID_ATTRIBUTE),
							connectionName,query,joinKey,dynaTransCode);
				} else { //last chance to find reformat code is in transform attribute
					if (xattribs.exists(XML_TRANSFORM_ATTRIBUTE)) {
						dbjoin = new DBJoin(xattribs.getString(Node.XML_ID_ATTRIBUTE),
								connectionName,query,joinKey,
								xattribs.getString(XML_TRANSFORM_ATTRIBUTE), true);
					} else {
						throw new RuntimeException("Can't create DynamicJavaCode object - source code not found !");
					}
				}
			}
			dbjoin.setTransformationParameters(xattribs.attributes2Properties(new String[]{XML_TRANSFORM_CLASS_ATTRIBUTE}));
			if (xattribs.exists(XML_DB_METADATA_ATTRIBUTE)){
				dbjoin.setDbMetadata(xattribs.getString(XML_DB_METADATA_ATTRIBUTE));
			}
		} catch (Exception ex) {
			System.err.println(COMPONENT_TYPE + ":" + ((xattribs.exists(XML_ID_ATTRIBUTE)) ? xattribs.getString(Node.XML_ID_ATTRIBUTE) : " unknown ID ") + ":" + ex.getMessage());
			return null;
		}
		return dbjoin;
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
	private void setDbMetadata(String dbMetadata) {
		this.metadataName = dbMetadata;
	}
	
}
