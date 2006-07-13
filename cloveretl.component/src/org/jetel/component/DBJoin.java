
/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2005-06  David Pavlis <david_pavlis@hotmail.com>
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
	private String[] slaveKey = null;
	private String connectionName;
	private String query;
	private String metadataName;

	private Properties transformationParameters=null;
	
	private DBLookupTable lookupTable;
	private RecordKey recordKey;
	private DataRecordMetadata dbMetadata;
	
	static Log logger = LogFactory.getLog(Reformat.class);
	
	/**
	 * @param id
	 */
	public DBJoin(String id,String connectionName,String query,String[] joinKey,
			String metadata){
		super(id);
		this.connectionName = connectionName;
		this.query = query;
		this.joinKey = joinKey;
		this.metadataName = metadata;
	}
	
	public DBJoin(String id,String connectionName,String query, String[] joinKey, 
			String metadata, String transformClass) {
		this(id,connectionName,query,joinKey,metadata);
		this.transformClassName = transformClass;
	}

	/**
	 *Constructor for the DBJoin object
	 *
	 * @param  id              unique identification of component
	 * @param  transform       source of transformation in internal format
	 */
	public DBJoin(String id, String connectionName,String query, String[] joinKey,
			String metadata, String transform, boolean distincter) {
		this(id,connectionName,query,joinKey,metadata);
		this.transformSource = transform;
	}

	/**
	 *Constructor for the DBJoin object
	 *
	 * @param  id              unique identification of component
	 * @param  transformClass  Object of class implementing RecordTransform interface
	 */
	public DBJoin(String id,String connectionName,String query, String[] joinKey, 
			String metadata, RecordTransform transformClass) {
		this(id,connectionName,query,joinKey,metadata);
		this.transformation = transformClass;
	}

	/**
	 *Constructor for the DBJoin object
	 *
	 * @param  id           unique identification of component
	 * @param  dynamicCode  DynamicJavaCode object
	 */
	public DBJoin(String id,String connectionName,String query, String[] joinKey, 
			String metadata, DynamicJavaCode dynamicCode) {
		this(id,connectionName,query,joinKey,metadata);
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
		InputPort inPort=getInputPort(WRITE_TO_PORT);
		DataRecord inRecord = new DataRecord(inPort.getMetadata());
		inRecord.init();
		DataRecord[] outRecord = {new DataRecord(getOutputPort(READ_FROM_PORT).getMetadata())};
		outRecord[0].init();
		DataRecord tmpRecord = new DataRecord(dbMetadata);
		tmpRecord.init();
		DataRecord[] inRecords = {inRecord,tmpRecord};
		while (inRecord!=null && runIt) {
			try {
				inRecord = inPort.readRecord(inRecord);// readRecord(READ_FROM_PORT,inRecord);
				if (inRecord!=null) {
					tmpRecord = lookupTable.get(inRecord);
					while (tmpRecord!=null){
						if (transformation.transform(inRecords, outRecord)) {
							writeRecord(WRITE_TO_PORT,outRecord[0]);
						}
						tmpRecord = lookupTable.getNext();
					}
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
        if (dbMetadata == null){
            throw new ComponentNotReadyException("Can't find Metadta ID: " + metadataName);
        }
        lookupTable = new DBLookupTable("LOOKUP_TABLE_FROM_"+XML_ID_ATTRIBUTE,(DBConnection) conn,dbMetadata,query);
		lookupTable.init();
		if (slaveKey==null){
			slaveKey = joinKey;
		}
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
		String metadata;
		
		try{
			connectionName = xattribs.getString(XML_DBCONNECTION_ATTRIBUTE);
			query = xattribs.getString(XML_SQL_QUERY_ATTRIBUTE);
			joinKey = xattribs.getString(XML_JOIN_KEY_ATTRIBUTE).split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);
			metadata = xattribs.getString(XML_DB_METADATA_ATTRIBUTE);
		}catch(Exception ex){
			System.err.println(COMPONENT_TYPE + ":" + ex.getMessage());
			return null;
		}

		try {
			//if transform class defined (as an attribute) use it first
			if (xattribs.exists(XML_TRANSFORM_CLASS_ATTRIBUTE)) {
				dbjoin= new DBJoin(xattribs.getString(Node.XML_ID_ATTRIBUTE),
						connectionName,query,joinKey,metadata,
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
							connectionName,query,joinKey,metadata,dynaTransCode);
				} else { //last chance to find reformat code is in transform attribute
					if (xattribs.exists(XML_TRANSFORM_ATTRIBUTE)) {
						dbjoin = new DBJoin(xattribs.getString(Node.XML_ID_ATTRIBUTE),
								connectionName,query,joinKey,metadata,
								xattribs.getString(XML_TRANSFORM_ATTRIBUTE), true);
					} else {
						throw new RuntimeException("Can't create DynamicJavaCode object - source code not found !");
					}
				}
			}
			dbjoin.setTransformationParameters(xattribs.attributes2Properties(new String[]{XML_TRANSFORM_CLASS_ATTRIBUTE}));
//			if (xattribs.exists(XML_SLAVE_OVERWRITE_KEY_ATTRIBUTE)){
//				dbjoin.setSlaveKey(xattribs.getString(XML_SLAVE_OVERWRITE_KEY_ATTRIBUTE).split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX));
//			}
//			
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
	
}
