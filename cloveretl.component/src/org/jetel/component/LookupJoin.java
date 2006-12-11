
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
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.RecordKey;
import org.jetel.data.lookup.LookupTable;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.TransformException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.ComponentXMLAttributes;
import org.w3c.dom.Element;

/**
 * @author avackova (agata.vackova@javlinconsulting.cz) ; 
 * (c) JavlinConsulting s.r.o.
 *  www.javlinconsulting.cz
 *
 * @since Dec 11, 2006
 *
 */
public class LookupJoin extends Node {

	private static final String XML_LOOKUP_TABLE_ATTRIBUTE = "lookupTable";
	private static final String XML_JOIN_KEY_ATTRIBUTE = "joinKey";
	private static final String XML_TRANSFORM_CLASS_ATTRIBUTE = "transformClass";
	private static final String XML_TRANSFORM_ATTRIBUTE = "transform";
	private static final String XML_LEFTOUTERJOIN_ATTRIBUTE = "leftOuterJoin";

	public final static String COMPONENT_TYPE = "LOOKUP_JOIN";
	
	private final static int WRITE_TO_PORT = 0;
	private final static int READ_FROM_PORT = 0;
	
	private String transformClassName = null;
	private RecordTransform transformation = null;
	private String transformSource = null;
	private String lookupTableName;
	
	private String[] joinKey;
	private boolean leftOuterJoin = false;

	private Properties transformationParameters=null;
	
	private LookupTable lookupTable;
	private RecordKey recordKey;
	private DataRecordMetadata lookupMetadata;
	
	static Log logger = LogFactory.getLog(Reformat.class);

	public LookupJoin(String id, String lookupTableName, String[] joinKey, 
			String transform, String transformClass){
		super(id);
		this.lookupTableName = lookupTableName;
		this.joinKey = joinKey;
		this.transformClassName = transformClass;
		this.transformSource = transform;
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.graph.Node#getType()
	 */
	@Override
	public String getType() {
		return COMPONENT_TYPE;
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.Node#run()
	 */
	@Override
	public void run() {
		//initialize in and out records
		InputPort inPort=getInputPort(WRITE_TO_PORT);
		DataRecord[] outRecord = {new DataRecord(getOutputPort(READ_FROM_PORT).getMetadata())};
		outRecord[0].init();
		DataRecord inRecord = new DataRecord(inPort.getMetadata());
		inRecord.init();
		DataRecord[] inRecords = new DataRecord[] {inRecord,null};
		while (inRecord!=null && runIt) {
			try {
				inRecord = inPort.readRecord(inRecord);
				if (inRecord!=null) {
					//find slave record in database
					inRecords[1] = lookupTable.get(inRecord);
					do{
						if ((inRecords[1] != null || leftOuterJoin ) && 
								transformation.transform(inRecords, outRecord)) {
							writeRecord(WRITE_TO_PORT,outRecord[0]);
						}
						//get next record from database with the same key
						inRecords[1] = lookupTable.getNext();					
					}while (inRecords[1] != null);
				}
            } catch (TransformException ex) {
                resultMsg = "Error occurred in nested transformation: " + ex.getMessage();
                resultCode = Node.RESULT_ERROR;
                closeAllOutputPorts();
                return;
			} catch (IOException ex) {
				resultMsg = ex.getMessage();
				resultCode = Node.RESULT_ERROR;
				closeAllOutputPorts();
				return;
			}catch (RuntimeException ex){
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
    @Override
    public ConfigurationStatus checkConfig(ConfigurationStatus status) {
        //TODO
        return status;
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
		//Initializing lookup table
        lookupTable = getGraph().getLookupTable(lookupTableName);
        if (lookupTable == null){
        	throw new ComponentNotReadyException("Lookup table \"" + lookupTableName + 
        			"\" not found.");
        }
		lookupTable.init();
        lookupMetadata = lookupTable.getMetadata();
		DataRecordMetadata inMetadata[]={ getInputPort(READ_FROM_PORT).getMetadata(),lookupMetadata};
		DataRecordMetadata outMetadata[]={getOutputPort(WRITE_TO_PORT).getMetadata()};
        try {
			recordKey = new RecordKey(joinKey, inMetadata[0]);
			recordKey.init();
			lookupTable.setLookupKey(recordKey);
			transformation = RecordTransformFactory.createTransform(
					transformSource, transformClassName, this, inMetadata,
					outMetadata, transformationParameters);
		} catch (Exception e) {
			throw new ComponentNotReadyException(this, e);
		}
	}
    
    public static Node fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException {
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(xmlElement, graph);
		LookupJoin join;
		String[] joinKey;
		//get necessary parameters
		try{
			joinKey = xattribs.getString(XML_JOIN_KEY_ATTRIBUTE).split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);
		
            join = new LookupJoin( xattribs.getString(XML_ID_ATTRIBUTE),
                    xattribs.getString(XML_LOOKUP_TABLE_ATTRIBUTE),joinKey,
                    xattribs.getString(XML_TRANSFORM_ATTRIBUTE, null), 
                    xattribs.getString(XML_TRANSFORM_CLASS_ATTRIBUTE, null));
			join.setTransformationParameters(xattribs.attributes2Properties(new String[]{XML_TRANSFORM_CLASS_ATTRIBUTE}));
			if (xattribs.exists(XML_LEFTOUTERJOIN_ATTRIBUTE)){
				join.setLeftOuterJoin(xattribs.getBoolean(XML_LEFTOUTERJOIN_ATTRIBUTE));
			}
		} catch (Exception ex) {
            throw new XMLConfigurationException(COMPONENT_TYPE + ":" + xattribs.getString(XML_ID_ATTRIBUTE," unknown ID ") + ":" + ex.getMessage(),ex);
        }
        
		return join;
	}
    
    /**
     * @param transformationParameters The transformationParameters to set.
     */
    public void setTransformationParameters(Properties transformationParameters) {
        this.transformationParameters = transformationParameters;
    }

	private void setLeftOuterJoin(boolean leftOuterJoin) {
		this.leftOuterJoin = leftOuterJoin;
	}

}
