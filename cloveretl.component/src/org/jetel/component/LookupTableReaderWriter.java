
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

import org.jetel.data.DataRecord;
import org.jetel.data.lookup.LookupTable;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.exception.ConfigurationStatus.Priority;
import org.jetel.exception.ConfigurationStatus.Severity;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.util.SynchronizeUtils;
import org.jetel.util.property.ComponentXMLAttributes;
import org.w3c.dom.Element;

/**
 *  <h3>LookupTableReaderWriter Component</h3> <!--  Reads/writes records from/to 
 *  	lookup table. -->
 *
 * <table border="1">
 *
 *    <th>
 *      Component:
 *    </th>
 *    <tr><td>
 *        <h4><i>Name:</i> </h4></td><td>LookupTableReaderWriter</td>
 *    </tr>
 *    <tr><td><h4><i>Category:</i> </h4></td><td></td>
 *    </tr>
 *    <tr><td><h4><i>Description:</i> </h4></td>
 *    Depending on ports connected (if there is any output port read mode is turned on)
 *    records are read from lookup table or put to it. 
 *      <td>
 *      </td>
 *    </tr>
 *    <tr><td><h4><i>Inputs:</i> </h4></td>
 *    <td>
 *        [0] - input records<br>
 *    </td></tr>
 *    <tr><td> <h4><i>Outputs:</i> </h4>
 *      </td>
 *      <td>
 *        [0] - records from lookup table
 *      </td></tr>
 *    <tr><td><h4><i>Comment:</i> </h4>
 *      </td>
 *      <td></td>
 *    </tr>
 *  </table>
 *  <br>
 *  <table border="1">
 *    <th>XML attributes:</th>
 *    <tr><td><b>type</b></td><td>"LOOKUP_TABLE_READER_WRITER"</td></tr>
 *    <tr><td><b>id</b></td><td>component identification</td></tr>
 * <td><b>lookupTable</b></td>
 * <td>name of lookup table for reading or writing records</td>
 *  <tr><td><b>freeLookupTable</b><i>optional</i><td>true/false<I> default: FALSE</I> idicates if close lookup table after 
 *  	finishing execute() method. All records, which are stored only in memory will be lost</td>
 *    </table>
 *    <h4>Example:</h4> <pre>
 * &lt;Node id="READ" type="LOOKUP_TABLE_READER_WRITER"&gt;
 * &lt;attr name="lookupTable"&gt;LookupTable1&lt;/attr&gt;
 * &lt;/Node&gt;
 *    
 * @author avackova (agata.vackova@javlinconsulting.cz) ; 
 * (c) JavlinConsulting s.r.o.
 *  www.javlinconsulting.cz
 *
 * @since Dec 11, 2006
 *
 */
public class LookupTableReaderWriter extends Node {

	private static final String XML_LOOKUP_TABLE_ATTRIBUTE = "lookupTable";
	private static final String XML_FREE_LOOKUP_TABLE_ATTRIBUTE = "freeLookupTable";

	public final static String COMPONENT_TYPE = "LOOKUP_TABLE_READER_WRITER";

	private String lookupTableName;
	
	private final static int READ_FROM_PORT = 0;
	
	private boolean readFromTable = false;
	private boolean writeToTable = false;
	private LookupTable lookupTable;
	private boolean freeLookupTable;

	public LookupTableReaderWriter(String id, String lookupTableName, boolean freeLookupTable) {
		super(id);
		this.lookupTableName = lookupTableName;
		this.freeLookupTable = freeLookupTable;
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.Node#getType()
	 */
	@Override
	public String getType() {
		return COMPONENT_TYPE;
	}


	@Override
	public void init() throws ComponentNotReadyException {
        if(isInitialized()) return;
        super.init();
        
		//set reading/writing mode
		readFromTable = outPorts.size() > 0;
		writeToTable = inPorts.size() > 0;
		
		//init lookup table
		lookupTable = getGraph().getLookupTable(lookupTableName);
		if (lookupTable == null) {
        	throw new ComponentNotReadyException("Lookup table \"" + lookupTableName + 
			"\" not found.");
		}
		if (!lookupTable.isInitialized()) {
			lookupTable.init();
		}		
	}
	
	@Override
	public synchronized void reset() throws ComponentNotReadyException {
		super.reset();
	}
	
	@Override
	public Result execute() throws Exception {
		if (writeToTable) {//putting records to lookup table
			InputPort inPort = getInputPort(READ_FROM_PORT);
			DataRecord inRecord = new DataRecord(inPort.getMetadata());
			inRecord.init();
			while ((inRecord = inPort.readRecord(inRecord)) != null && runIt) {
				lookupTable.put(inRecord);
				SynchronizeUtils.cloverYield();
			}			
		}
		if (readFromTable) {
			//for each record from lookup table send to to the edge
			for (DataRecord record : lookupTable) {
				if (!runIt) break;
				writeRecordBroadcast(record);
				SynchronizeUtils.cloverYield();
			}
		}		
		broadcastEOF();
        return runIt ? Result.FINISHED_OK : Result.ABORTED;
	}
	
	@Override
	public void free() {
        if(!isInitialized()) return;
		super.free();
		
		if (freeLookupTable && lookupTable != null){
			lookupTable.free();
		}
	}
	
    public static Node fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException {
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(xmlElement, graph);
		try{
			return new LookupTableReaderWriter(xattribs.getString(XML_ID_ATTRIBUTE), 
					xattribs.getString(XML_LOOKUP_TABLE_ATTRIBUTE), 
					xattribs.getBoolean(XML_FREE_LOOKUP_TABLE_ATTRIBUTE, false));
		} catch (Exception ex) {
            throw new XMLConfigurationException(COMPONENT_TYPE + ":" + xattribs.getString(XML_ID_ATTRIBUTE," unknown ID ") + ":" + ex.getMessage(),ex);
        }
 	}
    
	public void toXML(org.w3c.dom.Element xmlElement) {
		super.toXML(xmlElement);
		xmlElement.setAttribute(XML_LOOKUP_TABLE_ATTRIBUTE,this.lookupTable.getId());
		xmlElement.setAttribute(XML_FREE_LOOKUP_TABLE_ATTRIBUTE, String.valueOf(freeLookupTable));
	}
    
	@Override
	public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		super.checkConfig(status);
 
		if(!checkInputPorts(status, 0, 1)
				|| !checkOutputPorts(status, 0, Integer.MAX_VALUE)) {
			return status;
		}

        lookupTable = getGraph().getLookupTable(lookupTableName);
		if (lookupTable == null) {
            ConfigurationProblem problem = new ConfigurationProblem("Lookup table \"" + lookupTableName + 
        			"\" not found.", ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL);
             problem.setAttributeName(XML_LOOKUP_TABLE_ATTRIBUTE);
             status.add(problem);
		}

        return status;
	}
}
