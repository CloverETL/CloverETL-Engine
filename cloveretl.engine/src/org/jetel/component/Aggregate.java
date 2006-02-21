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
import java.util.Iterator;

import org.jetel.component.aggregate.AggregateFunction;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.RecordKey;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPort;
import org.jetel.util.ComponentXMLAttributes;

/**
 *  <h3>Aggregate Component</h3>
 *
 * <!-- Aggregate functions ara applied on input data flow base on specified key.-->
 *
 * <table border="1">
 *  <th>Component:</th>
 * <tr><td><h4><i>Name:</i></h4></td>
 * <td>Aggregate</td></tr>
 * <tr><td><h4><i>Category:</i></h4></td>
 * <td></td></tr>
 * <tr><td><h4><i>Description:</i></h4></td>
 * <td>Aggregate functions are applied on input data flow base on specified key.<br>
 *  The key is name (or combination of names) of field(s) from input record.
 *  Data flow can be sorted or not.</td></tr>
 * <tr><td><h4><i>Inputs:</i></h4></td>
 * <td>[0]- input records</td></tr>
 * <tr><td><h4><i>Outputs:</i></h4></td>
 * <td>At least one connected output port.</td></tr>
 * <tr><td><h4><i>Comment:</i></h4></td>
 * <td></td></tr>
 * </table>
 *  <br>
 *  <table border="1">
 *  <th>XML attributes:</th>
 *  <tr><td><b>type</b></td><td>"AGGREGATE"</td></tr>
 *  <tr><td><b>id</b></td><td>component identification</td>
 *  <tr><td><b>aggregateKey</b></td><td>field names separated by :;|  {colon, semicolon, pipe}</td>
 *  <tr><td><b>aggregateFunction</b></td><td>aggregate functions separated by :;|  {colon, semicolon, pipe} available functions are count, min, max, sum, avg, stdev</td>
 *  <tr><td><b>sorted</b></td><td>if input data flow is sorted (true)</td>
 *  </tr>
 *  </table>
 *
 *  <h4>Example:</h4>
 *  <pre>&lt;Node id="AGGREGATE_NODE" type="AGGREGATE" aggregateKey="FirstName" aggregateFunctions="count(); min(Age); avg(Salery); min(HireDate)" sorted="false" /&gt;</pre>
 *
 * @author      Martin Zatopek, OpenTech, s.r.o (www.opentech.cz)
 * @since       June 27, 2005
 * @revision    $Revision$
 */
public class Aggregate extends Node {

    private static final String XML_EQUAL_NULL_ATTRIBUTE = "equalNULL";

	public final static String COMPONENT_TYPE = "AGGREGATE";

	private final static int WRITE_TO_PORT = 0;
	private final static int READ_FROM_PORT = 0;

	private boolean sorted;
	private String[] aggregateKeys;
	private String aggregateFunctionStr;
	private RecordKey recordKey;
	private AggregateFunction aggregateFunction;
	private boolean equalNULLs;


	/**
	 *Constructor for the Aggregate object
	 *
	 * @param  id         Description of the Parameter
	 * @param  dedupKeys  Description of the Parameter
	 * @param  keepFirst  Description of the Parameter
	 */
	public Aggregate(String id, String[] aggregateKeys, String aggregateFunctions, boolean sorted) {
		super(id);
		this.sorted = sorted;
		this.aggregateKeys = aggregateKeys;
		this.aggregateFunctionStr = aggregateFunctions;
	}

	/**
	 *  Main processing method for the Aggregate object
	 *
	 */
	public void run() {
		if(sorted) {
			boolean firstLoop = true;
			InputPort inPort = getInputPort(READ_FROM_PORT);
			OutputPort outPort = getOutputPort(WRITE_TO_PORT);
			DataRecord currentRecord = new DataRecord(inPort.getMetadata());
			DataRecord previousRecord = new DataRecord(inPort.getMetadata());
			DataRecord tempRecord;
			DataRecord outRecord = new DataRecord(outPort.getMetadata());
			
			currentRecord.init();
			previousRecord.init();
			outRecord.init();
	
			while (currentRecord != null && runIt) {
				try {
					currentRecord = inPort.readRecord(currentRecord);
					if (currentRecord == null || recordKey.compare(currentRecord, previousRecord) != 0) { //next group founded
						if(!firstLoop) writeRecordBroadcast(aggregateFunction.getRecordForGroup(previousRecord, outRecord));
						else firstLoop = false;
					}
					//switch previous and current record
					if(currentRecord != null) {
						aggregateFunction.addSortedRecord(currentRecord);

						tempRecord = previousRecord;
						previousRecord = currentRecord;
						currentRecord = tempRecord;
					}
				} catch (IOException ex) {
					resultMsg = ex.getMessage();
					resultCode = Node.RESULT_ERROR;
					closeAllOutputPorts();
					return;
				} catch (Exception ex) {
					resultMsg = ex.getClass().getName()+" : "+ ex.getMessage();
					resultCode = Node.RESULT_FATAL_ERROR;
					//closeAllOutputPorts();
					return;
				}
			}
		} else { //sorted == false
			InputPort inPort = getInputPort(READ_FROM_PORT);
			OutputPort outPort = getOutputPort(WRITE_TO_PORT);
			DataRecord currentRecord = new DataRecord(inPort.getMetadata());
			DataRecord outRecord = new DataRecord(outPort.getMetadata());
	
			currentRecord.init();
			outRecord.init();
			
			try {
				//read all data from input port to aggregateRecord
				while ((currentRecord = inPort.readRecord(currentRecord)) != null && runIt) {
						aggregateFunction.addUnsortedRecord(currentRecord);
				}
				//write agragated data to outputport from aggregateRecord
				for(Iterator i = aggregateFunction.iterator(outRecord); i.hasNext(); ) {
					writeRecordBroadcast((DataRecord) i.next());
				}
			} catch (IOException ex) {
				resultMsg = ex.getMessage();
				resultCode = Node.RESULT_ERROR;
				closeAllOutputPorts();
				return;
			} catch (Exception ex) {
				resultMsg = ex.getClass().getName()+" : "+ ex.getMessage();
				resultCode = Node.RESULT_FATAL_ERROR;
				//closeAllOutputPorts();
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


	/**
	 *  Initialize method of aggregate component
	 */
	public void init() throws ComponentNotReadyException {
		// test that we have at least one input port and one output
		if (inPorts.size() != 1) {
			throw new ComponentNotReadyException("Exact one input port has to be defined!");
		} else if (outPorts.size() < 1) {
			throw new ComponentNotReadyException("At least one output port has to be defined!");
		}
		
		recordKey = new RecordKey(aggregateKeys, getInputPort(READ_FROM_PORT).getMetadata());
		recordKey.init();
		// for AGGREGATE component, specify whether two fields with NULL value indicator set
		// are considered equal
		recordKey.setEqualNULLs(equalNULLs);

		aggregateFunction = new AggregateFunction(aggregateFunctionStr, getInputPort(READ_FROM_PORT).getMetadata(), getOutputPort(WRITE_TO_PORT).getMetadata(), recordKey, sorted);
		aggregateFunction.init();
	}


	/**
	 *  Description of the Method
	 *
	 * @return    Description of the Returned Value
	 * @since     May 21, 2002
	 */
	public org.w3c.dom.Node toXML() {
		// TODO
		return null;
	}


	/**
	 *  Description of the Method
	 *
	 * @param  nodeXML  Description of Parameter
	 * @return          Description of the Returned Value
	 * @since           May 21, 2002
	 */
	public static Node fromXML(org.w3c.dom.Node nodeXML) {
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(nodeXML);

		try {
		    Aggregate agg;
			agg = new Aggregate(xattribs.getString("id"),
					xattribs.getString("aggregateKey").split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX),
					xattribs.getString("aggregateFunctions"),
					xattribs.getString("sorted").matches("^[Tt].*"));
			if (xattribs.exists(XML_EQUAL_NULL_ATTRIBUTE)){
			    agg.setEqualNULLs(xattribs.getBoolean(XML_EQUAL_NULL_ATTRIBUTE));
			}
			return agg;
		} catch (Exception ex) {
			System.err.println(COMPONENT_TYPE + ":" + ((xattribs.exists(XML_ID_ATTRIBUTE)) ? xattribs.getString(Node.XML_ID_ATTRIBUTE) : " unknown ID ") + ":" + ex.getMessage());
			return null;
		}
	}


	/**  Description of the Method */
	public boolean checkConfig() {
		return true;
	}
	
	public String getType(){
		return COMPONENT_TYPE;
	}
	
	public void setEqualNULLs(boolean equal){
	    this.equalNULLs=equal;
	}

}

