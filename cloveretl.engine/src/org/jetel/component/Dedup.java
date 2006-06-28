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

import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.RecordKey;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.TransformationGraph;
import org.jetel.util.ComponentXMLAttributes;
import org.w3c.dom.Element;

/**
 *  <h3>Dedup Component</h3>
 *
 * <!-- Removes duplicates (based on specified key) from data flow of sorted records-->
 *
 * <table border="1">
 *  <th>Component:</th>
 * <tr><td><h4><i>Name:</i></h4></td>
 * <td>Dedup</td></tr>
 * <tr><td><h4><i>Category:</i></h4></td>
 * <td></td></tr>
 * <tr><td><h4><i>Description:</i></h4></td>
 * <td>Dedup (remove duplicate records) from sorted incoming records based on specified key.<br>
 *  The key is name (or combination of names) of field(s) from input record.
 *  It keeps either First or Last record from the group based on the parameter <emp>{keep}</emp> specified.</td></tr>
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
 *  <tr><td><b>type</b></td><td>"DEDUP"</td></tr>
 *  <tr><td><b>id</b></td><td>component identification</td>
 *  <tr><td><b>dedupKey</b></td><td>field names separated by :;|  {colon, semicolon, pipe}</td>
 *  <tr><td><b>keep</b></td><td>one of "First|Last|Unique" {the fist letter is sufficient, if not defined, then First}</td></tr>
 *  <tr><td><b>equalNULL</b><br><i>optional</i></td><td>specifies whether two fields containing NULL values are considered equal. Default is TRUE.</td></tr>
 *  </table>
 *
 *  <h4>Example:</h4>
 *  <pre>&lt;Node id="DISTINCT" type="DEDUP" dedupKey="Name" keep="First"/&gt;</pre>
 *
 * @author      dpavlis
 * @since       April 4, 2002
 * @revision    $Revision$
 */
public class Dedup extends Node {

	private static final String XML_KEEP_ATTRIBUTE = "keep";
	private static final String XML_DEDUPKEY_ATTRIBUTE = "dedupKey";
	private static final String XML_EQUAL_NULL_ATTRIBUTE = "equalNULL";
	
	/**  Description of the Field */
	public final static String COMPONENT_TYPE = "DEDUP";

	private final static int WRITE_TO_PORT = 0;
	private final static int READ_FROM_PORT = 0;
	
	private final static int KEEP_FIRST = 1;
	private final static int KEEP_LAST = -1;
	private final static int KEEP_UNIQUE = 0;
	

	private int keep;
	private String[] dedupKeys;
	private RecordKey recordKey;
	private boolean equalNULLs = true;


	/**
	 *Constructor for the Dedup object
	 *
	 * @param  id         unique id of the component
	 * @param  dedupKeys  definitio of key fields used to compare records
	 * @param  keep  (1 - keep first; 0 - keep unique; -1 - keep last)
	 */
	public Dedup(String id, String[] dedupKeys, int keep) {
		super(id);
		this.keep = keep;
		this.dedupKeys = dedupKeys;
	}


	/**
	 *  Gets the change attribute of the Dedup object
	 *
	 * @param  a  Description of the Parameter
	 * @param  b  Description of the Parameter
	 * @return    The change value
	 */
	private final boolean isChange(DataRecord a, DataRecord b) {
		if (recordKey.compare(a, b) != 0) {
			return true;
		} else {
			return false;
		}
	}


	/**
	 *  Main processing method for the SimpleCopy object
	 *
	 * @since    April 4, 2002
	 */
	public void run() {
		int current;
		int previous;
		int groupItems;
		boolean isFirst = true; // special treatment for 1st record
		InputPort inPort = getInputPort(READ_FROM_PORT);
		DataRecord[] records = {new DataRecord(inPort.getMetadata()), new DataRecord(inPort.getMetadata())};
		records[0].init();
		records[1].init();
		current = 1;
		previous = 0;
		groupItems=0;
		
		while (records[current] != null && runIt) {
			try {
				records[current] = inPort.readRecord(records[current]);
				if (records[current] != null) {
				    if (isFirst) {
				        if (keep==KEEP_FIRST) {
				            writeRecordBroadcast(records[current]);
				        }
				        isFirst = false;
				    } else { 
				        if (isChange(records[current], records[previous])) {
				            switch(keep){
				            case KEEP_FIRST:
				                writeRecordBroadcast(records[current]);
				                break;
				            case KEEP_LAST:
				                writeRecordBroadcast(records[previous]);
				                break;
				            case KEEP_UNIQUE:
				                if (groupItems==1){
				                    writeRecordBroadcast(records[previous]);
				                }
				                break;
				            }
				            groupItems=0;
				        }else{
				            
				        }
				    }
				    groupItems++;
					// swap indexes
					current = current ^ 1;
					previous = previous ^ 1;
				} else {
					if (!isFirst && (keep==KEEP_LAST || (keep==KEEP_UNIQUE && groupItems==1))) {
						writeRecordBroadcast(records[previous]);
					}
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
	 *  Description of the Method
	 *
	 * @exception  ComponentNotReadyException  Description of the Exception
	 * @since                                  April 4, 2002
	 */
	public void init() throws ComponentNotReadyException {
		// test that we have at least one input port and one output
		if (inPorts.size() < 1) {
			throw new ComponentNotReadyException("At least one input port has to be defined!");
		} else if (outPorts.size() < 1) {
			throw new ComponentNotReadyException("At least one output port has to be defined!");
		}
		recordKey = new RecordKey(dedupKeys, getInputPort(READ_FROM_PORT).getMetadata());
		recordKey.init();
		// for DEDUP component, specify whether two fields with NULL value indicator set
		// are considered equal
		recordKey.setEqualNULLs(equalNULLs);
	}


	/**
	 *  Description of the Method
	 *
	 * @return    Description of the Returned Value
	 * @since     May 21, 2002
	 */
	public void toXML(Element xmlElement) {
		super.toXML(xmlElement);
		// dedupKeys attribute
		if (dedupKeys != null) {
			String keys = this.dedupKeys[0];
			for (int i=1; i<this.dedupKeys.length; i++) {
				keys += Defaults.Component.KEY_FIELDS_DELIMITER + this.dedupKeys[i];
			}
			xmlElement.setAttribute(XML_DEDUPKEY_ATTRIBUTE,keys);
		}
		
		// keep attribute
		switch(this.keep){
			case KEEP_FIRST: xmlElement.setAttribute(XML_KEEP_ATTRIBUTE, "First");
				break;
			case KEEP_LAST: xmlElement.setAttribute(XML_KEEP_ATTRIBUTE, "Last");
				break;
			case KEEP_UNIQUE: xmlElement.setAttribute(XML_KEEP_ATTRIBUTE, "Unique");
				break;
		}
		
		// equal NULL attribute
		xmlElement.setAttribute(XML_EQUAL_NULL_ATTRIBUTE, String.valueOf(equalNULLs));
	}


	/**
	 *  Description of the Method
	 *
	 * @param  nodeXML  Description of Parameter
	 * @return          Description of the Returned Value
	 * @since           May 21, 2002
	 */
	public static Node fromXML(TransformationGraph graph, org.w3c.dom.Node nodeXML) {
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(nodeXML, graph);
		Dedup dedup;
		try {
			dedup=new Dedup(xattribs.getString(Node.XML_ID_ATTRIBUTE),
					xattribs.getString(XML_DEDUPKEY_ATTRIBUTE).split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX),
					xattribs.getString(XML_KEEP_ATTRIBUTE).matches("^[Ff].*") ? KEEP_FIRST :
					    xattribs.getString(XML_KEEP_ATTRIBUTE).matches("^[Ll].*") ? KEEP_LAST : KEEP_UNIQUE);
			if (xattribs.exists(XML_EQUAL_NULL_ATTRIBUTE)){
			    dedup.setEqualNULLs(xattribs.getBoolean(XML_EQUAL_NULL_ATTRIBUTE));
			}
			
		} catch (Exception ex) {
			System.err.println(COMPONENT_TYPE + ":" + ((xattribs.exists(XML_ID_ATTRIBUTE)) ? xattribs.getString(Node.XML_ID_ATTRIBUTE) : " unknown ID ") + ":" + ex.getMessage());
			return null;
		}
		return dedup;
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

