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
import org.jetel.graph.*;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.RecordKey;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.util.ComponentXMLAttributes;

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
 *  <tr><td><b>keep</b></td><td>one of "First|Last" {the fist letter is sufficient, if not defined, then First}</td>
 *  </tr>
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

	/**  Description of the Field */
	public final static String COMPONENT_TYPE = "DEDUP";

	private final static int WRITE_TO_PORT = 0;
	private final static int READ_FROM_PORT = 0;

	private boolean keepFirst;
	private String[] dedupKeys;
	private RecordKey recordKey;


	/**
	 *Constructor for the Dedup object
	 *
	 * @param  id         Description of the Parameter
	 * @param  dedupKeys  Description of the Parameter
	 * @param  keepFirst  Description of the Parameter
	 */
	public Dedup(String id, String[] dedupKeys, boolean keepFirst) {
		super(id);
		this.keepFirst = keepFirst;
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
		boolean isFirst = true;
		InputPort inPort = getInputPort(READ_FROM_PORT);
		DataRecord[] records = {new DataRecord(inPort.getMetadata()), new DataRecord(inPort.getMetadata())};
		DataRecord inRecord;
		records[0].init();
		records[1].init();
		current = 1;
		previous = 0;

		while (records[current] != null && runIt) {
			try {
				records[current] = inPort.readRecord(records[current]);// readRecord(READ_FROM_PORT,inRecord);
				if (records[current] != null) {
					if (isFirst) {
						if (keepFirst) {
							writeRecordBroadcast(records[current]);
						}
						isFirst = false;
					} else if (isChange(records[current], records[previous])) {
						if (keepFirst) {
							writeRecordBroadcast(records[current]);
						} else {
							writeRecordBroadcast(records[previous]);
						}
					}
					// swap indexes
					current = current ^ 1;
					previous = previous ^ 1;
				} else {
					if (!keepFirst) {
						writeRecordBroadcast(records[previous]);
					}
				}
			} catch (IOException ex) {
				resultMsg = ex.getMessage();
				resultCode = Node.RESULT_ERROR;
				closeAllOutputPorts();
				return;
			} catch (Exception ex) {
				resultMsg = ex.getMessage();
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
			return new Dedup(xattribs.getString("id"),
					xattribs.getString("dedupKey").split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX),
					xattribs.getString("keep").matches("^[Ff].*"));
		} catch (Exception ex) {
			System.err.println(ex.getMessage());
			return null;
		}
	}


	/**  Description of the Method */
	public boolean checkConfig() {
		return true;
	}
}

