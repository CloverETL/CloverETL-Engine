/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2002  David Pavlis
*
*    This program is free software; you can redistribute it and/or modify
*    it under the terms of the GNU General Public License as published by
*    the Free Software Foundation; either version 2 of the License, or
*    (at your option) any later version.
*    This program is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
*    GNU General Public License for more details.
*
*    You should have received a copy of the GNU General Public License
*    along with this program; if not, write to the Free Software
*    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/

package org.jetel.component;

import java.util.*;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.BufferOverflowException;
import org.w3c.dom.NamedNodeMap;
import org.jetel.graph.*;
import org.jetel.data.DataRecord;
import org.jetel.data.SortDataRecordInternal;
import org.jetel.data.Defaults;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.util.ComponentXMLAttributes;
/**
  *  <h3>Sort Component</h3>
 *
 * <!-- Sorts the incoming records based on specified key -->
 * 
 * <table border="1">
 *  <th>Component:</th>
 * <tr><td><h4><i>Name:</i></h4></td>
 * <td>Sort</td></tr>
 * <tr><td><h4><i>Category:</i></h4></td>
 * <td></td></tr>
 * <tr><td><h4><i>Description:</i></h4></td>
 * <td>Sorts the incoming records based on specified key.<br>
 *  The key is name (or combination of names) of field(s) from input record.
 *  The sort order is either Ascending (default) or Descending.</td></tr>
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
 *  <tr><td><b>type</b></td><td>"SORT"</td></tr>
 *  <tr><td><b>id</b></td><td>component identification</td>
 *  <tr><td><b>sortKey</b></td><td>field names separated by :;|  {colon, semicolon, pipe}</td>
 *  <tr><td><b>sortOrder</b></td><td>one of "Ascending|Descending" {the fist letter is sufficient, if not defined, then Ascending}</td>
 *  </tr>
 *  </table>  
 * 
 *  <h4>Example:</h4>
 *  <pre>&lt;Node id="SORT_CUSTOMER" type="SORT" sortKey="Name:Address" sortOrder="A"/&gt;</pre>
 *
 * @author     dpavlis
 * @since    April 4, 2002
 */
public class Sort extends Node {

	public static final String COMPONENT_TYPE="SORT";
	
	private static final int WRITE_TO_PORT = 0;
	private static final int READ_FROM_PORT = 0;

	private SortDataRecordInternal sorter;
	private boolean sortOrderAscending;
	private String[] sortKeys;
	private ByteBuffer recordBuffer;
	
	public Sort(String id,String[] sortKeys, boolean sortOrder){
		super(id);
		this.sortOrderAscending=sortOrder;
		this.sortKeys=sortKeys;
	}
	
	
	/**
	 *  Gets the Type attribute of the SimpleCopy object
	 *
	 * @return    The Type value
	 * @since     April 4, 2002
	 */
	public String getType() {
		return COMPONENT_TYPE;
	}


	/**
	 *  Main processing method for the SimpleCopy object
	 *
	 * @since    April 4, 2002
	 */
	public void run() {
		InputPort inPort=getInputPort(READ_FROM_PORT);
		DataRecord inRecord=new DataRecord(inPort.getMetadata());
		inRecord.init();

		while(inRecord!=null && runIt){
			try{
				inRecord=inPort.readRecord(inRecord); // readRecord(READ_FROM_PORT,inRecord);
				if (inRecord!=null){
					sorter.put(inRecord);
				}
			}catch(BufferOverflowException ex){
				resultMsg="Buffer Overflow";
				resultCode=Node.RESULT_ERROR;
				closeAllOutputPorts();
				return;
			}catch(IOException ex){
				resultMsg=ex.getMessage();
				resultCode=Node.RESULT_ERROR;
				closeAllOutputPorts();
				return;
			}catch(Exception ex){
				resultMsg=ex.getMessage();
				resultCode=Node.RESULT_FATAL_ERROR;
				//closeAllOutputPorts();
				return;
			}
		}
		// sort the records now
		try{
			sorter.sort();
		}catch(Exception ex){
			resultMsg="Error when sorting: "+ex.getMessage();
			resultCode=Node.RESULT_FATAL_ERROR;
			//closeAllOutputPorts();
			return;
		}
		// we read directly into buffer so we don't waste time with deserialization of record
		// it will happen if needed on the other side
		recordBuffer.clear();
		while( sorter.getNext(recordBuffer) && runIt){
			try{
				writeRecordBroadcastDirect(recordBuffer);
				recordBuffer.clear();
			}catch(IOException ex){
				resultMsg=ex.getMessage();
				resultCode=Node.RESULT_ERROR;
				closeAllOutputPorts();
				return;
			}catch(Exception ex){
				resultMsg=ex.getMessage();
				resultCode=Node.RESULT_FATAL_ERROR;
				//closeAllOutputPorts();
				return;
			}
		}
		broadcastEOF();
		if (runIt) resultMsg="OK"; else resultMsg="STOPPED";
		resultCode=Node.RESULT_OK;
	}	


	/**
	 *  Description of the Method
	 *
	 * @since    April 4, 2002
	 */
	public void init() throws ComponentNotReadyException {
		// test that we have at least one input port and one output
		if (inPorts.size()<1){
			throw new ComponentNotReadyException("At least one input port has to be defined!");
		}else if (outPorts.size()<1){
			throw new ComponentNotReadyException("At least one output port has to be defined!");
		}
		recordBuffer=ByteBuffer.allocateDirect(Defaults.Record.MAX_RECORD_SIZE);
		if (recordBuffer==null){
			throw new ComponentNotReadyException("Can NOT allocate internal record buffer ! Required size:"+
			Defaults.Record.MAX_RECORD_SIZE);
		}
		
		sorter=new SortDataRecordInternal(getInputPort(READ_FROM_PORT).getMetadata(), sortKeys, sortOrderAscending);
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
		ComponentXMLAttributes xattribs=new ComponentXMLAttributes(nodeXML);

		try{
			return new Sort(xattribs.getString("id"),
				xattribs.getString("sortKey").split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX),
				xattribs.getString("sortOrder").matches("^[Aa].*"));
		}catch(Exception ex){
			System.err.println(ex.getMessage());
			return null;
		}
		
	}
	
}

