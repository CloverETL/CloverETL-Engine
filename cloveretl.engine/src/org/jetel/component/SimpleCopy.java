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
import org.w3c.dom.NamedNodeMap;
import org.jetel.graph.*;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.exception.ComponentNotReadyException;

/**
 *  <h3>Simple Copy Component</h3>
 *
 * <!-- All records from input port:0 are copied onto all connected output ports (multiplies number of records by number of defined
 * output ports -->
 * 
 * <table border="1">
 *  <th>Component:</th>
 * <tr><td><h4><i>Name:</i></h4></td>
 * <td>Simple Copy</td></tr>
 * <tr><td><h4><i>Category:</i></h4></td>
 * <td></td></tr>
 * <tr><td><h4><i>Description:</i></h4></td>
 * <td>All records from input port:0 are copied onto all connected output ports.</td></tr>
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
 *  <tr><td><b>type</b></td><td>"SIMPLE_COPY"</td></tr>
 *  <tr><td><b>id</b></td>
 *  <td>component identification</td>
 *  </tr>
 *  </table>  
 *
 * @author     dpavlis
 * @since    April 4, 2002
 * @revision   $Revision$
 * @see		org.jetel.graph.TransformationGraph
 * @see		org.jetel.graph.Node
 * @see 	org.jetel.graph.Edge
 */
public class SimpleCopy extends Node {

	public static final String COMPONENT_TYPE="SIMPLE_COPY";
	private static final int READ_FROM_PORT=0;
	
	/* not really needed as record gets broadcasted to all defined output ports */
	private static final int WRITE_TO_PORT=0;
	
	private ByteBuffer recordBuffer;
	
	public SimpleCopy(String id){
		super(id);
		
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
		InputPortDirect inPort=(InputPortDirect)getInputPort(READ_FROM_PORT);
		boolean isData=true;
		while(isData && runIt){
			try{
				isData=inPort.readRecordDirect(recordBuffer);
				if(isData){
					writeRecordBroadcastDirect(recordBuffer);
				}
			}catch(IOException ex){
				resultMsg=ex.getMessage();
				resultCode=Node.RESULT_ERROR;
				closeAllOutputPorts();
				return;
			}catch(Exception ex){
				resultMsg=ex.getMessage();
				resultCode=Node.RESULT_FATAL_ERROR;
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
		NamedNodeMap attribs=nodeXML.getAttributes();
		
		if (attribs!=null){
			String id=attribs.getNamedItem("id").getNodeValue();
			if (id!=null){
				return new SimpleCopy(id);
			}
		}
		return null;
	}

}

