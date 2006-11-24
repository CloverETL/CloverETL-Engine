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
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPort;
import org.jetel.graph.TransformationGraph;
import org.jetel.util.ComponentXMLAttributes;
import org.jetel.util.SynchronizeUtils;
import org.w3c.dom.Element;

/**
 *  <h3>Simple Gather Component</h3>
 *
 * <!-- All records from all input ports are gathered and copied onto output port [0] -->
 *
 * <table border="1">
 *  <th>Component:</th>
 * <tr><td><h4><i>Name:</i></h4></td>
 * <td>Simple Gather</td></tr>
 * <tr><td><h4><i>Category:</i></h4></td>
 * <td></td></tr>
 * <tr><td><h4><i>Description:</i></h4></td>
 * <td>All records from all input ports are gathered and copied onto output port [0].<br>
 *  It goes port by port (waiting/blocked) if there is currently no data on port.<br>
 *  Implements inverse RoundRobin.<br>
 * </td></tr>
 * <tr><td><h4><i>Inputs:</i></h4></td>
 * <td>At least one connected output port.</td></tr>
 * <tr><td><h4><i>Outputs:</i></h4></td>
 * <td>[0]- output records (gathered)</td></tr>
 * <tr><td><h4><i>Comment:</i></h4></td>
 * <td></td></tr>
 * </table>
 *  <br>
 *  <table border="1">
 *  <th>XML attributes:</th>
 *  <tr><td><b>type</b></td><td>"SIMPLE_GATHER"</td></tr>
 *  <tr><td><b>id</b></td>
 *  <td>component identification</td>
 *  </tr>
 *  </table>
 *
 * @author      dpavlis
 * @since       April 4, 2002
 * @revision    $Revision$
 */
public class SimpleGather extends Node {

	/**  Description of the Field */
	public final static String COMPONENT_TYPE = "SIMPLE_GATHER";

	private final static int WRITE_TO_PORT = 0;

	/*
	 * how many empty loops till thead wait() is called
	 */
	private final static int NUM_EMPTY_LOOPS_TRESHOLD = 29;
	
	/*	 how many millis to wait if we reached the specified number of empty loops (when no data
	 * has been read).
	 */
	private final static int EMPTY_LOOPS_WAIT = 10 ;  

	/**
	 *Constructor for the SimpleGather object
	 *
	 * @param  id  Description of the Parameter
	 */
	public SimpleGather(String id) {
		super(id);

	}


	/**
	 *  Main processing method for the SimpleGather object
	 *
	 * @since    April 4, 2002
	 */
	public void run() {
		OutputPort outPort = getOutputPort(WRITE_TO_PORT);
		InputPort inPort;
		/*
		 *  we need to keep track of all input ports - it they contain data or
		 *  signalized that they are empty.
		 */
		int numActive;
		int emptyLoopCounter=0;
		/*
		 *  we need to keep track of all input ports - it they contain data or
		 *  signalized that they are empty.
		 */
		int readFromPort;
		boolean[] isEOF = new boolean[getInPorts().size()];
		for (int i = 0; i < isEOF.length; i++) {
			isEOF[i] = false;
		}
		InputPort inputPorts[]= (InputPort[])getInPorts().toArray(new
		        InputPort[0]);
		numActive = inputPorts.length;// counter of still active ports - those without EOF status
		// the metadata is taken from output port definition
		DataRecord record = new DataRecord(outPort.getMetadata());
		DataRecord inRecord;
		record.init();

			try{
			readFromPort = 0;
			while (runIt && numActive > 0) {
				inPort = inputPorts[readFromPort];
				if (!isEOF[readFromPort] && (inPort.hasData() || !inPort.isOpen())) {
				    emptyLoopCounter=0;
				    try {
						inRecord = inPort.readRecord(record);
						if (inRecord != null) {
							outPort.writeRecord(inRecord);
						} else {
							isEOF[readFromPort] = true;
							numActive--;
						}
					} catch (IOException ex) {
						resultMsg = ex.getMessage();
						resultCode = Node.RESULT_ERROR;
						closeAllOutputPorts();
						return;
					} catch (Exception ex) {
						resultMsg = ex.getClass().getName()+" : "+ ex.getMessage();
						resultCode = Node.RESULT_FATAL_ERROR;
						return;
					}
					SynchronizeUtils.cloverYield();
				}
				readFromPort=(++readFromPort)%(inputPorts.length);
				// have we reached the maximum empty loops count ?
				if (emptyLoopCounter>NUM_EMPTY_LOOPS_TRESHOLD) {
				    try{
				        Thread.sleep(EMPTY_LOOPS_WAIT);
				    }catch(InterruptedException ex){
				        // end processing of data
				        resultMsg="Interrputed !";
				        resultCode=Node.RESULT_ERROR;
				        return;
				    }catch(Exception ex){
				        resultMsg=ex.getMessage();
				        resultCode=Node.RESULT_FATAL_ERROR;
				        return;
				    }
                } else {
                    emptyLoopCounter++;
				}
		}
			}catch(Exception ex){
			    ex.printStackTrace();
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
			throw new ComponentNotReadyException(getId() + " at least one input port has to be defined!");
		} else if (outPorts.size() < 1) {
			throw new ComponentNotReadyException(getId() + " at least one output port has to be defined!");
		}
	}


	/**
	 *  Description of the Method
	 *
	 * @return    Description of the Returned Value
	 * @since     May 21, 2002
	 */
	public void toXML(Element xmlElement) {
		super.toXML(xmlElement);
	}


	/**
	 *  Description of the Method
	 *
	 * @param  nodeXML  Description of Parameter
	 * @return          Description of the Returned Value
	 * @since           May 21, 2002
	 */
	   public static Node fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException {
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(xmlElement, graph);

		try {
			return new SimpleGather(xattribs.getString(XML_ID_ATTRIBUTE));
		} catch (Exception ex) {
	           throw new XMLConfigurationException(COMPONENT_TYPE + ":" + xattribs.getString(XML_ID_ATTRIBUTE," unknown ID ") + ":" + ex.getMessage(),ex);
		}
	}


	/**  Description of the Method */
        @Override
        public ConfigurationStatus checkConfig(ConfigurationStatus status) {
            //TODO
            return status;
        }
	
	public String getType(){
		return COMPONENT_TYPE;
	}
}

