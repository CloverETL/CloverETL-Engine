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
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;

import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.JetelException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.InputPortDirect;
import org.jetel.graph.Node;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.util.ComponentXMLAttributes;
import org.jetel.util.FileUtils;
import org.jetel.util.StringUtils;
import org.jetel.util.SynchronizeUtils;
import org.w3c.dom.Element;

/**
 *  <h3>Trash Component</h3>
 *
 * <!-- All records from input port:0 are discarded. This component is deemed for debugging !  -->
 *
 * <table border="1">
 *  <th>Component:</th>
 * <tr><td><h4><i>Name:</i></h4></td>
 * <td>Trash</td></tr>
 * <tr><td><h4><i>Category:</i></h4></td>
 * <td></td></tr>
 * <tr><td><h4><i>Description:</i></h4></td>
 * <td>All records from input port:0 are discarded.</td></tr>
 * <tr><td><h4><i>Inputs:</i></h4></td>
 * <td>[0]- input records</td></tr>
 * <tr><td><h4><i>Outputs:</i></h4></td>
 * <td><i>No output port needs to be connected.</i></td></tr>
 * <tr><td><h4><i>Comment:</i></h4></td>
 * <td></td></tr>
 * </table>
 *  <br>
 *  <table border="1">
 *  <th>XML attributes:</th>
 *  <tr><td><b>type</b></td><td>"TRASH"</td></tr>
 *  <tr><td><b>id</b></td>
 *  <td>component identification</td>
 *  </tr>
 *  <tr><td><b>debugPrint</b><br><i>optional</i></td>
 *  <td>True/False indicates whether input records should be printed to stdout. Default is False (no print).</td></tr>
 *  <tr><td><b>debugFilename</b><br><i>optional</i></td>
 *  <td>Filename - if defined, debugging output is sent to this file.</td></tr>
 *  </table>
 *
 * @author      dpavlis
 * @since       April 4, 2002
 * @revision    $Revision$
 * @see         org.jetel.graph.TransformationGraph
 * @see         org.jetel.graph.Node
 * @see         org.jetel.graph.Edge
 */
public class Trash extends Node {

	private static final String XML_DEBUGFILENAME_ATTRIBUTE = "debugFilename";
	private static final String XML_DEBUGPRINT_ATTRIBUTE = "debugPrint";
	/**  Description of the Field */
	public final static String COMPONENT_TYPE = "TRASH";
	private final static int READ_FROM_PORT = 0;
	private boolean debugPrint;
	private String debugFilename;
	private PrintWriter outStream;

	private ByteBuffer recordBuffer;


	/**
	 *Constructor for the Trash object
	 *
	 * @param  id  Description of the Parameter
	 */
	public Trash(String id) {
		super(id);
		debugPrint = false;
		debugFilename = null;
		outStream = null;

	}


	/**
	 *  Switches on/off printing of incoming records
	 *
	 * @param  print  The new debugPrint value
	 * @since         April 4, 2002
	 */
	public void setDebugPrint(boolean print) {
		debugPrint = print;
	}

    public boolean isDebugPrint() {
        return debugPrint;
    }

	/**
	 *  Sets the debugFile attribute of the Trash object
	 *
	 * @param  filename  The new debugFile value
	 */
	public void setDebugFile(String filename) {
		debugFilename = filename;
	}

	@Override
	public Result execute() throws Exception {
		int recCounter = 0;
		InputPortDirect inPort = (InputPortDirect) getInputPort(READ_FROM_PORT);
		boolean isData = true;
		DataRecord dataRecord = null;
		if (outStream != null) {
			dataRecord = new DataRecord(getInputPort(READ_FROM_PORT).getMetadata());
			dataRecord.init();
		}
		String resultMsg = null;
		try {
			while (isData && runIt) {
				isData = inPort.readRecordDirect(recordBuffer);
				if (outStream != null && isData) {
					dataRecord.deserialize(recordBuffer);
					outStream.println("*** Record# " + recCounter++ + " ***");
					outStream.print(dataRecord);
					if (debugFilename == null)
						outStream.flush(); // if we debug into stdout
				}
				SynchronizeUtils.cloverYield();
			}
		} catch (Exception ex) {
			resultMsg = ex.getClass().getName() + " : " + ex.getMessage();
			throw new JetelException(resultMsg);
		} finally {
	        // close debug file
			if (outStream != null && debugFilename != null) { // debug is
																// routed into
																// file
				outStream.println("EOF with result " + resultMsg == null ? "OK"
						: resultMsg);
				outStream.close();
			}
			broadcastEOF();
		}
        return runIt ? Result.FINISHED_OK : Result.ABORTED;
	}

	/**
	 *  Description of the Method
	 *
	 * @exception  ComponentNotReadyException  Description of the Exception
	 * @since                                  April 4, 2002
	 */
	public void init() throws ComponentNotReadyException {
		super.init();
		// test that we have at least one input port and one output
		recordBuffer = ByteBuffer.allocateDirect(Defaults.Record.MAX_RECORD_SIZE);
		if (recordBuffer == null) {
			throw new ComponentNotReadyException("Can NOT allocate internal record buffer ! Required size:" +
					Defaults.Record.MAX_RECORD_SIZE);
		}
		if (debugPrint) {
            if(debugFilename != null) {
          		try {
    				outStream = new PrintWriter(Channels.newWriter(FileUtils.getWritableChannel(getGraph() != null ? getGraph().getProjectURL() : null, debugFilename, false), Defaults.DataFormatter.DEFAULT_CHARSET_ENCODER));
    			} catch (IOException ex) {
    				throw new ComponentNotReadyException(ex.getMessage());
    			}
            } else {
                outStream = new PrintWriter(System.out);
            }
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
		xmlElement.setAttribute(XML_DEBUGPRINT_ATTRIBUTE, String.valueOf(this.debugPrint));
		if (debugFilename != null) {
			xmlElement.setAttribute(XML_DEBUGFILENAME_ATTRIBUTE,this.debugFilename);
		}
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
		Trash trash;

		try {
			trash = new Trash(xattribs.getString(XML_ID_ATTRIBUTE));
			if (xattribs.exists(XML_DEBUGPRINT_ATTRIBUTE)) {
				trash.setDebugPrint(xattribs.getBoolean(XML_DEBUGPRINT_ATTRIBUTE));
			}
			if (xattribs.exists(XML_DEBUGFILENAME_ATTRIBUTE)) {
				trash.setDebugFile(xattribs.getString(XML_DEBUGFILENAME_ATTRIBUTE));
			}
		} catch (Exception ex) {
	           throw new XMLConfigurationException(COMPONENT_TYPE + ":" + xattribs.getString(XML_ID_ATTRIBUTE," unknown ID ") + ":" + ex.getMessage(),ex);
		}
		return trash;
	}


	/**
	 *  Description of the Method
	 *
	 * @return    Description of the Return Value
	 */
        @Override
        public ConfigurationStatus checkConfig(ConfigurationStatus status) {
    		super.checkConfig(status);
   		 
    		checkInputPorts(status, 1, 1);
            checkOutputPorts(status, 0, 0);

            try {
        		recordBuffer = ByteBuffer.allocateDirect(Defaults.Record.MAX_RECORD_SIZE);
        		if (recordBuffer == null) {
        			throw new ComponentNotReadyException("Can NOT allocate internal record buffer ! Required size:" +
        					Defaults.Record.MAX_RECORD_SIZE);
        		}
        		recordBuffer = null;
        		if (debugPrint) {
                    if(debugFilename != null && !FileUtils.canWrite(
                    		getGraph() != null ? getGraph().getProjectURL() : null, 
                    				debugFilename)) {
                		ComponentNotReadyException ex = new ComponentNotReadyException(this,"Can't write to file: " + debugFilename);
                		ex.setAttributeName(XML_DEBUGFILENAME_ATTRIBUTE);
                		throw ex;
                    }
                 }
//                init();
//                free();
            } catch (ComponentNotReadyException e) {
                ConfigurationProblem problem = new ConfigurationProblem(e.getMessage(), ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL);
                if(!StringUtils.isEmpty(e.getAttributeName())) {
                    problem.setAttributeName(e.getAttributeName());
                }
                status.add(problem);
            }
            
            return status;
        }
	
	public String getType(){
		return COMPONENT_TYPE;
	}
}

