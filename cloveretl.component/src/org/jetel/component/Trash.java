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
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.formatter.TextTableFormatter;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.InputPort;
import org.jetel.graph.InputPortDirect;
import org.jetel.graph.Node;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.util.MultiFileWriter;
import org.jetel.util.SynchronizeUtils;
import org.jetel.util.bytes.SystemOutByteChannel;
import org.jetel.util.bytes.WritableByteChannelIterator;
import org.jetel.util.file.FileUtils;
import org.jetel.util.property.ComponentXMLAttributes;
import org.w3c.dom.Element;

/**
 *  <h3>Trash Component</h3>
 *
 * <!-- All records from input port:0 are discarded or written to file. This component is deemed for debugging !  -->
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
 *  <tr><td><b>debugAppend</b><br><i>optional</i></td>
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
	private static final String XML_CHARSET_ATTRIBUTE = "charset";
	private static final String XML_DEBUGPRINT_ATTRIBUTE = "debugPrint";
	private static final String XML_DEBUGAPPEND_ATTRIBUTE = "debugAppend";
	private static final String XML_COMPRESSLEVEL_ATTRIBUTE = "compressLevel";
	
	/**  Description of the Field */
	public final static String COMPONENT_TYPE = "TRASH";
	private final static int READ_FROM_PORT = 0;
	private final static int OUTPUT_PORT = 0;
	private boolean debugPrint;
	private String debugFilename;

	private TextTableFormatter formatter;
	private MultiFileWriter writer;
	private WritableByteChannel writableByteChannel;
    private boolean debugAppend = false;
    private String charSet = Defaults.DataFormatter.DEFAULT_CHARSET_ENCODER;

	private int compressLevel = -1;

	/**
	 *Constructor for the Trash object
	 *
	 * @param  id  Description of the Parameter
	 */
	public Trash(String id) {
		super(id);
		debugPrint = false;
		debugFilename = null;
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
		if (writer != null) {
			return executeWithWriter();
		} else {
			return executeWithoutWriter();
		}
	}

	private Result executeWithWriter() throws Exception {
		InputPort inPort = getInputPort(READ_FROM_PORT);
		DataRecord record = new DataRecord(inPort.getMetadata());
		record.init();
//		int count = 0;
		while ((record = inPort.readRecord(record)) != null && runIt) {
			writer.write(record);
//				if (debugFilename == null) {
//					if (count >= TextTableFormatter.MAX_ROW_ANALYZED)
//						formatter.flush(); // if we debug into stdout
//				}
//				count++;
			SynchronizeUtils.cloverYield();
		}
		writer.finish();
		if (debugFilename != null && debugPrint){
			formatter.finish();
		}
        return runIt ? Result.FINISHED_OK : Result.ABORTED;
	}
	
	private Result executeWithoutWriter() throws Exception {
		InputPortDirect inPort = getInputPortDirect(READ_FROM_PORT);
		ByteBuffer recordBuffer = ByteBuffer.allocateDirect(Defaults.Record.MAX_RECORD_SIZE);
		
		while (inPort.readRecordDirect(recordBuffer) && runIt) {
			SynchronizeUtils.cloverYield();
		}
		
		if (debugFilename != null && debugPrint){
			formatter.finish();
		}
        return runIt ? Result.FINISHED_OK : Result.ABORTED;
	}
	
	/*
	 * (non-Javadoc)
	 * @see org.jetel.graph.GraphElement#free()
	 */
	@Override
	public synchronized void free() {
		super.free();
		if (writer != null) 
			writer.close();
	}


	/**
	 *  Description of the Method
	 *
	 * @exception  ComponentNotReadyException  Description of the Exception
	 * @since                                  April 4, 2002
	 */
	public void init() throws ComponentNotReadyException {
        if(isInitialized()) return;
		super.init();
		TransformationGraph graph = getGraph();
		
		if (debugPrint) {
            if(debugFilename != null) {
        		formatter = new TextTableFormatter(charSet);
       	        try {
					writer = new MultiFileWriter(formatter, new WritableByteChannelIterator(
							FileUtils.getWritableChannel(graph != null ? graph.getProjectURL() : null, debugFilename, 
									debugAppend, compressLevel )
					));
				} catch (IOException e) {
					e.printStackTrace();
				}
            } else {
    			if (writableByteChannel == null) {
    				formatter = new TextTableFormatter(charSet);
    		        writableByteChannel = new SystemOutByteChannel();
        	        writer = new MultiFileWriter(formatter, new WritableByteChannelIterator(writableByteChannel));
    			}
            }
            if (writer != null) {
    	        writer.setAppendData(debugAppend);
    	        writer.setDictionary(graph.getDictionary());
    	        writer.setOutputPort(getOutputPort(OUTPUT_PORT)); //for port protocol: target file writes data
                writer.init(getInputPort(READ_FROM_PORT).getMetadata());
            	formatter.showCounter("Record", "# ");
            }
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.jetel.graph.Node#reset()
	 */
	@Override
	public synchronized void reset() throws ComponentNotReadyException {
		super.reset();
		if (writer != null){
			if (debugPrint) {
	            if(debugFilename != null) {
	       	        try {
						writer.setChannels( new WritableByteChannelIterator(
								FileUtils.getWritableChannel(getGraph() != null ? getGraph().getProjectURL() : null, 
										debugFilename, debugAppend, compressLevel)
						));
					} catch (IOException e) {
						e.printStackTrace();
					}
	            } else {
	       	        writer.setChannels(new WritableByteChannelIterator(writableByteChannel));
	            }
			}
			writer.reset();
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
		if (charSet != null) {
			xmlElement.setAttribute(XML_CHARSET_ATTRIBUTE, charSet);
		}
		if (compressLevel > -1){
			xmlElement.setAttribute(XML_COMPRESSLEVEL_ATTRIBUTE,String.valueOf(compressLevel));
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
			if (xattribs.exists(XML_DEBUGAPPEND_ATTRIBUTE)) {
				trash.setDebugAppend( xattribs.getBoolean(XML_DEBUGAPPEND_ATTRIBUTE) );
			}
			if (xattribs.exists(XML_CHARSET_ATTRIBUTE)) {
				trash.setCharset( xattribs.getString(XML_CHARSET_ATTRIBUTE) );
			}
			trash.setCompressLevel(xattribs.getInteger(XML_COMPRESSLEVEL_ATTRIBUTE,-1));
			
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
   		 
    		if(!checkInputPorts(status, 1, 1)
    				|| !checkOutputPorts(status, 0, 1)) {
    			return status;
    		}

            if (debugPrint && debugFilename != null) {
                try {
                	FileUtils.canWrite(getGraph() != null ? 
                			getGraph().getProjectURL() : null, debugFilename);
                } catch (ComponentNotReadyException e) {
	                status.add(e, ConfigurationStatus.Severity.ERROR, this, 
	                		ConfigurationStatus.Priority.NORMAL, XML_DEBUGFILENAME_ATTRIBUTE);
				}
    		}
            
    		return status;
        }
	
	public String getType(){
		return COMPONENT_TYPE;
	}


	public boolean isDebugAppend() {
		return debugAppend;
	}


	public void setDebugAppend(boolean debugAppend) {
		this.debugAppend = debugAppend;
	}
	
	public void setCharset(String charSet) {
		this.charSet = charSet;
	}


	public int getCompressLevel() {
		return compressLevel;
	}


	public void setCompressLevel(int compressLevel) {
		this.compressLevel = compressLevel;
	}
	
}

