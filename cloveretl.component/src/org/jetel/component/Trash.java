/*
 * jETeL/CloverETL - Java based ETL application framework.
 * Copyright (c) Javlin, a.s. (info@cloveretl.com)
 *  
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */
package org.jetel.component;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import org.jetel.util.bytes.LogOutByteChannel;
import org.jetel.util.bytes.WritableByteChannelIterator;
import org.jetel.util.file.FileURLParser;
import org.jetel.util.file.FileUtils;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.property.RefResFlag;
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
	public enum Mode {
		FULL_DESERIALIZE, PERFORMANCE,
	}
	
	private static Log logger = LogFactory.getLog(Trash.class);

	private static final String XML_DEBUGFILENAME_ATTRIBUTE = "debugFilename";
	private static final String XML_CHARSET_ATTRIBUTE = "charset";
	private static final String XML_DEBUGPRINT_ATTRIBUTE = "debugPrint";
	private static final String XML_DEBUGAPPEND_ATTRIBUTE = "debugAppend";
	private static final String XML_COMPRESSLEVEL_ATTRIBUTE = "compressLevel";
	private static final String XML_MK_DIRS_ATTRIBUTE = "makeDirs";
	private static final String XML_PRINT_TRASH_ID_ATTRIBUTE = "printTrashID";
	private static final String XML_MODE ="mode";
	
	private static final String FULL_DESERIALIZE="full_deserialize";
	private static final String PERFORMANCE = "performance";

	/**  Description of the Field */
	public final static String COMPONENT_TYPE = "TRASH";
	private final static int READ_FROM_PORT = 0;
	private final static int OUTPUT_PORT = 0;
	private boolean debugPrint;
	private String debugFilename;
	private boolean printTrashID;
	private Mode mode;

	private TextTableFormatter formatter;
	private MultiFileWriter writer;
	private WritableByteChannel writableByteChannel;
    private boolean debugAppend = false;
    private String charSet = Defaults.DataFormatter.DEFAULT_CHARSET_ENCODER;

	private int compressLevel = -1;
	private boolean mkDir;

	/**
	 *Constructor for the Trash object
	 *
	 * @param  id  Description of the Parameter
	 */
	public Trash(String id) {
		super(id);
		debugPrint = false;
		debugFilename = null;
		this.mode = Mode.PERFORMANCE;
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
    public void preExecute() throws ComponentNotReadyException {
    	super.preExecute();
    	if (writer!=null) {
	    	if (firstRun()) {//a phase-dependent part of initialization
				writer.init(getInputPort(READ_FROM_PORT).getMetadata());
            	formatter.showCounter("Record", "# ");
            	if (printTrashID) formatter.showTrashID("Trash ID ", getId());
	    	}
	    	else {
				if (debugPrint) {
		            if(debugFilename != null) {
		       	        try {
							writer.setChannels( new WritableByteChannelIterator(
									FileUtils.getWritableChannel(getGraph() != null ? getGraph().getRuntimeContext().getContextURL() : null, 
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
    }    


	
	@Override
	public Result execute() throws Exception {
		if (writer != null) {
			return executeWithWriter();
		} else {
			return executeWithoutWriter();
		}
	}
    @Override
    public void postExecute() throws ComponentNotReadyException {
    	super.postExecute();
    	
    	try {
    		if (writer!=null) {
    			writer.close();
    		}
    	}
    	catch (Exception e) {
    		throw new ComponentNotReadyException(COMPONENT_TYPE + ": " + e.getMessage(),e);
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
		writer.close();
        return runIt ? Result.FINISHED_OK : Result.ABORTED;
	}
	
	private Result executeWithoutWriter() throws Exception {
		InputPortDirect inPort = getInputPortDirect(READ_FROM_PORT);
		DataRecord record = new DataRecord(inPort.getMetadata());
		ByteBuffer recordBuffer = ByteBuffer.allocateDirect(Defaults.Record.MAX_RECORD_SIZE);
		if(mode.equals(Mode.FULL_DESERIALIZE))
			record.init();

		while (inPort.readRecordDirect(recordBuffer) && runIt) {
			if(mode.equals(Mode.FULL_DESERIALIZE)) {
				record.deserialize(recordBuffer);
			}
			
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
			try {
				writer.close();
			} catch(Throwable t) {
				logger.warn("Resource releasing failed for '" + getId() + "'. " + t.getMessage(), t);
			}
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
		
    	// creates necessary directories
        if (mkDir) FileUtils.makeDirs(graph != null ? graph.getRuntimeContext().getContextURL() : null, 
        		new File(FileURLParser.getMostInnerAddress(debugFilename)).getParent());

		if (debugPrint) {
            if(debugFilename != null) {
        		formatter = new TextTableFormatter(charSet);
       	        try {
					writer = new MultiFileWriter(formatter, new WritableByteChannelIterator(
							FileUtils.getWritableChannel(graph != null ? graph.getRuntimeContext().getContextURL() : null, debugFilename, 
									debugAppend, compressLevel )
					));
				} catch (IOException e) {
					throw new ComponentNotReadyException(this, "Output file '" + debugFilename + "' does not exist.", e);
				}
            } else {
    			if (writableByteChannel == null) {
    				formatter = new TextTableFormatter(charSet);
    		        writableByteChannel = new LogOutByteChannel(logger, charSet);
        	        writer = new MultiFileWriter(formatter, new WritableByteChannelIterator(writableByteChannel));
    			}
            }
            if (writer != null) {
    	        writer.setAppendData(debugAppend);
    	        writer.setDictionary(graph.getDictionary());
    	        writer.setOutputPort(getOutputPort(OUTPUT_PORT)); //for port protocol: target file writes data
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
		if( mode != null || mode.equals(Mode.PERFORMANCE)) {
			xmlElement.setAttribute(XML_MODE, PERFORMANCE);
		}
		else if(mode.equals(Mode.FULL_DESERIALIZE)) {
			xmlElement.setAttribute(XML_MODE, FULL_DESERIALIZE);
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
				trash.setDebugFile(xattribs.getStringEx(XML_DEBUGFILENAME_ATTRIBUTE,RefResFlag.SPEC_CHARACTERS_OFF));
			}
			if (xattribs.exists(XML_DEBUGAPPEND_ATTRIBUTE)) {
				trash.setDebugAppend( xattribs.getBoolean(XML_DEBUGAPPEND_ATTRIBUTE) );
			}
			if (xattribs.exists(XML_CHARSET_ATTRIBUTE)) {
				trash.setCharset( xattribs.getString(XML_CHARSET_ATTRIBUTE) );
			}
			if(xattribs.exists(XML_MK_DIRS_ATTRIBUTE)) {
				trash.setMkDirs(xattribs.getBoolean(XML_MK_DIRS_ATTRIBUTE));
            }
			if(xattribs.exists(XML_PRINT_TRASH_ID_ATTRIBUTE)) {
				trash.setPrintTrashID(xattribs.getBoolean(XML_PRINT_TRASH_ID_ATTRIBUTE));
            }
			if (xattribs.exists(XML_MODE)) {
				trash.setMode(xattribs.getString(XML_MODE));
			}
			else trash.setMode(PERFORMANCE);
			
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
                	FileUtils.canWrite(getGraph() != null ? getGraph().getRuntimeContext().getContextURL() : null, debugFilename, mkDir);
                } catch (ComponentNotReadyException e) {
	                status.add(e, ConfigurationStatus.Severity.ERROR, this, 
	                		ConfigurationStatus.Priority.NORMAL, XML_DEBUGFILENAME_ATTRIBUTE);
				}
                
                try {
					if (debugAppend && FileURLParser.isArchiveURL(debugFilename) && FileURLParser.isServerURL(debugFilename)) {
					    status.add("Append true is not supported on remote archive files.", ConfigurationStatus.Severity.WARNING, this,
					    		ConfigurationStatus.Priority.NORMAL, XML_DEBUGAPPEND_ATTRIBUTE);
					}
				} catch (MalformedURLException e) {
	                status.add(e.toString(), ConfigurationStatus.Severity.ERROR, this, 
	                		ConfigurationStatus.Priority.NORMAL, XML_DEBUGAPPEND_ATTRIBUTE);
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
	
	
	/**
	 * Sets make directory.
	 * @param mkDir - true - creates output directories for output file
	 */
	private void setMkDirs(boolean mkDir) {
		this.mkDir = mkDir;
	}
	
	/**
	 * Sets whether print trash ID.
	 * @param printTrashID
	 */
	private void setPrintTrashID(boolean printTrashID) {
		this.printTrashID = printTrashID;
	}
	
	/**
	 * Sets mode
	 * @param mode
	 */
	 private void setMode(String mode) {
		 if(mode == null || mode.equalsIgnoreCase(PERFORMANCE))
			 this.mode = Mode.PERFORMANCE;
		 else if(mode.equalsIgnoreCase(FULL_DESERIALIZE))
			 this.mode = Mode.FULL_DESERIALIZE;
	 }
}

