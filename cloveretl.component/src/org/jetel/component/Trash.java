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
import java.nio.channels.WritableByteChannel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.Defaults;
import org.jetel.data.formatter.TextTableFormatter;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.ConfigurationStatus.Priority;
import org.jetel.exception.ConfigurationStatus.Severity;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.InputPort;
import org.jetel.graph.InputPortDirect;
import org.jetel.graph.Node;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.runtime.tracker.ComponentTokenTracker;
import org.jetel.graph.runtime.tracker.ReaderWriterComponentTokenTracker;
import org.jetel.util.MultiFileWriter;
import org.jetel.util.bytes.CloverBuffer;
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
		VALIDATE_RECORDS, PERFORMANCE,
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
	
	private static final String VALIDATE_RECORDS="validate_records";
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
	 * Constructor for the Trash object
	 *
	 * @param  id  Description of the Parameter
	 */
	public Trash(String id) {
		super(id);
		debugPrint = false;
		debugFilename = null;
		this.mode = Mode.PERFORMANCE;
	}
	
	protected boolean checkPortNumbers(ConfigurationStatus status) {
		return checkInputPorts(status, 1, Integer.MAX_VALUE, false) && checkOutputPorts(status, 0, 1);
	}
	
	@Override
	public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		super.checkConfig(status);

		if (!checkPortNumbers(status)) {
			return status;
		}

		if (debugPrint) {
			if (inPorts.size() > 1) {
				status.add("Debug printing is supported with only one input port connected.", Severity.WARNING, this, Priority.NORMAL, XML_DEBUGAPPEND_ATTRIBUTE);
			}
			
			if (debugFilename != null) {
				try {
					FileUtils.canWrite(getGraph() != null ? getGraph().getRuntimeContext().getContextURL() : null, debugFilename, mkDir);
				} catch (ComponentNotReadyException e) {
					status.add(e, Severity.ERROR, this, Priority.NORMAL, XML_DEBUGFILENAME_ATTRIBUTE);
				}

				try {
					if (debugAppend && FileURLParser.isArchiveURL(debugFilename) && FileURLParser.isServerURL(debugFilename)) {
						status.add("Append true is not supported on remote archive files.", Severity.WARNING, this, Priority.NORMAL, XML_DEBUGAPPEND_ATTRIBUTE);
					}
				} catch (MalformedURLException e) {
					status.add(e.toString(), Severity.ERROR, this, Priority.NORMAL, XML_DEBUGAPPEND_ATTRIBUTE);
				}
			}
		}

		return status;
	}
	
	@Override
	public void init() throws ComponentNotReadyException {
		if (isInitialized()) {
			return;
		}
		
		super.init();
		
		TransformationGraph graph = getGraph();

		// creates necessary directories
		if (mkDir && (debugFilename != null)) {
			FileUtils.makeDirs(graph != null ? graph.getRuntimeContext().getContextURL() : null, new File(FileURLParser.getMostInnerAddress(debugFilename)).getParent());
		}
		
		if (debugPrint && inPorts.size() == 1) {
			if (debugFilename != null) {
				formatter = new TextTableFormatter(charSet);
				try {
					writer = new MultiFileWriter(formatter, new WritableByteChannelIterator(FileUtils.getWritableChannel(graph != null ? graph.getRuntimeContext().getContextURL() : null, debugFilename, debugAppend, compressLevel)));
				} catch (IOException e) {
					throw new ComponentNotReadyException(this, "Output file '" + debugFilename + "' does not exist.", e);
				}
			} else if (writableByteChannel == null) {
				formatter = new TextTableFormatter(charSet);
				writableByteChannel = new LogOutByteChannel(logger, charSet);
				writer = new MultiFileWriter(formatter, new WritableByteChannelIterator(writableByteChannel));
			}
			if (writer != null) {
				writer.setAppendData(debugAppend);
				writer.setDictionary(graph.getDictionary());
				writer.setOutputPort(getOutputPort(OUTPUT_PORT)); // for port protocol: target file writes data
			}
		}
	}

	@Override
	public void preExecute() throws ComponentNotReadyException {
		super.preExecute();
		
		if (writer != null) {
			if (firstRun()) {// a phase-dependent part of initialization
				writer.init(getInputPort(READ_FROM_PORT).getMetadata());
				formatter.showCounter("Record", "# ");
				if (printTrashID)
					formatter.showTrashID("Trash ID ", getId());
			} else {
				if (debugPrint) {
					if (debugFilename != null) {
						try {
							writer.setChannels(new WritableByteChannelIterator(FileUtils.getWritableChannel(getGraph() != null ? getGraph().getRuntimeContext().getContextURL() : null, debugFilename, debugAppend, compressLevel)));
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
	
	private Result executeWithWriter() throws Exception {
		InputPort inPort = getInputPort(READ_FROM_PORT);
		DataRecord record = DataRecordFactory.newRecord(inPort.getMetadata());
		record.init();
		
		while ((record = inPort.readRecord(record)) != null && runIt) {
			writer.write(record);
		}
		
		writer.finish();
		writer.close();
		
		return runIt ? Result.FINISHED_OK : Result.ABORTED;
	}

	private Result executeWithoutWriter() throws Exception {
		InputReader[] readers = new InputReader[inPorts.size()];
		int i = 0;
		for (InputPort inPort : inPorts.values()) {
			InputReader reader = new InputReader((InputPortDirect) inPort);
			reader.start();
			readers[i++] = reader;
		}
		
		boolean killIt = false;
		for (InputReader inputReader : readers) {
			while (inputReader.getState() != Thread.State.TERMINATED) {
				if (killIt) {
					inputReader.interrupt();
					break;
				}
				killIt = !runIt;
				try {
					inputReader.join(1000);
				} catch (InterruptedException e) {
					logger.warn(getId() + " thread interrupted, it will interrupt child threads", e);
					killIt = true;
				}
			}
		}

		return runIt ? Result.FINISHED_OK : Result.ABORTED;
	}

	@Override
	public void postExecute() throws ComponentNotReadyException {
		super.postExecute();

		try {
			if (writer != null) {
				writer.close();
			}
		} catch (Exception e) {
			throw new ComponentNotReadyException(COMPONENT_TYPE + ": " + e.getMessage(), e);
		}
	}
	
	@Override
	public synchronized void free() {
		super.free();
		
		if (writer != null) {
			try {
				writer.close();
			} catch (Throwable t) {
				logger.warn("Resource releasing failed for '" + getId() + "'. " + t.getMessage(), t);
			}
		}
	}

	@Override
	public void toXML(Element xmlElement) {
		super.toXML(xmlElement);

		xmlElement.setAttribute(XML_DEBUGPRINT_ATTRIBUTE, String.valueOf(this.debugPrint));
		if (debugFilename != null) {
			xmlElement.setAttribute(XML_DEBUGFILENAME_ATTRIBUTE, this.debugFilename);
		}
		if (charSet != null) {
			xmlElement.setAttribute(XML_CHARSET_ATTRIBUTE, charSet);
		}
		if (compressLevel > -1) {
			xmlElement.setAttribute(XML_COMPRESSLEVEL_ATTRIBUTE, String.valueOf(compressLevel));
		}
		if (mode != null) {
			if (mode.equals(Mode.PERFORMANCE)) {
				xmlElement.setAttribute(XML_MODE, PERFORMANCE);
			} else if (mode.equals(Mode.VALIDATE_RECORDS)) {
				xmlElement.setAttribute(XML_MODE, VALIDATE_RECORDS);
			}
		}
	}

	public static Node fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException, AttributeNotFoundException {
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(xmlElement, graph);
		Trash trash = new Trash(xattribs.getString(XML_ID_ATTRIBUTE));
		trash.loadAttributesFromXML(xattribs);
		return trash;
	}

    protected void loadAttributesFromXML(ComponentXMLAttributes componentAttributes) throws XMLConfigurationException {
        try {
			if (componentAttributes.exists(XML_DEBUGPRINT_ATTRIBUTE)) {
				setDebugPrint(componentAttributes.getBoolean(XML_DEBUGPRINT_ATTRIBUTE));
			}
			if (componentAttributes.exists(XML_DEBUGFILENAME_ATTRIBUTE)) {
				setDebugFile(componentAttributes.getStringEx(XML_DEBUGFILENAME_ATTRIBUTE, RefResFlag.SPEC_CHARACTERS_OFF));
			}
			if (componentAttributes.exists(XML_DEBUGAPPEND_ATTRIBUTE)) {
				setDebugAppend(componentAttributes.getBoolean(XML_DEBUGAPPEND_ATTRIBUTE));
			}
			if (componentAttributes.exists(XML_CHARSET_ATTRIBUTE)) {
				setCharset(componentAttributes.getString(XML_CHARSET_ATTRIBUTE));
			}
			if (componentAttributes.exists(XML_MK_DIRS_ATTRIBUTE)) {
				setMkDirs(componentAttributes.getBoolean(XML_MK_DIRS_ATTRIBUTE));
			}
			if (componentAttributes.exists(XML_PRINT_TRASH_ID_ATTRIBUTE)) {
				setPrintTrashID(componentAttributes.getBoolean(XML_PRINT_TRASH_ID_ATTRIBUTE));
			}
			if (componentAttributes.exists(XML_MODE)) {
				setMode(componentAttributes.getString(XML_MODE));
			} else {
				setMode(PERFORMANCE);
			}

			setCompressLevel(componentAttributes.getInteger(XML_COMPRESSLEVEL_ATTRIBUTE, -1));
        } catch (AttributeNotFoundException exception) {
            throw new XMLConfigurationException("Missing a required attribute!", exception);
        } catch (Exception exception) {
            throw new XMLConfigurationException("Error creating the component!", exception);
        }
    }

	@Override
	public String getType() {
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
	 * Switches on/off printing of incoming records
	 * 
	 * @param print The new debugPrint value
	 * @since April 4, 2002
	 */
	public void setDebugPrint(boolean print) {
		debugPrint = print;
	}

	public boolean isDebugPrint() {
		return debugPrint;
	}

	/**
	 * Sets the debugFile attribute of the Trash object
	 * 
	 * @param filename The new debugFile value
	 */
	public void setDebugFile(String filename) {
		debugFilename = filename;
	}

	/**
	 * Sets make directory.
	 * 
	 * @param mkDir - if true creates output directories for output file
	 */
	private void setMkDirs(boolean mkDir) {
		this.mkDir = mkDir;
	}

	/**
	 * Sets whether print trash ID.
	 * 
	 * @param printTrashID
	 */
	private void setPrintTrashID(boolean printTrashID) {
		this.printTrashID = printTrashID;
	}

	/**
	 * Sets mode
	 * 
	 * @param mode
	 */
	private void setMode(String mode) {
		if (mode == null || mode.equalsIgnoreCase(PERFORMANCE)) {
			this.mode = Mode.PERFORMANCE;
		} else if (mode.equalsIgnoreCase(VALIDATE_RECORDS)) {
			this.mode = Mode.VALIDATE_RECORDS;
		}
	}
	

	private class InputReader extends Thread {
		private InputPortDirect inPort;

		public InputReader(InputPortDirect inPort) {
			super(Thread.currentThread().getName() + ".InputThread#" + inPort.getInputPortNumber());
			this.inPort = inPort;
		}

		@Override
		public void run() {
			DataRecord record = DataRecordFactory.newRecord(inPort.getMetadata());
			CloverBuffer recordBuffer = CloverBuffer.allocateDirect(Defaults.Record.RECORD_INITIAL_SIZE, Defaults.Record.RECORD_LIMIT_SIZE);
			if (mode.equals(Mode.VALIDATE_RECORDS)) {
				record.init();
			}

			try {
				while (inPort.readRecordDirect(recordBuffer) && runIt) {
					if (mode.equals(Mode.VALIDATE_RECORDS)) {
						record.deserialize(recordBuffer);
					}
				}
			} catch (InterruptedException e) {
				logger.error(getId() + ": thread forcibly aborted", e);
				return;
			} catch (IOException e) {
				logger.error(getId() + ": thread failed", e);
				return;
			}
		}
	}
	
	@Override
	protected ComponentTokenTracker createComponentTokenTracker() {
		return new ReaderWriterComponentTokenTracker(this);
	}
	
}
