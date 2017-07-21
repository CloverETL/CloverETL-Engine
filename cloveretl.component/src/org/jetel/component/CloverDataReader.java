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

import java.io.IOException;
import java.net.URL;
import java.nio.channels.ReadableByteChannel;
import java.util.Iterator;
import java.util.List;

import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.Defaults;
import org.jetel.data.parser.CloverDataParser;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.ConfigurationStatus.Severity;
import org.jetel.exception.JetelException;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPort;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.AutoFilling;
import org.jetel.util.ExceptionUtils;
import org.jetel.util.SynchronizeUtils;
import org.jetel.util.bytes.CloverBuffer;
import org.jetel.util.file.FileURLParser;
import org.jetel.util.file.FileUtils;
import org.jetel.util.file.WcardPattern;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.property.RefResFlag;
import org.w3c.dom.Element;

/**
 *  <h3>Clover Data Reader Component</h3>
 *
 * <!-- Reads data saved in Clover internal format and send the records to out 
 * ports. -->
 *
 * <table border="1">
 *  <th>Component:</th>
 * <tr><td><h4><i>Name:</i></h4></td>
 * <td>CloverReader</td></tr>
 * <tr><td><h4><i>Category:</i></h4></td>
 * <td></td></tr>
 * <tr><td><h4><i>Description:</i></h4></td>
 * <td>Reads data saved in Clover internal format and send the records to out 
 * ports.</td></tr>
 * <tr><td><h4><i>Inputs:</i></h4></td>
 * <td></td></tr>
 * <tr><td><h4><i>Outputs:</i></h4></td>
 * <td>At least one output port defined/connected.</tr>
 * <tr><td><h4><i>Comment:</i></h4></td></tr>
 * </table>
 *  <br>
 *  <table border="1">
 *  <th>XML attributes:</th>
 *  <tr><td><b>type</b></td><td>"CLOVER_READER"</td></tr>
 *  <tr><td><b>id</b></td><td>component identification</td>
 *  <tr><td><b>fileURL</b></td><td>path to the data file. </td>
 *  <tr><td><b>indexFileURL</b><br><i>optional</i></td><td>if index file is not 
 *  in the same directory as data file or has not expected name (fileURL.idx)</td>
 *  <tr><td><b>startRecord</b><br><i>optional</i></td><td>index of first parsed record</td>
 *  <tr><td><b>finalRecord</b><br><i>optional</i></td><td>index of final parsed record</td>
 *  </tr>
 *  </table>
 *
 *  <h4>Example:</h4>
 *  <pre>&lt;Node fileURL="DATA/customers.clv" finalRecord="2" id="CLOVER_READER0" 
 *  startRecord="1" type="CLOVER_READER"/&gt;
 * 
 * <pre>&lt;Node fileURL="customers.clv.zip" id="CLOVER_READER0" type="CLOVER_READER"/&gt;
 * 
 * @author avackova <agata.vackova@javlinconsulting.cz> ; 
 * (c) JavlinConsulting s.r.o.
 *  www.javlinconsulting.cz
 *
 * @since Oct 13, 2006
 * @see CloverDataParser.java
 *
 */


public class CloverDataReader extends Node {

	public final static String COMPONENT_TYPE = "CLOVER_READER";

	/** XML attribute names */
	private final static String XML_FILE_ATTRIBUTE = "fileURL";
	private final static String XML_INDEXFILEURL_ATTRIBUTE = "indexFileURL";
	private static final String XML_STARTRECORD_ATTRIBUTE = "startRecord";
	private static final String XML_FINALRECORD_ATTRIBUTE = "finalRecord";
	private static final String XML_SKIPROWS_ATTRIBUTE = "skipRows";
	private static final String XML_NUMRECORDS_ATTRIBUTE = "numRecords";
	private static final String XML_SKIP_SOURCE_ROWS_ATTRIBUTE = "skipSourceRows";
	private static final String XML_NUM_SOURCE_RECORDS_ATTRIBUTE = "numSourceRecords";

	private final static int OUTPUT_PORT = 0;

	private String fileURL;
	private String indexFileURL;
	private CloverDataParser parser;

	private int skipRows;
	private int numRecords = -1;
	private int skipSourceRows = -1;
	private int numSourceRecords = -1;
    private int skipped = 0; // number of records skipped to satisfy the skipRows attribute, records skipped to satisfy skipSourceRows are not included
    
    private AutoFilling autoFilling = new AutoFilling();

	private Iterator<String> filenameItor;

	private boolean inputSource;
	
	/**
	 * Used if there are no autofilled fields in the output metadata.
	 * @see #executeDirect()
	 */
	private boolean directReading = false;
    
	/**
	 * @param id
	 * @param fileURL
	 * @param indexFileURL
	 */
	public CloverDataReader(String id, String fileURL, String indexFileURL) {
		super(id);
		this.fileURL = fileURL;
		this.indexFileURL = indexFileURL;
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.Node#getType()
	 */
	@Override
	public String getType() {
		return COMPONENT_TYPE;
	}
	
    @Override
    public void preExecute() throws ComponentNotReadyException {
    	super.preExecute();
    	
    	if (firstRun()) {//a phase-dependent part of initialization
    	}
    	else {
    		initFileIterator();
    		parser.reset();
    		autoFilling.reset();
    	}
		inputSource = setDataSource(); //assigns data source to a parser and returns true if succeeds
		skip();
    }    

    /**
     * Reads records from the source file without deserialization.
     * <p>
     * Used if there are no autofilling fields.
     * </p>
     * 
     * @throws Exception
     */
    private void executeDirect() throws Exception {
    	CloverBuffer recordBuffer = CloverBuffer.allocateDirect(Defaults.Record.RECORD_INITIAL_SIZE, Defaults.Record.RECORD_LIMIT_SIZE);
		if (inputSource) {
			do {
				if (checkRow() && (parser.getNextDirect(recordBuffer) != null)) {
					autoFilling.incCounters();
				    writeRecordBroadcastDirect(recordBuffer);
					SynchronizeUtils.cloverYield();
				} else {
					// prepare next file
					if (!nextSource()) {
						break;
					}
				}				
			} while (runIt);			
		}
	}
	
    /**
     * Parses records from the input file,
     * sets autofilling fields,
     * serializes the records and writes them to the output port(s).
     * <p>
     * Should be deprecated in the future, since autofilling probably
     * doesn't work anyway.
     * </p>
     * 
     * @throws Exception
     */
	private void executeParsing() throws Exception {
		DataRecord record = DataRecordFactory.newRecord(getOutputPort(OUTPUT_PORT).getMetadata());
        record.init();
        DataRecord rec;
		if (inputSource) {
			do {
				if (checkRow() && (rec = parser.getNext(record)) != null) {
			        autoFilling.setLastUsedAutoFillingFields(rec);
				    writeRecordBroadcast(rec);
					SynchronizeUtils.cloverYield();
				} else {
					// prepare next file
					if (!nextSource()) {
						break;
					}
				}				
			} while (runIt);			
		}
	}
	
	@Override
	public Result execute() throws Exception {
		if (directReading) {
			executeDirect();
		} else {
			executeParsing();
		}
		broadcastEOF();
        return runIt ? Result.FINISHED_OK : Result.ABORTED;
	}


    @Override
    public void postExecute() throws ComponentNotReadyException {
    	super.postExecute();
		parser.close();
    }    
	
	
	
	/**
	 * Checks numRecords. Returns true if the source could return a record.
	 * @return
	 * @throws ComponentNotReadyException 
	 */
	private final boolean checkRow() {
        if(numRecords > 0 && numRecords <= autoFilling.getGlobalCounter()) {
            return false;
        }
        if (numSourceRecords > 0 && numSourceRecords <= autoFilling.getSourceCounter()) {
        	return false;
        }
       	return true;
	}

	private boolean nextSource() throws ComponentNotReadyException {
		// close previous source
		parser.close();
		
		// reset autofilling
		autoFilling.resetGlobalSourceCounter();
		autoFilling.resetSourceCounter();
		
		// prepare next source
		if (!filenameItor.hasNext()) return false;
		String fName = filenameItor.next();
		if (indexFileURL != null) {
			parser.setDataSource(new String[]{fName,indexFileURL});
		} else {
			parser.setDataSource(fName);
		}
		skip();
		return true;
	}
	
	public static Node fromXML(TransformationGraph graph, Element nodeXML) throws AttributeNotFoundException {
		CloverDataReader aDataReader = null;
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(nodeXML, graph);

		aDataReader = new CloverDataReader(xattribs.getString(Node.XML_ID_ATTRIBUTE),
					xattribs.getStringEx(XML_FILE_ATTRIBUTE, RefResFlag.URL),
					xattribs.getStringEx(XML_INDEXFILEURL_ATTRIBUTE, null, RefResFlag.URL));
		if (xattribs.exists(XML_STARTRECORD_ATTRIBUTE)){
			aDataReader.setStartRecord(xattribs.getInteger(XML_STARTRECORD_ATTRIBUTE));
		}
		if (xattribs.exists(XML_FINALRECORD_ATTRIBUTE)){
			aDataReader.setFinalRecord(xattribs.getInteger(XML_FINALRECORD_ATTRIBUTE));
		}
		if (xattribs.exists(XML_SKIPROWS_ATTRIBUTE)){
			aDataReader.setSkipRows(xattribs.getInteger(XML_SKIPROWS_ATTRIBUTE));
		}
		if (xattribs.exists(XML_NUMRECORDS_ATTRIBUTE)){
			aDataReader.setNumRecords(xattribs.getInteger(XML_NUMRECORDS_ATTRIBUTE));
		}
		if (xattribs.exists(XML_SKIP_SOURCE_ROWS_ATTRIBUTE)){
			aDataReader.setSkipSourceRows(xattribs.getInteger(XML_SKIP_SOURCE_ROWS_ATTRIBUTE));
		}
		if (xattribs.exists(XML_NUM_SOURCE_RECORDS_ATTRIBUTE)){
			aDataReader.setNumSourceRecords(xattribs.getInteger(XML_NUM_SOURCE_RECORDS_ATTRIBUTE));
		}
		
		return aDataReader;
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.GraphElement#checkConfig()
	 */
    @Override
    public ConfigurationStatus checkConfig(ConfigurationStatus status) {
        super.checkConfig(status);
        
        // check ports and metadata
        if(!checkInputPorts(status, 0, 0)
        		|| !checkOutputPorts(status, 1, Integer.MAX_VALUE)) {
        	return status;
        }
        checkMetadata(status, getOutMetadata());
        
        ReadableByteChannel tempChannel = null;
        // check files
    	try {
    		String fName; 
    		initFileIterator();
    		while (filenameItor.hasNext()) {
				fName = filenameItor.next();
				URL url = FileUtils.getFileURL(getGraph().getRuntimeContext().getContextURL(), FileURLParser.getMostInnerAddress(fName));
				if (FileUtils.isServerURL(url)) {
					//FileUtils.checkServer(url); //this is very long operation
					continue;
				}
				tempChannel = FileUtils.getReadableChannel(getGraph().getRuntimeContext().getContextURL(), url.toString());
    		}
		} catch (Exception e) {
			status.add(new ConfigurationProblem(ExceptionUtils.getMessage(e), Severity.WARNING, this, ConfigurationStatus.Priority.NORMAL));
        } finally {
            try {
                if (tempChannel != null && tempChannel.isOpen()) {
                    tempChannel.close();
                }
            } catch (IOException e) {
            }
        }
        
        return status;
    }

	/* (non-Javadoc)
	 * @see org.jetel.graph.GraphElement#init()
	 */
	@Override
	public void init() throws ComponentNotReadyException {
        if(isInitialized()) return;
		super.init();
		parser = new CloverDataParser(getOutputPort(OUTPUT_PORT).getMetadata());
		
		initFileIterator();

        // skip/number source rows
        if (skipSourceRows == -1) {
        	OutputPort outputPort = getOutputPort(OUTPUT_PORT);
        	DataRecordMetadata metadata;
        	if (outputPort != null && (metadata = outputPort.getMetadata()) != null) {
            	int ssr = metadata.getSkipSourceRows();
            	if (ssr > 0) {
                    skipSourceRows = ssr;
            	}
        	}
        }

		DataRecordMetadata metadata = getOutputPort(OUTPUT_PORT).getMetadata();
		parser.init();
		parser.setProjectURL(getGraph().getRuntimeContext().getContextURL());
		
    	if (metadata != null) {
    		this.directReading = autoFilling.isAutofillingDisabled(metadata);
    		autoFilling.addAutoFillingFields(metadata);
    	}
    	autoFilling.setFilename(fileURL);
	}
	
	private boolean setDataSource() throws ComponentNotReadyException {
		if (filenameItor.hasNext()) {
			String fName = filenameItor.next();
			if (indexFileURL != null) {
				parser.setDataSource(new String[]{fName,indexFileURL});
			}else{
				parser.setDataSource(fName);
			}
			return true;
		}
		return false;
	}
	
	/**
	 * Performs skip operation at the start of a data source. Per-source skip is always performed.
	 * If the target number of globally skipped records has not yet been reached, it is followed
	 * by global skip.  
	 * Increases per-source number of records by number of records skipped to satisfy global skip attribute.
	 * @throws JetelException 
	 */
	private void skip() {
		int skippedInCurrentSource = 0;
		try {
			if (skipSourceRows > 0) {
				parser.skip(skipSourceRows);
			}
			if (skipped >= skipRows) {
				return;
			}
			int globalSkipInCurrentSource = skipRows - skipped;
			if (numSourceRecords >= 0 && numSourceRecords < globalSkipInCurrentSource) {
				// records for global skip in local file are limited by max number of records from local file
				globalSkipInCurrentSource = numSourceRecords;
			}
			skippedInCurrentSource = parser.skip(globalSkipInCurrentSource);
		} catch (JetelException e) {			
		}
        autoFilling.incSourceCounter(skippedInCurrentSource);
        skipped += skippedInCurrentSource;
    }
    
	private void initFileIterator() throws ComponentNotReadyException {
		WcardPattern pat = new WcardPattern();
		pat.setParent(getGraph().getRuntimeContext().getContextURL());
        pat.addPattern(fileURL, Defaults.DEFAULT_PATH_SEPARATOR_REGEX);
        List<String> files;
        try {
			files = pat.filenames();
		} catch (IOException e) {
			throw new ComponentNotReadyException(e);
		}
        this.filenameItor = files.iterator();
	}

	@Override
	public synchronized void free() {
		super.free();
		if (parser != null) {
			parser.close();
		}
	}
	
	@Override
	public String[] getUsedUrls() {
		return new String[] { fileURL };
	}

	@Deprecated
	public void setStartRecord(int startRecord){
		setSkipRows(startRecord);
	}

	@Deprecated
	public void setFinalRecord(int finalRecord) {
		setNumRecords(finalRecord-skipRows);
	}
	
	/**
	 * @param startRecord The startRecord to set.
	 */
	public void setSkipRows(int skipRows) {
		this.skipped = 0;
		this.skipRows = Math.max(skipRows, 0);
	}
	
	/**
	 * @param finalRecord The finalRecord to set.
	 */
	public void setNumRecords(int numRecords) {
		this.numRecords = Math.max(numRecords, 0);
	}

	/**
	 * @param how many rows to skip for every source
	 */
	public void setSkipSourceRows(int skipSourceRows) {
		this.skipSourceRows = Math.max(skipSourceRows, 0);
	}
	
	/**
	 * @param how many rows to process for every source
	 */
	public void setNumSourceRecords(int numSourceRecords) {
		this.numSourceRecords = Math.max(numSourceRecords, 0);
	}

}
