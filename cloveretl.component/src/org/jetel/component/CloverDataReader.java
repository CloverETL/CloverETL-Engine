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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.Defaults;
import org.jetel.data.formatter.CloverDataFormatter;
import org.jetel.data.parser.CloverDataParser;
import org.jetel.data.parser.CloverDataParser.FileConfig;
import org.jetel.data.parser.CloverDataParser35;
import org.jetel.data.parser.ICloverDataParser;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.ConfigurationStatus.Severity;
import org.jetel.exception.IParserExceptionHandler;
import org.jetel.exception.JetelException;
import org.jetel.exception.PolicyType;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPort;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.AutoFilling;
import org.jetel.util.ExceptionUtils;
import org.jetel.util.MultiFileListener;
import org.jetel.util.MultiFileReader;
import org.jetel.util.SynchronizeUtils;
import org.jetel.util.bytes.CloverBuffer;
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


public class CloverDataReader extends Node implements MultiFileListener {

	private final static Log logger = LogFactory.getLog(CloverDataReader.class);
	
	public final static String COMPONENT_TYPE = "CLOVER_READER";

	/** XML attribute names */
	private final static String XML_FILE_ATTRIBUTE = "fileURL";
	private static final String XML_STARTRECORD_ATTRIBUTE = "startRecord";
	private static final String XML_FINALRECORD_ATTRIBUTE = "finalRecord";
	private static final String XML_SKIPROWS_ATTRIBUTE = "skipRows";
	private static final String XML_NUMRECORDS_ATTRIBUTE = "numRecords";
	private static final String XML_SKIP_SOURCE_ROWS_ATTRIBUTE = "skipSourceRows";
	private static final String XML_NUM_SOURCE_RECORDS_ATTRIBUTE = "numSourceRecords";

	private final static int OUTPUT_PORT = 0;
	private final static int INPUT_PORT = 0;

	private String fileURL;
	private ICloverDataParser parser;
	private MultiFileReader reader;
	
	
	private int skipRows;
	private int numRecords = -1;
	private int skipSourceRows = -1;
	private int numSourceRecords = -1;
    
	/**
	 * Used if there are no autofilled fields in the output metadata.
	 * @see #executeDirect()
	 */
	private boolean attemptDirectReading = false;
	private boolean readDirect = false;
	
    
	/**
	 * @param id
	 * @param fileURL
	 */
	public CloverDataReader(String id, String fileURL) {
		super(id);
		this.fileURL = fileURL;
	}

    @Override
    public void preExecute() throws ComponentNotReadyException {
    	super.preExecute();
    	 try {
             reader.preExecute();
         } catch(ComponentNotReadyException e) {
             e.setAttributeName(XML_FILE_ATTRIBUTE);
             throw e;
         }
    	 
    }
    
    @Override
    public Result execute() throws Exception {
    	CloverBuffer recordBuffer = null;
    	DataRecord record = null;
    	int status;
    	// direct reading may not be supported for all input files, e.g. mixed versions in one directory
    	if (attemptDirectReading && parser.isDirectReadingSupported()) {
    		if (recordBuffer == null) {
    			recordBuffer = CloverBuffer.allocateDirect(Defaults.Record.RECORD_INITIAL_SIZE, Defaults.Record.RECORD_LIMIT_SIZE);
    		}
    	} 
    	if (record == null) {
    		record = DataRecordFactory.newRecord(getOutputPort(OUTPUT_PORT).getMetadata());
    		record.init();
    		record.setDeserializeAutofilledFields(false); // CLO-4591
    	}
    	while (runIt) {
    		if (readDirect){
    			status = reader.getNextDirect(recordBuffer); 
    			if (status==1){
    				writeRecordBroadcastDirect(recordBuffer);
    				SynchronizeUtils.cloverYield();
    				continue;
    			}else if (status==0){
    				break;
    			}else{
    				// status -1 -> need to read default way
    				readDirect=false;
    			}
    		}
    		record = reader.getNext(record); 
    		if (record!=null){
    			 writeRecordBroadcast(record);
    		}else{
    			break;
    		}
    		SynchronizeUtils.cloverYield();
    	}
    	broadcastEOF();
    	return runIt ? Result.FINISHED_OK : Result.ABORTED;
    }


    @Override
    public void postExecute() throws ComponentNotReadyException {
    	super.postExecute();
		parser.postExecute();
    }    
	
	
	private void prepareParser() throws ComponentNotReadyException {
		this.parser=new InternalParser(getOutputPort(OUTPUT_PORT).getMetadata(), getGraph().getRuntimeContext().getContextURL());
		this.parser.init();
		
	}
	
	private void prepareMultiFileReader() throws ComponentNotReadyException {
		// initialize multifile reader based on prepared parser
		TransformationGraph graph = getGraph();
        reader = new MultiFileReader(parser, getContextURL(), fileURL);
        reader.setLogger(logger);
        reader.setSkip(skipRows);
        reader.setNumSourceRecords(numSourceRecords);
        reader.setNumRecords(numRecords);
        reader.setInputPort(getInputPort(INPUT_PORT)); //for port protocol: ReadableChannelIterator reads data
        reader.setPropertyRefResolver(getPropertyRefResolver());
        reader.setDictionary(graph.getDictionary());
        reader.setSkipSourceRows(skipSourceRows);

        reader.init(getOutputPort(OUTPUT_PORT).getMetadata());
	}
	
	
	public static Node fromXML(TransformationGraph graph, Element nodeXML) throws AttributeNotFoundException {
		CloverDataReader aDataReader = null;
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(nodeXML, graph);

		aDataReader = new CloverDataReader(xattribs.getString(Node.XML_ID_ATTRIBUTE),
					xattribs.getStringEx(XML_FILE_ATTRIBUTE, RefResFlag.URL));
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
        if(!checkInputPorts(status, 0, 1)
        		|| !checkOutputPorts(status, 1, Integer.MAX_VALUE)) {
        	return status;
        }
        checkMetadata(status, getOutMetadata());
        
        // check files
    	try {
    		prepareParser();
    		prepareMultiFileReader();
    		reader.checkConfig(getOutputPort(OUTPUT_PORT).getMetadata());
    	} catch (Exception e) {
			status.add(new ConfigurationProblem(ExceptionUtils.getMessage(e), Severity.WARNING, this, ConfigurationStatus.Priority.NORMAL));
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

		prepareParser();
		prepareMultiFileReader();
		reader.addFileChangeListener(this); // register as listener for source change
		
		DataRecordMetadata metadata = getOutputPort(OUTPUT_PORT).getMetadata();		
    	if (metadata != null) {
    		// the Autofilling instance is only used to determine if the metadata contain autofilled fields
    	    AutoFilling autoFilling = new AutoFilling();
    		this.attemptDirectReading = autoFilling.isAutofillingDisabled(metadata);
    		this.readDirect = attemptDirectReading && parser.isDirectReadingSupported();
    	}
	}
	
	
	@Override
	public synchronized void free() {
		super.free();
		try{
			if (parser != null) {
				parser.free();
			}
		}catch(Exception ex){
			//do nothing;
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
	
	@Override
	public void fileChanged(Object newFile) {
		// check whether call getNext() or getNextDirect()
		readDirect = attemptDirectReading && parser.isDirectReadingSupported() && parser.getVersion().raw;
	}
	
	
	static class InternalParser implements ICloverDataParser{
		private CloverDataParser parserNew;
		private CloverDataParser35 parser35;
		private ICloverDataParser currentParser;
		
		InternalParser(DataRecordMetadata metadata,URL contextURL){
			parserNew = new CloverDataParser(metadata);
			parser35 = new CloverDataParser35(metadata);
			parserNew.setProjectURL(contextURL);
			parser35.setProjectURL(contextURL);
			currentParser=parserNew;
		}

		@Override
		public DataRecord getNext() throws JetelException {
				return currentParser.getNext();
		}

		@Override
		public boolean isDirectReadingSupported() {
				return currentParser.isDirectReadingSupported();
		}

		@Override
		public int getNextDirect(CloverBuffer buffer) throws JetelException {
				return currentParser.getNextDirect(buffer);
		}

		@Override
		public int skip(int nRec) throws JetelException {
				return currentParser.skip(nRec);
		}

		@Override
		public void init() throws ComponentNotReadyException {
			parserNew.init();
			parser35.init();
		}

		@Override
		public void setDataSource(Object inputDataSource) throws IOException, ComponentNotReadyException {
			parserNew.setDataSource(inputDataSource);
			if(parserNew.getVersion().formatVersion!=CloverDataFormatter.DataFormatVersion.VERSION_40){
				parser35.setVersion(parserNew.getVersion());
				parser35.setDataSource(inputDataSource);
				currentParser=parser35;
			}else{
				currentParser=parserNew;
			}
		}

		@Override
		public void setReleaseDataSource(boolean releaseInputSource) {
			currentParser.setReleaseDataSource(releaseInputSource);
		}

		@Override
		public void close() throws IOException {
			currentParser.close();
		}

		@Override
		public DataRecord getNext(DataRecord record) throws JetelException {
			return currentParser.getNext(record);
		}

		@Override
		public void setExceptionHandler(IParserExceptionHandler handler) {
			currentParser.setExceptionHandler(handler);
		}

		@Override
		public IParserExceptionHandler getExceptionHandler() {
			return currentParser.getExceptionHandler();
		}

		@Override
		public PolicyType getPolicyType() {
			return currentParser.getPolicyType();
		}

		@Override
		public void reset() throws ComponentNotReadyException {
			currentParser.reset();
		}

		@Override
		public Object getPosition() {
			return currentParser.getPosition();
		}

		@Override
		public void movePosition(Object position) throws IOException {
			currentParser.movePosition(position);
			
		}

		@Override
		public void preExecute() throws ComponentNotReadyException {
			currentParser.preExecute();
		}

		@Override
		public void postExecute() throws ComponentNotReadyException {
			currentParser.postExecute();
			
		}

		@Override
		public void free() throws ComponentNotReadyException, IOException {
			parserNew.free();
			parser35.free();
			
		}

		@Override
		public boolean nextL3Source() {
			return currentParser.nextL3Source();
		}

		@Override
		public DataSourceType getPreferredDataSourceType() {
			return currentParser.getPreferredDataSourceType();
		}

		@Override
		public FileConfig getVersion() {
			return currentParser.getVersion();
		}
		
		
	}
}
