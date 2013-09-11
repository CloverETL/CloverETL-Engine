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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.Defaults;
import org.jetel.data.formatter.StructureFormatter;
import org.jetel.data.formatter.provider.StructureFormatterProvider;
import org.jetel.data.lookup.LookupTable;
import org.jetel.enums.PartitionFileTagType;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.ConfigurationStatus.Priority;
import org.jetel.exception.ConfigurationStatus.Severity;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.util.MultiFileWriter;
import org.jetel.util.SynchronizeUtils;
import org.jetel.util.bytes.SystemOutByteChannel;
import org.jetel.util.bytes.WritableByteChannelIterator;
import org.jetel.util.file.FileURLParser;
import org.jetel.util.file.FileUtils;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.property.RefResFlag;
import org.w3c.dom.Element;

/**
 *  <h3>StructureWriter Component</h3>
 *
 * <!-- All records from "body input port" [0] are formatted due to given mask and written to specified file -->
 * 
 * <table border="1">
 *  <th>Component:</th>
 * <tr><td><h4><i>Name:</i></h4></td>
 * <td>StructureWriter</td></tr>
 * <tr><td><h4><i>Category:</i></h4></td>
 * <td></td></tr>
 * <tr><td><h4><i>Description:</i></h4></td>
 * <td>All records from "body input port" [0] are formatted due to given mask and written to specified file.
 * There can be specified different masks for "header input port" [1] and "footer input port" [2]. If there is specified
 * header mask, all records from "header input port" [1] are  formatted due to this mask and written to file,
 * then are processed records from "body input port" [0] and then is written footer, created from records read 
 * from "footer input port" [2]. Header and footer are created in the same time and footer is stored in memory
 * until all records from "body port" are not written to the file.</td></tr>
 * <tr><td><h4><i>Inputs:</i></h4></td>
 * <td>[0] - input records - "body port"<br>
 * 	   [1] (optional) - input records - "header port"<br>
 * 	   [2] (optional) - input records - "footer port"
 * </td></tr>
 * <tr><td><h4><i>Outputs:</i></h4></td>
 * <td></td></tr>
 * <tr><td><h4><i>Comment:</i></h4></td>
 * <td>This component uses java.nio.* classes.</td></tr>
 * </table>
 *  <br>  
 *  <table border="1">
 *  <th>XML attributes:</th>
 *  <tr><td><b>type</b></td><td>"STRUCTURE_WRITER"</td></tr>
 *  <tr><td><b>id</b></td><td>component identification</td>
 *  <tr><td><b>fileURL</b></td><td>Output files mask.
 *  Use wildcard '#' to specify where to insert sequential number of file. Number of consecutive wildcards specifies
 *  minimal length of the number. Name without wildcard specifies only one file.</td>
 *  <tr><td><b>charset</b></td><td>character encoding of the output file (if not specified, then ISO-8859-1 is used)</td>
 *  <tr><td><b>append</b></td><td>whether to append data at the end if output file exists or replace it (values: true/false)</td>
 *  <tr><td><b>mask</b></td><td>template for formating records. Every occurrence 
 *  of $fieldName will be replaced by value of the fieldName. The rest of text will
 *  be unchanged. If not given there is used default mask:
 *  &lt; recordName field1=$field1 field2=$field2 ... fieldn=$fieldn /&gt;
 *  where field1 ,.., fieldn are record's fields from metadata</td>
 *  <tr><td><b>header</b></td><td>template for formating records. Every occurrence of $fieldName will be replaced by value of the fieldName.</td>
 *  <tr><td><b>footer</b></td><td>template for formating records. Every occurrence of $fieldName will be replaced by value of the fieldName.</td>
 *  <tr><td><b>recordsPerFile</b></td><td>max number of records in one output file</td>
 *  <tr><td><b>bytesPerFile</b></td><td>Max size of output files. To avoid splitting a record to two files, max size could be slightly overreached.</td>
 *  <tr><td><b>recordSkip</b></td><td>number of skipped records</td>
 *  <tr><td><b>recordCount</b></td><td>number of written records</td>
 *  </tr>
 *  </table>  
 *
 * <h4>Example:</h4>
 * <pre>&lt;Node append="true" fileURL="${WORKSPACE}/output/structured_customers.txt"
 *  id="STRUCTURE_WRITER0" type="STRUCTURE_WRITER"&gt;
 * &lt;attr name="header"&gt;dir = ${WORKSPACE}&lt;/attr&gt;
 * &lt;attr name="mask"&gt;
 * &lt;Customer id=$customer_id&gt;
 * 	&lt;last name = $lname&gt;
 *	&lt;first name = $fname&gt;
 * &lt;/Customer&gt;
 * &lt;/attr&gt;
 * &lt;attr name="header"&gt;
 * &lt;Header recordd&gt;
 * 	&lt;id = $customer_id&gt;
 * &lt;/Header record&gt;
 * &lt;/attr&gt;
 * &lt;attr name="footer"&gt;
 * &lt;Footer recordd&gt;
 * 	&lt;number = $account_no&gt;
 * &lt;/Footer record&gt;
 * &lt;/attr&gt;
 * &lt;/Node&gt;
 * 
 * 
 * @author avackova (agata.vackova@javlinconsulting.cz) ; 
 * (c) JavlinConsulting s.r.o.
 *  www.javlinconsulting.cz
 *
 * @since Oct 30, 2006
 *
 */
public class StructureWriter extends Node {

	public static final String XML_APPEND_ATTRIBUTE = "append";
	public static final String XML_FILEURL_ATTRIBUTE = "fileURL";
	public static final String XML_CHARSET_ATTRIBUTE = "charset";
	public static final String XML_MASK_ATTRIBUTE = "mask";
	public static final String XML_HEADER_ATTRIBUTE = "header";
	public static final String XML_FOOTER_ATTRIBUTE = "footer";
	public static final String XML_RECORD_SKIP_ATTRIBUTE = "recordSkip";
	public static final String XML_RECORD_COUNT_ATTRIBUTE = "recordCount";
	private static final String XML_RECORDS_PER_FILE = "recordsPerFile";
	private static final String XML_BYTES_PER_FILE = "bytesPerFile";
	private static final String XML_PARTITIONKEY_ATTRIBUTE = "partitionKey";
	private static final String XML_PARTITION_ATTRIBUTE = "partition";
	private static final String XML_PARTITION_OUTFIELDS_ATTRIBUTE = "partitionOutFields";
	private static final String XML_PARTITION_FILETAG_ATTRIBUTE = "partitionFileTag";
	private static final String XML_PARTITION_UNASSIGNED_FILE_NAME_ATTRIBUTE = "partitionUnassignedFileName";
	private static final String XML_SORTED_INPUT_ATTRIBUTE = "sortedInput";
	private static final String XML_MK_DIRS_ATTRIBUTE = "makeDirs";

	private String fileURL;
	private boolean appendData;
	private StructureFormatterProvider formatterProvider;
	private MultiFileWriter writer;
	private StructureFormatter headerFormatter, footerFormatter;
	private ByteArrayOutputStream headerOutput, footerOutput;
    private int skip;
	private int numRecords;
	private WritableByteChannel writableByteChannel;
	private int recordsPerFile;
	private int bytesPerFile;

	private String partition;
	private String attrPartitionKey;
	private LookupTable lookupTable;
	private String attrPartitionOutFields;
	private PartitionFileTagType partitionFileTagType = PartitionFileTagType.NUMBER_FILE_TAG;
	private String partitionUnassignedFileName;
	private String headerMask;
	private String footerMask;
	private boolean sortedInput = false;
	
	private static Log logger = LogFactory.getLog(StructureWriter.class);

	public final static String COMPONENT_TYPE = "STRUCTURE_WRITER";

	private final static int BODY_PORT = 0;
	private final static int HEADER_PORT = 1;
	private final static int FOOTER_PORT = 2;
	private final static int OUTPUT_PORT = 0;
	
	private String charset;
	private boolean mkDir;

	/**
	 * Constructor
	 * 
	 * @param id
	 * @param fileURL
	 * @param charset
	 * @param appendData
	 * @param mask
	 */
	@Deprecated
	public StructureWriter(String id, String fileURL, String charset, 
			boolean appendData, String mask) {
		this(id, fileURL, charset, appendData, null, mask, null);
	}

	/**
	 * @param id
	 * @param fileURL
	 * @param charset
	 * @param appendData
	 * @param headerMask
	 * @param mask
	 * @param footerMask
	 */
	public StructureWriter(String id, String fileURL, String charset, 
			boolean appendData, String headerMask, String mask, String footerMask) {
		super(id);
		this.fileURL = fileURL;
		this.charset = charset;
		this.appendData = appendData;
		this.headerMask = headerMask;
		this.footerMask = footerMask;
		formatterProvider = new StructureFormatterProvider(charset != null ? charset : Defaults.DataFormatter.DEFAULT_CHARSET_ENCODER);
		formatterProvider.setMask(mask);
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
		
		if (firstRun()) {
			//this initialization was moved to execute method, since has to be called
			//after the header and footer is prepared
	        //writer.init(getInputPort(BODY_PORT).getMetadata());
		}
		else {
			writer.reset();
			if (headerFormatter != null) {
				headerFormatter.reset();
			}
			if (footerFormatter != null) {
				footerFormatter.reset();
			}
		}
	}

	
	@Override
	public Result execute() throws Exception {
		
		Producer headerProducer = null, footerProducer = null;
		//prepare header
		if (headerFormatter != null) {
			headerProducer = new Producer(getInputPort(HEADER_PORT), headerFormatter);
			headerProducer.run(); // FIXME apparently we want to call start() here, but is it safe now?
		}
		//prepare footer
		if (footerFormatter != null){
			footerProducer = new Producer(getInputPort(FOOTER_PORT), footerFormatter);
			footerProducer.run(); // FIXME apparently we want to call start() here, but is it safe now?
		}
		//wait for header
		if (headerProducer != null) {
			headerProducer.join();
		}
		//wait for footer
		if (footerProducer != null){
			footerProducer.join();
		}
		
		if (headerFormatter != null) {
			formatterProvider.setHeader(headerOutput.toString(headerFormatter.getCharsetName()));
		}
		if (footerFormatter != null){
			formatterProvider.setFooter(footerOutput.toString(footerFormatter.getCharsetName()));
		}
		//main loop: processing "body" records
		InputPort bodyPort = getInputPort(BODY_PORT);
		DataRecord record = DataRecordFactory.newRecord(bodyPort.getMetadata());
		record.init();

		//this initialization has to be here not in pre-execute method,
		//since the header and the footer were prepared in execute() method
		writer.init(bodyPort.getMetadata());
		
		while (record != null && runIt) {
			record = bodyPort.readRecord(record);
			if (record != null) {
		        writer.write(record);
			}
			SynchronizeUtils.cloverYield();
		}
		writer.finish();
        return runIt ? Result.FINISHED_OK : Result.ABORTED;
	}
	
	@Override
	public void postExecute() throws ComponentNotReadyException {
		super.postExecute();
		try {
			writer.close();
		}
		catch (IOException e) {
			throw new ComponentNotReadyException(e);
		}
	}
	

	/* (non-Javadoc)
	 * @see org.jetel.graph.GraphElement#checkConfig()
	 */
    @Override
    public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		super.checkConfig(status);

		//this is just a copy of checkInputPorts() method without test of connected edges - second port can be in fact non-assigned
    	Collection<InputPort> inPorts = getInPorts();
        if(inPorts.size() < 1) {
            status.add(new ConfigurationProblem("At least 1 input port must be defined!", Severity.ERROR, this, Priority.NORMAL));
            return status;
        }
        if(inPorts.size() > 3) {
            status.add(new ConfigurationProblem("At most 3 input ports can be defined!", Severity.ERROR, this, Priority.NORMAL));
            return status;
        }
        //////////////

		if(!checkOutputPorts(status, 0, 1)) {
			return status;
		}
		
		if (charset != null && !Charset.isSupported(charset)) {
        	status.add(new ConfigurationProblem(
            		"Charset "+charset+" not supported!", 
            		ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL));
        }

        try {
        	FileUtils.canWrite(getGraph() != null ? getGraph().getRuntimeContext().getContextURL() : null, fileURL, mkDir);
        } catch (ComponentNotReadyException e) {
            status.add(e,ConfigurationStatus.Severity.ERROR,this,
            		ConfigurationStatus.Priority.NORMAL,XML_FILEURL_ATTRIBUTE);
        }
        
        try {
			if (appendData && FileURLParser.isArchiveURL(fileURL) && FileURLParser.isServerURL(fileURL)) {
			    status.add("Append true is not supported on remote archive files.", ConfigurationStatus.Severity.WARNING, this,
			    		ConfigurationStatus.Priority.NORMAL, XML_APPEND_ATTRIBUTE);
			}
		} catch (MalformedURLException e) {
            status.add(e.toString(),ConfigurationStatus.Severity.ERROR,this,
            		ConfigurationStatus.Priority.NORMAL,XML_APPEND_ATTRIBUTE);
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
		TransformationGraph graph = getGraph();
		
		initLookupTable();

		// based on file mask, create/open output file
		if (fileURL != null) {
	        writer = new MultiFileWriter(formatterProvider, graph != null ? graph.getRuntimeContext().getContextURL() : null, fileURL);
		} else {
			if (writableByteChannel == null) {
		        writableByteChannel =  new SystemOutByteChannel();
			}
	        writer = new MultiFileWriter(formatterProvider, new WritableByteChannelIterator(writableByteChannel));
		}
        writer.setLogger(logger);
        writer.setBytesPerFile(bytesPerFile);
        writer.setRecordsPerFile(recordsPerFile);
        writer.setAppendData(appendData);
        writer.setSkip(skip);
        writer.setNumRecords(numRecords);
        writer.setDictionary(graph != null ? graph.getDictionary() : null);
        writer.setOutputPort(getOutputPort(OUTPUT_PORT)); //for port protocol: target file writes data
        if (attrPartitionKey != null) {
            writer.setLookupTable(lookupTable);
            writer.setPartitionKeyNames(attrPartitionKey.split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX));
            writer.setPartitionFileTag(partitionFileTagType);
        	writer.setPartitionUnassignedFileName(partitionUnassignedFileName);
        	writer.setSortedInput(sortedInput);
        	
        	if (attrPartitionOutFields != null) {
        		writer.setPartitionOutFields(attrPartitionOutFields.split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX));
        	}
        }
        writer.setMkDir(mkDir);
		if (headerMask != null) {
			if (getInputPort(HEADER_PORT) != null) {
				headerFormatter = new StructureFormatter(charset != null ? charset : Defaults.DataFormatter.DEFAULT_CHARSET_ENCODER);
				headerFormatter.setMask(headerMask);
			}else{
				formatterProvider.setHeader(headerMask);
			}
		}
		if (footerMask != null){
			if (getInputPort(FOOTER_PORT) != null) {
				footerFormatter = new StructureFormatter(charset != null ? charset : Defaults.DataFormatter.DEFAULT_CHARSET_ENCODER);
				footerFormatter.setMask(footerMask);
			}else{
				formatterProvider.setFooter(footerMask);
			}
		}
        if (headerFormatter != null) {
        	headerOutput = new ByteArrayOutputStream();
        	headerFormatter.init(getInputPort(HEADER_PORT).getMetadata());
        	headerFormatter.setDataTarget(Channels.newChannel(headerOutput));
        }
        if (footerFormatter != null){
        	footerOutput = new ByteArrayOutputStream();
        	footerFormatter.init(getInputPort(FOOTER_PORT).getMetadata());
        	footerFormatter.setDataTarget(Channels.newChannel(footerOutput));
        }
	}
	
	@Override
	public synchronized void free() {
		super.free();
		if (writer != null)
			try {
				writer.close();
			} catch(Throwable t) {
				logger.warn("Resource releasing failed for '" + getId() + "'.", t);
			}
	}
	
	/**
	 * Creates and initializes lookup table.
	 * 
	 * @throws ComponentNotReadyException
	 */
	private void initLookupTable() throws ComponentNotReadyException {
		if (partition == null) return;
		
		// Initializing lookup table
		lookupTable = getGraph().getLookupTable(partition);
		if (lookupTable == null) {
			throw new ComponentNotReadyException("Lookup table \"" + partition + "\" not found.");
		}
		if (!lookupTable.isInitialized()) {
			lookupTable.init();
		}
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.Node#fromXML(org.jetel.graph.TransformationGraph, org.w3c.dom.Element)
	 */
	public static Node fromXML(TransformationGraph graph, Element nodeXML) throws AttributeNotFoundException {
		ComponentXMLAttributes xattribs=new ComponentXMLAttributes(nodeXML, graph);
		StructureWriter aDataWriter = null;
		
		aDataWriter = new StructureWriter(xattribs.getString(Node.XML_ID_ATTRIBUTE),
								xattribs.getStringEx(XML_FILEURL_ATTRIBUTE, RefResFlag.URL),
								xattribs.getString(XML_CHARSET_ATTRIBUTE,null),
								xattribs.getBoolean(XML_APPEND_ATTRIBUTE, false),
								xattribs.getString(XML_HEADER_ATTRIBUTE,null),
								xattribs.getString(XML_MASK_ATTRIBUTE, null),
								xattribs.getString(XML_FOOTER_ATTRIBUTE, null));
		if (xattribs.exists(XML_RECORD_SKIP_ATTRIBUTE)){
			aDataWriter.setSkip(Integer.parseInt(xattribs.getString(XML_RECORD_SKIP_ATTRIBUTE)));
		}
		if (xattribs.exists(XML_RECORD_COUNT_ATTRIBUTE)){
			aDataWriter.setNumRecords(Integer.parseInt(xattribs.getString(XML_RECORD_COUNT_ATTRIBUTE)));
		}
        if(xattribs.exists(XML_RECORDS_PER_FILE)) {
        	aDataWriter.setRecordsPerFile(xattribs.getInteger(XML_RECORDS_PER_FILE));
        }
        if(xattribs.exists(XML_BYTES_PER_FILE)) {
        	aDataWriter.setBytesPerFile(xattribs.getInteger(XML_BYTES_PER_FILE));
        }
		if(xattribs.exists(XML_PARTITIONKEY_ATTRIBUTE)) {
			aDataWriter.setPartitionKey(xattribs.getString(XML_PARTITIONKEY_ATTRIBUTE));
        }
		if(xattribs.exists(XML_PARTITION_ATTRIBUTE)) {
			aDataWriter.setPartition(xattribs.getString(XML_PARTITION_ATTRIBUTE));
        }
		if(xattribs.exists(XML_PARTITION_FILETAG_ATTRIBUTE)) {
			aDataWriter.setPartitionFileTag(xattribs.getString(XML_PARTITION_FILETAG_ATTRIBUTE));
        }
		if(xattribs.exists(XML_PARTITION_OUTFIELDS_ATTRIBUTE)) {
			aDataWriter.setPartitionOutFields(xattribs.getString(XML_PARTITION_OUTFIELDS_ATTRIBUTE));
        }
		if(xattribs.exists(XML_PARTITION_UNASSIGNED_FILE_NAME_ATTRIBUTE)) {
			aDataWriter.setPartitionUnassignedFileName(xattribs.getStringEx(XML_PARTITION_UNASSIGNED_FILE_NAME_ATTRIBUTE, RefResFlag.URL));
        }
		if(xattribs.exists(XML_SORTED_INPUT_ATTRIBUTE)) {
			aDataWriter.setSortedInput(xattribs.getBoolean(XML_SORTED_INPUT_ATTRIBUTE));
        }
		if(xattribs.exists(XML_MK_DIRS_ATTRIBUTE)) {
			aDataWriter.setMkDirs(xattribs.getBoolean(XML_MK_DIRS_ATTRIBUTE));
        }
		
		return aDataWriter;
	}

	@Deprecated
	public void setFooter(String footer) {
		this.footerMask = footer;
	}

	@Deprecated
	public void setHeader(String header) {
		this.headerMask = header;
	}

    /**
     * Sets number of skipped records in next call of getNext() method.
     * @param skip
     */
    public void setSkip(int skip) {
        this.skip = skip;
    }

    /**
     * Sets number of written records.
     * @param numRecords
     */
    public void setNumRecords(int numRecords) {
        this.numRecords = numRecords;
    }

    public void setBytesPerFile(int bytesPerFile) {
        this.bytesPerFile = bytesPerFile;
    }

    public void setRecordsPerFile(int recordsPerFile) {
        this.recordsPerFile = recordsPerFile;
    }

    /**
     * Sets lookup table for data partition.
     * 
     * @param lookupTable
     */
	public void setLookupTable(LookupTable lookupTable) {
		this.lookupTable = lookupTable;
	}

	/**
	 * Gets lookup table for data partition.
	 * 
	 * @return
	 */
	public LookupTable getLookupTable() {
		return lookupTable;
	}

	/**
	 * Gets partition (lookup table id) for data partition.
	 * 
	 * @param partition
	 */
	public void setPartition(String partition) {
		this.partition = partition;
	}

	/**
	 * Gets partition (lookup table id) for data partition.
	 * 
	 * @return
	 */
	public String getPartition() {
		return partition;
	}

	/**
	 * Sets partition key for data partition.
	 * 
	 * @param partitionKey
	 */
	public void setPartitionKey(String partitionKey) {
		this.attrPartitionKey = partitionKey;
	}

	/**
	 * Gets partition key for data partition.
	 * 
	 * @return
	 */
	public String getPartitionKey() {
		return attrPartitionKey;
	}
	
	/**
	 * Sets number file tag for data partition.
	 * 
	 * @param partitionKey
	 */
	public void setPartitionFileTag(String partitionFileTagType) {
		this.partitionFileTagType = PartitionFileTagType.valueOfIgnoreCase(partitionFileTagType);
	}

	/**
	 * Sets fields which are used for file output name.
	 * 
	 * @param partitionOutFields
	 */
	public void setPartitionOutFields(String partitionOutFields) {
		attrPartitionOutFields = partitionOutFields;
	}

	/**
	 * Gets number file tag for data partition.
	 * 
	 * @return
	 */
	public PartitionFileTagType getPartitionFileTag() {
		return partitionFileTagType;
	}
	
	/**
	 * @return the sortedInput
	 */
	public boolean isSortedInput() {
		return sortedInput;
	}

	/**
	 * @param sortedInput the sortedInput to set
	 */
	public void setSortedInput(boolean sortedInput) {
		this.sortedInput = sortedInput;
	}

	/**
	 * Sets partition unassigned file name.
	 * 
	 * @param partitionUnassignedFileName
	 */
    private void setPartitionUnassignedFileName(String partitionUnassignedFileName) {
    	this.partitionUnassignedFileName = partitionUnassignedFileName;
	}

	/**
	 * Sets make directory.
	 * @param mkDir - true - creates output directories for output file
	 */
	private void setMkDirs(boolean mkDir) {
		this.mkDir = mkDir;
	}

	/**
	 * This is class for format header/footer from input records
	 * 
	 * @author Agata Vackova (agata.vackova@javlinconsulting.cz) ; 
	 * (c) JavlinConsulting s.r.o.
	 *  www.javlinconsulting.cz
	 *
	 * @since Nov 22, 2007
	 *
	 */
	private class Producer extends Thread{
		
		private InputPort inputPort;
		private StructureFormatter formatter;

		/**
		 * @param inPort port for raeding records
		 * @param formatter formatter to format input records
		 */
		Producer(InputPort inPort, StructureFormatter formatter){
			this.inputPort = inPort;
			this.formatter = formatter;
		}

		@Override
		public void run() {
			DataRecord record = DataRecordFactory.newRecord(inputPort.getMetadata());
			record.init();
			try {
				while (record != null && runIt) {
					record = inputPort.readRecord(record);
					if (record != null) {
				        formatter.write(record);
					}
					SynchronizeUtils.cloverYield();
				}
			} catch (Exception e) {
				throw new RuntimeException(e);
			}finally{
				formatter.close();
			}
		}
		
	}


}
