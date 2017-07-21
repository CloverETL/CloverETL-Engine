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
import java.net.MalformedURLException;
import java.nio.channels.WritableByteChannel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.Defaults;
import org.jetel.data.formatter.provider.DataFormatterProvider;
import org.jetel.data.lookup.LookupTable;
import org.jetel.enums.PartitionFileTagType;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.ConfigurationStatus.Priority;
import org.jetel.exception.ConfigurationStatus.Severity;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.ExceptionUtils;
import org.jetel.util.MultiFileWriter;
import org.jetel.util.SynchronizeUtils;
import org.jetel.util.bytes.SystemOutByteChannel;
import org.jetel.util.bytes.WritableByteChannelIterator;
import org.jetel.util.file.FileURLParser;
import org.jetel.util.file.FileUtils;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.property.RefResFlag;
import org.jetel.util.string.QuotingDecoder;
import org.jetel.util.string.StringUtils;
import org.w3c.dom.Element;

/**
 *  <h3>DelimitedDataWriter Component</h3>
 *
 * <!-- All records from input port [0] are formatted with delimiter and written to specified file -->
 * 
 * <table border="1">
 *  <th>Component:</th>
 * <tr><td><h4><i>Name:</i></h4></td>
 * <td>DataWriter</td></tr>
 * <tr><td><h4><i>Category:</i></h4></td>
 * <td></td></tr>
 * <tr><td><h4><i>Description:</i></h4></td>
 * <td>All records from input port [0] are formatted to the delimited or fixlen form and written to specified file.<br>
 * Type of formatting is taken from metadata specified for port[0] data flow.</td></tr>
 * <tr><td><h4><i>Inputs:</i></h4></td>
 * <td>[0]- input records</td></tr>
 * <tr><td><h4><i>Outputs:</i></h4></td>
 * <td></td></tr>
 * <tr><td><h4><i>Comment:</i></h4></td>
 * <td>This component uses java.nio.* classes.</td></tr>
 * </table>
 *  <br>  
 *  <table border="1">
 *  <th>XML attributes:</th>
 *  <tr><td><b>type</b></td><td>"DATA_WRITER"</td></tr>
 *  <tr><td><b>id</b></td><td>component identification</td>
 *  <tr><td><b>fileURL</b></td><td>Output files mask.
 *  Use wildcard '#' to specify where to insert sequential number of file. Number of consecutive wildcards specifies
 *  minimal length of the number. Name without wildcard specifies only one file.</td>
 *  <tr><td><b>charset</b></td><td>character encoding of the output file (if not specified, then ISO-8859-1 is used)</td>
 *  <tr><td><b>append</b></td><td>whether to append data at the end if output file exists or replace it (values: true/false)</td>
 *  <tr><td><b>outputFieldNames</b><br><i>optional</i></td><td>print names of individual fields into output file - as a first row (values: true/false, default:false)</td> 
 *  <tr><td><b>recordsPerFile</b></td><td>max number of records in one output file</td>
 *  <tr><td><b>bytesPerFile</b></td><td>Max size of output files. To avoid splitting a record to two files, max size could be slightly overreached.</td>
 *  <tr><td><b>recordSkip</b></td><td>number of skipped records</td>
 *  <tr><td><b>recordCount</b></td><td>number of written records</td>
 *  <tr><td><b>excludeFields</b></td><td>	 sequence of field names from incoming data flow (separated by semicolon) to exclude from writing </td>
 *  </tr>
 *  </table>  
 *
 * <h4>Example:</h4>
 * <pre>&lt;Node type="DATA_WRITER" id="Writer" fileURL="/tmp/transfor.out" append="true" /&gt;</pre>
 * 
 * @author     dpavlis
 * @since    April 4, 2002
 */
public class DataWriter extends Node {
	private static final String XML_APPEND_ATTRIBUTE = "append";
	private static final String XML_FILEURL_ATTRIBUTE = "fileURL";
	private static final String XML_CHARSET_ATTRIBUTE = "charset";
    private static final String XML_OUTPUT_FIELD_NAMES = "outputFieldNames";
	private static final String XML_RECORDS_PER_FILE = "recordsPerFile";
	private static final String XML_BYTES_PER_FILE = "bytesPerFile";
	private static final String XML_RECORD_SKIP_ATTRIBUTE = "recordSkip";
	private static final String XML_RECORD_COUNT_ATTRIBUTE = "recordCount";
	private static final String XML_PARTITIONKEY_ATTRIBUTE = "partitionKey";
	private static final String XML_PARTITION_ATTRIBUTE = "partition";
	private static final String XML_PARTITION_OUTFIELDS_ATTRIBUTE = "partitionOutFields";
	private static final String XML_PARTITION_FILETAG_ATTRIBUTE = "partitionFileTag";
	private static final String XML_PARTITION_UNASSIGNED_FILE_NAME_ATTRIBUTE = "partitionUnassignedFileName";
	private static final String XML_MK_DIRS_ATTRIBUTE = "makeDirs";
    private static final String XML_EXCLUDE_FIELDS_ATTRIBUTE = "excludeFields";
	private static final String XML_QUOTEDSTRINGS_ATTRIBUTE = "quotedStrings";
	private static final String XML_QUOTECHAR_ATTRIBUTE = "quoteCharacter";
	private static final String XML_SORTED_INPUT_ATTRIBUTE = "sortedInput";
	
	private String fileURL;
	private boolean appendData;
	protected DataFormatterProvider formatterProvider;
    protected MultiFileWriter writer;
    private boolean outputFieldNames;
	private int bytesPerFile;
	private int recordsPerFile;
	private WritableByteChannel writableByteChannel;
    private int skip;
	private int numRecords;
	private boolean quotedStrings;
	private Character quoteChar = '\"';
	private String charset;	
	
	private String partition;
	private String attrPartitionKey;
	private LookupTable lookupTable;
	private String attrPartitionOutFields;
	private PartitionFileTagType partitionFileTagType = PartitionFileTagType.NUMBER_FILE_TAG;
	private String partitionUnassignedFileName;
	private boolean mkDir;
	private boolean quotedStringsHasDefaultValue = true;
	private boolean sortedInput = false;

    private String excludeFields;

    static Log logger = LogFactory.getLog(DataWriter.class);

	public final static String COMPONENT_TYPE = "DATA_WRITER";
	private final static int READ_FROM_PORT = 0;
	private final static int OUTPUT_PORT = 0;
	
	/**
	 *Constructor for the DataWriter object
	 *
	 * @param  id          Description of Parameter
	 * @param  fileURL     Description of Parameter
	 * @param  appendData  Description of Parameter
	 * @since              April 16, 2002
	 */
	public DataWriter(String id, String fileURL, String charset, boolean appendData) {
		super(id);
		this.fileURL = fileURL;
		this.appendData = appendData;
		this.charset = charset;
	}
	
	public DataWriter(String id, WritableByteChannel writableByteChannel, String charset) {
		super(id);
		this.writableByteChannel = writableByteChannel;
		this.charset = charset;
	}

	@Override
	public Result execute() throws Exception {
		InputPort inPort = getInputPort(READ_FROM_PORT);
		DataRecord record = DataRecordFactory.newRecord(inPort.getMetadata());
		record.init();
		while (record != null && runIt) {
			record = inPort.readRecord(record);
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

	/**
	 *  Description of the Method
	 *
	 * @exception  ComponentNotReadyException  Description of Exception
	 * @since                                  April 4, 2002
	 */
	@Override
	public void init() throws ComponentNotReadyException {
        if(isInitialized()) return;
		super.init();
		TransformationGraph graph = getGraph();

		//prepare formatter provider
		formatterProvider = new DataFormatterProvider(charset != null ? charset : Defaults.DataFormatter.DEFAULT_CHARSET_ENCODER);
		if (quotedStringsHasDefaultValue) {
			//quoted strings has default value -> set the quoted string field from metadata
			formatterProvider.setQuotedStrings(getInMetadata().get(0).isQuotedStrings());
			formatterProvider.setQuoteChar(getInMetadata().get(0).getQuoteChar());
		} else {
			//quoted string is set by the user
			formatterProvider.setQuotedStrings(quotedStrings);
			formatterProvider.setQuoteChar(quoteChar);
		}
		
		initLookupTable();

		// initialize multifile writer based on prepared formatter
		if (fileURL != null) {
	        writer = new MultiFileWriter(formatterProvider, graph != null ? graph.getRuntimeContext().getContextURL() : null, fileURL);
		} else {
			if (writableByteChannel == null) {
		        writableByteChannel = new SystemOutByteChannel();
			}
	        writer = new MultiFileWriter(formatterProvider, new WritableByteChannelIterator(writableByteChannel));
		}
        writer.setLogger(logger);
        writer.setBytesPerFile(bytesPerFile);
        writer.setRecordsPerFile(recordsPerFile);
        writer.setAppendData(appendData);
        writer.setSkip(skip);
        writer.setNumRecords(numRecords);
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
        
        ConfigurationStatus status; //TODO remove when the DataRecordMetadata have an interface, see checkConfig, Clover 3?
        if (checkPorts(status = new ConfigurationStatus())) {
        	throw new ComponentNotReadyException(status.getFirst().getMessage());
        }

        String[] excludedFieldNames = null;

        if (!StringUtils.isEmpty(excludeFields)) {
            excludedFieldNames = excludeFields.split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);
            formatterProvider.setExcludedFieldNames(excludedFieldNames);
        }

        DataRecordMetadata metadata = getInputPort(READ_FROM_PORT).getMetadata();
        if(outputFieldNames) {
        	formatterProvider.setHeader(metadata.getFieldNamesHeader(excludedFieldNames, formatterProvider.getQuotedStrings(), formatterProvider.getQuoteChar()));
        }

        writer.setDictionary(graph.getDictionary());
        writer.setOutputPort(getOutputPort(OUTPUT_PORT)); //for port protocol: target file writes data
        writer.setMkDir(mkDir);
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.Node#preExecute()
	 */
	@Override
	public void preExecute() throws ComponentNotReadyException {
		super.preExecute();
		prepareWriter();
	}
	
	protected void prepareWriter() throws ComponentNotReadyException {
		if (firstRun()) {
	        try {
	            writer.init(getInputPort(READ_FROM_PORT).getMetadata());
	        } catch(ComponentNotReadyException e) {
	            e.setAttributeName(XML_FILEURL_ATTRIBUTE);
	            throw e;
	        }
		}
		else {
			writer.reset();
		}
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.graph.Node#getUsedUrls()
	 */
	@Override
	public String[] getUsedUrls() {
		return new String[] { fileURL };
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

	/**
	 *  Description of the Method
	 *
	 * @param  nodeXML  Description of Parameter
	 * @return          Description of the Returned Value
	 * @throws XMLConfigurationException 
	 * @throws AttributeNotFoundException 
	 * @since           May 21, 2002
	 */
	public static Node fromXML(TransformationGraph graph, Element nodeXML) throws XMLConfigurationException, AttributeNotFoundException {
		ComponentXMLAttributes xattribs=new ComponentXMLAttributes(nodeXML, graph);
		DataWriter aDataWriter = null;
		
		aDataWriter = new DataWriter(xattribs.getString(Node.XML_ID_ATTRIBUTE),
								xattribs.getStringEx(XML_FILEURL_ATTRIBUTE, RefResFlag.URL),
								xattribs.getString(XML_CHARSET_ATTRIBUTE, null),
								xattribs.getBoolean(XML_APPEND_ATTRIBUTE, false));
        if (xattribs.exists(XML_OUTPUT_FIELD_NAMES)){
            aDataWriter.setOutputFieldNames(xattribs.getBoolean(XML_OUTPUT_FIELD_NAMES));
        }
        if(xattribs.exists(XML_RECORDS_PER_FILE)) {
            aDataWriter.setRecordsPerFile(xattribs.getInteger(XML_RECORDS_PER_FILE));
        }
        if(xattribs.exists(XML_BYTES_PER_FILE)) {
            aDataWriter.setBytesPerFile(xattribs.getInteger(XML_BYTES_PER_FILE));
        }
		if (xattribs.exists(XML_RECORD_SKIP_ATTRIBUTE)){
			aDataWriter.setSkip(Integer.parseInt(xattribs.getString(XML_RECORD_SKIP_ATTRIBUTE)));
		}
		if (xattribs.exists(XML_RECORD_COUNT_ATTRIBUTE)){
			aDataWriter.setNumRecords(Integer.parseInt(xattribs.getString(XML_RECORD_COUNT_ATTRIBUTE)));
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
		if(xattribs.exists(XML_MK_DIRS_ATTRIBUTE)) {
			aDataWriter.setMkDirs(xattribs.getBoolean(XML_MK_DIRS_ATTRIBUTE));
        }
        if(xattribs.exists(XML_EXCLUDE_FIELDS_ATTRIBUTE)) {
            aDataWriter.setExcludeFields(xattribs.getString(XML_EXCLUDE_FIELDS_ATTRIBUTE));
        }
        if(xattribs.exists(XML_QUOTEDSTRINGS_ATTRIBUTE)) {
            aDataWriter.setQuotedStrings(xattribs.getBoolean(XML_QUOTEDSTRINGS_ATTRIBUTE));
            aDataWriter.quotedStringsHasDefaultValue = false;
        }
        if (xattribs.exists(XML_QUOTECHAR_ATTRIBUTE)) {
        	aDataWriter.setQuoteChar(QuotingDecoder.quoteCharFromString(xattribs.getString(XML_QUOTECHAR_ATTRIBUTE)));
        }
        if (xattribs.exists(XML_SORTED_INPUT_ATTRIBUTE)) {
        	aDataWriter.setSortedInput(xattribs.getBoolean(XML_SORTED_INPUT_ATTRIBUTE));
        }
		
		return aDataWriter;
	}

    protected boolean checkPorts(ConfigurationStatus status) {
        return !checkInputPorts(status, 1, 1) || !checkOutputPorts(status, 0, 1);
	}

	@Override
    public ConfigurationStatus checkConfig(ConfigurationStatus status) {
        super.checkConfig(status);
        
        if(checkPorts(status)) {
        	return status;
        }

        try {
        	FileUtils.canWrite(getGraph() != null ? getGraph().getRuntimeContext().getContextURL() : null, fileURL, mkDir);
        } catch (ComponentNotReadyException e) {
            status.add(e,ConfigurationStatus.Severity.ERROR,this,
            		ConfigurationStatus.Priority.NORMAL,XML_FILEURL_ATTRIBUTE);
        }

        try {
	        if (appendData && FileURLParser.isServerURL(fileURL) && FileURLParser.isArchiveURL(fileURL)) {
	        	status.add("Append true is not supported on remote archive files.", ConfigurationStatus.Severity.WARNING, this,
	            		ConfigurationStatus.Priority.NORMAL, XML_APPEND_ATTRIBUTE);
	        }
        } catch (MalformedURLException e) {
            status.add(e.toString(),ConfigurationStatus.Severity.ERROR,this,
            		ConfigurationStatus.Priority.NORMAL, XML_APPEND_ATTRIBUTE);        	
        }
        
        if (!StringUtils.isEmpty(excludeFields)) {
            DataRecordMetadata metadata = getInputPort(READ_FROM_PORT).getMetadata();
            int[] includedFieldIndices = null;

            try {
                includedFieldIndices = metadata.fieldsIndicesComplement(
                        excludeFields.split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX));

                if (includedFieldIndices.length == 0) {
                    status.add(new ConfigurationProblem("All data fields excluded!", Severity.ERROR, this,
                            Priority.NORMAL, XML_EXCLUDE_FIELDS_ATTRIBUTE));
                }
            } catch (IllegalArgumentException exception) {
                status.add(new ConfigurationProblem(ExceptionUtils.getMessage(exception), Severity.ERROR, this,
                        Priority.NORMAL, XML_EXCLUDE_FIELDS_ATTRIBUTE));
            }
        }

        return status;
    }
	
	@Override
	public String getType(){
		return COMPONENT_TYPE;
	}


    public int getBytesPerFile() {
        return bytesPerFile;
    }


    public void setBytesPerFile(int bytesPerFile) {
        this.bytesPerFile = bytesPerFile;
    }

    public int getRecordsPerFile() {
        return recordsPerFile;
    }

    public void setRecordsPerFile(int recordsPerFile) {
        this.recordsPerFile = recordsPerFile;
    }

    public void setOutputFieldNames(boolean outputFieldNames) {
        this.outputFieldNames = outputFieldNames;
    }
    
    public boolean getOutputFieldNames(){
    	return outputFieldNames;
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
	 * Sets fields which are used for file output name.
	 * 
	 * @param partitionOutFields
	 */
	public void setPartitionOutFields(String partitionOutFields) {
		attrPartitionOutFields = partitionOutFields;
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
	 * Gets number file tag for data partition.
	 * 
	 * @return
	 */
	public PartitionFileTagType getPartitionFileTag() {
		return partitionFileTagType;
	}

	/**
	 * Sets partition unassigned file name.
	 * 
	 * @param partitionUnassignedFileName
	 */
    public void setPartitionUnassignedFileName(String partitionUnassignedFileName) {
    	this.partitionUnassignedFileName = partitionUnassignedFileName;
	}

	/**
	 * Sets make directory.
	 * @param mkDir - true - creates output directories for output file
	 */
	public void setMkDirs(boolean mkDir) {
		this.mkDir = mkDir;
	}

    public void setExcludeFields(String excludeFields) {
        this.excludeFields = excludeFields;
    }
    
    public String getExcludedFields(){
    	return excludeFields;
    }

    public void setQuotedStrings(boolean quotedStrings) {
    	this.quotedStrings = quotedStrings;
    }
    
	public void setQuoteChar(Character quoteChar) {
		this.quoteChar = quoteChar;
	}
    
    public boolean getQuotedStrings() {
    	return quotedStrings;
    }
    
    public void setFileURL(String fileURL){
    	this.fileURL = fileURL;
    }
    
    public void setSortedInput(boolean sortedInput) {
    	this.sortedInput = sortedInput;
    }
    
    public boolean isSortedInput() {
    	return sortedInput;
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
	
}
