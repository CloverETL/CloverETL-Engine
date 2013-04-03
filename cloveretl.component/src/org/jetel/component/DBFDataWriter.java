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
import java.net.URL;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.Defaults;
import org.jetel.data.formatter.provider.DBFDataFormatterProvider;
import org.jetel.data.lookup.LookupTable;
import org.jetel.database.dbf.DBFTypes;
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
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.metadata.DataRecordParsingType;
import org.jetel.util.ExceptionUtils;
import org.jetel.util.MultiFileWriter;
import org.jetel.util.SynchronizeUtils;
import org.jetel.util.bytes.SystemOutByteChannel;
import org.jetel.util.bytes.WritableByteChannelIterator;
import org.jetel.util.file.FileUtils;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.string.StringUtils;
import org.w3c.dom.Element;

/**
 *  <h3>DBFDataWriter Component</h3>
 *
 * <!-- All records from input port [0] are formatted to dBase/FoxBase style file-->
 * 
 * <table border="1">
 *  <th>Component:</th>
 * <tr><td><h4><i>Name:</i></h4></td>
 * <td>DBFDataWriter</td></tr>
 * <tr><td><h4><i>Category:</i></h4></td>
 * <td></td></tr>
 * <tr><td><h4><i>Description:</i></h4></td>
 * <td>All records from input port [0] are formatted to dBase/FoxBase style table and written to specified file.</td></tr>
 * <tr><td><h4><i>Inputs:</i></h4></td>
 * <td>[0]- input records</td></tr>
 * <tr><td><h4><i>Outputs:</i></h4></td>
 * <td></td></tr>
 * <tr><td><h4><i>Comment:</i></h4></td>
 * <td></td></tr>
 * </table>
 *  <br>  
 *  <table border="1">
 *  <th>XML attributes:</th>
 *  <tr><td><b>type</b></td><td>"DBF_DATA_WRITER"</td></tr>
 *  <tr><td><b>id</b></td><td>component identification</td>
 *  <tr><td><b>fileURL</b></td><td>Output files mask.
 *  Use wildcard '#' to specify where to insert sequential number of file. Number of consecutive wildcards specifies
 *  minimal length of the number. Name without wildcard specifies only one file.</td>
 *  <tr><td><b>charset</b></td><td>character encoding of the output file (if not specified, then ISO-8859-1 is used)</td>
 *  <tr><td><b>append</b></td><td>whether to append data at the end if output file exists or replace it (values: true/false)</td>
 *  <tr><td><b>recordsPerFile</b></td><td>max number of records in one output file</td>
 *  <tr><td><b>bytesPerFile</b></td><td>Max size of output files. To avoid splitting a record to two files, max size could be slightly overreached.</td>
 *  <tr><td><b>recordSkip</b></td><td>number of skipped records</td>
 *  <tr><td><b>recordCount</b></td><td>number of written records</td>
 *  </tr>
 *  </table>  
 *
 * <h4>Example:</h4>
 * <pre>&lt;Node append="true" fileURL="${WORKSPACE}/output/customers.dbf"
 *  id="DBF_DATA_WRITER" type="DBF_DATA_WRITER"&gt;
 * &lt;/Node&gt;
 * 
 * 
 *  @author dpavlis (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *         
 * @since Mar, 13, 2012
 *
 */
public class DBFDataWriter extends Node {

	public final static String COMPONENT_TYPE = "DBF_DATA_WRITER";
	
	private final static int READ_FROM_PORT = 0;
	private final static int OUTPUT_PORT = 0;
	
	private static final String XML_FILEURL_ATTRIBUTE = "fileURL";
	private static final String XML_APPEND_ATTRIBUTE = "append";
	private static final String XML_CHARSET_ATTRIBUTE = "charset";
	private static final String XML_DBF_TYPE = "dbfType";
	
	private static final String XML_MK_DIRS_ATTRIBUTE = "makeDirs";
	private static final String XML_RECORDS_PER_FILE = "recordsPerFile";
	private static final String XML_RECORD_SKIP_ATTRIBUTE = "recordSkip";
	private static final String XML_RECORD_COUNT_ATTRIBUTE = "recordCount";
	private static final String XML_EXCLUDE_FIELDS_ATTRIBUTE = "excludeFields";
	private static final String XML_PARTITIONKEY_ATTRIBUTE = "partitionKey";
	private static final String XML_PARTITION_ATTRIBUTE = "partition";
	private static final String XML_PARTITION_OUTFIELDS_ATTRIBUTE = "partitionOutFields";
	private static final String XML_PARTITION_FILETAG_ATTRIBUTE = "partitionFileTag";
	private static final String XML_PARTITION_UNASSIGNED_FILE_NAME_ATTRIBUTE = "partitionUnassignedFileName";
	
	private static final byte[] TYPES = new byte[] {0x02, 0x03, 0x30, 0x31, 0x32, 0x43, (byte) 0xFB};

	private static Log logger = LogFactory.getLog(DBFDataWriter.class);
	
	private String fileURL;
	private boolean appendData;
	private String charsetName;
	private int dbfType;
	
	private boolean mkDir;
	private int recordsPerFile;
    private int skip;
	private int numRecords;
	private String excludeFields;
	private String partition;
	private String attrPartitionKey;
	private LookupTable lookupTable;
	private String attrPartitionOutFields;
	private PartitionFileTagType partitionFileTagType = PartitionFileTagType.NUMBER_FILE_TAG;
	private String partitionUnassignedFileName;

	private DBFDataFormatterProvider formatterProvider;
	private MultiFileWriter writer;
	private WritableByteChannel writableByteChannel;
	
	/**
	 * Constructor.
	 * 
	 * @param id
	 * @param fileURL
	 * @param charset
	 * @param appendData
	 * @param fields
	 */
	public DBFDataWriter(String id, String fileURL, String charset, boolean appendData, byte dbfType) {
		super(id);
		this.fileURL = fileURL;
		this.appendData = appendData;
		this.charsetName = charset == null ? Defaults.DataFormatter.DEFAULT_CHARSET_ENCODER : charset;
		formatterProvider = new DBFDataFormatterProvider(charsetName, dbfType);
	}
	
	/**
	 * Constructor.
	 * 
	 * @param id
	 * @param writableByteChannel
	 * @param charset
	 * @param appendData
	 * @param fields
	 */
	public DBFDataWriter(String id, WritableByteChannel writableByteChannel, String charset, 
			boolean appendData, String[] fields) {
		super(id);
		this.writableByteChannel = writableByteChannel;
		this.appendData = appendData;
		this.charsetName = charset;
		throw new UnsupportedOperationException("Can't work with WritableByteChannel !");
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
	        writer.init(getInputPort(READ_FROM_PORT).getMetadata());
		} else {
			writer.reset();
		}
	}

	@Override
	public Result execute() throws Exception {
		InputPort inPort = getInputPort(READ_FROM_PORT);
		DataRecord record = DataRecordFactory.newRecord(inPort.getMetadata());
		record.init();
		while (record != null && runIt) {
			record = inPort.readRecord(record);
			if (record != null) {
				int recordsAtFile = writer.getCountOfRecordsAtCurrentTarget();
				if (recordsPerFile > 0 && recordsAtFile > 0 && recordsAtFile % recordsPerFile == 0) {
					//not very nice but we need to "close the file" = write footer and reset counter of records 
					//for current target hold by the formatter if maximum count of records is written at the file
					//this means that following records will be written to some other file
		        	formatterProvider.getCurrentFormatter().writeFooter();
		        	formatterProvider.getCurrentFormatter().resetRecordCounter();
		        }
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

	@Override
	public synchronized void free() {
		super.free();
		if (writer != null)
			try {
				writer.close();
			} catch(Throwable t) {
				logger.warn("Resource releasing failed.", t);
			}
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.GraphElement#checkConfig()
	 */
    @Override
    public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		super.checkConfig(status);
		
		if(!checkInputPorts(status, 1, 1)
				|| !checkOutputPorts(status, 0, 1)) {
			return status;
		}
		
		try {
			URL url = FileUtils.getFileURL(fileURL);
			if (!url.getProtocol().equals("file")) {
				status.add(new ConfigurationProblem("Protocol " + url.getProtocol() + " is not supported by the component.", 
						Severity.ERROR, this, Priority.NORMAL, XML_FILEURL_ATTRIBUTE));
			}
		} catch (MalformedURLException e1) {
			//nothing to do here - error for invalid URL is reported by another check 
		}

		CharsetEncoder encoder = Charset.forName(charsetName).newEncoder();
		if (encoder.maxBytesPerChar() != 1) {
			status.add(new ConfigurationProblem("Invalid charset used. 8bit fixed-width encoding needs to be used.", 
					Severity.ERROR, this, Priority.NORMAL));
		}
		
        try {
        	FileUtils.canWrite(getGraph() != null ? getGraph().getRuntimeContext().getContextURL() : null, 
        			fileURL, mkDir);
        } catch (ComponentNotReadyException e) {
            status.add(e,ConfigurationStatus.Severity.ERROR,this,
            		ConfigurationStatus.Priority.NORMAL,XML_FILEURL_ATTRIBUTE);
        }
        
        if (getInputPort(READ_FROM_PORT).getMetadata().getParsingType() != DataRecordParsingType.FIXEDLEN){
        	status.add("Component DBFWriter supports only fixed-length metadata on input port.",
        			ConfigurationStatus.Severity.ERROR,this, ConfigurationStatus.Priority.NORMAL,XML_FILEURL_ATTRIBUTE);
		}
        
        for(DataFieldMetadata field : getInputPort(READ_FROM_PORT).getMetadata()){
        	try{
        		DBFTypes.cloverType2dbf(field.getDataType());
        	}catch(Exception ex){
        		status.add(String.format("Error at field \"%s\". %s",field.getName(),ExceptionUtils.getMessage(ex)),ConfigurationStatus.Severity.ERROR,this,
            		ConfigurationStatus.Priority.NORMAL,XML_FILEURL_ATTRIBUTE);
        	}
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
        writer.setRecordsPerFile(recordsPerFile);
        writer.setAppendData(appendData);
        writer.setSkip(skip);
        writer.setNumRecords(numRecords);
        writer.setDictionary(graph.getDictionary());
        if (attrPartitionKey != null) {
            writer.setLookupTable(lookupTable);
            writer.setPartitionKeyNames(attrPartitionKey.split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX));
            writer.setPartitionFileTag(partitionFileTagType);
            writer.setPartitionUnassignedFileName(partitionUnassignedFileName);
        	if (attrPartitionOutFields != null) {
        		writer.setPartitionOutFields(attrPartitionOutFields.split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX));
        	}
        }
        
        String[] excludedFieldNames = null;
        if (!StringUtils.isEmpty(excludeFields)) {
            excludedFieldNames = excludeFields.split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);
            formatterProvider.setExcludedFieldNames(excludedFieldNames);
        }
        
        writer.setOutputPort(getOutputPort(OUTPUT_PORT)); //for port protocol: target file writes data
        writer.setMkDir(mkDir);
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
	public static Node fromXML(TransformationGraph graph, Element nodeXML) throws XMLConfigurationException, AttributeNotFoundException {
		ComponentXMLAttributes xattribs=new ComponentXMLAttributes(nodeXML, graph);
		DBFDataWriter aDataWriter = null;
		
		aDataWriter = new DBFDataWriter(xattribs.getString(Node.XML_ID_ATTRIBUTE),
								xattribs.getString(XML_FILEURL_ATTRIBUTE),
								xattribs.getString(XML_CHARSET_ATTRIBUTE,null),
								xattribs.getBoolean(XML_APPEND_ATTRIBUTE, false),
								TYPES[xattribs.getInteger(XML_DBF_TYPE, 1)]);
		if (xattribs.exists(XML_RECORD_SKIP_ATTRIBUTE)){
			aDataWriter.setSkip(Integer.parseInt(xattribs.getString(XML_RECORD_SKIP_ATTRIBUTE)));
		}
		if (xattribs.exists(XML_RECORD_COUNT_ATTRIBUTE)){
			aDataWriter.setNumRecords(Integer.parseInt(xattribs.getString(XML_RECORD_COUNT_ATTRIBUTE)));
		}
        if(xattribs.exists(XML_RECORDS_PER_FILE)) {
        	aDataWriter.setRecordsPerFile(xattribs.getInteger(XML_RECORDS_PER_FILE));
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
		if(xattribs.exists(XML_MK_DIRS_ATTRIBUTE)) {
			aDataWriter.setMkDirs(xattribs.getBoolean(XML_MK_DIRS_ATTRIBUTE));
        }
		if(xattribs.exists(XML_EXCLUDE_FIELDS_ATTRIBUTE)) {
            aDataWriter.setExcludeFields(xattribs.getString(XML_EXCLUDE_FIELDS_ATTRIBUTE));
        }
		if(xattribs.exists(XML_PARTITION_UNASSIGNED_FILE_NAME_ATTRIBUTE)) {
			aDataWriter.setPartitionUnassignedFileName(xattribs.getString(XML_PARTITION_UNASSIGNED_FILE_NAME_ATTRIBUTE));
        }
		
		return aDataWriter;
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
    
    /**
	 * Sets partition unassigned file name.
	 * 
	 * @param partitionUnassignedFileName
	 */
    public void setPartitionUnassignedFileName(String partitionUnassignedFileName) {
    	this.partitionUnassignedFileName = partitionUnassignedFileName;
	}
}
