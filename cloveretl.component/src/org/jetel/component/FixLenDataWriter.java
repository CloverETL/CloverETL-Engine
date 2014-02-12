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
import java.nio.channels.WritableByteChannel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.Defaults;
import org.jetel.data.formatter.provider.FixLenDataFormatterProvider;
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
import org.jetel.util.MultiFileWriter;
import org.jetel.util.bytes.SystemOutByteChannel;
import org.jetel.util.bytes.WritableByteChannelIterator;
import org.jetel.util.file.FileUtils;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.property.RefResFlag;
import org.w3c.dom.Element;

/**
 *  <h3>FixLenDataWriter Component</h3>
 *
 * <!-- All records from input port [0] are formatted with delimiter and written to specified file -->
 *
 * <table border="1">
 *  <th>Component:</th>
 * <tr><td><h4><i>Name:</i></h4></td>
 * <td>FixLenDataWriter</td></tr>
 * <tr><td><h4><i>Category:</i></h4></td>
 * <td></td></tr>
 * <tr><td><h4><i>Description:</i></h4></td>
 * <td>All records from input port [0] are formatted with sizes specified in metadata and written to specified file.<br>
 * Sizes are taken from metadata specified for port[0] data flow.</td></tr>
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
 *  <tr><td><b>type</b></td><td>"FIXLEN_DATA_WRITER"</td></tr>
 *  <tr><td><b>id</b></td><td>component identification</td>
 *  <tr><td><b>fileURL</b></td><td>Output files mask.
 *  Use wildcard '#' to specify where to insert sequential number of file. Number of consecutive wildcards specifies
 *  minimal length of the number. Name without wildcard specifies only one file.</td>
 *  <tr><td><b>charset</b><br><i>optional</i></td><td>character encoding of the output file (if not specified, then ISO-8859-1 is used)</td>
 *  <tr><td><b>append</b><br><i>optional</i></td><td>whether to append data at the end if output file exists or replace it (values: true/false). Default is false</td>
 *  <tr><td><b>outputFieldNames</b><br><i>optional</i></td><td>print names of individual fields into output file - as a first row (values: true/false, default:false)</td>
 *  <tr><td><b>filler</b><br><i>optional</i></td><td>allows specifying what character will be used for padding output fields. Default is " " (space)></td>
 *  <tr><td><b>recordFiller</b><br><i>optional</i></td><td>allows specifying what character will be used for padding gaps between fields in output records. Default is "."></td>
 *  <tr><td><b>leftAlign</b><br><i>optional</i></td><td>specify if values should be align left or right. It is used when filler is defined. Default is "true"></td>
 *  <tr><td><b>recordsPerFile</b></td><td>max number of records in one output file</td>
 *  <tr><td><b>bytesPerFile</b></td><td>Max size of output files. To avoid splitting a record to two files, max size could be slightly overreached.</td>
 *  <tr><td><b>recordSkip</b></td><td>number of skipped records</td>
 *  <tr><td><b>recordCount</b></td><td>number of written records</td>
 *  </table>
 *
 * <h4>Example:</h4>
 * <pre>&lt;Node type="FIXLEN_DATA_WRITER" id="Writer" fileURL="/tmp/transfor.out" append="true" /&gt;</pre>
 * 
 * <pre>&lt;Node type="FIXLEN_DATA_WRITER" id="Writer" fileURL="/tmp/transfor.out" append="true" OneRecordPerLine="true" LineSeparator="\r\n" /&gt;</pre>
 *
 *
 * @author      dpavlis
 * @since       April 4, 2002
 */
public class FixLenDataWriter extends Node {
	private static final String XML_APPEND_ATTRIBUTE = "append";
	private static final String XML_FILEURL_ATTRIBUTE = "fileURL";
	private static final String XML_CHARSET_ATTRIBUTE = "charset";
	private static final String XML_OUTPUT_FIELD_NAMES = "outputFieldNames";
	private static final String XML_FIELD_FILLER = "fieldFiller";
	private static final String XML_RECORD_FILLER = "recordFiller";
	private static final String XML_RECORDS_PER_FILE = "recordsPerFile";
	private static final String XML_BYTES_PER_FILE = "bytesPerFile";
	private static final String XML_RECORD_SKIP_ATTRIBUTE = "recordSkip";
	private static final String XML_RECORD_COUNT_ATTRIBUTE = "recordCount";
	private static final String XML_PARTITIONKEY_ATTRIBUTE = "partitionKey";
	private static final String XML_PARTITION_ATTRIBUTE = "partition";
	private static final String XML_PARTITION_OUTFIELDS_ATTRIBUTE = "partitionOutFields";
	private static final String XML_PARTITION_FILETAG_ATTRIBUTE = "partitionFileTag";
	private static final String XML_LEFT_ALIGN_ATTRIBUTE = "leftAlign";
	private static final String XML_PARTITION_UNASSIGNED_FILE_NAME_ATTRIBUTE = "partitionUnassignedFileName";

	private static final boolean DEFAULT_APPEND=false;
	
	private String fileURL;
	private boolean appendData;
	private FixLenDataFormatterProvider formatterProvider;
    private MultiFileWriter writer;
	private boolean outputFieldNames=false;
	private int recordsPerFile;
	private int bytesPerFile;
    private int skip;
	private int numRecords;
	private WritableByteChannel writableByteChannel;

	private String partition;
	private String attrPartitionKey;
	private LookupTable lookupTable;
	private String attrPartitionOutFields;
	private PartitionFileTagType partitionFileTagType = PartitionFileTagType.NUMBER_FILE_TAG;
	private String partitionUnassignedFileName;

	static Log logger = LogFactory.getLog(FixLenDataWriter.class);

	/**  Description of the Field */
	public final static String COMPONENT_TYPE = "FIXLEN_DATA_WRITER";
	private final static int READ_FROM_PORT = 0;
	private final static int OUTPUT_PORT = 0;


	/**
	 *Constructor for the FixLenDataWriterNIO object
	 *
	 * @param  id          Description of the Parameter
	 * @param  fileURL     Description of the Parameter
	 * @param  charset     Description of the Parameter
	 * @param  appendData  Description of the Parameter
	 */
	public FixLenDataWriter(String id, String fileURL, String charset, boolean appendData) {
		super(id);
		this.fileURL = fileURL;
		this.appendData = appendData;
		formatterProvider = new FixLenDataFormatterProvider(charset != null ? charset : Defaults.DataFormatter.DEFAULT_CHARSET_ENCODER);
	}

	public FixLenDataWriter(String id, WritableByteChannel writableByteChannel, String charset) {
		super(id);
		this.writableByteChannel = writableByteChannel;
		formatterProvider = new FixLenDataFormatterProvider(charset != null ? charset : Defaults.DataFormatter.DEFAULT_CHARSET_ENCODER);
	}

	@Override
	public void preExecute() throws ComponentNotReadyException {
		super.preExecute();
		
		if (firstRun()) {
	        writer.init(getInputPort(READ_FROM_PORT).getMetadata());
		}
		else {
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
		        writer.write(record);
			}
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
		
		initLookupTable();
		
        // initialize multifile writer based on prepared formatter
		if (fileURL != null) {
	        writer = new MultiFileWriter(formatterProvider, getContextURL(), fileURL);
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

        	if (attrPartitionOutFields != null) {
        		writer.setPartitionOutFields(attrPartitionOutFields.split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX));
        	}
        }
        if(outputFieldNames) {
        	formatterProvider.setHeader(getInputPort(READ_FROM_PORT).getMetadata().getFieldNamesHeader(false, null));
        }
        writer.setDictionary(graph != null ? graph.getDictionary() : null);
        writer.setOutputPort(getOutputPort(OUTPUT_PORT)); //for port protocol: target file writes data
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
	 * @throws AttributeNotFoundException 
	 * @since           May 21, 2002
	 */
    public static Node fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException, AttributeNotFoundException {
		FixLenDataWriter aFixLenDataWriterNIO = null;
		ComponentXMLAttributes xattribs=new ComponentXMLAttributes(xmlElement, graph);
		
		aFixLenDataWriterNIO = new FixLenDataWriter(
				xattribs.getString(XML_ID_ATTRIBUTE), 
				xattribs.getStringEx(XML_FILEURL_ATTRIBUTE, RefResFlag.URL),
				xattribs.getString(XML_CHARSET_ATTRIBUTE, null),
				xattribs.getBoolean(XML_APPEND_ATTRIBUTE,DEFAULT_APPEND));

		if (xattribs.exists(XML_OUTPUT_FIELD_NAMES)){
		    aFixLenDataWriterNIO.setOutputFieldNames(xattribs.getBoolean(XML_OUTPUT_FIELD_NAMES));
		}
	
		if (xattribs.exists(XML_FIELD_FILLER)){
		    aFixLenDataWriterNIO.setFieldFiller(xattribs.getString(XML_FIELD_FILLER));
		}
		if (xattribs.exists(XML_RECORD_FILLER)){
		    aFixLenDataWriterNIO.setRecordFiller(xattribs.getString(XML_RECORD_FILLER));
		}
		if(xattribs.exists(XML_LEFT_ALIGN_ATTRIBUTE)) {
			aFixLenDataWriterNIO.setLeftAlign(
					Boolean.valueOf(xattribs.getBoolean(XML_LEFT_ALIGN_ATTRIBUTE)));
        }
        if(xattribs.exists(XML_RECORDS_PER_FILE)) {
            aFixLenDataWriterNIO.setRecordsPerFile(xattribs.getInteger(XML_RECORDS_PER_FILE));
        }
        if(xattribs.exists(XML_BYTES_PER_FILE)) {
            aFixLenDataWriterNIO.setBytesPerFile(xattribs.getInteger(XML_BYTES_PER_FILE));
        }
		if (xattribs.exists(XML_RECORD_SKIP_ATTRIBUTE)){
			aFixLenDataWriterNIO.setSkip(Integer.parseInt(xattribs.getString(XML_RECORD_SKIP_ATTRIBUTE)));
		}
		if (xattribs.exists(XML_RECORD_COUNT_ATTRIBUTE)){
			aFixLenDataWriterNIO.setNumRecords(Integer.parseInt(xattribs.getString(XML_RECORD_COUNT_ATTRIBUTE)));
		}
		if(xattribs.exists(XML_PARTITIONKEY_ATTRIBUTE)) {
			aFixLenDataWriterNIO.setPartitionKey(xattribs.getString(XML_PARTITIONKEY_ATTRIBUTE));
        }
		if(xattribs.exists(XML_PARTITION_ATTRIBUTE)) {
			aFixLenDataWriterNIO.setPartition(xattribs.getString(XML_PARTITION_ATTRIBUTE));
        }
		if(xattribs.exists(XML_PARTITION_FILETAG_ATTRIBUTE)) {
			aFixLenDataWriterNIO.setPartitionFileTag(xattribs.getString(XML_PARTITION_FILETAG_ATTRIBUTE));
        }
		if(xattribs.exists(XML_PARTITION_OUTFIELDS_ATTRIBUTE)) {
			aFixLenDataWriterNIO.setPartitionOutFields(xattribs.getString(XML_PARTITION_OUTFIELDS_ATTRIBUTE));
        }
		if(xattribs.exists(XML_PARTITION_UNASSIGNED_FILE_NAME_ATTRIBUTE)) {
			aFixLenDataWriterNIO.setPartitionUnassignedFileName(xattribs.getStringEx(XML_PARTITION_UNASSIGNED_FILE_NAME_ATTRIBUTE, RefResFlag.URL));
        }
		
		return aFixLenDataWriterNIO;
	}

	 /**
     * @param outputFieldNames The outputFieldNames to set.
     */
    public void setOutputFieldNames(boolean outputFieldNames) {
        this.outputFieldNames = outputFieldNames;
    }
	
	/**
	 *  Description of the Method
	 *
	 * @return    Description of the Return Value
	 */
    @Override
    public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		super.checkConfig(status);
		 
        status.add(new ConfigurationProblem(
        		"Component is of type FIXLEN_DATA_WRITER, which is deprecated",
        		Severity.WARNING, this, Priority.NORMAL));

		if(!checkInputPorts(status, 1, 1)
				|| !checkOutputPorts(status, 0, 1)) {
			return status;
		}

        try {
        	FileUtils.canWrite(getContextURL(), fileURL);
        } catch (ComponentNotReadyException e) {
            status.add(e,ConfigurationStatus.Severity.ERROR,this,
            		ConfigurationStatus.Priority.NORMAL,XML_FILEURL_ATTRIBUTE);
        }
        
        return status;
    }
	
    /**
     * Which character (1st from specified string) will
     * be used as filler for padding output fields
     * 
     * @param filler The filler to set.
     */
    public void setFieldFiller(String filler) {
        this.formatterProvider.setFieldFiller(filler.charAt(0));
    }

    /**
     * Which character (1st from specified string) will
     * be used as record filler for padding output records
     * 
     * @param filler The filler to be set.
     */
    public void setRecordFiller(String filler) {
        this.formatterProvider.setRecordFiller(filler.charAt(0));
    }

    public void setLeftAlign(Boolean leftAlign) {
    	this.formatterProvider.setLeftAlign(leftAlign);
    }
    
    public void setBytesPerFile(int bytesPerFile) {
        this.bytesPerFile = bytesPerFile;
    }

    public void setRecordsPerFile(int recordsPerFile) {
        this.recordsPerFile = recordsPerFile;
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
    private void setPartitionUnassignedFileName(String partitionUnassignedFileName) {
    	this.partitionUnassignedFileName = partitionUnassignedFileName;
	}

}

