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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.Defaults;
import org.jetel.data.formatter.provider.CloverDataFormatterProvider;
import org.jetel.data.lookup.LookupTable;
import org.jetel.enums.PartitionFileTagType;
import org.jetel.enums.ProcessingType;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.InputPortDirect;
import org.jetel.graph.Node;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataFieldContainerType;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataFieldType;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.MultiFileWriter;
import org.jetel.util.SynchronizeUtils;
import org.jetel.util.bytes.CloverBuffer;
import org.jetel.util.file.FileURLParser;
import org.jetel.util.file.FileUtils;
import org.jetel.util.file.FileUtils.PortURL;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.property.RefResFlag;
import org.jetel.util.string.StringUtils;
import org.w3c.dom.Element;

/**
 *  <h3>Clover Data Writer Component</h3>
 *
 * <!-- Writes data in Clover internal format to binary file. -->
 *
 * <table border="1">
 *  <th>Component:</th>
 * <tr><td><h4><i>Name:</i></h4></td>
 * <td>CloverDataWriter</td></tr>
 * <tr><td><h4><i>Category:</i></h4></td>
 * <td>Writers</td></tr>
 * <tr><td><h4><i>Description:</i></h4></td>
 * <td>Reads data from input port and writes them to binary file in Clover internal
 *  format. With records can be saved indexes of records in binary file (for 
 *  reading not all records afterward) and metadata definition. If compressData 
 *  attribuet is set to "true", data are saved in zip file with the structure:<br>DATA/fileName<br>INDEX/fileName.idx<br>
 *   METADATA/fileName.fmt<br>If compressData attribute is set to "false", all files
 *   are saved in the same directory (as specified in fileURL attribute)</td></tr>
 * <tr><td><h4><i>Inputs:</i></h4></td>
 * <td>one input port defined/connected.</td></tr>
 * <tr><td><h4><i>Outputs:</i></h4></td>
 * <td></tr>
 * <tr><td><h4><i>Comment:</i></h4></td></tr>
 * </table>
 *  <br>
 *  <table border="1">
 *  <th>XML attributes:</th>
 *  <tr><td><b>type</b></td><td>"CLOVER_WRITER"</td></tr>
 *  <tr><td><b>id</b></td><td>component identification</td>
 *  <tr><td><b>fileURL</b></td><td>path to the output file </td>
 *  <tr><td><b>append</b><br><i>optional</i></td><td>whether to append data at
 *   the end if output file exists or replace it (true/false - default true)</td>
 *  <tr><td><b>saveIndex</b><br><i>optional</i></td><td>indicates if indexes to records 
 *  in binary file are saved or not (true/false - default false)</td>
 *  <tr><td><b>saveMetadata</b><br><i>optional</i></td><td>indicates if metadata
 *   definition is saved or not (true/false - default false)</td>
 *  <tr><td><b>compressLevel</b><br><i>optional</i></td><td>Sets the compression level. The default
 *   setting is to compress using default ZIP compression level. 
 *  </tr>
 *  <tr><td><b>recordSkip</b></td><td>number of skipped records</td>
 *  <tr><td><b>recordCount</b></td><td>number of written records</td>
 *  </table>
 *
 *  <h4>Example:</h4>
 *  <pre>&lt;Node compressLevel="0" fileURL="customers.clv" id="CLOVER_WRITER0"
 *   saveIndex="true" saveMetadata="true" type="CLOVER_WRITER"/&gt;
 *  
 *  <pre>&lt;Node fileURL="customers.clv" id="CLOVER_WRITER0"
 *   saveIndex="true" type="CLOVER_WRITER"/&gt;
 *
/**
 * @author avackova (agata.vackova@javlinconsulting.cz) ; 
 * (c) JavlinConsulting s.r.o.
 *  www.javlinconsulting.cz
 *
 * @since Oct 12, 2006
 * @see CloverDataFormater
 *
 */
public class CloverDataWriter extends Node {

	private static final String XML_FILEURL_ATTRIBUTE = "fileURL";
	private static final String XML_APPEND_ATTRIBUTE = "append";
	private static final String XML_COMPRESSLEVEL_ATTRIBUTE = "compressLevel";
	private static final String XML_RECORD_SKIP_ATTRIBUTE = "recordSkip";
	private static final String XML_RECORD_COUNT_ATTRIBUTE = "recordCount";
	private static final String XML_MK_DIRS_ATTRIBUTE = "makeDirs";
    private static final String XML_EXCLUDE_FIELDS_ATTRIBUTE = "excludeFields";
	private static final String XML_RECORDS_PER_FILE = "recordsPerFile"; // FIXME does not work well because of the compression
	private static final String XML_BYTES_PER_FILE = "bytesPerFile";
	private static final String XML_PARTITIONKEY_ATTRIBUTE = "partitionKey";
	private static final String XML_PARTITION_ATTRIBUTE = "partition";
	private static final String XML_PARTITION_OUTFIELDS_ATTRIBUTE = "partitionOutFields";
	private static final String XML_PARTITION_FILETAG_ATTRIBUTE = "partitionFileTag";
	private static final String XML_PARTITION_UNASSIGNED_FILE_NAME_ATTRIBUTE = "partitionUnassignedFileName";
	private static final String XML_SORTED_INPUT_ATTRIBUTE = "sortedInput";
	private static final String XML_CREATE_EMPTY_FILES_ATTRIBUTE = "createEmptyFiles";

	public final static String COMPONENT_TYPE = "CLOVER_WRITER";
	private final static int READ_FROM_PORT = 0;
	private final static int OUTPUT_PORT = 0;

	private String fileURL;
	private boolean append;
	private CloverDataFormatterProvider formatterProvider;
	private InputPortDirect inPort;
	private int compressLevel;
    private int skip;
	private int numRecords = -1;
	
	private MultiFileWriter writer;
	
	private int bytesPerFile; // FIXME does not work well because of the compression
	private int recordsPerFile;
	private String partition;
	private String attrPartitionKey;
	private LookupTable lookupTable;
	private String attrPartitionOutFields;
	private PartitionFileTagType partitionFileTagType = PartitionFileTagType.NUMBER_FILE_TAG;
	private String partitionUnassignedFileName;
	private boolean mkDir;
	private boolean sortedInput = false;
	private boolean createEmptyFiles = true;

    private String excludeFields;

	static Log logger = LogFactory.getLog(CloverDataWriter.class);

 	public CloverDataWriter(String id, String fileURL) {
		super(id);
		this.fileURL = fileURL;
	}

	protected void prepareWriter() throws ComponentNotReadyException {
		if (firstRun()) {
	        try {
	            writer.init(inPort.getMetadata());
	        } catch(ComponentNotReadyException e) {
	            e.setAttributeName(XML_FILEURL_ATTRIBUTE);
	            throw e;
	        }
		}
		else {
			writer.reset();
		}
	}
	
	@Override
    public void preExecute() throws ComponentNotReadyException {
    	super.preExecute();
    	prepareWriter();
    }
	
	@Override
	public Result execute() throws Exception {
		// CLO-2657: use direct input port reading
		CloverBuffer recordBuffer = CloverBuffer.allocateDirect(Defaults.Record.RECORD_INITIAL_SIZE);
		while (inPort.readRecordDirect(recordBuffer) && runIt) {
			writer.writeDirect(recordBuffer);
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
		} catch (IOException e) {
			throw new ComponentNotReadyException(e);
		}
	}
    
	@Override
	public synchronized void free() {
		super.free();
		if (writer != null) {
			try {
				writer.close();
			} catch(Throwable t) {
				logger.warn("Resource releasing failed for '" + getId() + "'.", t);
			}
		}
	}
	
	private void checkFileURL() throws ComponentNotReadyException {
		if (FileUtils.isPortURL(fileURL)) {
			PortURL portUrl = FileUtils.getPortURL(fileURL);
			if (portUrl.getProcessingType() == ProcessingType.SOURCE) {
				throw new ComponentNotReadyException("Unsupported output method: " + portUrl.getProcessingType());
			}
			String fieldName = portUrl.getFieldName();
			DataRecordMetadata record = getOutputPort(OUTPUT_PORT).getMetadata();
			DataFieldMetadata field = record.getField(fieldName);
			if (field == null) {
				throw new ComponentNotReadyException("The field not found for the statement: '" + fileURL + "'");
			}
			if ((field.getDataType() != DataFieldType.BYTE) && (field.getDataType() != DataFieldType.CBYTE)) {
				throw new ComponentNotReadyException("Unsupported output field type: '" + field.getDataType() + "'. Use 'byte' or 'cbyte' instead.");
			}
			if (field.getContainerType() != DataFieldContainerType.SINGLE) {
				throw new ComponentNotReadyException("Unsupported output field container type: '" + field.getContainerType() + "'");
			}
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

        if (StringUtils.isEmpty(fileURL)) {
            status.addError(this, XML_FILEURL_ATTRIBUTE, "Attribute 'fileURL' is required.");
        	return status;
        }
        
        try {
        	FileUtils.canWrite(getContextURL(), fileURL, mkDir);
        } catch (ComponentNotReadyException e) {
            status.addError(this, XML_FILEURL_ATTRIBUTE, e);
        }
        
        try {
			if (append && FileURLParser.isArchiveURL(fileURL) && FileURLParser.isServerURL(fileURL)) {
			    status.addWarning(this, XML_APPEND_ATTRIBUTE, "Append true is not supported on remote archive files.");
			}
		} catch (MalformedURLException e) {
            status.addError(this, XML_APPEND_ATTRIBUTE, e);
		}
        
        try {
        	checkFileURL();
        } catch (Exception e) {
        	status.addError(this, XML_FILEURL_ATTRIBUTE, e);
        }
        
        return status;
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
	 * @see org.jetel.graph.GraphElement#init()
	 */
	@Override
	public void init() throws ComponentNotReadyException {
        if(isInitialized()) return;
		super.init();
		
		checkFileURL();
		
		//prepare formatter provider
		formatterProvider = new CloverDataFormatterProvider();
		formatterProvider.setAppend(this.append);
		formatterProvider.setCompressLevel(compressLevel);

        if (!StringUtils.isEmpty(excludeFields)) {
        	String[] excludedFieldNames = excludeFields.split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);
            formatterProvider.setExcludedFieldNames(excludedFieldNames);
        }
		
		initLookupTable();

		writer = new MultiFileWriter(formatterProvider, getContextURL(), fileURL);
		writer.setLogger(logger);
        writer.setBytesPerFile(bytesPerFile);
        writer.setRecordsPerFile(recordsPerFile);
		writer.setAppendData(append);
		writer.setSkip(skip);
		writer.setNumRecords(numRecords);
		writer.setDictionary(getGraph().getDictionary());
        writer.setOutputPort(getOutputPort(OUTPUT_PORT)); //for port protocol: target file writes data
		writer.setMkDir(mkDir);
		writer.setUseChannel(false); // prefer OutputStream
		writer.setCreateEmptyFiles(createEmptyFiles);
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

		inPort = getInputPortDirect(READ_FROM_PORT);
	}

	@Override
	public String[] getUsedUrls() {
		return new String[] { fileURL };
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
		CloverDataWriter aDataWriter = null;
		
		aDataWriter = new CloverDataWriter(xattribs.getString(Node.XML_ID_ATTRIBUTE),
				xattribs.getStringEx(XML_FILEURL_ATTRIBUTE, null, RefResFlag.URL));
		aDataWriter.setAppend(xattribs.getBoolean(XML_APPEND_ATTRIBUTE,false));
		aDataWriter.setCompressLevel(xattribs.getInteger(XML_COMPRESSLEVEL_ATTRIBUTE,-1));
		if (xattribs.exists(XML_RECORD_SKIP_ATTRIBUTE)){
			aDataWriter.setSkip(Integer.parseInt(xattribs.getString(XML_RECORD_SKIP_ATTRIBUTE)));
		}
		if (xattribs.exists(XML_RECORD_COUNT_ATTRIBUTE)){
			aDataWriter.setNumRecords(Integer.parseInt(xattribs.getString(XML_RECORD_COUNT_ATTRIBUTE)));
		}
		if(xattribs.exists(XML_MK_DIRS_ATTRIBUTE)) {
			aDataWriter.setMkDirs(xattribs.getBoolean(XML_MK_DIRS_ATTRIBUTE));
        }
        if(xattribs.exists(XML_EXCLUDE_FIELDS_ATTRIBUTE)) {
            aDataWriter.setExcludeFields(xattribs.getString(XML_EXCLUDE_FIELDS_ATTRIBUTE));
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
        if (xattribs.exists(XML_SORTED_INPUT_ATTRIBUTE)) {
        	aDataWriter.setSortedInput(xattribs.getBoolean(XML_SORTED_INPUT_ATTRIBUTE));
        }
        if (xattribs.exists(XML_CREATE_EMPTY_FILES_ATTRIBUTE)) {
        	aDataWriter.setCreateEmptyFiles(xattribs.getBoolean(XML_CREATE_EMPTY_FILES_ATTRIBUTE));
        }
		
		return aDataWriter;
	}

	public void setCompressLevel(int compressLevel) {
		this.compressLevel = compressLevel;
	}

	public void setAppend(boolean append) {
		this.append = append;
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
	 * Sets make directory.
	 * @param mkDir - true - creates output directories for output file
	 */
	private void setMkDirs(boolean mkDir) {
		this.mkDir = mkDir;
	}

	private void setBytesPerFile(int bytesPerFile) {
		this.bytesPerFile = bytesPerFile;
	}

	private void setRecordsPerFile(int recordsPerFile) {
		this.recordsPerFile = recordsPerFile;
	}

	private void setPartition(String partition) {
		this.partition = partition;
	}

	private void setPartitionKey(String partitionKey) {
		this.attrPartitionKey = partitionKey;
	}

	private void setPartitionOutFields(String partitionOutFields) {
		this.attrPartitionOutFields = partitionOutFields;
	}

	private void setPartitionFileTag(String partitionFileTag) {
		this.partitionFileTagType = PartitionFileTagType.valueOfIgnoreCase(partitionFileTag);
	}

	private void setPartitionUnassignedFileName(String partitionUnassignedFileName) {
		this.partitionUnassignedFileName = partitionUnassignedFileName;
	}

	private void setSortedInput(boolean sortedInput) {
		this.sortedInput = sortedInput;
	}

	private void setExcludeFields(String excludeFields) {
		this.excludeFields = excludeFields;
	}

	private void setCreateEmptyFiles(boolean createEmptyFiles) {
		this.createEmptyFiles = createEmptyFiles;
	}

}
