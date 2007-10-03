
/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2005-06  Javlin Consulting <info@javlinconsulting.cz>
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

import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.formatter.getter.TextTableFormatterGetter;
import org.jetel.data.lookup.LookupTable;
import org.jetel.enums.PartitionFileTagType;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.util.ComponentXMLAttributes;
import org.jetel.util.FileUtils;
import org.jetel.util.MultiFileWriter;
import org.jetel.util.SynchronizeUtils;
import org.jetel.util.WritableByteChannelIterator;
import org.w3c.dom.Element;

/**
 *  <h3>TextTableWriter Component</h3>
 *
 * <!-- All records from input port [0] are formatted to table and written to specified file or on screen-->
 * 
 * <table border="1">
 *  <th>Component:</th>
 * <tr><td><h4><i>Name:</i></h4></td>
 * <td>TextTableWriter</td></tr>
 * <tr><td><h4><i>Category:</i></h4></td>
 * <td></td></tr>
 * <tr><td><h4><i>Description:</i></h4></td>
 * <td>All records from input port [0] are formatted to table and written to specified file or on screen.</td></tr>
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
 *  <tr><td><b>type</b></td><td>"STRUCTURE_WRITER"</td></tr>
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
 * <pre>&lt;Node append="true" fileURL="${WORKSPACE}/output/structured_customers.txt"
 *  id="TEXT_TABLE_WRITER0" type="TEXT_TABLE_WRITER"&gt;
 * &lt;/Node&gt;
 * 
 * 
 * @author ausperger; 
 * (c) JavlinConsulting s.r.o.
 *  www.javlinconsulting.cz
 *
 * @since Feb 6, 2007
 *
 */
public class TextTableWriter extends Node {

	private static final String XML_APPEND_ATTRIBUTE = "append";
	private static final String XML_FILEURL_ATTRIBUTE = "fileURL";
	private static final String XML_CHARSET_ATTRIBUTE = "charset";
	private static final String XML_MASK_ATTRIBUTE = "mask";
	private static final String XML_RECORD_SKIP_ATTRIBUTE = "recordSkip";
	private static final String XML_RECORD_COUNT_ATTRIBUTE = "recordCount";
	private static final String XML_OUTPUT_FIELD_NAMES = "outputFieldNames";
	private static final String XML_RECORDS_PER_FILE = "recordsPerFile";
	private static final String XML_BYTES_PER_FILE = "bytesPerFile";
	private static final String XML_PARTITIONKEY_ATTRIBUTE = "partitionKey";
	private static final String XML_PARTITION_ATTRIBUTE = "partition";
	private static final String XML_PARTITION_FILETAG_ATTRIBUTE = "partitionFileTag";

	private String fileURL;
	private boolean appendData;
	private TextTableFormatterGetter formatterGetter;
	private MultiFileWriter writer;
    private int skip;
	private int numRecords;
	private WritableByteChannel writableByteChannel;
	private boolean outputFieldNames=true;
	private int recordsPerFile;
	private int bytesPerFile;
	
	private String partition;
	private String attrPartitionKey;
	private String[] partitionKey;
	private LookupTable lookupTable;
	private PartitionFileTagType partitionFileTagType = PartitionFileTagType.NUMBERFILETAG;

	public final static String COMPONENT_TYPE = "TEXT_TABLE_WRITER";
	private final static int READ_FROM_PORT = 0;

	private static Log logger = LogFactory.getLog(TextTableWriter.class);

	/**
	 * Constructor
	 * 
	 * @param id
	 * @param fileURL
	 * @param charset
	 * @param appendData
	 * @param fields
	 */
	public TextTableWriter(String id, String fileURL, String charset, 
			boolean appendData, String[] fields) {
		super(id);
		this.fileURL = fileURL;
		this.appendData = appendData;
		formatterGetter = charset == null ? new TextTableFormatterGetter(Defaults.DataFormatter.DEFAULT_CHARSET_ENCODER) : new TextTableFormatterGetter(charset);
		formatterGetter.setMask(fields);
	}
	
	/**
	 * Constructor
	 * 
	 * @param id
	 * @param writableByteChannel
	 * @param charset
	 * @param appendData
	 * @param fields
	 */
	public TextTableWriter(String id, WritableByteChannel writableByteChannel, String charset, 
			boolean appendData, String[] fields) {
		super(id);
		this.writableByteChannel = writableByteChannel;
		this.appendData = appendData;
		formatterGetter = charset == null ? new TextTableFormatterGetter(Defaults.DataFormatter.DEFAULT_CHARSET_ENCODER) : new TextTableFormatterGetter(charset);
		formatterGetter.setMask(fields);
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.Node#getType()
	 */
	@Override
	public String getType() {
		return COMPONENT_TYPE;
	}

	@Override
	public Result execute() throws Exception {
		InputPort inPort = getInputPort(READ_FROM_PORT);
		DataRecord record = new DataRecord(inPort.getMetadata());
		record.init();
		try {
			while (record != null && runIt) {
				record = inPort.readRecord(record);
				if (record != null) {
			        writer.write(record);
				}
				SynchronizeUtils.cloverYield();
			}
		} catch (Exception e) {
			throw e;
		}finally{
			writer.close();
		}
        return runIt ? Result.FINISHED_OK : Result.ABORTED;
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.GraphElement#checkConfig()
	 */
    @Override
    public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		super.checkConfig(status);
		
		checkInputPorts(status, 1, 1);
        checkOutputPorts(status, 0, 0);

        try {
        	FileUtils.canWrite(getGraph() != null ? getGraph().getRuntimeParameters().getProjectURL() 
        			: null, fileURL);
        } catch (ComponentNotReadyException e) {
            status.add(e,ConfigurationStatus.Severity.ERROR,this,
            		ConfigurationStatus.Priority.NORMAL,XML_FILEURL_ATTRIBUTE);
        }
        
        return status;
    }
	
	/* (non-Javadoc)
	 * @see org.jetel.graph.GraphElement#init()
	 */
	@Override
	public void init() throws ComponentNotReadyException {
		super.init();
		initLookupTable();

		// based on file mask, create/open output file
		if (fileURL != null) {
	        writer = new MultiFileWriter(formatterGetter, getGraph() != null ? getGraph().getRuntimeParameters().getProjectURL() : null, fileURL);
		} else {
			if (writableByteChannel == null) {
		        writableByteChannel = Channels.newChannel(System.out);
			}
	        writer = new MultiFileWriter(formatterGetter, new WritableByteChannelIterator(writableByteChannel));
		}
        writer.setLogger(logger);
        writer.setBytesPerFile(bytesPerFile);
        writer.setRecordsPerFile(recordsPerFile);
        writer.setAppendData(appendData);
        writer.setSkip(skip);
        writer.setNumRecords(numRecords);
        writer.setLookupTable(lookupTable);
        if (attrPartitionKey != null) partitionKey = attrPartitionKey.split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);
        writer.setPartitionKeyNames(partitionKey);
        writer.setPartitionFileTag(partitionFileTagType);
       	formatterGetter.setOutputFieldNames(outputFieldNames);
        writer.init(getInputPort(READ_FROM_PORT).getMetadata());
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
	public static Node fromXML(TransformationGraph graph, Element nodeXML) {
		ComponentXMLAttributes xattribs=new ComponentXMLAttributes(nodeXML, graph);
		TextTableWriter aDataWriter = null;
		
		try{
			String fields = xattribs.getString(XML_MASK_ATTRIBUTE,null);
			String[] aFields = fields == null ? null : fields.split(";");
			aDataWriter = new TextTableWriter(xattribs.getString(Node.XML_ID_ATTRIBUTE),
									xattribs.getString(XML_FILEURL_ATTRIBUTE),
									xattribs.getString(XML_CHARSET_ATTRIBUTE,null),
									xattribs.getBoolean(XML_APPEND_ATTRIBUTE, false),
									aFields);
			if (xattribs.exists(XML_RECORD_SKIP_ATTRIBUTE)){
				aDataWriter.setSkip(Integer.parseInt(xattribs.getString(XML_RECORD_SKIP_ATTRIBUTE)));
			}
			if (xattribs.exists(XML_RECORD_COUNT_ATTRIBUTE)){
				aDataWriter.setNumRecords(Integer.parseInt(xattribs.getString(XML_RECORD_COUNT_ATTRIBUTE)));
			}
			if (xattribs.exists(XML_OUTPUT_FIELD_NAMES)){
				aDataWriter.setOutputFieldNames(xattribs.getBoolean(XML_OUTPUT_FIELD_NAMES));
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
		}catch(Exception ex){
			System.err.println(COMPONENT_TYPE + ":" + xattribs.getString(Node.XML_ID_ATTRIBUTE,"unknown ID") + ":" + ex.getMessage());
			return null;
		}
		
		return aDataWriter;
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.Node#toXML(org.w3c.dom.Element)
	 */
	public void toXML(org.w3c.dom.Element xmlElement) {
		super.toXML(xmlElement);
		xmlElement.setAttribute(XML_FILEURL_ATTRIBUTE,this.fileURL);
		String charSet = this.formatterGetter.getCharsetName();
		if (charSet != null) {
			xmlElement.setAttribute(XML_CHARSET_ATTRIBUTE, this.formatterGetter.getCharsetName());
		}
		xmlElement.setAttribute(XML_APPEND_ATTRIBUTE, String.valueOf(this.appendData));
		if (skip != 0){
			xmlElement.setAttribute(XML_RECORD_SKIP_ATTRIBUTE, String.valueOf(skip));
		}
		if (numRecords != 0){
			xmlElement.setAttribute(XML_RECORD_COUNT_ATTRIBUTE,String.valueOf(numRecords));
		}
		if (outputFieldNames){
		    xmlElement.setAttribute(XML_OUTPUT_FIELD_NAMES, Boolean.toString(outputFieldNames));
		}
		if (recordsPerFile > 0) {
			xmlElement.setAttribute(XML_RECORDS_PER_FILE, Integer.toString(recordsPerFile));
		}
		if (bytesPerFile > 0) {
			xmlElement.setAttribute(XML_BYTES_PER_FILE, Integer.toString(bytesPerFile));
		}
		if (partition != null) {
			xmlElement.setAttribute(XML_PARTITION_ATTRIBUTE, partition);
		} else if (lookupTable != null) {
			xmlElement.setAttribute(XML_PARTITION_ATTRIBUTE, lookupTable.getId());
		}
		if (attrPartitionKey != null) {
			xmlElement.setAttribute(XML_PARTITIONKEY_ATTRIBUTE, attrPartitionKey);
		}
		xmlElement.setAttribute(XML_PARTITION_FILETAG_ATTRIBUTE, partitionFileTagType.name());
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
     * @param outputFieldNames The outputFieldNames to set.
     */
    public void setOutputFieldNames(boolean outputFieldNames) {
        this.outputFieldNames = outputFieldNames;
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
	 * Gets number file tag for data partition.
	 * 
	 * @return
	 */
	public PartitionFileTagType getPartitionFileTag() {
		return partitionFileTagType;
	}
}
