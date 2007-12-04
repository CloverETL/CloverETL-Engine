
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
import org.jetel.data.formatter.provider.StructureFormatterProvider;
import org.jetel.data.lookup.LookupTable;
import org.jetel.enums.PartitionFileTagType;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.util.MultiFileWriter;
import org.jetel.util.SynchronizeUtils;
import org.jetel.util.bytes.WritableByteChannelIterator;
import org.jetel.util.file.FileUtils;
import org.jetel.util.property.ComponentXMLAttributes;
import org.w3c.dom.Element;

/**
 *  <h3>IDocWriter Component</h3>
 *
 * <!-- All records from "body input port" [0] are formatted due to given mask and written to specified file -->
 * 
 * <table border="1">
 *  <th>Component:</th>
 * <tr><td><h4><i>Name:</i></h4></td>
 * <td>IDocWriter</td></tr>
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
public class IDocWriter extends Node {

	public static final String XML_APPEND_ATTRIBUTE = "append";
	public static final String XML_FILEURL_ATTRIBUTE = "fileURL";
	public static final String XML_CHARSET_ATTRIBUTE = "charset";
	public static final String XML_MASK_ATTRIBUTE = "mask";
	public static final String XML_HEADER_ATTRIBUTE = "header";
	public static final String XML_FOOTER_ATTRIBUTE = "footer";
	public static final String XML_RECORD_SKIP_ATTRIBUTE = "recordSkip";
	public static final String XML_RECORD_COUNT_ATTRIBUTE = "recordCount";
	private static final String XML_PARTITIONKEY_ATTRIBUTE = "partitionKey";
	private static final String XML_PARTITION_ATTRIBUTE = "partition";
	private static final String XML_PARTITION_FILETAG_ATTRIBUTE = "partitionFileTag";

	private String fileURL;
	private boolean appendData;
	private StructureFormatterProvider bodyFormatterProvider, headerFormatterProvider;
	private MultiFileWriter headerWriter, bodyWriter;
    private int skip;
	private int numRecords;
	private WritableByteChannel writableByteChannel;

	private String partition;
	private String attrPartitionKey;
	private String[] partitionKey;
	private LookupTable lookupTable;
	private PartitionFileTagType partitionFileTagType = PartitionFileTagType.NUMBER_FILE_TAG;
	private String headerMask, bodyMask;
	private String footer;

	private static Log logger = LogFactory.getLog(IDocWriter.class);

	public final static String COMPONENT_TYPE = "IDOC_WRITER";

	private final static int HEADER_PORT = 0;
	private final static int BODY_PORT = 1;
	
	private String charset;

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
	public IDocWriter(String id, String fileURL, String charset, 
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
	 * @param footer
	 */
	public IDocWriter(String id, String fileURL, String charset, 
			boolean appendData, String headerMask, String mask, String footer) {
		super(id);
		this.fileURL = fileURL;
		this.charset = charset;
		this.appendData = appendData;
		this.headerMask = headerMask;
		this.bodyMask = mask;
		this.footer = footer;
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
		
		InputPort headerPort = getInputPort(HEADER_PORT);
		DataRecord record = new DataRecord(headerPort.getMetadata());
		record.init();
		try {
			while (record != null && runIt) {
				record = headerPort.readRecord(record);
				if (record != null) {
			        headerWriter.write(record);
				}
				SynchronizeUtils.cloverYield();
			}
		} catch (Exception e) {
			throw e;
		}finally{
			headerWriter.close();
		}

		InputPort bodyPort = getInputPort(BODY_PORT);
		record = new DataRecord(bodyPort.getMetadata());
		record.init();
		try {
			while (record != null && runIt) {
				record = bodyPort.readRecord(record);
				if (record != null) {
			        bodyWriter.write(record);
				}
				SynchronizeUtils.cloverYield();
			}
		} catch (Exception e) {
			throw e;
		}finally{
			bodyWriter.close();
		}
        return runIt ? Result.FINISHED_OK : Result.ABORTED;
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.GraphElement#checkConfig()
	 */
    @Override
    public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		super.checkConfig(status);
		 
		checkInputPorts(status, 2, 2);
        checkOutputPorts(status, 0, 0);

        try {
        	FileUtils.canWrite(getGraph() != null ? getGraph().getProjectURL() 
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
        if(isInitialized()) return;
		super.init();
		
		initLookupTable();

		headerFormatterProvider = new StructureFormatterProvider(charset != null ? charset : Defaults.DataFormatter.DEFAULT_CHARSET_ENCODER);
		headerFormatterProvider.setMask(headerMask);

		bodyFormatterProvider = new StructureFormatterProvider(charset != null ? charset : Defaults.DataFormatter.DEFAULT_CHARSET_ENCODER);
		bodyFormatterProvider.setMask(bodyMask);

		// based on file mask, create/open output file
		if (fileURL != null) {
			headerWriter = new MultiFileWriter(headerFormatterProvider, getGraph() != null ? getGraph().getProjectURL() : null, fileURL);
	        bodyWriter = new MultiFileWriter(bodyFormatterProvider, getGraph() != null ? getGraph().getProjectURL() : null, fileURL);
		} else {
			if (writableByteChannel == null) {
		        writableByteChannel = Channels.newChannel(System.out);
			}
			headerWriter = new MultiFileWriter(bodyFormatterProvider, new WritableByteChannelIterator(writableByteChannel));
	        bodyWriter = new MultiFileWriter(bodyFormatterProvider, new WritableByteChannelIterator(writableByteChannel));
		}
		headerWriter.setLogger(logger);
        bodyWriter.setLogger(logger);
        headerWriter.setAppendData(appendData);
        bodyWriter.setAppendData(true);
        headerWriter.setSkip(skip);
        bodyWriter.setSkip(skip);
        headerWriter.setNumRecords(numRecords);
        bodyWriter.setNumRecords(numRecords);
        headerWriter.setLookupTable(lookupTable);
        bodyWriter.setLookupTable(lookupTable);
        if (attrPartitionKey != null) partitionKey = attrPartitionKey.split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);
        headerWriter.setPartitionKeyNames(partitionKey);
        bodyWriter.setPartitionKeyNames(partitionKey);
        headerWriter.setPartitionFileTag(partitionFileTagType);
        bodyWriter.setPartitionFileTag(partitionFileTagType);
        
		if (footer != null){
			bodyFormatterProvider.setFooter(footer);
		}
        headerWriter.init(getInputPort(HEADER_PORT).getMetadata());
        bodyWriter.init(getInputPort(BODY_PORT).getMetadata());
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
		IDocWriter aDataWriter = null;
		
		try{
			aDataWriter = new IDocWriter(xattribs.getString(Node.XML_ID_ATTRIBUTE),
									xattribs.getString(XML_FILEURL_ATTRIBUTE),
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
		String charSet = this.bodyFormatterProvider.getCharsetName();
		if (charSet != null) {
			xmlElement.setAttribute(XML_CHARSET_ATTRIBUTE, this.bodyFormatterProvider.getCharsetName());
		}
		xmlElement.setAttribute(XML_APPEND_ATTRIBUTE, String.valueOf(this.appendData));
		if (headerMask != null){
			xmlElement.setAttribute(XML_HEADER_ATTRIBUTE,headerMask);
		}
		if (footer != null){
			xmlElement.setAttribute(XML_FOOTER_ATTRIBUTE,footer);
		}
		if (skip != 0){
			xmlElement.setAttribute(XML_RECORD_SKIP_ATTRIBUTE, String.valueOf(skip));
		}
		if (numRecords != 0){
			xmlElement.setAttribute(XML_RECORD_COUNT_ATTRIBUTE,String.valueOf(numRecords));
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
	
	@Deprecated
	public void setFooter(String footer) {
		this.footer = footer;
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
