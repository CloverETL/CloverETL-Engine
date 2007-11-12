
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
import org.jetel.data.formatter.provider.XLSFormatterProvider;
import org.jetel.data.lookup.LookupTable;
import org.jetel.enums.PartitionFileTagType;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.util.MultiFileWriter;
import org.jetel.util.SynchronizeUtils;
import org.jetel.util.bytes.WritableByteChannelIterator;
import org.jetel.util.file.FileUtils;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.string.StringUtils;
import org.w3c.dom.Element;

/**
 *  <h3>XLS Writer Component</h3>
 *
 * <!-- Reads data from input port and writes them to given xls sheet in xls file. -->
 *
 * <table border="1">
 *  <th>Component:</th>
 * <tr><td><h4><i>Name:</i></h4></td>
 * <td>XLSWriter</td></tr>
 * <tr><td><h4><i>Category:</i></h4></td>
 * <td>Writers</td></tr>
 * <tr><td><h4><i>Description:</i></h4></td>
 * <td>Reads data from input port and writes them to given xls sheet in xls file. If 
 *  in one graph you want to write to the same file but to different sheets each XLSWriter
 *  has to have another phase<br>JExcel can handle 
 * with files up to ~5.7MB in flat file. For more data it is requested more memory.</td></tr>
 * <tr><td><h4><i>Inputs:</i></h4></td>
 * <td>one input port defined/connected.</td></tr>
 * <tr><td><h4><i>Outputs:</i></h4></td>
 * <td></tr>
 * <tr><td><h4><i>Comment:</i></h4></td></tr>
 * </table>
 *  <br>
 *  <table border="1">
 *  <th>XML attributes:</th>
 *  <tr><td><b>type</b></td><td>"XLS_WRITER"</td></tr>
 *  <tr><td><b>id</b></td><td>component identification</td>
 *  <tr><td><b>fileURL</b></td><td>path to the output file</td>
 *  <tr><td><b>namesRow</b></td><td>index of row, where to write metadata names</td>
 *  <tr><td><b>firstDataRow</b></td><td>index of row, where to write first data record</td>
 *  <tr><td><b>firstColumn</b></td><td>code of column from which data will be written</td>
 *  <tr><td><b>sheetName</b></td><td>name of sheet for writing data. If it is not set 
 *   new sheet with default name is created</td>
 *  <tr><td><b>recordSkip</b></td><td>number of skipped records</td>
 *  <tr><td><b>recordCount</b></td><td>number of written records</td>
 *  <tr><td><b>recordsPerFile</b></td><td>max number of records in one output file</td>
 *  <tr><td><b>sheetNumber</b></td><td>number of sheet for writing data (starting from 0).
 *   If it is not set new sheet with default name is created. If sheetName and sheetNumber 
 *   are both set, sheetNumber is ignored</td>
 *  <tr><td><b>append</b></td><td>indicates if given sheet exist new data are append
 *   to the sheet or old data are deleted and rewritten by new ones (true/false - default 
 *   false)</td>
 *  </tr>
 *  </table>
 *
 *  <h4>Example:</h4>
 *  <pre>&lt;Node fileURL="output/orders.partitioned.xls" firstColumn="f" 
 *   id="XLS_WRITER0" namesRow="2" sheetName="via1" type="XLS_WRITER"/&gt;
 * 
 * <pre>&lt;Node fileURL="output/orders.partitioned.xls" firstDataRow="10" 
 * id="XLS_WRITER1" sheetName="via2" type="XLS_WRITER"/&gt;
 *
 * <pre>&lt;Node append="true" fileURL="output/orders.partitioned.xls" 
 * id="XLS_WRITER2" namesRow="1" firstDataRow="3" sheetName="via3" type="XLS_WRITER"/&gt;
 * 
/**
* @author avackova <agata.vackova@javlinconsulting.cz> ; 
* (c) JavlinConsulting s.r.o.
*	www.javlinconsulting.cz
*	@created October 10, 2006
 */
public class XLSWriter extends Node {

	private static final String XML_FILEURL_ATTRIBUTE = "fileURL";
	private static final String XML_SHEETNAME_ATTRIBUTE = "sheetName";
	private static final String XML_SHEETNUMBER_ATTRIBUTE = "sheetNumber";
	private static final String XML_APPEND_ATTRIBUTE = "append";
	private static final String XML_FIRSTDATAROW_ATTRIBUTE = "firstDataRow";
	private static final String XML_FIRSTCOLUMN_ATTRIBUTE = "firstColumn";
	private static final String XML_NAMESROW_ATTRIBUTE = "namesRow";
	private static final String XML_RECORD_SKIP_ATTRIBUTE = "recordSkip";
	private static final String XML_RECORD_COUNT_ATTRIBUTE = "recordCount";
	private static final String XML_RECORDS_PER_FILE = "recordsPerFile";
	private static final String XML_PARTITIONKEY_ATTRIBUTE = "partitionKey";
	private static final String XML_PARTITION_ATTRIBUTE = "partition";
	private static final String XML_PARTITION_FILETAG_ATTRIBUTE = "partitionFileTag";

	public final static String COMPONENT_TYPE = "XLS_WRITER";
	private final static int READ_FROM_PORT = 0;

	private String partition;
	private String attrPartitionKey;
	private String[] partitionKey;
	private LookupTable lookupTable;
	private PartitionFileTagType partitionFileTagType = PartitionFileTagType.NUMBER_FILE_TAG;

	private static Log logger = LogFactory.getLog(XLSWriter.class);
	
	private String fileURL;
	private XLSFormatterProvider formatterProvider;
	private MultiFileWriter writer;
    private int skip;
	private int numRecords;
	private WritableByteChannel writableByteChannel;
	private int recordsPerFile;
	
	/**
	 * Constructor
	 * 
	 * @param id
	 * @param fileURL output file
	 * @param saveNames indicates if save metadata names
	 * @param append indicates if new data are appended or rewrite old data 
	 */
	public XLSWriter(String id,String fileURL, boolean append){
		super(id);
		this.fileURL = fileURL;
		formatterProvider = new XLSFormatterProvider(append);
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
            ConfigurationProblem problem = new ConfigurationProblem(e.getMessage(), ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL);
            if(!StringUtils.isEmpty(e.getAttributeName())) {
                problem.setAttributeName(e.getAttributeName());
            }
            status.add(problem);
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
		
		if (fileURL != null) {
	        writer = new MultiFileWriter(formatterProvider, getGraph() != null ? getGraph().getRuntimeParameters().getProjectURL() : null, fileURL);
		} else {
			if (writableByteChannel == null) {
		        writableByteChannel = Channels.newChannel(System.out);
			}
	        writer = new MultiFileWriter(formatterProvider, new WritableByteChannelIterator(writableByteChannel));
		}
        writer.setLogger(logger);
        writer.setRecordsPerFile(recordsPerFile);
        writer.setAppendData(true);
        writer.setSkip(skip);
        writer.setNumRecords(numRecords);
		writer.setUseChannel(false);
        writer.setLookupTable(lookupTable);
        if (attrPartitionKey != null) partitionKey = attrPartitionKey.split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);
        writer.setPartitionKeyNames(partitionKey);
        writer.setPartitionFileTag(partitionFileTagType);
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

	public static Node fromXML(TransformationGraph graph, Element nodeXML) throws XMLConfigurationException {
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(nodeXML, graph);
		XLSWriter xlsWriter;
		try{
			xlsWriter = new XLSWriter(xattribs.getString(XML_ID_ATTRIBUTE),
					xattribs.getString(XML_FILEURL_ATTRIBUTE),
					xattribs.getBoolean(XML_APPEND_ATTRIBUTE,false));
			if (xattribs.exists(XML_SHEETNAME_ATTRIBUTE)){
				xlsWriter.setSheetName(xattribs.getString(XML_SHEETNAME_ATTRIBUTE));
			}
			else if (xattribs.exists(XML_SHEETNUMBER_ATTRIBUTE)){
				xlsWriter.setSheetNumber(xattribs.getInteger(XML_SHEETNUMBER_ATTRIBUTE));
			}
			xlsWriter.setFirstColumn(xattribs.getString(XML_FIRSTCOLUMN_ATTRIBUTE,"A"));
			xlsWriter.setFirstRow(xattribs.getInteger(XML_FIRSTDATAROW_ATTRIBUTE,1));
			xlsWriter.setNamesRow(xattribs.getInteger(XML_NAMESROW_ATTRIBUTE,0));
			if (xattribs.exists(XML_RECORD_SKIP_ATTRIBUTE)){
				xlsWriter.setSkip(Integer.parseInt(xattribs.getString(XML_RECORD_SKIP_ATTRIBUTE)));
			}
			if (xattribs.exists(XML_RECORD_COUNT_ATTRIBUTE)){
				xlsWriter.setNumRecords(Integer.parseInt(xattribs.getString(XML_RECORD_COUNT_ATTRIBUTE)));
			}
            if(xattribs.exists(XML_RECORDS_PER_FILE)) {
            	xlsWriter.setRecordsPerFile(xattribs.getInteger(XML_RECORDS_PER_FILE));
            }
			if(xattribs.exists(XML_PARTITIONKEY_ATTRIBUTE)) {
				xlsWriter.setPartitionKey(xattribs.getString(XML_PARTITIONKEY_ATTRIBUTE));
            }
			if(xattribs.exists(XML_PARTITION_ATTRIBUTE)) {
				xlsWriter.setPartition(xattribs.getString(XML_PARTITION_ATTRIBUTE));
            }
			if(xattribs.exists(XML_PARTITION_FILETAG_ATTRIBUTE)) {
				xlsWriter.setPartitionFileTag(xattribs.getString(XML_PARTITION_FILETAG_ATTRIBUTE));
            }
			return xlsWriter;
		} catch (Exception ex) {
		    throw new XMLConfigurationException(COMPONENT_TYPE + ":" + xattribs.getString(XML_ID_ATTRIBUTE," unknown ID ") + ":" + ex.getMessage(),ex);
		}
	}

	/**
	 *  Description of the Method
	 *
	 * @return    Description of the Returned Value
	 * @since     May 21, 2002
	 */
	public void toXML(org.w3c.dom.Element xmlElement) {
		super.toXML(xmlElement);
		xmlElement.setAttribute(XML_FILEURL_ATTRIBUTE,this.fileURL);
		xmlElement.setAttribute(XML_APPEND_ATTRIBUTE, String.valueOf(formatterProvider.isAppend()));
		xmlElement.setAttribute(XML_FIRSTCOLUMN_ATTRIBUTE,String.valueOf(formatterProvider.getFirstColumn()));
		xmlElement.setAttribute(XML_FIRSTDATAROW_ATTRIBUTE, String.valueOf(formatterProvider.getFirstRow()+1));
		xmlElement.setAttribute(XML_NAMESROW_ATTRIBUTE, String.valueOf(formatterProvider.getNamesRow()+1));
		if (formatterProvider.getSheetName() != null) {
			xmlElement.setAttribute(XML_SHEETNAME_ATTRIBUTE,formatterProvider.getSheetName());
		}
		if (skip != 0){
			xmlElement.setAttribute(XML_RECORD_SKIP_ATTRIBUTE, String.valueOf(skip));
		}
		if (numRecords != 0){
			xmlElement.setAttribute(XML_RECORD_COUNT_ATTRIBUTE,String.valueOf(numRecords));
		}
		if (recordsPerFile > 0) {
			xmlElement.setAttribute(XML_RECORDS_PER_FILE, Integer.toString(recordsPerFile));
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

	private void setSheetName(String sheetName) {
		formatterProvider.setSheetName(sheetName);
	}

	private void setSheetNumber(int sheetNumber) {
		formatterProvider.setSheetNumber(sheetNumber);
	}
	
	private void setFirstColumn(String firstColumn){
		formatterProvider.setFirstColumn(firstColumn);
	}
	
	private void setFirstRow(int firstRow){
		formatterProvider.setFirstRow(firstRow-1);
	}
	
	private void setNamesRow(int namesRow){
		formatterProvider.setNamesRow(namesRow-1);
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
