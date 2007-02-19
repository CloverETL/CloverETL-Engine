/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2002-04  David Pavlis <david_pavlis@hotmail.com>
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
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.formatter.DataFormatter;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.util.ComponentXMLAttributes;
import org.jetel.util.FileUtils;
import org.jetel.util.MultiFileWriter;
import org.jetel.util.SynchronizeUtils;
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
	public static final String XML_RECORD_SKIP_ATTRIBUTE = "recordSkip";
	public static final String XML_RECORD_COUNT_ATTRIBUTE = "recordCount";
	private String fileURL;
	private boolean appendData;
	private DataFormatter formatter;
    private MultiFileWriter writer;
    private boolean outputFieldNames;
	private int bytesPerFile;
	private int recordsPerFile;
	private WritableByteChannel writableByteChannel;
    private int skip;
	private int numRecords;

	static Log logger = LogFactory.getLog(DataWriter.class);

	public final static String COMPONENT_TYPE = "DATA_WRITER";
	private final static int READ_FROM_PORT = 0;

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
		formatter = new DataFormatter(charset != null ? charset : Defaults.DataFormatter.DEFAULT_CHARSET_ENCODER);
	}
	
	public DataWriter(String id, WritableByteChannel writableByteChannel, String charset, boolean appendData) {
		super(id);
		this.writableByteChannel = writableByteChannel;
		this.appendData = appendData;
		formatter = new DataFormatter(charset != null ? charset : Defaults.DataFormatter.DEFAULT_CHARSET_ENCODER);
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


	/**
	 *  Description of the Method
	 *
	 * @exception  ComponentNotReadyException  Description of Exception
	 * @since                                  April 4, 2002
	 */
	public void init() throws ComponentNotReadyException {
		super.init();

		// initialize multifile writer based on prepared formatter
		if (fileURL != null) {
	        writer = new MultiFileWriter(formatter, getGraph() != null ? getGraph().getProjectURL() : null, fileURL);
		} else {
			if (writableByteChannel == null) {
		        writableByteChannel = Channels.newChannel(System.out);
			}
	        writer = new MultiFileWriter(formatter, new WritableByteChannelIterator(writableByteChannel));
		}
        writer.setLogger(logger);
        writer.setBytesPerFile(bytesPerFile);
        writer.setRecordsPerFile(recordsPerFile);
        writer.setAppendData(appendData);
        writer.setSkip(skip);
        writer.setNumRecords(numRecords);
        if(outputFieldNames) {
            writer.setHeader(getInputPort(READ_FROM_PORT).getMetadata().getFieldNamesHeader());
        }
        writer.init(getInputPort(READ_FROM_PORT).getMetadata());
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
		String charSet = this.formatter.getCharsetName();
		if (charSet != null) {
			xmlElement.setAttribute(XML_CHARSET_ATTRIBUTE, this.formatter.getCharsetName());
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
		if (skip != 0){
			xmlElement.setAttribute(XML_RECORD_SKIP_ATTRIBUTE, String.valueOf(skip));
		}
		if (numRecords != 0){
			xmlElement.setAttribute(XML_RECORD_COUNT_ATTRIBUTE,String.valueOf(numRecords));
		}
		xmlElement.setAttribute(XML_APPEND_ATTRIBUTE, String.valueOf(this.appendData));
	}

	
	/**
	 *  Description of the Method
	 *
	 * @param  nodeXML  Description of Parameter
	 * @return          Description of the Returned Value
	 * @throws XMLConfigurationException 
	 * @since           May 21, 2002
	 */
	public static Node fromXML(TransformationGraph graph, Element nodeXML) throws XMLConfigurationException {
		ComponentXMLAttributes xattribs=new ComponentXMLAttributes(nodeXML, graph);
		DataWriter aDataWriter = null;
		
		try{
			aDataWriter = new DataWriter(xattribs.getString(Node.XML_ID_ATTRIBUTE),
									xattribs.getString(XML_FILEURL_ATTRIBUTE),
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
        } catch (Exception ex) {
            throw new XMLConfigurationException(COMPONENT_TYPE + ":" + xattribs.getString(XML_ID_ATTRIBUTE," unknown ID ") + ":" + ex.getMessage(),ex);
        }
		
		return aDataWriter;
	}

	
    @Override
    public ConfigurationStatus checkConfig(ConfigurationStatus status) {
        super.checkConfig(status);
        
        checkInputPorts(status, 1, 1);
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
	
    private class WritableByteChannelIterator implements Iterator<WritableByteChannel> {
    	WritableByteChannel writableByteChannel;
    	
    	public WritableByteChannelIterator(WritableByteChannel writableByteChannel) {
    		this.writableByteChannel = writableByteChannel;
    	}
    	
		public boolean hasNext() {
			return true;
		}

		public WritableByteChannel next() {
			return writableByteChannel;
		}

		public void remove() {}
    }
    
}
