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
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.Iterator;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.formatter.DataFormatter;
import org.jetel.data.parser.FixLenDataParser3;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.TransformationGraph;
import org.jetel.util.ComponentXMLAttributes;
import org.jetel.util.MultiOutFile;
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
 *  <tr><td><b>recordsPerFile</b></td><td>max number of records in one output file</td>
 *  <tr><td><b>bytesPerFile</b></td><td>Max size of output files. To avoid splitting a record to two files, max size could be slightly overreached.</td>
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
	private static final String XML_RECORDS_PER_FILE = "recordsPerFile";
	private static final String XML_BYTES_PER_FILE = "bytesPerFile";
	private String fileURL;
	private boolean appendData;
	private DataFormatter formatter;
	private Iterator<String> filenameItor;
	private int maxRecords;
	private int maxBytes;

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
	public DataWriter(String id, String fileURL, String charset, boolean appendData,
		int maxRecords, int maxBytes) {
		super(id);
		this.maxRecords = maxRecords;
		this.maxBytes = maxBytes;
		this.fileURL = fileURL;
		this.appendData = appendData;
		formatter = new DataFormatter(charset != null ? charset : Defaults.DataFormatter.DEFAULT_CHARSET_ENCODER);
		filenameItor = new MultiOutFile(fileURL); 
	}


	/**
	 *  Main processing method for the DataWriter object
	 */
	public void run() {
		InputPort inPort = getInputPort(READ_FROM_PORT);
		DataRecord record = new DataRecord(inPort.getMetadata());
		record.init();
		int recCnt = 0;
		int size = 0;

		try {
			while (record != null && runIt) {
				record = inPort.readRecord(record);
				if (record != null) {
					if ((maxRecords > 0 && recCnt >= maxRecords) || (maxBytes > 0 && size >= maxBytes)) {
						setNextOutput();
						recCnt = 0;
						size = 0;
					}
					size += formatter.writeRecord(record);
					recCnt++;
				}
			}
			SynchronizeUtils.cloverYield();
		}
		catch (IOException ex) {
			resultMsg=ex.getMessage();
			resultCode=Node.RESULT_ERROR;
			closeAllOutputPorts();
			return;
		}
		catch (Exception ex) {
			resultMsg=ex.getClass().getName()+" : "+ ex.getMessage();
			resultCode=Node.RESULT_FATAL_ERROR;
			return;
		}
		formatter.close();
		if (runIt) resultMsg="OK"; else resultMsg="STOPPED";
		resultCode=Node.RESULT_OK;
	}

	private static WritableByteChannel getChannel(String input, boolean appendData) throws IOException {
        String strURL = input;
		OutputStream os;
		URL url;
		
        //resolve url format for zip files
		if(input.startsWith("zip:")) {
        	strURL = input.substring(input.indexOf(':') + 1, input.lastIndexOf('#'));
        }
        
		//open channel
		if(!strURL.startsWith("ftp")) {
			os = new FileOutputStream(strURL, appendData);
		} else {
			try {
				url = new URL(strURL); 
			} catch(MalformedURLException e) {
				// try to patch the url
				try {
					url = new URL("file:" + strURL);
				} catch(MalformedURLException ex) {
					throw new RuntimeException("Wrong URL of file specified: " + ex.getMessage());
				}
			}
			os = url.openConnection().getOutputStream();
		}
		//resolve url format for zip files
		if(input.startsWith("zip:")) {
			String zipAnchor = input.substring(input.lastIndexOf('#') + 1);
			ZipOutputStream zout = new ZipOutputStream(os);
			ZipEntry entry = new ZipEntry(zipAnchor);
			zout.putNextEntry(entry);
			return Channels.newChannel(zout);
        } else {
        	return Channels.newChannel(os);
        }
	}

	private void setNextOutput() throws FileNotFoundException {
		if (!filenameItor.hasNext()) {
			logger.warn(getId() + ": Unable to open new output file. This may be caused by missing wildcard in filename specification. "
					+ "Size of output file will exceed specified limit");
			return;
		}
		formatter.open(new FileOutputStream(filenameItor.next(), appendData), null);
	}

	/**
	 *  Description of the Method
	 *
	 * @exception  ComponentNotReadyException  Description of Exception
	 * @since                                  April 4, 2002
	 */
	public void init() throws ComponentNotReadyException {
		// test that we have at least one input port
		if (inPorts.size() < 1) {
			throw new ComponentNotReadyException("At least one input port has to be defined!");
		}
		// based on file mask, create/open output file
		try {
			formatter.open(new FileOutputStream(filenameItor.next(), appendData), getInputPort(READ_FROM_PORT).getMetadata());
		} catch (IOException ex) {
			throw new ComponentNotReadyException(getId() + "IOError: " + ex.getMessage());
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
		String charSet = this.formatter.getCharsetName();
		if (charSet != null) {
			xmlElement.setAttribute(XML_CHARSET_ATTRIBUTE, this.formatter.getCharsetName());
		}
		xmlElement.setAttribute(XML_APPEND_ATTRIBUTE, String.valueOf(this.appendData));
	}

	
	/**
	 *  Description of the Method
	 *
	 * @param  nodeXML  Description of Parameter
	 * @return          Description of the Returned Value
	 * @since           May 21, 2002
	 */
	public static Node fromXML(TransformationGraph graph, Element nodeXML) {
		ComponentXMLAttributes xattribs=new ComponentXMLAttributes(nodeXML, graph);
		DataWriter aDataWriter = null;
		
		try{
			aDataWriter = new DataWriter(xattribs.getString(Node.XML_ID_ATTRIBUTE),
									xattribs.getString(XML_FILEURL_ATTRIBUTE),
									xattribs.getString(XML_CHARSET_ATTRIBUTE, null),
									xattribs.getBoolean(XML_APPEND_ATTRIBUTE, false),
									xattribs.getInteger(XML_RECORDS_PER_FILE, 0),
									xattribs.getInteger(XML_BYTES_PER_FILE, 0));
		}catch(Exception ex){
			System.err.println(COMPONENT_TYPE + ":" + xattribs.getString(Node.XML_ID_ATTRIBUTE,"unknown ID") + ":" + ex.getMessage());
			return null;
		}
		
		return aDataWriter;
	}

	
	public boolean checkConfig(){
		return true;
	}
	
	public String getType(){
		return COMPONENT_TYPE;
	}
	
}
