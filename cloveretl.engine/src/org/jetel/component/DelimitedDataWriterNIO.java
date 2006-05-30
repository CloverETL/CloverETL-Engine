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

import org.jetel.data.DataRecord;
import org.jetel.data.formatter.DelimitedDataFormatterNIO;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.TransformationGraph;
import org.jetel.util.ComponentXMLAttributes;
import org.jetel.util.SynchronizeUtils;

/**
 *  <h3>DelimitedDataWriter Component</h3>
 *
 * <!-- All records from input port [0] are formatted with delimiter and written to specified file -->
 * 
 * <table border="1">
 *  <th>Component:</th>
 * <tr><td><h4><i>Name:</i></h4></td>
 * <td>DelimitedDataWriter</td></tr>
 * <tr><td><h4><i>Category:</i></h4></td>
 * <td></td></tr>
 * <tr><td><h4><i>Description:</i></h4></td>
 * <td>All records from input port [0] are formatted with delimiter and written to specified file.<br>
 * Delimiters are taken from metadata specified for port[0] data flow.</td></tr>
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
 *  <tr><td><b>type</b></td><td>"DELIMITED_DATA_WRITER_NIO"</td></tr>
 *  <tr><td><b>id</b></td><td>component identification</td>
 *  <tr><td><b>fileURL</b></td><td>path to the input file</td>
 *  <tr><td><b>charset</b></td><td>character encoding of the output file (if not specified, then ISO-8859-1 is used)</td>
 *  <tr><td><b>append</b></td><td>whether to append data at the end if output file exists or replace it (values: true/false)</td>
 *  <tr><td><b>outputFieldNames</b><br><i>optional</i></td><td>print names of individual fields into output file - as a first row (values: true/false, default:false)</td> 
 *  <tr><td><b>OneRecordPerLine</b><br><i>optional</i></td><td>whether to put one or all records on one line. (values: true/false).  Default value is false.</td>
 *  </tr>
 *  </table>  
 *
 * <h4>Example:</h4>
 * <pre>&lt;Node type="DELIMITED_DATA_WRITER_NIO" id="Writer" fileURL="/tmp/transfor.out" append="true" /&gt;</pre>
 * 
 * @author     dpavlis10000
 * @since    April 4, 2002
 */
public class DelimitedDataWriterNIO extends Node {
	private static final String XML_ONERECORDPERLINE_ATTRIBUTE = "OneRecordPerLine";
	private static final String XML_APPEND_ATTRIBUTE = "append";
	private static final String XML_FILEURL_ATTRIBUTE = "fileURL";
	private static final String XML_CHARSET_ATTRIBUTE = "charset";
	private static final String XML_OUTPUT_FIELD_NAMES = "outputFieldNames";
	
	private static final boolean APPEND_DATA_AS_DEFAULT = false;
	
	private String fileURL;
	private boolean appendData;
	private DelimitedDataFormatterNIO formatter;
	private boolean outputFieldNames=false;

	public final static String COMPONENT_TYPE = "DELIMITED_DATA_WRITER_NIO";
	private final static int READ_FROM_PORT = 0;


	/**
	 *Constructor for the DelimitedDataWriter object
	 *
	 * @param  id          Description of Parameter
	 * @param  fileURL     Description of Parameter
	 * @param  appendData  Description of Parameter
	 * @since              April 16, 2002
	 */
	public DelimitedDataWriterNIO(String id, String fileURL, boolean appendData) {
		super(id);
		this.fileURL = fileURL;
		this.appendData = appendData;
		formatter = new DelimitedDataFormatterNIO();
	}

	public DelimitedDataWriterNIO(String id, String fileURL, String charset, boolean appendData) {
		super(id);
		this.fileURL = fileURL;
		this.appendData = appendData;
		formatter = new DelimitedDataFormatterNIO(charset);
	}


	/**
	 *  Main processing method for the SimpleCopy object
	 *
	 * @since    April 4, 2002
	 */
	public void run() {
		InputPort inPort = getInputPort(READ_FROM_PORT);
		DataRecord record = new DataRecord(inPort.getMetadata());
		record.init();
		
		// shall we print field names to the output ?
		if (outputFieldNames){
		    try {
		        formatter.writeFieldNames();
		    }
		    catch (IOException ex) {
		        resultMsg=ex.getMessage();
		        resultCode=Node.RESULT_ERROR;
		        closeAllOutputPorts();
		        return;
		    }
		}
		
		while (record != null && runIt) {
			try {
				record = inPort.readRecord(record);
				if (record != null) {
					formatter.write(record);
				}
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
			SynchronizeUtils.cloverYield();
		}
		formatter.close();
		if (runIt) resultMsg="OK"; else resultMsg="STOPPED";
		resultCode=Node.RESULT_OK;
	}


	/**
	 *  Description of the Method
	 *
	 * @exception  ComponentNotReadyException  Description of Exception
	 * @since                                  April 4, 2002
	 */
	public void init() throws ComponentNotReadyException {
		// test that we have at least one input port and one output
		if (inPorts.size() < 1) {
			throw new ComponentNotReadyException("At least one input port has to be defined!");
		}
		// based on file mask, create/open output file
		try {
			formatter.open(new FileOutputStream(fileURL, appendData), getInputPort(READ_FROM_PORT).getMetadata());
		}
		catch (FileNotFoundException ex) {
			throw new ComponentNotReadyException(getID() + "IOError: " + ex.getMessage());
		}
//		catch (IOException ex){
//			throw new ComponentNotReadyException(getID() + "IOError: " + ex.getMessage());
//		}
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
		if (this.formatter.getOneRecordPerLinePolicy()) {
			xmlElement.setAttribute(XML_ONERECORDPERLINE_ATTRIBUTE,
					String.valueOf(this.formatter.getOneRecordPerLinePolicy()));
		}
		if (outputFieldNames){
		    xmlElement.setAttribute(XML_OUTPUT_FIELD_NAMES, Boolean.toString(outputFieldNames));
		}
	}

	
	/**
	 *  Description of the Method
	 *
	 * @param  nodeXML  Description of Parameter
	 * @return          Description of the Returned Value
	 * @since           May 21, 2002
	 */
	public static Node fromXML(TransformationGraph graph, org.w3c.dom.Node nodeXML) {
		ComponentXMLAttributes xattribs=new ComponentXMLAttributes(nodeXML, graph);
		DelimitedDataWriterNIO aDelimitedDataWriterNIO = null;
		
		try{
			if (xattribs.exists(XML_CHARSET_ATTRIBUTE)){
				aDelimitedDataWriterNIO = new DelimitedDataWriterNIO(xattribs.getString(Node.XML_ID_ATTRIBUTE),
										xattribs.getString(XML_FILEURL_ATTRIBUTE),
										xattribs.getString(XML_CHARSET_ATTRIBUTE),
										xattribs.getBoolean(XML_APPEND_ATTRIBUTE,APPEND_DATA_AS_DEFAULT));	
			}else{
				aDelimitedDataWriterNIO = new DelimitedDataWriterNIO(xattribs.getString(Node.XML_ID_ATTRIBUTE),
										xattribs.getString(XML_FILEURL_ATTRIBUTE),
										xattribs.getBoolean(XML_APPEND_ATTRIBUTE,APPEND_DATA_AS_DEFAULT));	
			}
			if (xattribs.exists(XML_ONERECORDPERLINE_ATTRIBUTE)){
				if(xattribs.getBoolean(XML_ONERECORDPERLINE_ATTRIBUTE)){
					aDelimitedDataWriterNIO.setOneRecordPerLinePolicy(true);
				}else{
					aDelimitedDataWriterNIO.setOneRecordPerLinePolicy(false);
				}
			}else{
				// sets the default policy
				aDelimitedDataWriterNIO.setOneRecordPerLinePolicy(false);
			}
			if (xattribs.exists(XML_OUTPUT_FIELD_NAMES)){
			    aDelimitedDataWriterNIO.setOutputFieldNames(xattribs.getBoolean(XML_OUTPUT_FIELD_NAMES));
			}
			
		}catch(Exception ex){
			System.err.println(COMPONENT_TYPE + ":" + ((xattribs.exists(XML_ID_ATTRIBUTE)) ? xattribs.getString(Node.XML_ID_ATTRIBUTE) : " unknown ID ") + ":" + ex.getMessage());
			return null;
		}
		
		return aDelimitedDataWriterNIO;
	}

	/**
	 * True allows only one record per line.  False puts all records 
	 * on one line.
	 * @param b
	 */
	private void setOneRecordPerLinePolicy(boolean b) {
		formatter.setOneRecordPerLinePolicy(b);
	}
	
	public boolean checkConfig(){
		return true;
	}
	
	public String getType(){
		return COMPONENT_TYPE;
	}
	
    /**
     * @return Returns the outputFieldNames.
     */
    public boolean isOutputFieldNames() {
        return outputFieldNames;
    }
    /**
     * @param outputFieldNames The outputFieldNames to set.
     */
    public void setOutputFieldNames(boolean outputFieldNames) {
        this.outputFieldNames = outputFieldNames;
    }
}

