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
import org.jetel.data.formatter.FixLenDataFormatter;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.TransformationGraph;
import org.jetel.util.ComponentXMLAttributes;
import org.jetel.util.StringUtils;
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
 *  <tr><td><b>fileURL</b></td><td>path to the input file</td>
 *  <tr><td><b>charset</b><br><i>optional</i></td><td>character encoding of the output file (if not specified, then ISO-8859-1 is used)</td>
 *  <tr><td><b>append</b><br><i>optional</i></td><td>whether to append data at the end if output file exists or replace it (values: true/false). Default is false</td>
 *  <tr><td><b>OneRecordPerLine</b><br><i>optional</i></td><td>whether to put one or all records on one line. (values: true/false).  Default value is false.</td>
 *  <tr><td><b>LineSeparator</b><br><i>optional</i></td><td>characters to be output as line/record separator (if OneRecordPerLine is set to true). Control
 * characters "\n", "\r", "\t" may be used as well as all printable characers.</td>
 *  </tr>
 *  <tr><td><b>outputFieldNames</b><br><i>optional</i></td><td>print names of individual fields into output file - as a first row (values: true/false, default:false)</td>
 *  <tr><td><b>filler</b><br><i>optional</i></td><td>allows specifying what character will be used for padding output fields. Default is " " (space)></td>
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
 * @revision    $Revision$
 */
public class FixLenDataWriter extends Node {
	private static final String XML_LINESEPARATOR_ATTRIBUTE = "LineSeparator";
	private static final String XML_ONERECORDPERLINE_ATTRIBUTE = "OneRecordPerLine";
	private static final String XML_APPEND_ATTRIBUTE = "append";
	private static final String XML_FILEURL_ATTRIBUTE = "fileURL";
	private static final String XML_CHARSET_ATTRIBUTE = "charset";
	private static final String XML_OUTPUT_FIELD_NAMES = "outputFieldNames";
	private static final String XML_FILLER = "filler";
	
	private static final boolean DEFAULT_ONE_REC_PER_LINE=false;
	private static final boolean DEFAULT_APPEND=false;
	
	private String fileURL;
	private boolean appendData;
	private FixLenDataFormatter formatter;
	private boolean outputFieldNames=false;
	private String filler;

	/**  Description of the Field */
	public final static String COMPONENT_TYPE = "FIXLEN_DATA_WRITER_NIO";
	private final static int READ_FROM_PORT = 0;


	/**
	 *Constructor for the DelimitedDataWriter object
	 *
	 * @param  id          Description of Parameter
	 * @param  fileURL     Description of Parameter
	 * @param  appendData  Description of Parameter
	 * @since              April 16, 2002
	 */
	public FixLenDataWriter(String id, String fileURL, boolean appendData) {
		super(id);
		this.fileURL = fileURL;
		this.appendData = appendData;
		formatter = new FixLenDataFormatter();
	}


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
		formatter = new FixLenDataFormatter(charset);
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
		
//		 shall we print field names to the output ?
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
			} catch (IOException ex) {
				resultMsg = ex.getMessage();
				resultCode = Node.RESULT_ERROR;
				closeAllOutputPorts();
				return;
			} catch (Exception ex) {
				resultMsg = ex.getClass().getName()+" : "+ ex.getMessage();
				resultCode = Node.RESULT_FATAL_ERROR;
				return;
			}

		}
		formatter.close();
		if (runIt) {
			resultMsg = "OK";
		} else {
			resultMsg = "STOPPED";
		}
		resultCode = Node.RESULT_OK;
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
		} catch (FileNotFoundException ex) {
			throw new ComponentNotReadyException(getId() + "IOError: " + ex.getMessage());
		}
//		catch (IOException ex){
//			throw new ComponentNotReadyException(getID() + "IOError: " + ex.getMessage());
//		}
		
		// if filler is defined, use it
		if (filler!=null){
		    formatter.setFiller(filler.charAt(0));
		}
	}


	/**
	 *  Description of the Method
	 *
	 * @return    Description of the Returned Value
	 * @since     May 21, 2002
	 */
	public void toXML(Element xmlElement) {
		super.toXML(xmlElement);
		xmlElement.setAttribute(XML_FILEURL_ATTRIBUTE,this.fileURL);
		xmlElement.setAttribute(XML_CHARSET_ATTRIBUTE,
				((FixLenDataFormatter)this.formatter).getCharSetName());
		if (this.appendData) {
			xmlElement.setAttribute(XML_APPEND_ATTRIBUTE,String.valueOf(this.appendData));
		}
		
		if (((FixLenDataFormatter)this.formatter).getOneRecordPerLinePolicy()) {
			xmlElement.setAttribute(XML_ONERECORDPERLINE_ATTRIBUTE,
					String.valueOf(((FixLenDataFormatter)this.formatter).getOneRecordPerLinePolicy()));
		}
		if (outputFieldNames){
		    xmlElement.setAttribute(XML_OUTPUT_FIELD_NAMES, Boolean.toString(outputFieldNames));
		}
		
		if (filler!=null){
		    xmlElement.setAttribute(XML_FILLER,filler);
		}
		
	}


	/**
	 *  Description of the Method
	 *
	 * @param  nodeXML  Description of Parameter
	 * @return          Description of the Returned Value
	 * @since           May 21, 2002
	 */
    public static Node fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException {
		FixLenDataWriter aFixLenDataWriterNIO = null;
		ComponentXMLAttributes xattribs=new ComponentXMLAttributes(xmlElement, graph);
		
		
		try{
		
			if (xattribs.exists(XML_CHARSET_ATTRIBUTE)){
				aFixLenDataWriterNIO = new FixLenDataWriter(
						xattribs.getString(XML_ID_ATTRIBUTE), 
						xattribs.getString(XML_FILEURL_ATTRIBUTE),
						xattribs.getString(XML_CHARSET_ATTRIBUTE),
						xattribs.getBoolean(XML_APPEND_ATTRIBUTE,DEFAULT_APPEND));
			}else{
				aFixLenDataWriterNIO = new FixLenDataWriter(
						xattribs.getString(XML_ID_ATTRIBUTE), 
						xattribs.getString(XML_FILEURL_ATTRIBUTE),
						xattribs.getBoolean(XML_APPEND_ATTRIBUTE,DEFAULT_APPEND));
			}
			aFixLenDataWriterNIO.setOneRecordPerLinePolicy(xattribs.getBoolean(XML_ONERECORDPERLINE_ATTRIBUTE,DEFAULT_ONE_REC_PER_LINE));
			if (xattribs.exists(XML_LINESEPARATOR_ATTRIBUTE)){
				aFixLenDataWriterNIO.setLineSeparator(xattribs.getString(XML_LINESEPARATOR_ATTRIBUTE));
			}
			if (xattribs.exists(XML_OUTPUT_FIELD_NAMES)){
			    aFixLenDataWriterNIO.setOutputFieldNames(xattribs.getBoolean(XML_OUTPUT_FIELD_NAMES));
			}
		
			if (xattribs.exists(XML_FILLER)){
			    aFixLenDataWriterNIO.setFiller(xattribs.getString(XML_FILLER));
			}
			
			
		}catch(Exception ex){
	           throw new XMLConfigurationException(COMPONENT_TYPE + ":" + xattribs.getString(XML_ID_ATTRIBUTE," unknown ID ") + ":" + ex.getMessage(),ex);
		}
		
		
		return aFixLenDataWriterNIO;
	}


	/**
	 * True allows only one record per line.  False puts all records
	 * on one line.
	 *
	 * @param  b
	 */
	public void setOneRecordPerLinePolicy(boolean b) {
		formatter.setOneRecordPerLinePolicy(b);
	}

	/**
	 * Sets line separator char(s) - allows to specify
	 * different than default EOL character (\n).
	 * 
	 * @param separator
	 */
	public void setLineSeparator(String separator){
		// this should be somehow generalized
		if (formatter instanceof FixLenDataFormatter){
			((FixLenDataFormatter)formatter).setLineSeparator(StringUtils.stringToSpecChar(separator));
		}
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
	public boolean checkConfig() {
		return true;
	}
	
	public String getType(){
		return COMPONENT_TYPE;
	}
    /**
     * @return Returns the filler.
     */
    public String getFiller() {
        return filler;
    }
    /**
     * Which character (1st from specified string) will
     * be used as filler for padding output fields
     * 
     * @param filler The filler to set.
     */
    public void setFiller(String filler) {
        this.filler = filler;
    }
}

