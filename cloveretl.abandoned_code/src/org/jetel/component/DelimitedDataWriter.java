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
import org.jetel.data.formatter.DelimitedDataFormatter;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.TransformationGraph;
import org.jetel.util.ComponentXMLAttributes;
import org.w3c.dom.Element;

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
 * <td></td></tr>
 * </table>
 *  <br>
 *  <table border="1">
 *  <th>XML attributes:</th>
 *  <tr><td><b>type</b></td><td>"DELIMITED_DATA_WRITER"</td></tr>
 *  <tr><td><b>id</b></td><td>component identification</td>
 *  <tr><td><b>fileURL</b></td><td>path to the input file</td>
 *  <tr><td><b>append</b></td><td>whether to append data at the end if output file exists or replace it (values: true/false)</td>
 *  <tr><td><b>OneRecordPerLine</b><br><i>optional</i></td><td>whether to put one or all records on one line. (values: true/false).  Default value is false.</td>
 *  </tr>
 *  </table>
 *
 *  <h4>Example:</h4>
 *  <pre>&lt;Node type="DELIMITED_DATA_WRITER" id="Writer" fileURL="/tmp/transform.dat" append="true" /&gt;</pre>
 *
 *
 * @author      dpavlis
 * @since       April 4, 2002
 * @revision    $Revision$
 */
public class DelimitedDataWriter extends Node {
	public static final String XML_ONERECORDPERLINE_ATTRIBUTE = "OneRecordPerLine";
	public static final String XML_APPEND_ATTRIBUTE = "append";
	public static final String XML_FILEURL_ATTRIBUTE = "fileURL";
	private String fileURL;
	private boolean appendData;
	private DelimitedDataFormatter formatter;

	/**  Description of the Field */
	public final static String COMPONENT_TYPE = "DELIMITED_DATA_WRITER";
	private final static int READ_FROM_PORT = 0;


	/**
	 *Constructor for the DelimitedDataWriter object
	 *
	 * @param  id          Description of Parameter
	 * @param  fileURL     Description of Parameter
	 * @param  appendData  Description of Parameter
	 * @since              April 16, 2002
	 */
	public DelimitedDataWriter(String id, String fileURL, boolean appendData) {
		super(id);
		this.fileURL = fileURL;
		this.appendData = appendData;
		formatter = new DelimitedDataFormatter();
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
		while (record != null && runIt) {
			try {
				record = readRecord(READ_FROM_PORT, record);
				if (record != null) {
					formatter.write(record);
				}
			} catch (IOException ex) {
				resultMsg = ex.getMessage();
				resultCode = Node.RESULT_ERROR;
				closeAllOutputPorts();
				return;
			} catch (Exception ex) {
				resultMsg = ex.getMessage();
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
		xmlElement.setAttribute(XML_APPEND_ATTRIBUTE, String.valueOf(this.appendData));
		xmlElement.setAttribute(XML_ONERECORDPERLINE_ATTRIBUTE,
				String.valueOf(this.formatter.getOneRecordPerLinePolicy()));
	}


	/**
	 *  Description of the Method
	 *
	 * @param  nodeXML  Description of Parameter
	 * @return          Description of the Returned Value
	 * @since           May 21, 2002
	 */
    @Override public static Node fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException {
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(xmlElement, graph);
		DelimitedDataWriter aDelimitedDataWriter = null;

		try {
			aDelimitedDataWriter = new DelimitedDataWriter(xattribs.getString(XML_ID_ATTRIBUTE),
					xattribs.getString(XML_FILEURL_ATTRIBUTE),
					xattribs.getBoolean(XML_APPEND_ATTRIBUTE));
			if (xattribs.exists(XML_ONERECORDPERLINE_ATTRIBUTE)) {
				if (xattribs.getBoolean(XML_ONERECORDPERLINE_ATTRIBUTE)) {
					aDelimitedDataWriter.setOneRecordPerLinePolicy(true);
				} else {
					aDelimitedDataWriter.setOneRecordPerLinePolicy(false);
				}
			} else {
				aDelimitedDataWriter.setOneRecordPerLinePolicy(false);
			}

		} catch (Exception ex) {
	           throw new XMLConfigurationException(COMPONENT_TYPE + ":" + xattribs.getString(XML_ID_ATTRIBUTE," unknown ID ") + ":" + ex.getMessage(),ex);
		}
		return aDelimitedDataWriter;
	}


	/**
	 * True allows only one record per line.  False puts all records
	 * on one line.
	 *
	 * @param  b
	 */
	private void setOneRecordPerLinePolicy(boolean b) {
		formatter.setOneRecordPerLinePolicy(b);
	}


	/**  Description of the Method */
	public boolean checkConfig() {
		return true;
	}
	
	public String getType(){
		return COMPONENT_TYPE;
	}
}

