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
/*
 *  Created on Mar 19, 2003
 *
 *  To change this generated comment go to
 *  Window>Preferences>Java>Code Generation>Code and Comments
 */
package org.jetel.component;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.jetel.data.DataRecord;
import org.jetel.data.parser.FixLenDataParser2;
import org.jetel.exception.BadDataFormatExceptionHandler;
import org.jetel.exception.BadDataFormatExceptionHandlerFactory;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.Node;
import org.w3c.dom.NamedNodeMap;
import org.jetel.util.ComponentXMLAttributes;

/**
 *  <h3>Fixed Length Data NIO Reader Component</h3>
 *
 * <!-- Parses specified input data file and broadcasts the records to all connected out ports -->
 *
 * <table border="1">
 *  <th>Component:</th>
 * <tr><td><h4><i>Name:</i></h4></td>
 * <td>DelimitedDataReader</td></tr>
 * <tr><td><h4><i>Category:</i></h4></td>
 * <td></td></tr>
 * <tr><td><h4><i>Description:</i></h4></td>
 * <td>Parses specified fixed-length-record, input data file and broadcasts the records to all connected out ports.</td></tr>
 * <tr><td><h4><i>Inputs:</i></h4></td>
 * <td></td></tr>
 * <tr><td><h4><i>Outputs:</i></h4></td>
 * <td>At least one output port defined/connected.</td></tr>
 * <tr><td><h4><i>Comment:</i></h4></td>
 * <td></td></tr>
 * </table>
 *  <br>
 *  <table border="1">
 *  <th>XML attributes:</th>
 *  <tr><td><b>type</b></td><td>"FIXLEN_DATA_READER_NIO"</td></tr>
 *  <tr><td><b>id</b></td><td>component identification</td>
 *  <tr><td><b>fileURL</b></td><td>path to the input file</td>
 *  <tr><td><b>DataPolicy</b><br><i>optional</i></td><td>specifies how to handle misformatted or incorrect data.  'Strict' (default value) aborts processing, 'Controlled' logs the entire record while processing continues, and 'Lenient' attempts to set incorrect data to default values while processing continues.</td>
 *  <tr><td><b>OneRecordPerLine</b><br><i>optional</i></td><td>whether to put one or all records on one line. (values: true/false).  Default value is false.</td>
 *  </tr>
 *  </table>
 *
 *  <h4>Example:</h4>
 *  <pre>&lt;Node type="FIXED_DATA_READER_NIO" id="InputFile" fileURL="/tmp/mydata.dat" /&gt;</pre>
 *
 * @author      dpavlis, maciorowski
 * @since       April 4, 2002
 * @revision    $Revision$
 * @see         org.jetel.data.parser.FixLenDataParser, org.jetel.data.FixLenDataParser2
 */

public class FixLenDataReaderNIO extends Node {

	/**  Description of the Field */
	public final static String COMPONENT_TYPE = "FIXLEN_DATA_READER_NIO";

	private final static int OUTPUT_PORT = 0;
	private String fileURL;

	private FixLenDataParser2 parser;


	/**
	 *Constructor for the FixLenDataReaderNIO object
	 *
	 * @param  id       Description of the Parameter
	 * @param  fileURL  Description of the Parameter
	 */
	public FixLenDataReaderNIO(String id, String fileURL) {
		super(id);
		this.fileURL = fileURL;
		parser = new FixLenDataParser2();
	}


	/**
	 *Constructor for the FixLenDataReaderNIO object
	 *
	 * @param  id       Description of the Parameter
	 * @param  fileURL  Description of the Parameter
	 * @param  charset  Description of the Parameter
	 */
	public FixLenDataReaderNIO(String id, String fileURL, String charset) {
		super(id);
		this.fileURL = fileURL;
		parser = new FixLenDataParser2(charset);
	}



	/**
	 *  Main processing method for the SimpleCopy object
	 *
	 * @since    April 4, 2002
	 */
	public void run() {
		// we need to create data record - take the metadata from first output port
		DataRecord record = new DataRecord(getOutputPort(OUTPUT_PORT).getMetadata());
		record.init();

		try {
			// till it reaches end of data or it is stopped from outside
			while (((record = parser.getNext(record)) != null) && runIt) {
				//broadcast the record to all connected Edges
				writeRecordBroadcast(record);
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
		// we are done, close all connected output ports to indicate end of stream
		parser.close();
		broadcastEOF();
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
	 * @exception  ComponentNotReadyException  Description of the Exception
	 * @since                                  April 4, 2002
	 */
	public void init() throws ComponentNotReadyException {
		// test that we have at least one output port
		if (outPorts.size() < 1) {
			throw new ComponentNotReadyException(getID() + ": atleast one output port has to be defined!");
		}
		// try to open file & initialize data parser
		try {
			parser.open(new FileInputStream(fileURL), getOutputPort(OUTPUT_PORT).getMetadata());
		} catch (FileNotFoundException ex) {
			throw new ComponentNotReadyException(getID() + "IOError: " + ex.getMessage());
		}

	}


	/**
	 *  Description of the Method
	 *
	 * @return    Description of the Returned Value
	 * @since     May 21, 2002
	 */
	public org.w3c.dom.Node toXML() {
		// TODO
		return null;
	}


	/**
	 *  Description of the Method
	 *
	 * @param  nodeXML  Description of Parameter
	 * @return          Description of the Returned Value
	 * @since           May 21, 2002
	 */
	public static Node fromXML(org.w3c.dom.Node nodeXML) {
		FixLenDataReaderNIO aFixLenDataReaderNIO = null;
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(nodeXML);
		
		try {
			if (xattribs.exists("charset")) {
				aFixLenDataReaderNIO = new FixLenDataReaderNIO(xattribs.getString("id"),
						xattribs.getString("fileURL"),
						xattribs.getString("charset"));
			} else {
				aFixLenDataReaderNIO = new FixLenDataReaderNIO(xattribs.getString("id"),
						xattribs.getString("fileURL"));
			}
			if (xattribs.exists("DataPolicy")) {
				aFixLenDataReaderNIO.addBDFHandler(BadDataFormatExceptionHandlerFactory.getHandler(
					xattribs.getString("DataPolicy")));
			}
			if (xattribs.exists("OneRecordPerLine")){
				aFixLenDataReaderNIO.setOneRecordPerLinePolicy(xattribs.getBoolean("OneRecordPerLine"));
			}
			
		} catch (Exception ex) {
			System.err.println(ex.getMessage());
			return null;
		}

		return aFixLenDataReaderNIO;
	}


	/**
	 * Sets OneRecordPerLinePolicy.
	 *
	 * @param  b
	 */
	private void setOneRecordPerLinePolicy(boolean b) {
		parser.setOneRecordPerLinePolicy(b);
	}


	/**
	 * Adds BadDataFormatExceptionHandler to behave according to DataPolicy.
	 *
	 * @param  handler
	 */
	private void addBDFHandler(BadDataFormatExceptionHandler handler) {
		parser.addBDFHandler(handler);
	}


	/**  Description of the Method */
	public boolean checkConfig() {
		return true;
	}
}

