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
import java.io.*;
import org.w3c.dom.NamedNodeMap;
import org.jetel.graph.*;
import org.jetel.data.DataRecord;
import org.jetel.data.DataFormatter;
import org.jetel.data.FixLenDataFormatter;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.util.ComponentXMLAttributes;

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
 *  <tr><td><b>type</b></td><td>"FIXLEN_DATA_WRITER_NIO"</td></tr>
 *  <tr><td><b>id</b></td><td>component identification</td>
 *  <tr><td><b>fileURL</b></td><td>path to the input file</td>
 *  <tr><td><b>charset</b><br><i>optional</i></td><td>character encoding of the output file (if not specified, then ISO-8859-1 is used)</td>
 *  <tr><td><b>append</b><br><i>optional</i></td><td>whether to append data at the end if output file exists or replace it (values: true/false). Default is false</td>
 *  <tr><td><b>OneRecordPerLine</b><br><i>optional</i></td><td>whether to put one or all records on one line. (values: true/false).  Default value is false.</td>
 *  </tr>
 *  </table>
 *
 * <h4>Example:</h4>
 * <pre>&lt;Node type="FIXLEN_DATA_WRITER_NIO" id="Writer" fileURL="/tmp/transfor.out" append="true" /&gt;</pre>
 *
 *
 * @author      dpavlis
 * @since       April 4, 2002
 * @revision    $Revision$
 */
public class FixLenDataWriterNIO extends Node {
	private String fileURL;
	private boolean appendData;
	private DataFormatter formatter;

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
	public FixLenDataWriterNIO(String id, String fileURL, boolean appendData) {
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
	public FixLenDataWriterNIO(String id, String fileURL, String charset, boolean appendData) {
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
		while (record != null && runIt) {
			try {
				record = inPort.readRecord(record);
				if (record != null) {
					formatter.write(record);
				}
			} catch (IOException ex) {
				System.err.println("Writer IOException !");
				resultMsg = ex.getMessage();
				resultCode = Node.RESULT_ERROR;
				closeAllOutputPorts();
				return;
			} catch (Exception ex) {
				System.err.println("Writer Exception !");
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
		FixLenDataWriterNIO aFixLenDataWriterNIO = null;
		ComponentXMLAttributes xattribs=new ComponentXMLAttributes(nodeXML);
		final boolean _ONE_REC_PER_LINE_=false;
		final boolean _APPEND_=false;
		
		
		try{
		
			if (xattribs.exists("charset")){
				aFixLenDataWriterNIO = new FixLenDataWriterNIO(
						xattribs.getString("id"), 
						xattribs.getString("fileURL"),
						xattribs.getString("charset"),
						xattribs.getBoolean("append",_APPEND_));
			}else{
				aFixLenDataWriterNIO = new FixLenDataWriterNIO(
						xattribs.getString("id"), 
						xattribs.getString("fileURL"),
						xattribs.getBoolean("append",_APPEND_));
			}
			aFixLenDataWriterNIO.setOneRecordPerLinePolicy(xattribs.getBoolean("OneRecordPerLine",_ONE_REC_PER_LINE_));
		
		}catch(Exception ex){
			System.err.println(ex.getMessage());
			return null;
		}
		
		
		return aFixLenDataWriterNIO;
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


	/**
	 *  Description of the Method
	 *
	 * @return    Description of the Return Value
	 */
	public boolean checkConfig() {
		return true;
	}
}

