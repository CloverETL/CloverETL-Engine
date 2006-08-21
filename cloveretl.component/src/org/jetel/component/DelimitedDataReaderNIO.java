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

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.jetel.data.DataRecord;
import org.jetel.data.parser.DelimitedDataParserNIO;
import org.jetel.exception.BadDataFormatExceptionHandler;
import org.jetel.exception.BadDataFormatExceptionHandlerFactory;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.Node;
import org.jetel.graph.TransformationGraph;
import org.jetel.util.ComponentXMLAttributes;
import org.jetel.util.SynchronizeUtils;
import org.w3c.dom.Element;

/**
 *  <h3>Delimited Data Reader Component</h3>
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
 * <td>Parses specified input data file and broadcasts the records to all connected out ports.</td></tr>
 * <tr><td><h4><i>Inputs:</i></h4></td>
 * <td></td></tr>
 * <tr><td><h4><i>Outputs:</i></h4></td>
 * <td>At least one output port defined/connected.</td></tr>
 * <tr><td><h4><i>Comment:</i></h4></td>
 * <td>Uses java.nio.* classes</td></tr>
 * </table>
 *  <br>
 *  <table border="1">
 *  <th>XML attributes:</th>
 *  <tr><td><b>type</b></td><td>"DELIMITED_DATA_READER_NIO"</td></tr>
 *  <tr><td><b>id</b></td><td>component identification</td>
 *  <tr><td><b>fileURL</b></td><td>path to the input file</td>
 *  <tr><td><b>charset</b></td><td>character encoding of the input file (if not specified, then ISO-8859-1 is used)</td>
 *  <tr><td><b>DataPolicy</b></td><td>specifies how to handle misformatted or incorrect data.  'Strict' (default value) aborts processing, 'Controlled' logs the entire record while processing continues, and 'Lenient' attempts to set incorrect data to default values while processing continues.</td>
 *  <tr><td><b>skipRows</b><br><i>optional</i></td><td>specifies how many records/rows should be skipped from the source file. Good for handling files where first rows is a header not a real data. Dafault is 0.</td>
 *  </tr>
 *  </table>
 *
 *  <h4>Example:</h4>
 *  <pre>&lt;Node type="DELIMITED_DATA_READER_NIO" id="InputFile" fileURL="/tmp/mydata.dat" charset="ISO-8859-15"/&gt;</pre>
 *
 * @author      dpavlis
 * @since       April 4, 2002
 * @revision    $Revision$
 * @see         org.jetel.data.parser.DelimitedDataParser
 */
public class DelimitedDataReaderNIO extends Node {

	/**  Description of the Field */
	public final static String COMPONENT_TYPE = "DELIMITED_DATA_READER_NIO";

	/** XML attribute names */
	private final static String XML_FILE_ATTRIBUTE = "fileURL";
	private final static String XML_CHARSET_ATTRIBUTE = "charset";
	private final static String XML_DATAPOLICY_ATTRIBUTE = "DataPolicy";
    private static final String XML_SKIP_ROWS_ATTRIBUTE = "skipRows";
	
	private final static int OUTPUT_PORT = 0;
	private String fileURL;

	private DelimitedDataParserNIO parser;
    private int skipRows=-1; // do not skip rows by default


	/**
	 *Constructor for the DelimitedDataReaderNIO object
	 *
	 * @param  id       Description of the Parameter
	 * @param  fileURL  Description of the Parameter
	 */
	public DelimitedDataReaderNIO(String id, String fileURL) {
		super(id);
		this.fileURL = fileURL;
		parser = new DelimitedDataParserNIO();
	}


	/**
	 *Constructor for the DelimitedDataReaderNIO object
	 *
	 * @param  id       Description of the Parameter
	 * @param  fileURL  Description of the Parameter
	 * @param  charset  Description of the Parameter
	 */
	public DelimitedDataReaderNIO(String id, String fileURL, String charset) {
		super(id);
		this.fileURL = fileURL;
		parser = new DelimitedDataParserNIO(charset);
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

		//       shall we skip rows ?
        if (skipRows>0){
            try {
                for(int i=0;i<skipRows && runIt;i++){
                // till we skip required num of records or it reaches end of data or it is stopped from outside
                    if (parser.getNext(record) == null) break;
                    SynchronizeUtils.cloverYield();
                }
            } catch (Exception ex) {
                resultMsg = ex.getClass().getName()+" : "+ ex.getMessage();
                resultCode = Node.RESULT_FATAL_ERROR;
                return;
            }
        }
        
        // MAIN RUN LOOOP
        
		try {
			// till it reaches end of data or it is stopped from outside
			while (((record = parser.getNext(record)) != null) && runIt) {
				//broadcast the record to all connected Edges
				writeRecordBroadcast(record);
				SynchronizeUtils.cloverYield();
			}
		} catch (IOException ex) {
		    ex.printStackTrace();
			resultMsg = ex.getMessage();
			resultCode = Node.RESULT_ERROR;
			closeAllOutputPorts();
			return;
		} catch (Exception ex) {
		    ex.printStackTrace();
			resultMsg = ex.getClass().getName()+" : "+ ex.getMessage();
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
			throw new ComponentNotReadyException(getId() + ": atleast one output port has to be defined!");
		}
		// try to open file & initialize data parser
		try {
			parser.open(new FileInputStream(fileURL), getOutputPort(OUTPUT_PORT).getMetadata());
		} catch (FileNotFoundException ex) {
			throw new ComponentNotReadyException(this," IOError: " + ex.getMessage());
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
		xmlElement.setAttribute(XML_FILE_ATTRIBUTE, this.fileURL);
		// returns either user specified charset, or default value
		String charSet = this.parser.getCharsetName();
		if (charSet != null) {
			xmlElement.setAttribute(XML_CHARSET_ATTRIBUTE, charSet);
		}
		String policy = getDataPolicy();
		if (policy != null) {
			xmlElement.setAttribute(XML_DATAPOLICY_ATTRIBUTE, getDataPolicy());
		}
		
	}


	/**
	 *  Description of the Method
	 *
	 * @param  nodeXML  Description of Parameter
	 * @return          Description of the Returned Value
	 * @since           May 21, 2002
	 */
    @Override public static Node fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException {
		DelimitedDataReaderNIO aDelimitedDataReaderNIO = null;
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(xmlElement, graph);

		try {
			if (xattribs.exists(XML_CHARSET_ATTRIBUTE)) {
				aDelimitedDataReaderNIO = new DelimitedDataReaderNIO(xattribs.getString(XML_ID_ATTRIBUTE),
						xattribs.getString(XML_FILE_ATTRIBUTE),
						xattribs.getString(XML_CHARSET_ATTRIBUTE));
			} else {
				aDelimitedDataReaderNIO = new DelimitedDataReaderNIO(xattribs.getString(XML_ID_ATTRIBUTE),
						xattribs.getString(XML_FILE_ATTRIBUTE));
			}
			if (xattribs.exists(XML_DATAPOLICY_ATTRIBUTE)) {
				aDelimitedDataReaderNIO.addBDFHandler(BadDataFormatExceptionHandlerFactory.getHandler(
						xattribs.getString(XML_DATAPOLICY_ATTRIBUTE)));
			}
            if (xattribs.exists(XML_SKIP_ROWS_ATTRIBUTE)){
                aDelimitedDataReaderNIO.setSkipRows(xattribs.getInteger(XML_SKIP_ROWS_ATTRIBUTE));
            }
		} catch (Exception ex) {
	           throw new XMLConfigurationException(COMPONENT_TYPE + ":" + xattribs.getString(XML_ID_ATTRIBUTE," unknown ID ") + ":" + ex.getMessage(),ex);
		}

		return aDelimitedDataReaderNIO;
	}


	/**
	 * Adds BadDataFormatExceptionHandler to behave according to DataPolicy.
	 *
	 * @param  handler
	 */
	public void addBDFHandler(BadDataFormatExceptionHandler handler) {
		parser.addBDFHandler(handler);
	}


	/**
	 * Return data checking policy
	 * @return User defined data policy, or null if none was specified
	 * @see org.jetel.exception.BadDataFormatExceptionHandler
	 */
	public String getDataPolicy() {
		String policyType = this.parser.getBDFHandlerPolicyType();
		
		if (policyType != null) {
			return(policyType);
		}
		
		return(null);
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
     * @return Returns the skipRows.
     */
    public int getSkipRows() {
        return skipRows;
    }
    /**
     * @param skipRows The skipRows to set.
     */
    public void setSkipRows(int skipRows) {
        this.skipRows = skipRows;
    }
    
}

