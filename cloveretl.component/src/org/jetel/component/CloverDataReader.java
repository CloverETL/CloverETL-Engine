
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

import java.io.IOException;
import java.security.InvalidParameterException;

import org.jetel.data.DataRecord;
import org.jetel.data.parser.CloverDataParser;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.Node;
import org.jetel.graph.TransformationGraph;
import org.jetel.util.ComponentXMLAttributes;
import org.jetel.util.SynchronizeUtils;
import org.w3c.dom.Element;

/**
 *  <h3>Clover Data Reader Component</h3>
 *
 * <!-- Reads data saved in Clover internal format and send the records to out 
 * ports. -->
 *
 * <table border="1">
 *  <th>Component:</th>
 * <tr><td><h4><i>Name:</i></h4></td>
 * <td>CloverReader</td></tr>
 * <tr><td><h4><i>Category:</i></h4></td>
 * <td></td></tr>
 * <tr><td><h4><i>Description:</i></h4></td>
 * <td>Reads data saved in Clover internal format and send the records to out 
 * ports.</td></tr>
 * <tr><td><h4><i>Inputs:</i></h4></td>
 * <td></td></tr>
 * <tr><td><h4><i>Outputs:</i></h4></td>
 * <td>At least one output port defined/connected.</tr>
 * <tr><td><h4><i>Comment:</i></h4></td></tr>
 * </table>
 *  <br>
 *  <table border="1">
 *  <th>XML attributes:</th>
 *  <tr><td><b>type</b></td><td>"CLOVER_READER"</td></tr>
 *  <tr><td><b>id</b></td><td>component identification</td>
 *  <tr><td><b>fileURL</b></td><td>path to the data file. It can be zip file 
 *  storing data, data indexes and metadata description or binary file with data
 *  saved in Clover internal format. In such case it is predicted, that indexes
 *  (if needed) are stored in file ../INDEX/dataFileName.idx in relation to dataFileName</td>
 *  <tr><td><b>startRecord</b></td><td>index of first parsed record</td>
 *  <tr><td><b>finalRecord</b></td><td>index of final parsed record</td>
 *  </tr>
 *  </table>
 *
 *  <h4>Example:</h4>
 *  <pre>&lt;Node fileURL="DATA/customers.clv" finalRecord="2" id="CLOVER_READER0" 
 *  startRecord="1" type="CLOVER_READER"/&gt;
 * 
 * <pre>&lt;Node fileURL="customers.clv.zip" id="CLOVER_READER0" type="CLOVER_READER"/&gt;
 * 
 * @author avackova <agata.vackova@javlinconsulting.cz> ; 
 * (c) JavlinConsulting s.r.o.
 *  www.javlinconsulting.cz
 *
 * @since Oct 13, 2006
 * @see CloverDataParser.java
 *
 */


public class CloverDataReader extends Node {

	public final static String COMPONENT_TYPE = "CLOVER_READER";

	/** XML attribute names */
	private final static String XML_FILE_ATTRIBUTE = "fileURL";
	private static final String XML_STARTRECORD_ATTRIBUTE = "startRecord";
	private static final String XML_FINALRECORD_ATTRIBUTE = "finalRecord";

	private final static int OUTPUT_PORT = 0;

	private String fileURL;
	private CloverDataParser parser;
	private int startRecord = -1;
	private int finalRecord = -1;

	public CloverDataReader(String id, String fileURL) {
		super(id);
		this.fileURL = fileURL;
		parser = new CloverDataParser();
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.Node#getType()
	 */
	@Override
	public String getType() {
		return COMPONENT_TYPE;
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.Node#run()
	 */
	@Override
	public void run() {
		DataRecord record = new DataRecord(getOutputPort(OUTPUT_PORT).getMetadata());
        record.init();
		int diffRecord = (startRecord != -1) ? finalRecord - startRecord : finalRecord - 1;
		int recordCount = 0;
        try {
	        while ((record = parser.getNext(record))!=null){
				    writeRecordBroadcast(record);
					if(finalRecord != -1 && ++recordCount > diffRecord) {
						break;
					}
					SynchronizeUtils.cloverYield();
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
		parser.close();
		broadcastEOF();
		if (runIt) {
			resultMsg = "OK";
		} else {
			resultMsg = "STOPPED";
		}
		resultCode = Node.RESULT_OK;
	}

	public static Node fromXML(TransformationGraph graph, Element nodeXML) throws XMLConfigurationException {
		CloverDataReader aDataReader = null;
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(nodeXML, graph);

		try {
			aDataReader = new CloverDataReader(xattribs.getString(Node.XML_ID_ATTRIBUTE),
						xattribs.getString(XML_FILE_ATTRIBUTE));
			if (xattribs.exists(XML_STARTRECORD_ATTRIBUTE)){
				aDataReader.setStartRecord(xattribs.getInteger(XML_STARTRECORD_ATTRIBUTE));
			}
			if (xattribs.exists(XML_FINALRECORD_ATTRIBUTE)){
				aDataReader.setFinalRecord(xattribs.getInteger(XML_FINALRECORD_ATTRIBUTE));
			}
		} catch (Exception ex) {
		    throw new XMLConfigurationException(COMPONENT_TYPE + ":" + xattribs.getString(XML_ID_ATTRIBUTE," unknown ID ") + ":" + ex.getMessage(),ex);
		}

		return aDataReader;
	}

	public void toXML(Element xmlElement) {
	    super.toXML(xmlElement);
		xmlElement.setAttribute(XML_FILE_ATTRIBUTE, this.fileURL);
		if (finalRecord > -1) {
			xmlElement.setAttribute(XML_FINALRECORD_ATTRIBUTE,String.valueOf(finalRecord));
		}
		if (startRecord > -1){
			xmlElement.setAttribute(XML_STARTRECORD_ATTRIBUTE,String.valueOf(startRecord));
		}
	}
	/* (non-Javadoc)
	 * @see org.jetel.graph.GraphElement#checkConfig()
	 */
	@Override
	public boolean checkConfig() {
		return true;
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.GraphElement#init()
	 */
	@Override
	public void init() throws ComponentNotReadyException {
		if (outPorts.size() < 1) {
			throw new ComponentNotReadyException("At least one output port has to be defined!");
		}
		//set start record
		if (startRecord != -1) {
			try{
				parser.skip(startRecord);
			}catch (JetelException ex) {}
		}
		parser.open(fileURL, getOutputPort(OUTPUT_PORT).getMetadata());
	}
	
	public void setStartRecord(int startRecord){
		this.startRecord = startRecord;
	}

	public void setFinalRecord(int finalRecord) {
		if(finalRecord < 0 || (startRecord != -1 && startRecord > finalRecord)) {
			throw new InvalidParameterException("Invalid finalRecord parameter.");
		}
		this.finalRecord = finalRecord;
	}
	
}
