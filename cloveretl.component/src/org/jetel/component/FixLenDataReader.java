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
import java.nio.channels.ReadableByteChannel;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.parser.FixLenByteDataParser;
import org.jetel.data.parser.FixLenCharDataParser;
import org.jetel.data.parser.FixLenDataParser3;
import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.IParserExceptionHandler;
import org.jetel.exception.ParserExceptionHandlerFactory;
import org.jetel.exception.PolicyType;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.Node;
import org.jetel.graph.TransformationGraph;
import org.jetel.util.ComponentXMLAttributes;
import org.jetel.util.FileUtils;
import org.jetel.util.SynchronizeUtils;
import org.jetel.util.WcardPattern;
import org.w3c.dom.Element;

/**
 *  <h3>Fixed Length Data NIO Reader Component</h3>
 *
 * <!-- Parses specified input data file and broadcasts the records to all connected out ports -->
 *
 * <table border="1">
 *  <th>Component:</th>
 * <tr><td><h4><i>Name:</i></h4></td>
 * <td>FixLenDataReader</td></tr>
 * <tr><td><h4><i>Category:</i></h4></td>
 * <td></td></tr>
 * <tr><td><h4><i>Description:</i></h4></td>
 * <td>Parses specified fixed-length record, input data file and broadcasts the records to all connected out ports.
 * The length of record is fixed either in bytes or in chars. Byte mode is somewhat stricter than char mode.
 * It doesn't support record delimiters, incomplete records and skipping of leading/trailing blanks in fields.
 * </td></tr>
 * <tr><td><h4><i>Inputs:</i></h4></td>
 * <td>At least one input port or input file.</td></tr>
 * <tr><td><h4><i>Outputs:</i></h4></td>
 * <td>At least one output port defined/connected.</td></tr>
 * <tr><td><h4><i>Comment:</i></h4></td>
 * <td></td></tr>
 * </table>
 *  <br>
 *  <table border="1">
 *  <th>XML attributes:</th>
 *  <tr><td><b>type</b></td><td>"FIXLEN_DATA_READER"</td></tr>
 *  <tr><td><b>id</b></td><td>component identification</td>
 *  <tr><td><b>byteMode</b></td><td>Specifies parsing mode. true for byte mode, false for char mode.
 *  Default is false.</td>
 *  <tr><td><b>fileURL</b></td><td>path to the input file</td>
 *  <tr><td><b>skipLeadingBlanks</b></td><td>Values true/false. Sets on/off skipping of leading blanks in fields.
 *  Default is true. It doesn't have any effect in byte mode.</td>
 *  <tr><td><b>skipTrailingBlanks</b></td><td>Values true/false. Sets on/off skipping of trailing blanks in fields.
 *  Default is true. It doesn't have any effect in byte mode.</td>
 *  <tr><td><b>enableIncomplete</b></td><td>Values true/false. Sets on/off support for incomplete records. 
 *  Default is true. It doesn't have any effect in byte mode.</td>
 *  <tr><td><b>skipEmpty</b></td><td>Values true/false. Specifies whether empty records are to be ignored. 
 *  Default is true. It doesn't have any effect in byte mode.</td>
 *  <tr><td><b>charset</b></td><td>character encoding of the input file (if not specified, then ISO-8859-1 is used)</td>
 *  <tr><td><b>dataPolicy</b></td><td>specifies how to handle misformatted or incorrect data.  'Strict' (default value) aborts processing, 'Controlled' logs the entire record while processing continues, and 'Lenient' attempts to set incorrect data to default values while processing continues.</td>
 *  <tr><td><b>skipRows</b><br><i>optional</i></td><td>specifies how many records/rows should be skipped from the source file. Good for handling files where first rows is a header not a real data. Dafault is 0.</td>
 *  </tr>
 *  </table>
 *
 *  <h4>Example:</h4>
 *  <pre>&lt;Node type="FIXED_DATA_READER" id="InputFile" fileURL="/tmp/mydata.dat" /&gt;</pre>
 *
 * @author Jan Hadrava (jan.hadrava@javlinconsulting.cz), Javlin Consulting (www.javlinconsulting.cz)
 * @since 09/15/06  
 * @see         org.jetel.data.parser.FixLenDataParser3
 */

public class FixLenDataReader extends Node {

	private static final String XML_BYTEMODE_ATTRIBUTE = "byteMode";
	private static final String XML_SKIPLEADINGBLANKS_ATTRIBUTE = "skipLeadingBlanks";
	private static final String XML_SKIPTRAILINGBLANKS_ATTRIBUTE = "skipTrailingBlanks";
	private static final String XML_ENABLEINCOMPLETE_ATTRIBUTE = "enableIncomplete";
	private static final String XML_SKIPEMPTY_ATTRIBUTE = "skipEmpty";
	private static final String XML_FILEURL_ATTRIBUTE = "fileURL";
	private static final String XML_CHARSET_ATTRIBUTE = "charset";
	private final static String XML_DATAPOLICY_ATTRIBUTE = "dataPolicy";
	private static final String XML_SKIP_ROWS_ATTRIBUTE = "skipRows";
	
	static Log logger = LogFactory.getLog(FixLenDataParser3.class);
	
	/**  Description of the Field */
	public final static String COMPONENT_TYPE = "FIXLEN_DATA_READER";

	private final static int OUTPUT_PORT = 0;
	private String fileURL;

	private FixLenDataParser3 parser;
    private PolicyType policyType;
	
	private int skipRows= 0; // do not skip rows by default

	private boolean byteMode;

	/**
	 *Constructor for the FixLenDataReaderNIO object
	 *
	 * @param  id       Description of the Parameter
	 * @param  fileURL  Description of the Parameter
	 */
	public FixLenDataReader(String id, String fileURL,
			boolean byteMode) {
		super(id);
		this.byteMode = byteMode; 
		this.fileURL = fileURL;
		parser = byteMode ?
				new FixLenByteDataParser() :
				new FixLenCharDataParser();
	}


	/**
	 *Constructor for the FixLenDataReaderNIO object
	 *
	 * @param  id       Description of the Parameter
	 * @param  fileURL  Description of the Parameter
	 * @param  charset  Description of the Parameter
	 */
	public FixLenDataReader(String id, String fileURL, String charset,
			boolean byteMode) {
		super(id);
		this.byteMode = byteMode; 
		this.fileURL = fileURL;
		parser = byteMode ?
				new FixLenByteDataParser(charset) :
				new FixLenCharDataParser(charset);
	}



	/**
	 *  Main processing method for the SimpleCopy object
	 *
	 * @since    April 4, 2002
	 */
	public void run() {
		// we need to create data record - take the metadata from first output port
		DataRecord rec = new DataRecord(getOutputPort(OUTPUT_PORT).getMetadata());
		DataRecord record = null;
		rec.init();

		WcardPattern pat = new WcardPattern();
		pat.addPattern(fileURL, Defaults.DEFAULT_PATH_SEPARATOR_REGEX);
		Iterator<String> fileItor = pat.filenames().iterator();
		String filename = null;
		while (fileItor.hasNext() && runIt) {				
			ReadableByteChannel stream;
			filename = fileItor.next();
			logger.info("Start parsing file " + filename);
			try {
				stream = FileUtils.getReadableChannel(filename);
				parser.setDataSource(stream);
				parser.skip(skipRows);	// skip in each file
				record =rec;
				while (record != null && runIt) {
					//broadcast the record to all connected Edges
                    if((record = parser.getNext(record)) != null && runIt) {
						writeRecordBroadcast(record);
						SynchronizeUtils.cloverYield();
                    }
				}
            } catch(BadDataFormatException bdfe) {
                if(policyType == PolicyType.STRICT) {
                    parser.close();
                    broadcastEOF();
                    resultMsg = bdfe.getMessage();
                    resultCode = Node.RESULT_ERROR;
                    return;
                } else {
                    logger.info(bdfe.getMessage());
                }
			} catch (Exception ex) {
				logger.error("An error  occured while parsing file \"" + filename + "\": " + ex.getClass().getName()+" : "+ ex.getMessage());
			}
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
		// try initialize data parser
		parser.open(null, getOutputPort(OUTPUT_PORT).getMetadata());
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
		
		if (this.isByteMode()) {
			xmlElement.setAttribute(XML_BYTEMODE_ATTRIBUTE,
					String.valueOf(this.isByteMode()));
		}
		
		if (this.parser.getCharsetName() != null) {
			xmlElement.setAttribute(XML_CHARSET_ATTRIBUTE, this.parser.getCharsetName());
		}
		
		xmlElement.setAttribute(XML_SKIPLEADINGBLANKS_ATTRIBUTE,
				String.valueOf(this.parser.isSkipLeadingBlanks()));
		
		xmlElement.setAttribute(XML_SKIPTRAILINGBLANKS_ATTRIBUTE,
				String.valueOf(this.parser.isSkipTrailingBlanks()));
		
		xmlElement.setAttribute(XML_ENABLEINCOMPLETE_ATTRIBUTE,
				String.valueOf(this.parser.isEnableIncomplete()));
		
		xmlElement.setAttribute(XML_SKIPEMPTY_ATTRIBUTE,
				String.valueOf(this.parser.isSkipEmpty()));
		
		if (this.skipRows>0){
		    xmlElement.setAttribute(XML_SKIP_ROWS_ATTRIBUTE, String.valueOf(skipRows));
		}
		xmlElement.setAttribute(XML_DATAPOLICY_ATTRIBUTE, policyType.toString());		
	}


	/**
	 *  Description of the Method
	 *
	 * @param  nodeXML  Description of Parameter
	 * @return          Description of the Returned Value
	 * @since           May 21, 2002
	 */
    public static Node fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException {
		FixLenDataReader aFixLenDataReaderNIO = null;
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(xmlElement, graph);
		
		try {
			String charset = null;
			if (xattribs.exists(XML_CHARSET_ATTRIBUTE)) {
				charset = xattribs.getString(XML_CHARSET_ATTRIBUTE);
			}
			boolean byteMode = false;
			if (xattribs.exists(XML_BYTEMODE_ATTRIBUTE)){
				byteMode = xattribs.getBoolean(XML_BYTEMODE_ATTRIBUTE);
			}
			aFixLenDataReaderNIO = new FixLenDataReader(xattribs.getString(XML_ID_ATTRIBUTE),
						xattribs.getString(XML_FILEURL_ATTRIBUTE, ""),
						charset, byteMode);
			if (xattribs.exists(XML_ENABLEINCOMPLETE_ATTRIBUTE)){
				aFixLenDataReaderNIO.parser.setEnableIncomplete(xattribs.getBoolean(XML_ENABLEINCOMPLETE_ATTRIBUTE));
			}
			if (xattribs.exists(XML_SKIPEMPTY_ATTRIBUTE)){
				aFixLenDataReaderNIO.parser.setSkipEmpty(xattribs.getBoolean(XML_SKIPEMPTY_ATTRIBUTE));
			}
			if (xattribs.exists(XML_SKIPLEADINGBLANKS_ATTRIBUTE)){
				aFixLenDataReaderNIO.parser.setSkipLeadingBlanks(xattribs.getBoolean(XML_SKIPLEADINGBLANKS_ATTRIBUTE));
			}
			if (xattribs.exists(XML_SKIPTRAILINGBLANKS_ATTRIBUTE)){
				aFixLenDataReaderNIO.parser.setSkipTrailingBlanks(xattribs.getBoolean(XML_SKIPTRAILINGBLANKS_ATTRIBUTE));
			}
			if (xattribs.exists(XML_SKIP_ROWS_ATTRIBUTE)){
				aFixLenDataReaderNIO.setSkipRows(xattribs.getInteger(XML_SKIP_ROWS_ATTRIBUTE));
			}
			aFixLenDataReaderNIO.setPolicyType(xattribs.getString(XML_DATAPOLICY_ATTRIBUTE, null));			
		} catch (Exception ex) {
	           throw new XMLConfigurationException(COMPONENT_TYPE + ":" + xattribs.getString(XML_ID_ATTRIBUTE," unknown ID ") + ":" + ex.getMessage(),ex);
		}

		return aFixLenDataReaderNIO;
	}


	/**
	 * Adds BadDataFormatExceptionHandler to behave according to DataPolicy.
	 *
	 * @param  handler
	 */
	public void setExceptionHandler(IParserExceptionHandler handler) {
		parser.setExceptionHandler(handler);
	}


	/**  Description of the Method */
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
    
    public void setPolicyType(String strPolicyType) {
        setPolicyType(PolicyType.valueOfIgnoreCase(strPolicyType));
    }
    
	public void setPolicyType(PolicyType policyType) {
        this.policyType = policyType;
        parser.setExceptionHandler(ParserExceptionHandlerFactory.getHandler(policyType));
	}

	public PolicyType getPolicyType() {
		return this.parser.getPolicyType();
	}
	
    private boolean isByteMode() {
    	return byteMode;
    }
}

