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

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.security.InvalidParameterException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.IntegerDataField;
import org.jetel.data.StringDataField;
import org.jetel.data.parser.DataParser;
import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ParserExceptionHandlerFactory;
import org.jetel.exception.PolicyType;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.Node;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.ComponentXMLAttributes;
import org.jetel.util.FileUtils;
import org.jetel.util.SynchronizeUtils;
import org.w3c.dom.Element;

/**
 *  <h3>Data Reader Component</h3>
 *
 * <!-- Parses specified input data file and send the records to first out port. -->
 *
 * <table border="1">
 *  <th>Component:</th>
 * <tr><td><h4><i>Name:</i></h4></td>
 * <td>DataReader</td></tr>
 * <tr><td><h4><i>Category:</i></h4></td>
 * <td></td></tr>
 * <tr><td><h4><i>Description:</i></h4></td>
 * <td>Parses specified input data file and send the records to first out port.</td></tr>
 * <tr><td><h4><i>Inputs:</i></h4></td>
 * <td></td></tr>
 * <tr><td><h4><i>Outputs:</i></h4></td>
 * <td>One obligate output port defined/connected.</td></tr>
 * <td>One optional logging port defined/connected.</td></tr>
 * <tr><td><h4><i>Comment:</i></h4></td></tr>
 * </table>
 *  <br>
 *  <table border="1">
 *  <th>XML attributes:</th>
 *  <tr><td><b>type</b></td><td>"DATA_READER"</td></tr>
 *  <tr><td><b>id</b></td><td>component identification</td>
 *  <tr><td><b>fileURL</b></td><td>path to the input file</td>
 *  <tr><td><b>charset</b></td><td>character encoding of the input file (if not specified, then ISO-8859-1 is used)</td>
 *  <tr><td><b>dataPolicy</b></td><td>specifies how to handle misformatted or incorrect data.  'Strict' (default value) aborts processing, 'Controlled' logs the entire record while processing continues, and 'Lenient' attempts to set incorrect data to default values while processing continues.</td>
 *  <tr><td><b>skipLeadingBlanks</b><br><i>optional</i></td><td>specifies whether leading blanks at each fixlen field should be skipped. Default value is TRUE.<br>
 *  <i>Note: if this option is ON (TRUE), then field composed of all blanks/spaces is transformed to NULL (zero length string).</i></td>
 *  <tr><td><b>skipFirstLine</b></td><td>specifies whether first record/line should be skipped. Default value is FALSE. If record delimiter is specified than skip one record else first line of flat file.</td>
 *  <tr><td><b>startRecord</b></td><td>index of first parsed record</td>
 *  <tr><td><b>finalRecord</b></td><td>index of final parsed record</td>
 *  <tr><td><b>maxErrorCount</b></td><td>count of tolerated error records in input file</td>
 *  <tr><td><b>quotedStrings</b></td><td>string field can be quoted by '' or ""</td>
 *  <tr><td><b>treatMultipleDelimitersAsOne</b></td><td>if this option is true, then multiple delimiters are recognise as one delimiter</td>
 *  </tr>
 *  </table>
 *
 *  <h4>Example:</h4>
 *  <pre>&lt;Node type="DATA_READER" id="InputFile" fileURL="zip:http://www.store.com/data.zip#data.txt" charset="ISO-8859-15"/&gt;</pre>
 *
 * @author      Martin Zatopek, David Pavlis, OpenTech, s.r.o (www.opentech.cz)
 * @since       April 4, 2002
 * @revision    $Revision: 1.11 $
 * @see         org.jetel.data.parser.DelimitedDataParser
 */
public class DataReader extends Node {

    static Log logger = LogFactory.getLog(DataReader.class);

	/**  Description of the Field */
	public final static String COMPONENT_TYPE = "DATA_READER";

	/** XML attribute names */
	private static final String XML_SKIPLEADINGBLANKS_ATTRIBUTE = "skipLeadingBlanks";
	private static final String XML_SKIPFIRSTLINE_ATTRIBUTE = "skipFirstLine";
	private static final String XML_STARTRECORD_ATTRIBUTE = "startRecord";
	private static final String XML_FINALRECORD_ATTRIBUTE = "finalRecord";
	private static final String XML_MAXERRORCOUNT_ATTRIBUTE = "maxErrorCount";
	private static final String XML_QUOTEDSTRINGS_ATTRIBUTE = "quotedStrings";
	private static final String XML_TREATMULTIPLEDELIMITERSASONE_ATTRIBUTE = "treatMultipleDelimitersAsOne";
	private final static String XML_FILE_ATTRIBUTE = "fileURL";
	private final static String XML_CHARSET_ATTRIBUTE = "charset";
	private final static String XML_DATAPOLICY_ATTRIBUTE = "dataPolicy";
	
	private final static int OUTPUT_PORT = 0;
	private final static int LOG_PORT = 1;
	private String fileURL;
	private boolean skipFirstLine = false;
	private int startRecord = -1;
	private int finalRecord = -1;
	private int maxErrorCount = -1;
    
	private DataParser parser;
	private PolicyType policyType = PolicyType.STRICT;

	/**
	 *Constructor for the DelimitedDataReaderNIO object
	 *
	 * @param  id       Description of the Parameter
	 * @param  fileURL  Description of the Parameter
	 */
	public DataReader(String id, String fileURL) {
		super(id);
		this.fileURL = fileURL;
		parser = new DataParser();
	}


	/**
	 *Constructor for the DelimitedDataReaderNIO object
	 *
	 * @param  id       Description of the Parameter
	 * @param  fileURL  Description of the Parameter
	 * @param  charset  Description of the Parameter
	 */
	public DataReader(String id, String fileURL, String charset) {
		super(id);
		this.fileURL = fileURL;
		parser = new DataParser(charset);
	}



	/**
	 *  Main processing method for the SimpleCopy object
	 *
	 * @since    April 4, 2002
	 */
	public void run() {
        boolean logging = false;
        if(getOutPorts().size() == 2) {
            if(checkLogPortMetadata()) {
                logging = true;
            }
        }
		// we need to create data record - take the metadata from first output port
		DataRecord record = new DataRecord(getOutputPort(OUTPUT_PORT).getMetadata());
        record.init();
        // if we have second output port we can logging - create data record for log port
		DataRecord logRecord = null;
        if(logging) {
            logRecord = new DataRecord(getOutputPort(LOG_PORT).getMetadata());
            logRecord.init();
        }
		int diffRecord = (startRecord != -1) ? finalRecord - startRecord : finalRecord - 1;
		int errorCount = 0;
		
		try {
			//skip first line
			if(isSkipFirstLine()) {
				parser.skipFirstLine();
			}
			//skip first 'startRecord' lines
			if(startRecord != -1) {
				parser.skip(startRecord);
			}
			//parse
			while(!parser.endOfInputChannel() && runIt) {
                try {
    				if((parser.getNext(record)) != null) {
    				    writeRecord(OUTPUT_PORT, record);
    				}
                } catch(BadDataFormatException bdfe) {
                    if(policyType == PolicyType.STRICT) {
                        throw bdfe;
                    } else {
                        if(logging) {
                            //TODO implement log port framework
                            ((IntegerDataField) logRecord.getField(0)).setValue(bdfe.getRecordNumber());
                            ((StringDataField) logRecord.getField(1)).setValue(bdfe.getOffendingValue());
                            ((StringDataField) logRecord.getField(2)).setValue(bdfe.getMessage());
                            writeRecord(LOG_PORT, logRecord);
                        } else {
                            logger.info(bdfe.getMessage());
                        }
                        if(maxErrorCount != -1 && ++errorCount > maxErrorCount) {
                            logger.error("DataParser (" + getName() + "): Max error count exceeded.");
                            break;
                        }
                    }
                }
				if(finalRecord != -1 && parser.getRecordCount() > diffRecord) {
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


	private boolean checkLogPortMetadata() {
        DataRecordMetadata logMetadata = getOutputPort(LOG_PORT).getMetadata();

        boolean ret = logMetadata.getNumFields() == 3 
            && logMetadata.getField(0).getType() == DataFieldMetadata.INTEGER_FIELD
            && logMetadata.getField(1).getType() == DataFieldMetadata.STRING_FIELD
            && logMetadata.getField(2).getType() == DataFieldMetadata.STRING_FIELD;
        
        if(!ret) {
            logger.warn("The log port metedata has invalid format (expected data fields - integer, string, string)");
        }
        
        return ret;
    }
	
	/**
	 *  Description of the Method
	 *
	 * @exception  ComponentNotReadyException  Description of the Exception
	 * @since                                  April 4, 2002
	 */
	public void init() throws ComponentNotReadyException {
		// test that we have <1,2> output ports
        if (outPorts.size() < 1) {
            throw new ComponentNotReadyException(getId() + ": at least one output port can be defined!");
        }
		if (outPorts.size() > 2) {
			throw new ComponentNotReadyException(getId() + ": at most two output ports can be defined!");
		}
		// try to open input channel & initialize data parser
		try {
			parser.open(FileUtils.getChannel(fileURL), getOutputPort(OUTPUT_PORT).getMetadata());
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
	public void toXML(Element xmlElement) {
	    super.toXML(xmlElement);
		xmlElement.setAttribute(XML_FILE_ATTRIBUTE, this.fileURL);
		// returns either user specified charset, or default value
		String charSet = this.parser.getCharsetName();
		if (charSet != null) {
			xmlElement.setAttribute(XML_CHARSET_ATTRIBUTE, charSet);
		}
		xmlElement.setAttribute(XML_DATAPOLICY_ATTRIBUTE, policyType.toString());
		
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
		DataReader aDataReader = null;
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(nodeXML, graph);

		try {
			if (xattribs.exists(XML_CHARSET_ATTRIBUTE)) {
				aDataReader = new DataReader(xattribs.getString(Node.XML_ID_ATTRIBUTE),
						xattribs.getString(XML_FILE_ATTRIBUTE),
						xattribs.getString(XML_CHARSET_ATTRIBUTE));
			} else {
				aDataReader = new DataReader(xattribs.getString(Node.XML_ID_ATTRIBUTE),
						xattribs.getString(XML_FILE_ATTRIBUTE));
			}
			aDataReader.setPolicyType(xattribs.getString(XML_DATAPOLICY_ATTRIBUTE, null));
			if (xattribs.exists(XML_SKIPLEADINGBLANKS_ATTRIBUTE)){
				aDataReader.parser.setSkipLeadingBlanks(xattribs.getBoolean(XML_SKIPLEADINGBLANKS_ATTRIBUTE));
			}
			if (xattribs.exists(XML_SKIPFIRSTLINE_ATTRIBUTE)){
				aDataReader.setSkipFirstLine(xattribs.getBoolean(XML_SKIPFIRSTLINE_ATTRIBUTE));
			}
			if (xattribs.exists(XML_STARTRECORD_ATTRIBUTE)){
				aDataReader.setStartRecord(xattribs.getInteger(XML_STARTRECORD_ATTRIBUTE));
			}
			if (xattribs.exists(XML_FINALRECORD_ATTRIBUTE)){
				aDataReader.setFinalRecord(xattribs.getInteger(XML_FINALRECORD_ATTRIBUTE));
			}
			if (xattribs.exists(XML_MAXERRORCOUNT_ATTRIBUTE)){
				aDataReader.setMaxErrorCount(xattribs.getInteger(XML_MAXERRORCOUNT_ATTRIBUTE));
			}
			if (xattribs.exists(XML_QUOTEDSTRINGS_ATTRIBUTE)){
				aDataReader.parser.setQuotedStrings(xattribs.getBoolean(XML_QUOTEDSTRINGS_ATTRIBUTE));
			}
			if (xattribs.exists(XML_TREATMULTIPLEDELIMITERSASONE_ATTRIBUTE)){
				aDataReader.parser.setTreatMultipleDelimitersAsOne(xattribs.getBoolean(XML_TREATMULTIPLEDELIMITERSASONE_ATTRIBUTE));
			}
		} catch (Exception ex) {
		    throw new XMLConfigurationException(COMPONENT_TYPE + ":" + xattribs.getString(XML_ID_ATTRIBUTE," unknown ID ") + ":" + ex.getMessage(),ex);
		}

		return aDataReader;
	}
	
	public void setSkipFirstLine(boolean skip) {
		skipFirstLine = skip;
	}
	
	public boolean isSkipFirstLine() {
		return skipFirstLine;
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
	 * @return Returns the startRecord.
	 */
	public int getStartRecord() {
		return startRecord;
	}
	
	/**
	 * @param startRecord The startRecord to set.
	 */
	public void setStartRecord(int startRecord) {
		if(startRecord < 0 || (finalRecord != -1 && startRecord > finalRecord)) {
			throw new InvalidParameterException("Invalid StartRecord parametr.");
		}
		this.startRecord = startRecord;
	}
	
	/**
	 * @return Returns the finalRecord.
	 */
	
	public int getFinalRecord() {
		return finalRecord;
	}
	
	/**
	 * @param finalRecord The finalRecord to set.
	 */
	public void setFinalRecord(int finalRecord) {
		if(finalRecord < 0 || (startRecord != -1 && startRecord > finalRecord)) {
			throw new InvalidParameterException("Invalid finalRecord parameter.");
		}
		this.finalRecord = finalRecord;
	}

	/**
	 * @param finalRecord The finalRecord to set.
	 */
	public void setMaxErrorCount(int maxErrorCount) {
		if(maxErrorCount < 0) {
			throw new InvalidParameterException("Invalid maxErrorCount parameter.");
		}
		this.maxErrorCount = maxErrorCount;
	}
    
    public void setPolicyType(String strPolicyType) {
        setPolicyType(PolicyType.valueOfIgnoreCase(strPolicyType));
    }
    
    public void setPolicyType(PolicyType policyType) {
        this.policyType = policyType;
        parser.setExceptionHandler(ParserExceptionHandlerFactory.getHandler(policyType));
    }
}
