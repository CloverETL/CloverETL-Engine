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
import java.security.InvalidParameterException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.IntegerDataField;
import org.jetel.data.StringDataField;
import org.jetel.data.parser.DataParser;
import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.ParserExceptionHandlerFactory;
import org.jetel.exception.PolicyType;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.Node;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
//import org.jetel.graph.dictionary.IDictionaryValue;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.MultiFileReader;
import org.jetel.util.SynchronizeUtils;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.string.StringUtils;
import org.w3c.dom.Element;

/**
 *  <h3>Universal Data Reader Component</h3>
 *
 * <!-- Parses specified input data file and send the records to the first output port. 
 * Embeded parser covers both fixlen and delimited data format. -->
 *
 * <table border="1">
 *  <th>Component:</th>
 * <tr><td><h4><i>Name:</i></h4></td>
 * <td>DataReader</td></tr>
 * <tr><td><h4><i>Category:</i></h4></td>
 * <td></td></tr>
 * <tr><td><h4><i>Description:</i></h4></td>
 * <td>Parses specified input data file and send the records to the first output port. 
 * Embeded parser covers both fixlen and delimited data format.</td></tr>
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
 *  <tr><td><b>fileURL</b></td><td>path to the input files</td>
 *  <tr><td><b>charset</b></td><td>character encoding of the input file (if not specified, then ISO-8859-1 is used)</td>
 *  <tr><td><b>dataPolicy</b></td><td>specifies how to handle misformatted or incorrect data.  'Strict' (default value) aborts processing, 'Controlled' logs the entire record while processing continues, and 'Lenient' attempts to set incorrect data to default values while processing continues.</td>
 *  <tr><td><b>skipLeadingBlanks</b><br><i>optional</i></td><td>specifies whether leading blanks at each fixlen field should be skipped. Default value is TRUE.<br>
 *  <i>Note: if this option is ON (TRUE), then field composed of all blanks/spaces is transformed to NULL (zero length string).</i></td>
 *  <tr><td><b>trim</b><br><i>optional</i></td><td>specifies whether to trim strings before setting them to data fields.
 *  When not set, strings are trimmed depending on "trim" attribute of metadata.</td>
 *  <tr><td><b>skipFirstLine</b></td><td>specifies whether first record/line should be skipped. Default value is FALSE. If record delimiter is specified than skip one record else first line of flat file.</td>
 *  <tr><td><b>skipRows</b></td><td>specifies how many records/rows should be skipped from the source file; good for handling files where first rows is a header not a real data. Dafault is 0.</td>
 *  <tr><td><b>numRecords</b></td><td>max number of parsed records</td>
 *  <tr><td><b>maxErrorCount</b></td><td>count of tolerated error records in input file</td>
 *  <tr><td><b>quotedStrings</b></td><td>string field can be quoted by '' or ""</td>
 *  <tr><td><b>treatMultipleDelimitersAsOne</b></td><td>if this option is true, then multiple delimiters are recognise as one delimiter</td>
 *  </tr>
 *  </table>
 *
 *  <h4>Example:</h4>
 *  <pre>&lt;Node type="DATA_READER" id="InputFile" fileURL="zip:http://www.store.com/data.zip#data.txt" charset="ISO-8859-15"/&gt;</pre>
 *
 * @author      Martin Zatopek, David Pavlis, Javlin Consulting s.r.o. (www.javlinconsulting.cz)
 * @since       April 4, 2002
 * @revision    $Revision: 1.11 $
 * @see         org.jetel.data.parser.DelimitedDataParser
 */
public class DataReader extends Node {

    static Log logger = LogFactory.getLog(DataReader.class);

	/**  Description of the Field */
	public final static String COMPONENT_TYPE = "DATA_READER";

	/** XML attribute names */
	private static final String XML_TRIM_ATTRIBUTE = "trim";
	private static final String XML_SKIPLEADINGBLANKS_ATTRIBUTE = "skipLeadingBlanks";
	private static final String XML_SKIPTRAILINGBLANKS_ATTRIBUTE = "skipTrailingBlanks";
	private static final String XML_SKIPFIRSTLINE_ATTRIBUTE = "skipFirstLine";
	private static final String XML_SKIPROWS_ATTRIBUTE = "skipRows";
	private static final String XML_NUMRECORDS_ATTRIBUTE = "numRecords";
	private static final String XML_MAXERRORCOUNT_ATTRIBUTE = "maxErrorCount";
	private static final String XML_QUOTEDSTRINGS_ATTRIBUTE = "quotedStrings";
	private static final String XML_TREATMULTIPLEDELIMITERSASONE_ATTRIBUTE = "treatMultipleDelimitersAsOne";
	private final static String XML_FILE_ATTRIBUTE = "fileURL";
	private final static String XML_CHARSET_ATTRIBUTE = "charset";
	private final static String XML_DATAPOLICY_ATTRIBUTE = "dataPolicy";
	private static final String XML_INCREMENTAL_FILE_ATTRIBUTE = "incrementalFile";
	private static final String XML_INCREMENTAL_KEY_ATTRIBUTE = "incrementalKey";

	private final static int OUTPUT_PORT = 0;
	private final static int INPUT_PORT = 0;
	private final static int LOG_PORT = 1;
	private String fileURL;
	private boolean skipFirstLine = false;
	private int skipRows = -1;
	private int numRecords = -1;
	private int maxErrorCount = -1;
    private String incrementalFile;
    private String incrementalKey;

	private DataParser parser;
    private MultiFileReader reader;
    private PolicyType policyType = PolicyType.STRICT;

	private String charset;

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
		parser = new DataParser(this.charset = charset);
	}

	@Override
	public Result execute() throws Exception {
		boolean logging = false;
		if (getOutPorts().size() == 2) {
			if (checkLogPortMetadata()) {
				logging = true;
			}
		}
		// we need to create data record - take the metadata from first output
		// port
		DataRecord record = new DataRecord(getOutputPort(OUTPUT_PORT).getMetadata());
		record.init();
		// if we have second output port we can logging - create data record for
		// log port
		DataRecord logRecord = null;
		if (logging) {
			logRecord = new DataRecord(getOutputPort(LOG_PORT).getMetadata());
			logRecord.init();
		}
		int errorCount = 0;

		try {
			while (runIt) {
				try {
					if ((reader.getNext(record)) == null) {
						break;
					}
					writeRecord(OUTPUT_PORT, record);
				} catch (BadDataFormatException bdfe) {
					if (policyType == PolicyType.STRICT) {
						throw bdfe;
					} else {
						if (logging) {
							// TODO implement log port framework
							((IntegerDataField) logRecord.getField(0))
									.setValue(bdfe.getRecordNumber());
							((IntegerDataField) logRecord.getField(1))
									.setValue(bdfe.getFieldNumber() + 1);
							((StringDataField) logRecord.getField(2)).setValue(bdfe
									.getOffendingValue());
							((StringDataField) logRecord.getField(3)).setValue(bdfe
									.getMessage());
							writeRecord(LOG_PORT, logRecord);
						} else {
							logger.warn(bdfe.getMessage());
						}
						if (maxErrorCount != -1 && ++errorCount > maxErrorCount) {
							logger.error("DataParser (" + getName()
									+ "): Max error count exceeded.");
							return Result.ERROR;
						}
					}
				}
				SynchronizeUtils.cloverYield();
			}
		} catch (Exception e) {
			throw e;
		}finally{
			broadcastEOF();
		}
        return runIt ? Result.FINISHED_OK : Result.ABORTED;
	}


	private boolean checkLogPortMetadata() {
        DataRecordMetadata logMetadata = getOutputPort(LOG_PORT).getMetadata();

        boolean ret = logMetadata.getNumFields() == 4 
        	&& logMetadata.getField(0).getType() == DataFieldMetadata.INTEGER_FIELD
        	&& logMetadata.getField(1).getType() == DataFieldMetadata.INTEGER_FIELD
            && logMetadata.getField(2).getType() == DataFieldMetadata.STRING_FIELD
            && logMetadata.getField(3).getType() == DataFieldMetadata.STRING_FIELD;
        
        if(!ret) {
            logger.warn("The log port metadata has invalid format (expected data fields - integer (record number), integer (field number), string (raw record), string (error message)");
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
        if(isInitialized()) return;
        super.init();

        prepareMultiFileReader();
        
        try {
            reader.init(getOutputPort(OUTPUT_PORT).getMetadata());
        } catch(ComponentNotReadyException e) {
            e.setAttributeName(XML_FILE_ATTRIBUTE);
            throw e;
        }
	}

	private void prepareMultiFileReader() throws ComponentNotReadyException {
		// initialize multifile reader based on prepared parser
		TransformationGraph graph = getGraph();
        reader = new MultiFileReader(parser, graph != null ? graph.getProjectURL() : null, fileURL);
        reader.setLogger(logger);
        reader.setFileSkip(skipFirstLine ? 1 : 0);
        reader.setSkip(skipRows);
        reader.setNumRecords(numRecords);
        reader.setIncrementalFile(incrementalFile);
        reader.setIncrementalKey(incrementalKey);
        reader.setInputPort(getInputPort(INPUT_PORT)); //for port protocol: ReadableChannelIterator reads data
        reader.setCharset(charset);
        reader.setDictionary(graph.getDictionary());
	}

	@Override
	public synchronized void reset() throws ComponentNotReadyException {
		super.reset();
		reader.reset();

		/*
		// initialize multifile reader based on prepared parser
        reader = new MultiFileReader(parser, getGraph() != null ? getGraph().getProjectURL() : null, fileURL);
        reader.setLogger(logger);
        reader.setFileSkip(skipFirstLine ? 1 : 0);
        reader.setSkip(skipRows);
        reader.setNumRecords(numRecords);
        try {
            reader.init(getOutputPort(OUTPUT_PORT).getMetadata());
        } catch(ComponentNotReadyException e) {
            e.setAttributeName(XML_FILE_ATTRIBUTE);
            throw e;
        }
*/
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
		if (parser.getTrim() != null) {
			xmlElement.setAttribute(XML_TRIM_ATTRIBUTE, String.valueOf(parser.getTrim()));
		}		
		if (parser.getSkipLeadingBlanks() != null){
			xmlElement.setAttribute(XML_SKIPLEADINGBLANKS_ATTRIBUTE, String.valueOf(parser.getSkipLeadingBlanks()));
		}
		if (parser.getSkipTrailingBlanks() != null){
			xmlElement.setAttribute(XML_SKIPTRAILINGBLANKS_ATTRIBUTE, String.valueOf(parser.getSkipTrailingBlanks()));
		}
		
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
			if (xattribs.exists(XML_SKIPTRAILINGBLANKS_ATTRIBUTE)){
				aDataReader.parser.setSkipTrailingBlanks(xattribs.getBoolean(XML_SKIPTRAILINGBLANKS_ATTRIBUTE));
			}
			if (xattribs.exists(XML_TRIM_ATTRIBUTE)){
				aDataReader.parser.setTrim(xattribs.getBoolean(XML_TRIM_ATTRIBUTE));
			}
			if (xattribs.exists(XML_SKIPFIRSTLINE_ATTRIBUTE)){
				aDataReader.setSkipFirstLine(xattribs.getBoolean(XML_SKIPFIRSTLINE_ATTRIBUTE));
			}
			if (xattribs.exists(XML_SKIPROWS_ATTRIBUTE)){
				aDataReader.setSkipRows(xattribs.getInteger(XML_SKIPROWS_ATTRIBUTE));
			}
			if (xattribs.exists(XML_NUMRECORDS_ATTRIBUTE)){
				aDataReader.setNumRecords(xattribs.getInteger(XML_NUMRECORDS_ATTRIBUTE));
			}
			if (xattribs.exists(XML_MAXERRORCOUNT_ATTRIBUTE)){
				aDataReader.setMaxErrorCount(xattribs.getInteger(XML_MAXERRORCOUNT_ATTRIBUTE));
			}
			if (xattribs.exists(XML_QUOTEDSTRINGS_ATTRIBUTE)){
				aDataReader.setQuotedStrings(xattribs.getBoolean(XML_QUOTEDSTRINGS_ATTRIBUTE));
			}
			if (xattribs.exists(XML_TREATMULTIPLEDELIMITERSASONE_ATTRIBUTE)){
				aDataReader.setTreatMultipleDelimitersAsOne(xattribs.getBoolean(XML_TREATMULTIPLEDELIMITERSASONE_ATTRIBUTE));
			}
			if (xattribs.exists(XML_INCREMENTAL_FILE_ATTRIBUTE)){
				aDataReader.setIncrementalFile(xattribs.getString(XML_INCREMENTAL_FILE_ATTRIBUTE));
			}
			if (xattribs.exists(XML_INCREMENTAL_KEY_ATTRIBUTE)){
				aDataReader.setIncrementalKey(xattribs.getString(XML_INCREMENTAL_KEY_ATTRIBUTE));
			}
		} catch (Exception ex) {
		    throw new XMLConfigurationException(COMPONENT_TYPE + ":" + xattribs.getString(XML_ID_ATTRIBUTE," unknown ID ") + ":" + ex.getMessage(),ex);
		}

		return aDataReader;
	}
	
	public void setTreatMultipleDelimitersAsOne(boolean boolean1) {
		parser.setTreatMultipleDelimitersAsOne(boolean1);
	}


	public void setQuotedStrings(boolean boolean1) {
		parser.setQuotedStrings(boolean1);		
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
    @Override
    public ConfigurationStatus checkConfig(ConfigurationStatus status) {
        super.checkConfig(status);
        
        if(!checkInputPorts(status, 0, 1)
        		|| !checkOutputPorts(status, 1, 2)) {
        	return status;
        }
        
        

        try {
    		prepareMultiFileReader();
    		checkAutofilling(status, getOutputPort(OUTPUT_PORT).getMetadata());
    		reader.checkConfig(getOutputPort(OUTPUT_PORT).getMetadata());
        } catch (ComponentNotReadyException e) {
            ConfigurationProblem problem = new ConfigurationProblem(e.getMessage(), ConfigurationStatus.Severity.WARNING, this, ConfigurationStatus.Priority.NORMAL);
            if(!StringUtils.isEmpty(e.getAttributeName())) {
                problem.setAttributeName(e.getAttributeName());
            }
            status.add(problem);
        } finally {
        	free();
        }
        
        return status;
    }
	
	public String getType(){
		return COMPONENT_TYPE;
	}
	
	/**
	 * @param startRecord The startRecord to set.
	 */
	public void setSkipRows(int skipRows) {
		this.skipRows = Math.max(skipRows, 0);
	}
	
	/**
	 * @param finalRecord The finalRecord to set.
	 */
	public void setNumRecords(int numRecords) {
		this.numRecords = Math.max(numRecords, 0);
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

	@Override
	public synchronized void free() {
		super.free();
    	storeValues();
		reader.close();
	}
	
    /**
     * Stores all values as incremental reading.
     */
    private void storeValues() {
    	if (getPhase() != null && getPhase().getResult() == Result.FINISHED_OK) {
    		try {
    			Object dictValue = getGraph().getDictionary().getValue(Defaults.INCREMENTAL_STORE_KEY);
    			if (dictValue != null && dictValue == Boolean.FALSE) {
    				return;
    			}
				reader.storeIncrementalReading();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
    	}
    }
    
    public void setIncrementalFile(String incrementalFile) {
    	this.incrementalFile = incrementalFile;
    }

    public void setIncrementalKey(String incrementalKey) {
    	this.incrementalKey = incrementalKey;
    }
}
