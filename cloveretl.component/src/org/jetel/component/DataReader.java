/*
 * jETeL/CloverETL - Java based ETL application framework.
 * Copyright (c) Javlin, a.s. (info@cloveretl.com)
 *  
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */
package org.jetel.component;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.security.InvalidParameterException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.Defaults;
import org.jetel.data.IntegerDataField;
import org.jetel.data.parser.TextParser;
import org.jetel.data.parser.TextParserConfiguration;
import org.jetel.data.parser.TextParserFactory;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.ParserExceptionHandlerFactory;
import org.jetel.exception.PolicyType;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPort;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.DataRecordUtils;
import org.jetel.util.ExceptionUtils;
import org.jetel.util.MultiFileReader;
import org.jetel.util.SynchronizeUtils;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.property.PropertyRefResolver;
import org.jetel.util.property.RefResFlag;
import org.jetel.util.string.QuotingDecoder;
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
 *  <tr><td><b>verbose</b></td><td>verbose mode provides more comprehensive error notification; default is true</td>
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

    private final static Log logger = LogFactory.getLog(DataReader.class);

	/**  Description of the Field */
	public final static String COMPONENT_TYPE = "DATA_READER";

	/** XML attribute names */
	private static final String XML_TRIM_ATTRIBUTE = "trim";
	private static final String XML_SKIPLEADINGBLANKS_ATTRIBUTE = "skipLeadingBlanks";
	private static final String XML_SKIPTRAILINGBLANKS_ATTRIBUTE = "skipTrailingBlanks";
	private static final String XML_SKIPFIRSTLINE_ATTRIBUTE = "skipFirstLine";
	private static final String XML_SKIPROWS_ATTRIBUTE = "skipRows";
	private static final String XML_NUMRECORDS_ATTRIBUTE = "numRecords";
	private static final String XML_SKIP_SOURCE_ROWS_ATTRIBUTE = "skipSourceRows";
	private static final String XML_NUM_SOURCE_RECORDS_ATTRIBUTE = "numSourceRecords";
	private static final String XML_MAXERRORCOUNT_ATTRIBUTE = "maxErrorCount";
	private static final String XML_QUOTEDSTRINGS_ATTRIBUTE = "quotedStrings";
	private static final String XML_QUOTECHAR_ATTRIBUTE = "quoteCharacter";
	private static final String XML_TREATMULTIPLEDELIMITERSASONE_ATTRIBUTE = "treatMultipleDelimitersAsOne";
	private final static String XML_FILE_ATTRIBUTE = "fileURL";
	private final static String XML_CHARSET_ATTRIBUTE = "charset";
	private final static String XML_DATAPOLICY_ATTRIBUTE = "dataPolicy";
	private static final String XML_INCREMENTAL_FILE_ATTRIBUTE = "incrementalFile";
	private static final String XML_INCREMENTAL_KEY_ATTRIBUTE = "incrementalKey";
	private static final String XML_PARSER_ATTRIBUTE = "parser";
	private static final String XML_VERBOSE_ATTRIBUTE = "verbose";

	private final static int OUTPUT_PORT = 0;
	private final static int INPUT_PORT = 0;
	private final static int LOG_PORT = 1;
	private String fileURL;
	private boolean skipFirstLine = false;	//backward compatibility
	private int skipRows = -1;
	private int numRecords = -1;
	private int skipSourceRows = -1;
	private int numSourceRecords = -1;
	private int maxErrorCount = -1;
    private String incrementalFile;
    private String incrementalKey;
    private String parserClassName;
	private ClassLoader parserClassLoader;

	protected TextParser parser;
    private MultiFileReader reader;
    private PolicyType policyType = PolicyType.STRICT;

	private String charset;
	private boolean verbose;
	private boolean treatMultipleDelimitersAsOne;
	private boolean quotedStrings;
	private Character quoteChar;
	private Boolean skipLeadingBlanks;
	private Boolean skipTrailingBlanks;
	private Boolean trim;
	private boolean quotedStringsHasDefaultValue = true;
	
	//is the second port attached? - logging is enabled
	boolean logging = false;
	
	/**
	 *Constructor for the DelimitedDataReaderNIO object
	 *
	 * @param  id       Description of the Parameter
	 * @param  fileURL  Description of the Parameter
	 */
	public DataReader(String id, String fileURL) {
		this(id, fileURL, null, true);
	}


	/**
	 *Constructor for the DelimitedDataReaderNIO object
	 *
	 * @param  id       Description of the Parameter
	 * @param  fileURL  Description of the Parameter
	 * @param  charset  Description of the Parameter
	 */
	public DataReader(String id, String fileURL, String charset) {
		this(id, fileURL, charset, true);
	}

	/**
	 * @param id
	 * @param fileURL
	 * @param charset
	 * @param verbose
	 */
	public DataReader(String id, String fileURL, String charset, boolean verbose) {
		super(id);
		this.fileURL = fileURL;
		this.charset = charset;
		this.verbose = verbose;
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.Node#init()
	 */
	@Override
	public void init() throws ComponentNotReadyException {
        if(isInitialized()) return;
        super.init();

		//is the logging port attached?
		if (getOutputPort(LOG_PORT) != null) {
			if (checkLogPortMetadata()) {
				logging = true;
			} else {
				throw new ComponentNotReadyException(this.getName() + "|" + this.getId() + ": The log port metadata has invalid format " + 
						"(expected data fields - integer (record number), integer (field number), string (raw record), string (error message), string (file name - OPTIONAL");
			}
		}
		
        updeteSkipSourceRowsByMetadata();
		prepareParser();
        prepareMultiFileReader();
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.Node#preExecute()
	 */
	@Override
	public void preExecute() throws ComponentNotReadyException {
		super.preExecute();

        try {
            reader.preExecute();
        } catch(ComponentNotReadyException e) {
            e.setAttributeName(XML_FILE_ATTRIBUTE);
            throw e;
        }
	}
	
	@Override
	public Result execute() throws Exception {
		OutputPort outPort = getOutputPort(OUTPUT_PORT);
		// we need to create data record - take the metadata from first output
		// port
		DataRecord record = DataRecordFactory.newRecord(getOutputPort(OUTPUT_PORT).getMetadata());
		record.init();
		// if we have second output port we can logging - create data record for
		// log port
		DataRecord logRecord = null;
		boolean hasFileNameField = false;
		if (logging) {
			logRecord = DataRecordFactory.newRecord(getOutputPort(LOG_PORT).getMetadata());
			logRecord.init();
			hasFileNameField = logRecord.getNumFields() == 5;
		}
		int errorCount = 0;

		try {
			while (runIt) {
				try {
					if ((reader.getNext(record)) == null) {
						break;
					}
					outPort.writeRecord(record);
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
							setCharSequenceToField(bdfe.getRawRecord(), logRecord.getField(2));
							setCharSequenceToField(ExceptionUtils.getMessage(bdfe), logRecord.getField(3));
							if (hasFileNameField) {
								setCharSequenceToField(reader.getSourceName(), logRecord.getField(4));
							}
							writeRecord(LOG_PORT, logRecord);
						} else {
							logger.warn(ExceptionUtils.getMessage("Error in input source: " + reader.getSourceName(), bdfe));
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
	
	private void setCharSequenceToField(CharSequence charSeq, DataField field) {
		if (charSeq == null) {
			field.setNull(true);
		} else {
			field.setNull(false);
			
			if (field.getType() == DataFieldMetadata.STRING_FIELD) {
				field.setValue(charSeq);
			} else if (field.getType() == DataFieldMetadata.BYTE_FIELD || field.getType() == DataFieldMetadata.BYTE_FIELD_COMPRESSED) {
				String cs;
				if (charset != null) {
					cs = charset;
				} else {
					cs = Defaults.DataParser.DEFAULT_CHARSET_DECODER;
				}
				try {
					field.setValue(charSeq.toString().getBytes(cs));
				} catch (UnsupportedEncodingException e) {
					// if parameter charset set, encoding support was checked in checkConfig()
					logger.error(getId() + ": failed to write log record", e);
				}
			} else {
				throw new IllegalArgumentException("DataField type has to be string, byte or cbyte");
			}
		}
	}

	@Override
	public void postExecute() throws ComponentNotReadyException {
		super.postExecute();
		
		reader.postExecute();
	}
	
	@Override
	public void commit() {
		super.commit();
		storeValues();
	}


	private boolean checkLogPortMetadata() {
        DataRecordMetadata logMetadata = getOutputPort(LOG_PORT).getMetadata();

        int numFields = logMetadata.getNumFields();
        boolean ret = (numFields == 4 || numFields == 5)
        	&& logMetadata.getField(0).getType() == DataFieldMetadata.INTEGER_FIELD
        	&& logMetadata.getField(1).getType() == DataFieldMetadata.INTEGER_FIELD
            && isStringOrByte(logMetadata.getField(2))
            && isStringOrByte(logMetadata.getField(3))
            && (numFields != 5 || isStringOrByte(logMetadata.getField(4)));
        
//        if(!ret) {
//            logger.warn(this.getId() + ": The log port metadata has invalid format (expected data fields - integer (record number), integer (field number), string (raw record), string (error message)");
//        }
        
        return ret;
    }
	
	private boolean isStringOrByte(DataFieldMetadata field) {
		return field.getType() == DataFieldMetadata.STRING_FIELD || field.getType() == DataFieldMetadata.BYTE_FIELD || field.getType() == DataFieldMetadata.BYTE_FIELD_COMPRESSED;
	}
	
	private void prepareParser() {
		//create data parser
		final TextParserConfiguration parserCfg = new TextParserConfiguration();
		parserCfg.setMetadata(getOutputPort(OUTPUT_PORT).getMetadata());
		parserCfg.setCharset(charset);
		parserCfg.setVerbose(logging ? true : verbose); //verbose mode is true by default in case the logging port is used
        parserCfg.setTreatMultipleDelimitersAsOne(treatMultipleDelimitersAsOne);
        if (quotedStringsHasDefaultValue) {
			//quoted strings has default value -> set the quoted string field from metadata
        	parserCfg.setQuotedStrings(getOutMetadata().get(0).isQuotedStrings());
        	parserCfg.setQuoteChar(getOutMetadata().get(0).getQuoteChar());
		} else {
			//quoted string is set by the user
			parserCfg.setQuotedStrings(quotedStrings);
			parserCfg.setQuoteChar(quoteChar);
		}
        parserCfg.setSkipLeadingBlanks(skipLeadingBlanks);
        parserCfg.setSkipTrailingBlanks(skipTrailingBlanks);
        parserCfg.setTryToMatchLongerDelimiter(DataRecordUtils.containsPrefixDelimiters(parserCfg.getMetadata()));
        parserCfg.setTrim(trim);
        if( incrementalFile != null || incrementalKey != null || skipFirstLine || skipRows > 0 || skipSourceRows > 0 ) {
        	parserCfg.setSkipRows(true);
        }
        parser = TextParserFactory.getParser(parserCfg, parserClassName, parserClassLoader);
		if( logger.isDebugEnabled()){
			logger.debug("Component " + getId() + " uses parser " + parser.getClass().getName() );
		}
        parser.setExceptionHandler(ParserExceptionHandlerFactory.getHandler(policyType));
	}
	
	private void prepareMultiFileReader() throws ComponentNotReadyException {
		// initialize multifile reader based on prepared parser
		TransformationGraph graph = getGraph();
        reader = new MultiFileReader(parser, graph != null ? graph.getRuntimeContext().getContextURL() : null, fileURL);
        reader.setLogger(logger);
        reader.setSkip(skipRows);
        reader.setNumSourceRecords(numSourceRecords);
        reader.setNumRecords(numRecords);
        reader.setIncrementalFile(incrementalFile);
        reader.setIncrementalKey(incrementalKey);
        reader.setInputPort(getInputPort(INPUT_PORT)); //for port protocol: ReadableChannelIterator reads data
        reader.setCharset(charset);
        reader.setPropertyRefResolver(new PropertyRefResolver(graph.getGraphProperties()));
        reader.setDictionary(graph.getDictionary());
        reader.setSkipSourceRows(skipSourceRows > 0 ? skipSourceRows : (skipFirstLine ? 1 : 0));

        reader.init(getOutputPort(OUTPUT_PORT).getMetadata());
	}


	private void updeteSkipSourceRowsByMetadata() {
		// skip source rows
        if (skipSourceRows == -1) {
        	OutputPort outputPort = getOutputPort(OUTPUT_PORT); //only 1.output port without log port
        	DataRecordMetadata metadata;
        	if (outputPort != null && (metadata = outputPort.getMetadata()) != null) {
            	int ssr = metadata.getSkipSourceRows();
            	if (ssr > 0) {
                    skipSourceRows = ssr;
            	}
        	}
        }
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.Node#getUsedUrls()
	 */
	@Override
	public String[] getUsedUrls() {
		return new String[] { fileURL };
	}
	
	/**
	 *  Description of the Method
	 *
	 * @return    Description of the Returned Value
	 * @since     May 21, 2002
	 */
	@Override
	public void toXML(Element xmlElement) {
	    super.toXML(xmlElement);
		xmlElement.setAttribute(XML_FILE_ATTRIBUTE, this.fileURL);
		// returns either user specified charset, or default value
		String charSet = this.parser.getConfiguration().getCharset();
		if (charSet != null) {
			xmlElement.setAttribute(XML_CHARSET_ATTRIBUTE, charSet);
		}
		xmlElement.setAttribute(XML_DATAPOLICY_ATTRIBUTE, policyType.toString());
		if (parser.getConfiguration().getTrim() != null) {
			xmlElement.setAttribute(XML_TRIM_ATTRIBUTE, String.valueOf(parser.getConfiguration().getTrim()));
		}		
		if (parser.getConfiguration().getSkipLeadingBlanks() != null){
			xmlElement.setAttribute(XML_SKIPLEADINGBLANKS_ATTRIBUTE, String.valueOf(parser.getConfiguration().getSkipLeadingBlanks()));
		}
		if (parser.getConfiguration().getSkipTrailingBlanks() != null){
			xmlElement.setAttribute(XML_SKIPTRAILINGBLANKS_ATTRIBUTE, String.valueOf(parser.getConfiguration().getSkipTrailingBlanks()));
		}
		
	}


	/**
	 *  Description of the Method
	 *
	 * @param  nodeXML  Description of Parameter
	 * @return          Description of the Returned Value
	 * @throws XMLConfigurationException 
	 * @throws AttributeNotFoundException 
	 * @since           May 21, 2002
	 */
	public static Node fromXML(TransformationGraph graph, Element nodeXML) throws XMLConfigurationException, AttributeNotFoundException {
		DataReader aDataReader = null;
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(nodeXML, graph);

		aDataReader = new DataReader(xattribs.getString(Node.XML_ID_ATTRIBUTE),
				xattribs.getStringEx(XML_FILE_ATTRIBUTE,RefResFlag.SPEC_CHARACTERS_OFF),
				xattribs.getString(XML_CHARSET_ATTRIBUTE, null),
				xattribs.getBoolean(XML_VERBOSE_ATTRIBUTE, false));
		aDataReader.setPolicyType(xattribs.getString(XML_DATAPOLICY_ATTRIBUTE, null));
		if (xattribs.exists(XML_SKIPLEADINGBLANKS_ATTRIBUTE)){
			aDataReader.setSkipLeadingBlanks(xattribs.getBoolean(XML_SKIPLEADINGBLANKS_ATTRIBUTE));
		}
		if (xattribs.exists(XML_SKIPTRAILINGBLANKS_ATTRIBUTE)){
			aDataReader.setSkipTrailingBlanks(xattribs.getBoolean(XML_SKIPTRAILINGBLANKS_ATTRIBUTE));
		}
		if (xattribs.exists(XML_TRIM_ATTRIBUTE)){
			aDataReader.setTrim(xattribs.getBoolean(XML_TRIM_ATTRIBUTE));
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
		if (xattribs.exists(XML_SKIP_SOURCE_ROWS_ATTRIBUTE)){
			aDataReader.setSkipSourceRows(xattribs.getInteger(XML_SKIP_SOURCE_ROWS_ATTRIBUTE));
		}
		if (xattribs.exists(XML_NUM_SOURCE_RECORDS_ATTRIBUTE)){
			aDataReader.setNumSourceRecords(xattribs.getInteger(XML_NUM_SOURCE_RECORDS_ATTRIBUTE));
		}
		if (xattribs.exists(XML_MAXERRORCOUNT_ATTRIBUTE)){
			aDataReader.setMaxErrorCount(xattribs.getInteger(XML_MAXERRORCOUNT_ATTRIBUTE));
		}
		if (xattribs.exists(XML_QUOTEDSTRINGS_ATTRIBUTE)){
			aDataReader.setQuotedStrings(xattribs.getBoolean(XML_QUOTEDSTRINGS_ATTRIBUTE));
			aDataReader.quotedStringsHasDefaultValue = false;
		}
		if (xattribs.exists(XML_QUOTECHAR_ATTRIBUTE)) {
			aDataReader.setQuoteChar(QuotingDecoder.quoteCharFromString(xattribs.getString(XML_QUOTECHAR_ATTRIBUTE)));
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
		if (xattribs.exists(XML_PARSER_ATTRIBUTE)){
			aDataReader.setParserClassName(xattribs.getString(XML_PARSER_ATTRIBUTE));
		}

		return aDataReader;
	}
	
	public void setTreatMultipleDelimitersAsOne(boolean treatMultipleDelimitersAsOne) {
		this.treatMultipleDelimitersAsOne = treatMultipleDelimitersAsOne;
	}


	public void setQuotedStrings(boolean quotedStrings) {
		this.quotedStrings = quotedStrings;		
	}

	public void setQuoteChar(Character quoteChar) {
		this.quoteChar = quoteChar;		
	}

	public void setSkipFirstLine(boolean skip) {
		skipFirstLine = skip;
	}
	
	public boolean isSkipFirstLine() {
		return skipFirstLine;
	}

	
	/**
	 * Checks input and output ports
	 * 
	 * @param status
	 * @return <b>true</b> if all ports are configured properly, <b>false</b> in other case
	 */
	protected boolean checkPorts(ConfigurationStatus status) {
		return checkInputPorts(status, 0, 1) && checkOutputPorts(status, 1, 2);
	}
	
	/**
	 *  Description of the Method
	 *
	 * @return    Description of the Return Value
	 */
    @Override
    public ConfigurationStatus checkConfig(ConfigurationStatus status) {
        super.checkConfig(status);
        
        if(!checkPorts(status)) {
        	return status;
        }

        if (charset != null && !Charset.isSupported(charset)) {
        	status.add(new ConfigurationProblem(
            		"Charset "+charset+" not supported!", 
            		ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL));
        }
        
        try {
            updeteSkipSourceRowsByMetadata();
    		prepareParser();
            prepareMultiFileReader();
            
    		if (!getOutputPort(OUTPUT_PORT).getMetadata().hasFieldWithoutAutofilling()) {
    			status.add(new ConfigurationProblem(
                		"No field elements without autofilling for '" + getOutputPort(OUTPUT_PORT).getMetadata().getName() + "' have been found!", 
                		ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL));
    		}
    		reader.checkConfig(getOutputPort(OUTPUT_PORT).getMetadata());
        } catch (ComponentNotReadyException e) {
            ConfigurationProblem problem = new ConfigurationProblem(ExceptionUtils.getMessage(e), ConfigurationStatus.Severity.WARNING, this, ConfigurationStatus.Priority.NORMAL);
            if(!StringUtils.isEmpty(e.getAttributeName())) {
                problem.setAttributeName(e.getAttributeName());
            }
            status.add(problem);
        } finally {
        	free();
        }
        
        return status;
    }
	
	@Override
	public String getType(){
		return COMPONENT_TYPE;
	}
	
	public void setFileURL(String fileURL){
		this.fileURL = fileURL;
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
	 * @param how many rows to skip for every source
	 */
	public void setSkipSourceRows(int skipSourceRows) {
		this.skipSourceRows = Math.max(skipSourceRows, 0);
	}
	
	/**
	 * @param how many rows to process for every source
	 */
	public void setNumSourceRecords(int numSourceRecords) {
		this.numSourceRecords = Math.max(numSourceRecords, 0);
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
    }

	@Override
	public synchronized void free() {
		super.free();
		try {
	    	if (reader != null) {
	    		reader.close();
	    	}
		} catch (Exception e){
			logger.error(e);
		}
	}
	
    /**
     * Stores all values as incremental reading.
     */
	private void storeValues() {
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
    
    public void setIncrementalFile(String incrementalFile) {
    	this.incrementalFile = incrementalFile;
    }

    public void setIncrementalKey(String incrementalKey) {
    	this.incrementalKey = incrementalKey;
    }

    public void setSkipLeadingBlanks(Boolean skipLeadingBlanks) {
		this.skipLeadingBlanks = skipLeadingBlanks;
	}

	public void setSkipTrailingBlanks(Boolean skipTrailingBlanks) {
		this.skipTrailingBlanks = skipTrailingBlanks;
	}

	public void setTrim(Boolean trim) {
		this.trim = trim;
	}

	public String getParserClassName() {
		return parserClassName;
	}

	public void setParserClassName(String parserClassName) {
		this.parserClassName = parserClassName;
	}

	public void setParserClass(String parserClassName, ClassLoader parserClassLoader){
		this.parserClassName = parserClassName;
		this.parserClassLoader = parserClassLoader;
	}
}
