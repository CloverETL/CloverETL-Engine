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
import java.nio.charset.Charset;
import java.security.InvalidParameterException;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.Defaults;
import org.jetel.data.formatter.XLSFormatter;
import org.jetel.data.formatter.XLSFormatter.XLSType;
import org.jetel.data.parser.JExcelXLSDataParser;
import org.jetel.data.parser.XLSParser;
import org.jetel.data.parser.XLSXDataParser;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.ConfigurationStatus.Priority;
import org.jetel.exception.ConfigurationStatus.Severity;
import org.jetel.exception.ParserExceptionHandlerFactory;
import org.jetel.exception.PolicyType;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPort;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.ExceptionUtils;
import org.jetel.util.MultiFileReader;
import org.jetel.util.NumberIterator;
import org.jetel.util.SynchronizeUtils;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.property.RefResFlag;
import org.jetel.util.string.StringUtils;
import org.w3c.dom.Element;

/**
 * <h3>XLS Reader Component</h3>
 *
 * Parses data records from a XLS(X) file and sends them to specified output ports.
 *
 * <table border="1">
 *  <th>Component:</th>
 * <tr><td><h4><i>Name:</i></h4></td>
 * <td>XLSReader</td></tr>
 * <tr><td><h4><i>Category:</i></h4></td>
 * <td>Readers</td></tr>
 * <tr><td><h4><i>Description:</i></h4></td>
 * <td>Parses data from xls file and send the records to output ports.<br> JExcel can handle 
 * with files up to ~8.1MB in xls file - ~4.9MB in flat file. For more data it is requested more memory.</td></tr>
 * <tr><td><h4><i>Inputs:</i></h4></td>
 * <td></td></tr>
 * <tr><td><h4><i>Outputs:</i></h4></td>
 * <td>At least one output port defined/connected.</tr>
 * <tr><td><h4><i>Comment:</i></h4></td></tr>
 * </table>
 *  <br>
 *  <table border="1">
 *  <th>XML attributes:</th>
 *  <tr><td><b>type</b></td><td>"XLS_READER"</td></tr>
 *  <tr><td><b>id</b></td><td>component identification</td>
 *  <tr><td><b>parser</b></td><td>The type of a XLS(X) parser. Possible values: 'auto' (default) for automatic selection
 *   of a parser based on a file extension, 'XLS' for a classic XLS parser, 'XLSX' for a XLSX parser.</td>
 *  <tr><td><b>fileURL</b></td><td>path to the input file</td>
 *  <tr><td><b>dataPolicy</b></td><td>specifies how to handle misformatted or
 *   incorrect data.  'Strict' (default value) aborts processing, 'Controlled'
 *   logs the entire record while processing continues, and 'Lenient' attempts
 *   to set incorrect data to default values while processing continues.</td>
 *  <tr><td><b>startRow</b></td><td>index of first parsed record</td>
 *  <tr><td><b>finalRow</b></td><td>index of final parsed record</td>
 *  <tr><td><b>maxErrorCount</b></td><td>count of tolerated error records in input file</td>
 *  <tr><td><b>sheetName</b></td><td>name of sheet for reading data. Can be used with wild cards as '?' and '*'</td>
 *  <tr><td><b>sheetNumber</b></td><td>number of sheet for reading data (starting from 0). Can be set as mask: 
 * <ul><li>*</li>
 * <li>number</li>
 * <li>minNumber-maxNumber</li>
 * <li>*-maxNumber</li>
 * <li>minNumber-*</li></ul>
 * or as their combination separated by comma, eg. 1,3,5-7,9-*<br>
 *  This attribute has higher priority then sheetName. If no one of these attributes is set, data are read from first (0) sheet.</td>
 *  <tr><td><b>metadataRow</b></td><td>number of row where are names of columns</td>
 *  <tr><td><b>fieldMap</b></td><td>Pairs of clover fields and xls columns
 *   (cloverField=xlsColumn) separated by :;| {colon, semicolon, pipe}.
 *  Can be used for mapping clover fields and xls fields or for defining order 
 *  of reading columnspoi from xls sheet. Xls columns can be written as names given 
 *  in row specified by metadataRow attribute or as column's codes preceded 
 *  by $. Xls fields may be missing, then columns are read in order they are 
 *  in xls sheet and are given to proper metadata fields</td>
 *  </tr>
 *  <tr><td><b>charset</b></td><td>character encoding of the input file, default encoding is set to  ISO-8859-1 </td>
 *  </table>
 *
 *  <h4>Example:</h4>
 *  <pre>&lt;Node fieldMap="ORDER=ORDERID,N,20,5;CUSTOMERID=CUSTOMERID,C,5;
 *  EMPLOYEEID=EMPLOYEEID,N,20,5;ORDERDATE=ORDERDATE,D;REQUIREDDA=REQUIREDDA,
 *  D;SHIPCOUNTR=SHIPCOUNTR,C,15" fileURL="ORDERS.xls" id="XLS_READER1" metadataRow="1" 
 *  startRow="2" type="XLS_READER" /&gt;
 * 
 *  <pre>&lt;Node fieldMap="ORDER=$a;CUSTOMERID=$b;EMPLOYEEID=$c;ORDERDATE=$d;
 *  REQUIREDDA=$d;SHIPPEDDAT=$f;SHIPVIA=$g;FREIGHT=$h;SHIPNAME=$i;SHIPADDRES=$j;
 *  SHIPCITY=$k;SHIPREGION=$l;SHIPPOSTAL=$n;SHIPCOUNTR=$m" fileURL="ORDERS.xls"
 *  id="XLS_READER1" metadataRow="1" type="XLS_READER" /&gt;
 *  
 *  <pre>&lt;Node fieldMap="ORDER;CUSTOMERID=;EMPLOYEEID;ORDERDATE;SHIPCOUNTR" 
 *  fileURL="*.xls" sheetNumber="*" id="XLS_READER1" type="XLS_READER" /&gt;
 *
 * <pre>&lt;Node dataPolicy="strict" fileURL="example.xls" id="XLS_READER0" metadataRow="1" 
 * startRow="2" sheetName="Sheet?" type="XLS_READER"/&gt;
 * 
 * @author Agata Vackova, Javlin a.s. &lt;agata.vackova@javlin.eu&gt;
 * @author Martin Janik, Javlin a.s. &lt;martin.janik@javlin.eu&gt;
 *
 * @version 4th February 2009
 * @since 10th October 2006
 */
public class XLSReader extends Node {

    protected static final Log logger = LogFactory.getLog(XLSReader.class);

    public static final String COMPONENT_TYPE = AdditionalComponentAttributes.XLS_READER.getComponentType();

    public static final String XML_PARSER_ATTRIBUTE = "parser";
    public static final String XML_STARTROW_ATTRIBUTE = "startRow";
    public static final String XML_FINALROW_ATTRIBUTE = "finalRow";
	private static final String XML_SKIPROWS_ATTRIBUTE = "skipRows";
    public static final String XML_NUMRECORDS_ATTRIBUTE = "numRecords";
    public static final String XML_MAXERRORCOUNT_ATTRIBUTE = "maxErrorCount";
    public static final String XML_FILE_ATTRIBUTE = "fileURL";
    public static final String XML_DATAPOLICY_ATTRIBUTE = "dataPolicy";
    public static final String XML_SHEETNAME_ATTRIBUTE = "sheetName";
    public static final String XML_SHEETNUMBER_ATTRIBUTE = "sheetNumber";
    public static final String XML_METADATAROW_ATTRIBUTE = "metadataRow";
    public static final String XML_FIELDMAP_ATTRIBUTE = "fieldMap";
    public static final String XML_CHARSET_ATTRIBUTE = "charset";
    public static final String XML_INCREMENTAL_FILE_ATTRIBUTE = "incrementalFile";
    public static final String XML_INCREMENTAL_KEY_ATTRIBUTE = "incrementalKey";
	private static final String XML_SKIP_SOURCE_ROWS_ATTRIBUTE = "skipSourceRows";
	private static final String XML_NUM_SOURCE_RECORDS_ATTRIBUTE = "numSourceRecords";
	private static final String XML_SKIP_SHEET_ROWS_ATTRIBUTE = "skipSheetRows";
	private static final String XML_NUM_SHEET_RECORDS_ATTRIBUTE = "numSheetRecords";

    public static final String XLS_CELL_CODE_INDICATOR = "#";

    private static final int INPUT_PORT = 0;
    private static final int OUTPUT_PORT = 0;
    private static final int CLOVER_FIELDS = 0;
    private static final int XLS_FIELDS = 1;

    public static Node fromXML(TransformationGraph graph, Element nodeXML) throws XMLConfigurationException, AttributeNotFoundException {
        XLSReader aXLSReader = null;
        ComponentXMLAttributes xattribs = new ComponentXMLAttributes(nodeXML, graph);

        String[] fMap = null;
        String[][] fieldMap = null;

        if (xattribs.exists(XML_FIELDMAP_ATTRIBUTE)) {
            fMap = StringUtils.split(xattribs.getString(XML_FIELDMAP_ATTRIBUTE));
            fieldMap = new String[fMap.length][2];

            for (int i = 0; i < fieldMap.length; i++) {
                fieldMap[i] = fMap[i].split(Defaults.ASSIGN_SIGN + "|=");
            }
        }

        if (xattribs.exists(XML_CHARSET_ATTRIBUTE)) {
            aXLSReader = new XLSReader(xattribs.getString(Node.XML_ID_ATTRIBUTE),
                    xattribs.getStringEx(XML_FILE_ATTRIBUTE, RefResFlag.URL), fieldMap, xattribs.getString(XML_CHARSET_ATTRIBUTE));

        } else {
            aXLSReader = new XLSReader(xattribs.getString(Node.XML_ID_ATTRIBUTE),
                    xattribs.getStringEx(XML_FILE_ATTRIBUTE, RefResFlag.URL), fieldMap);
        }

        aXLSReader.setParserType(XLSType.valueOfIgnoreCase(xattribs.getString(XML_PARSER_ATTRIBUTE, null)));
        aXLSReader.setPolicyType(xattribs.getString(XML_DATAPOLICY_ATTRIBUTE, null));

        if (xattribs.exists(XML_SKIPROWS_ATTRIBUTE)) {
            aXLSReader.setSkipRows(xattribs.getInteger(XML_SKIPROWS_ATTRIBUTE));
        }
        if (xattribs.exists(XML_NUMRECORDS_ATTRIBUTE)) {
            aXLSReader.setNumRecords(xattribs.getInteger(XML_NUMRECORDS_ATTRIBUTE));
        }

        if (xattribs.exists(XML_MAXERRORCOUNT_ATTRIBUTE)) {
            aXLSReader.setMaxErrorCount(xattribs.getInteger(XML_MAXERRORCOUNT_ATTRIBUTE));
        }

        if (xattribs.exists(XML_SHEETNAME_ATTRIBUTE)) {
            aXLSReader.setSheetName(xattribs.getString(XML_SHEETNAME_ATTRIBUTE));
        } else if (xattribs.exists(XML_SHEETNUMBER_ATTRIBUTE)) {
            aXLSReader.setSheetNumber(xattribs.getString(XML_SHEETNUMBER_ATTRIBUTE));
        } 

        if (xattribs.exists(XML_INCREMENTAL_FILE_ATTRIBUTE)) {
            aXLSReader.setIncrementalFile(xattribs.getStringEx(XML_INCREMENTAL_FILE_ATTRIBUTE, RefResFlag.URL));
        }

        if (xattribs.exists(XML_INCREMENTAL_KEY_ATTRIBUTE)) {
            aXLSReader.setIncrementalKey(xattribs.getString(XML_INCREMENTAL_KEY_ATTRIBUTE));
        }
		if (xattribs.exists(XML_SKIP_SOURCE_ROWS_ATTRIBUTE)){
			aXLSReader.setSkipSourceRows(xattribs.getInteger(XML_SKIP_SOURCE_ROWS_ATTRIBUTE));
		}
		if (xattribs.exists(XML_NUM_SOURCE_RECORDS_ATTRIBUTE)){
			aXLSReader.setNumSourceRecords(xattribs.getInteger(XML_NUM_SOURCE_RECORDS_ATTRIBUTE));
		}
		aXLSReader.setSheetRange(
				xattribs.getInteger(XML_METADATAROW_ATTRIBUTE, -1),
				xattribs.getInteger(XML_STARTROW_ATTRIBUTE, -1),
				xattribs.getInteger(XML_FINALROW_ATTRIBUTE, -1),
				xattribs.getInteger(XML_SKIP_SHEET_ROWS_ATTRIBUTE, -1),
				xattribs.getInteger(XML_NUM_SHEET_RECORDS_ATTRIBUTE, -1)
				);

        return aXLSReader;
    }
    
    private XLSType parserType = XLSType.AUTO;
    private String fileURL;
    private int maxErrorCount = -1;
    private String incrementalFile;
    private String incrementalKey;
	private int skipSourceRows = -1;
	private int numRecords = -1;
	private int numSourceRecords = -1;
	private int skip = -1;
	private int skipSheetRows = -1;
	private int numSheetRecords = -1;

    private XLSParser parser;
    private MultiFileReader reader;
    private PolicyType policyType = PolicyType.STRICT;
    
    private String sheetName = null;
    private String sheetNumber = null;
    private int metadataRow = 0;
    private String[][] fieldMap;
    private String charset;
    
    /**
     * @param id
     */
    public XLSReader(String id, String fileURL, String[][] fieldMap) {
        super(id);
        this.fileURL = fileURL;
        this.fieldMap = fieldMap;
    }

    public XLSReader(String id, String fileURL, String[][] fieldMap, String charset) {
        super(id);
        this.fileURL = fileURL;
        this.fieldMap = fieldMap;
        this.charset = charset;
    }

    @Override
    public String getType() {
        return COMPONENT_TYPE;
    }

    public void setParserType(XLSType parserType) {
        this.parserType = parserType;
    }

    public void setPolicyType(String strPolicyType) {
        setPolicyType(PolicyType.valueOfIgnoreCase(strPolicyType));
    }

    public void setPolicyType(PolicyType policyType) {
        this.policyType = policyType;
    }

    /**
     * This function is used to set several mutually conflicting component attributes
     * used for sheet range specification,
     * setting them all in one place helps to avoid confusion caused by their interdependency
     */
    public void setSheetRange(int metadataRow, int startRow, int finalRow,
    		int skipSheetRows, int numSheetRecords) {
    	// first set defaults
    	this.metadataRow = metadataRow == -1 ? 0 : metadataRow;

    	if (startRow == -1) {
    		if (skipSheetRows == -1) {
    			this.skipSheetRows = this.metadataRow;
    		} else {
    			this.skipSheetRows = skipSheetRows; // 0th row (cell-names) is skipped unless startRow is defined
    		}
    	} else {
    		if (skipSheetRows == -1) {
    			this.skipSheetRows = startRow;
    		} else {
    			this.skipSheetRows = startRow + skipSheetRows;
    		}
    	}
    	
    	if (finalRow == -1) {
    		if (numSheetRecords == -1) {
    			this.numSheetRecords = -1;
    		} else {
    			this.numSheetRecords = numSheetRecords;
    		}
    	} else {
    		if (numSheetRecords == -1) {
    			this.numSheetRecords = finalRow - this.skipSheetRows;
    		} else {
    			this.numSheetRecords = Math.min(numSheetRecords, finalRow - this.skipSheetRows);
    		}
    	}
    }
    
	/**
	 * @param startRecord The startRecord to set.
	 */
	public void setSkipRows(int skipRows) {
		this.skip = skipRows; 
	}
	
	/**
	 * @param finalRecord The finalRecord to set.
	 */
	public void setNumRecords(int numRecords) {
		this.numRecords = numRecords;
	}

    /**
     * @param finalRow The finalRow to set.
     */
    public void setMaxErrorCount(int maxErrorCount) {
        if (maxErrorCount < 0) {
            throw new InvalidParameterException("Invalid maxErrorCount parameter.");
        }

        this.maxErrorCount = maxErrorCount;
    }

    public void setSheetName(String sheetName) {
        this.sheetName = sheetName;
    }

    public void setSheetNumber(String sheetNumber) {
        this.sheetNumber = sheetNumber;
    }

    public void setIncrementalFile(String incrementalFile) {
        this.incrementalFile = incrementalFile;
    }

    public void setIncrementalKey(String incrementalKey) {
        this.incrementalKey = incrementalKey;
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
	
    @Override
    public ConfigurationStatus checkConfig(ConfigurationStatus status) {
        super.checkConfig(status);

        if (!checkInputPorts(status, 0, 1) || !checkOutputPorts(status, 1, Integer.MAX_VALUE)) {
            return status;
        }
        
        if (charset != null && !Charset.isSupported(charset)) {
        	status.add(new ConfigurationProblem(
            		"Charset "+charset+" not supported!", 
            		ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL));
        }

        checkMetadata(status, getOutMetadata());

        try {
            if (sheetNumber != null) {
                Iterator<Integer> number = new NumberIterator(sheetNumber, 0, Integer.MAX_VALUE);
                if (!number.hasNext()) {
                    status.add(new ConfigurationProblem("There is no sheet with requested number.",
                            ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL));
                }
            } else if (sheetName == null) {
                sheetNumber = "0";
            }

            instantiateParser();

            parser.setExceptionHandler(ParserExceptionHandlerFactory.getHandler(policyType));

            try {
                parser.setMetadataRow(metadataRow - 1);
            } catch (ComponentNotReadyException e) {
                status.add(new ConfigurationProblem("Invalid metadaRow parameter.",
                        ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL));
            }

            if (sheetName != null) {
                parser.setSheetName(sheetName);
            } else {
                parser.setSheetNumber(sheetNumber);
            }

            reader = new MultiFileReader(parser, getGraph() != null ? getGraph().getRuntimeContext().getContextURL() : null, fileURL);
            try {
				reader.close();
			} catch (IOException e) {
				status.add(new ConfigurationProblem("Data source issue - cannot be closed.", Severity.ERROR, this, Priority.NORMAL));
			}
        } catch (IllegalArgumentException e) {
            ConfigurationProblem problem = new ConfigurationProblem(ExceptionUtils.getMessage(e), ConfigurationStatus.Severity.ERROR,
                    this, ConfigurationStatus.Priority.NORMAL);
            problem.setAttributeName(XML_SHEETNUMBER_ATTRIBUTE);
            status.add(problem);
        }

        return status;
    }

    @Override
    public void init() throws ComponentNotReadyException {
        if (isInitialized()) {
            return;
        }

        super.init();

        instantiateParser();

        parser.setExceptionHandler(ParserExceptionHandlerFactory.getHandler(policyType));

        parser.setMetadataRow(metadataRow - 1);

        if (sheetName != null) {
            parser.setSheetName(sheetName);
        } else {
            parser.setSheetNumber((sheetNumber != null) ? sheetNumber : "0");
        }

        //set proper mapping type between clover and xls fields
        if (fieldMap != null){
            String[] cloverFields = new String[fieldMap.length];
            String[] xlsFields = new String[fieldMap.length];
            for (int i=0;i<fieldMap.length;i++){
                cloverFields[i] = fieldMap[i][CLOVER_FIELDS];
                if (cloverFields[i].startsWith(Defaults.CLOVER_FIELD_INDICATOR)) {
                    cloverFields[i] = cloverFields[i].substring(Defaults.CLOVER_FIELD_INDICATOR.length());
                }
                if (fieldMap[i].length > 1) {
                    xlsFields[i] = fieldMap[i][XLS_FIELDS];
                }else {
                    xlsFields[i] = null;
                }
            }
            parser.setCloverFields(cloverFields);
            if (xlsFields[0] != null){
                if (xlsFields[0].startsWith("$") || xlsFields[0].startsWith(XLS_CELL_CODE_INDICATOR)){
                    for (int i=0;i<xlsFields.length;i++){
                        xlsFields[i] = xlsFields[i].substring(1);
                    }
                    parser.setMappingType(XLSParser.CLOVER_FIELDS_AND_XLS_NUMBERS);
                    parser.setXlsFields(xlsFields);
                }else{
                    parser.setMappingType(XLSParser.CLOVER_FIELDS_AND_XLS_NAMES);
                    parser.setXlsFields(xlsFields);
                }
            }else {
                parser.setMappingType(XLSParser.ONLY_CLOVER_FIELDS);
            }
        }else if (metadataRow != 0){
            parser.setMappingType(XLSParser.MAP_NAMES);
        }else{
            parser.setMappingType(XLSParser.NO_METADATA_INFO);
        }
        parser.useIncrementalReading(incrementalFile != null && incrementalKey != null);

        TransformationGraph graph = getGraph();
        reader = new MultiFileReader(parser, graph != null ? graph.getRuntimeContext().getContextURL() : null, fileURL);
        reader.setLogger(logger);
        reader.setIncrementalFile(incrementalFile);
        reader.setIncrementalKey(incrementalKey);
        reader.setInputPort(getInputPort(INPUT_PORT)); //for port protocol: ReadableChannelIterator reads data
        reader.setCharset(charset);
        reader.setPropertyRefResolver(getPropertyRefResolver());
        reader.setDictionary(graph.getDictionary());
        reader.setNumRecords(numRecords);
        reader.setNumSourceRecords(numSourceRecords);
        reader.setL3NumRecords(numSheetRecords);
        reader.setSkip(skip);
        reader.setSkipSourceRows(skipSourceRows);
        reader.setL3Skip(skipSheetRows);
        
        // skip source rows
        if (skipSourceRows == -1) {
        	OutputPort outputPort = getOutputPort(OUTPUT_PORT);
        	DataRecordMetadata metadata;
        	if (outputPort != null && (metadata = outputPort.getMetadata()) != null) {
            	int ssr = metadata.getSkipSourceRows();
            	if (ssr > 0) {
                    skipSourceRows = ssr;
            	}
        	}
        }
        reader.setSkipSourceRows(skipSourceRows > 0 ? skipSourceRows : 0);
        reader.init(getOutputPort(OUTPUT_PORT).getMetadata());
    }

    private void instantiateParser() {
        if ((parserType == XLSType.AUTO && fileURL.matches(XLSFormatter.XLSX_FILE_PATTERN)) || parserType == XLSType.XLSX) {
            parser = new XLSXDataParser(getOutputPort(OUTPUT_PORT).getMetadata());
        } else {
            parser = (charset != null) ? new JExcelXLSDataParser(getOutputPort(OUTPUT_PORT).getMetadata(),charset) : new JExcelXLSDataParser(getOutputPort(OUTPUT_PORT).getMetadata());
        }
    }

    @Override
    public void preExecute() throws ComponentNotReadyException {
    	super.preExecute();
    	reader.preExecute();
    }    
    
    @Override
    public Result execute() throws Exception {
        DataRecord record = DataRecordFactory.newRecord(getOutputPort(OUTPUT_PORT).getMetadata());
        record.init();

        int errorCount = 0;

        while ((record != null) && runIt) {
            try {
                record = reader.getNext(record);
                if (record != null) {
                    writeRecordBroadcast(record);
                }
            } catch (BadDataFormatException bdfe) {
                if (policyType == PolicyType.STRICT) {
                    broadcastEOF();
                    throw bdfe;
                } else {
                    logger.info(ExceptionUtils.getMessage(bdfe));
                    if (maxErrorCount != -1 && ++errorCount > maxErrorCount) {
                        logger.error("DataParser (" + getName() + "): Max error count exceeded.");
                        broadcastEOF();
                        throw bdfe;
                    }
                }
            }
            SynchronizeUtils.cloverYield();
        }

        broadcastEOF();
        return (runIt ? Result.FINISHED_OK : Result.ABORTED);
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

    @Override
    public void free() {
        if (!isInitialized()) {
            return;
        }

        super.free();        

        try {
			reader.close();
		} catch (IOException e) {
			logger.error(e);
		}
    }
    
}
