
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

import java.security.InvalidParameterException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.parser.JExcelXLSDataParser;
import org.jetel.data.parser.XLSDataParser;
import org.jetel.data.parser.XLSParser;
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
import org.jetel.util.ComponentXMLAttributes;
import org.jetel.util.MultiFileReader;
import org.jetel.util.StringUtils;
import org.jetel.util.SynchronizeUtils;
import org.w3c.dom.Element;

/**
 *  <h3>XLS Reader Component</h3>
 *
 * <!-- Parses data from xls file and send the records to output ports. -->
 *
 * <table border="1">
 *  <th>Component:</th>
 * <tr><td><h4><i>Name:</i></h4></td>
 * <td>XLSReader</td></tr>
 * <tr><td><h4><i>Category:</i></h4></td>
 * <td>Readers</td></tr>
 * <tr><td><h4><i>Description:</i></h4></td>
 * <td>Parses data from xls file and send the records to output ports.<br>Because 
 * POI currently uses a lot of memory for large sheets, it is impossible to read 
 * large data (over ~4.3MB in xls file - 2.1MB in flat file). JExcel can handle 
 * with bigger files (up to ~8.1MB in xls file - ~4.9MB in flat file)</td></tr>
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
 *  <tr><td><b>fileURL</b></td><td>path to the input file</td>
 *  <tr><td><b>dataPolicy</b></td><td>specifies how to handle misformatted or
 *   incorrect data.  'Strict' (default value) aborts processing, 'Controlled'
 *   logs the entire record while processing continues, and 'Lenient' attempts
 *   to set incorrect data to default values while processing continues.</td>
 *  <tr><td><b>startRow</b></td><td>index of first parsed record</td>
 *  <tr><td><b>finalRow</b></td><td>index of final parsed record</td>
 *  <tr><td><b>maxErrorCount</b></td><td>count of tolerated error records in input file</td>
 *  <tr><td><b>sheetName</b></td><td>name of sheet for reading data. </td>
 *  <tr><td><b>sheetNumber</b></td><td>number of sheet for reading data (starting from 0). 
 *  This attribute has higher priority then sheetName. One of theese atributes has to be set.</td>
 *  <tr><td><b>metadataRow</b></td><td>number of row where are names of columns</td>
 *  <tr><td><b>fieldMap</b></td><td>Pairs of clover fields and xls columns
 *   (cloverField=xlsColumn) separated by :;| {colon, semicolon, pipe}.
 *  Can be used for mapping clover fields and xls fields or for defining order 
 *  of reading columns from xls sheet. Xls columns can be written as names given 
 *  in row specified by metadataRow attribute or as column's codes preceded 
 *  by $. Xls fields may be missing, then columns are read in order they are 
 *  in xls sheet and are given to proper metadata fields</td>
 *  </tr>
 *  <tr><td><b>charset</b></td><td>character encoding of the input file. 
 *  Don't set it, if XSLReader uses POI library (it recognizes encoding automatically).
 *  When XLSReader uses JExcelAPI, default encoding is set to  ISO-8859-1 </td>
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
 *  fileURL="ORDERS.xls" id="XLS_READER1"type="XLS_READER" /&gt;
 *
 * <pre>&lt;Node dataPolicy="strict" fileURL="example.xls" id="XLS_READER0" metadataRow="1" 
 * startRow="2" type="XLS_READER"/&gt;
 * 
/**
* @author avackova <agata.vackova@javlinconsulting.cz> ; 
* (c) JavlinConsulting s.r.o.
*	www.javlinconsulting.cz
*	@created October 10, 2006
 */
public class XLSReader extends Node {

	public final static String COMPONENT_TYPE = "XLS_READER";
    static Log logger = LogFactory.getLog(XLSReader.class);

	/** XML attribute names */
	public static final String XML_STARTROW_ATTRIBUTE = "startRow";
	public static final String XML_FINALROW_ATTRIBUTE = "finalRow";
	public static final String XML_NUMRECORDS_ATTRIBUTE = "numRecords";
	public static final String XML_MAXERRORCOUNT_ATTRIBUTE = "maxErrorCount";
	public final static String XML_FILE_ATTRIBUTE = "fileURL";
	public final static String XML_DATAPOLICY_ATTRIBUTE = "dataPolicy";
	public final static String XML_SHEETNAME_ATTRIBUTE = "sheetName";
	public final static String XML_SHEETNUMBER_ATTRIBUTE = "sheetNumber";
	public final static String XML_METADATAROW_ATTRIBUTE = "metadataRow";
	public final static String XML_FIELDMAP_ATTRIBUTE = "fieldMap";
	public final static String XML_CHARSET_ATTRIBUTE = "charset";
	
	private final static String ASSIGMENT_STRING = "=";
	private final static int OUTPUT_PORT = 0;
	private final static int CLOVER_FIELDS = 0;
	private final static int XLS_FIELDS = 1;

	private String fileURL;
	private int startRow = 0;
	private int finalRow = -1;
	private int numRecords = -1;
	private int maxErrorCount = -1;
    
	private XLSParser parser;
	private MultiFileReader reader;
	private PolicyType policyType = PolicyType.STRICT;
	
	private String sheetName = null;
	private String sheetNumber = null;
	private int metadataRow = 0;
	private String[][] fieldMap;
	
	public final static boolean usePOI = true;

	/**
	 * @param id
	 */
	public XLSReader(String id, String fileURL, String[][] fieldMap) {
		super(id);
		this.fileURL = fileURL;
		this.fieldMap = fieldMap;
		if (usePOI) {
			this.parser = new XLSDataParser();
		}else{
			this.parser = new JExcelXLSDataParser();
		}
	}

	public XLSReader(String id, String fileURL, String[][] fieldMap, String charset) {
		super(id);
		this.fileURL = fileURL;
		this.fieldMap = fieldMap;
		if (usePOI) {
			this.parser = new XLSDataParser();
		}else{
			this.parser = new JExcelXLSDataParser(charset);
		}
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.Node#getType()
	 */
	@Override
	public String getType() {
		return COMPONENT_TYPE;
	}

	@Override
	public Result execute() throws Exception {
		DataRecord record = new DataRecord(getOutputPort(OUTPUT_PORT).getMetadata());
		record.init();
		int errorCount = 0;
		try {
			while (((record) != null) && runIt) {
				try {
					record = reader.getNext(record);
					if (record!=null){
						writeRecordBroadcast(record);
					}
				}catch(BadDataFormatException bdfe){
			        if(policyType == PolicyType.STRICT) {
			            throw bdfe;
			        } else {
			            logger.info(bdfe.getMessage());
			            if(maxErrorCount != -1 && ++errorCount > maxErrorCount) {
			                logger.error("DataParser (" + getName() + "): Max error count exceeded.");
			                break;
			            }
			        }
				}
				SynchronizeUtils.cloverYield();
			}
		} catch (Exception e) {
			throw e;
		}finally{
			parser.close();
			broadcastEOF();
		}
        return runIt ? Result.FINISHED_OK : Result.ABORTED;
	}

	@Override
	public void free() {
		super.free();
		parser.close();
	}
	/*
	 * (non-Javadoc)
	 * 
	 * @see org.jetel.graph.GraphElement#checkConfig()
	 */
    @Override
    public ConfigurationStatus checkConfig(ConfigurationStatus status) {
        super.checkConfig(status);
        
        checkInputPorts(status, 0, 0);
        checkOutputPorts(status, 1, Integer.MAX_VALUE);

    	//TODO
//        try {
//            init();
//            free();
//        } catch (ComponentNotReadyException e) {
//            ConfigurationProblem problem = new ConfigurationProblem(e.getMessage(), ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL);
//            if(!StringUtils.isEmpty(e.getAttributeName())) {
//                problem.setAttributeName(e.getAttributeName());
//            }
//            status.add(problem);
//        }
        
        return status;
    }

	public static Node fromXML(TransformationGraph graph, Element nodeXML) throws XMLConfigurationException {
		XLSReader aXLSReader = null;
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(nodeXML, graph);

		try {
			String[] fMap =null;
			String[][] fieldMap = null;
			if (xattribs.exists(XML_FIELDMAP_ATTRIBUTE)){
				fMap = xattribs.getString(XML_FIELDMAP_ATTRIBUTE).split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);
				fieldMap = new String[fMap.length][2];
				for (int i=0;i<fieldMap.length;i++){
					fieldMap[i] = fMap[i].split(ASSIGMENT_STRING);
				}
			}
			if (xattribs.exists(XML_CHARSET_ATTRIBUTE)){
				aXLSReader = new XLSReader(xattribs.getString(Node.XML_ID_ATTRIBUTE),
						xattribs.getString(XML_FILE_ATTRIBUTE),fieldMap, 
						xattribs.getString(XML_CHARSET_ATTRIBUTE));
				
			}else{
				aXLSReader = new XLSReader(xattribs.getString(Node.XML_ID_ATTRIBUTE),
							xattribs.getString(XML_FILE_ATTRIBUTE),fieldMap);
			}
			aXLSReader.setPolicyType(xattribs.getString(XML_DATAPOLICY_ATTRIBUTE, null));
			aXLSReader.setStartRow(xattribs.getInteger(XML_STARTROW_ATTRIBUTE,1));
			if (xattribs.exists(XML_FINALROW_ATTRIBUTE)){
				aXLSReader.setFinalRow(xattribs.getInteger(XML_FINALROW_ATTRIBUTE));
			}
			if (xattribs.exists(XML_NUMRECORDS_ATTRIBUTE)){
				aXLSReader.setNumRecords(xattribs.getInteger(XML_NUMRECORDS_ATTRIBUTE));
			}
			if (xattribs.exists(XML_MAXERRORCOUNT_ATTRIBUTE)){
				aXLSReader.setMaxErrorCount(xattribs.getInteger(XML_MAXERRORCOUNT_ATTRIBUTE));
			}
			if (xattribs.exists(XML_SHEETNAME_ATTRIBUTE)){
				aXLSReader.setSheetName(xattribs.getString(XML_SHEETNAME_ATTRIBUTE));
			}else if (xattribs.exists(XML_SHEETNUMBER_ATTRIBUTE)){
				aXLSReader.setSheetNumber(xattribs.getString(XML_SHEETNUMBER_ATTRIBUTE));
			}
			if (xattribs.exists(XML_METADATAROW_ATTRIBUTE)){
				aXLSReader.setMetadataRow(xattribs.getInteger(XML_METADATAROW_ATTRIBUTE));
			}
		} catch (Exception ex) {
		    throw new XMLConfigurationException(COMPONENT_TYPE + ":" + xattribs.getString(XML_ID_ATTRIBUTE," unknown ID ") + ":" + ex.getMessage(),ex);
		}

		return aXLSReader;
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
		xmlElement.setAttribute(XML_DATAPOLICY_ATTRIBUTE, policyType.toString());
		if (fieldMap != null){
			String[] fm = new String[fieldMap.length];
			for (int i=0;i<fm.length;i++){
				fm[i] = StringUtils.stringArraytoString(fieldMap[i],ASSIGMENT_STRING.charAt(0));
			}
			xmlElement.setAttribute(XML_FIELDMAP_ATTRIBUTE,StringUtils.stringArraytoString(fm,';'));
		}
		xmlElement.setAttribute(XML_STARTROW_ATTRIBUTE,String.valueOf(parser.getFirstRow()));
		if (finalRow > -1) {
			xmlElement.setAttribute(XML_FINALROW_ATTRIBUTE,String.valueOf(this.finalRow));
		}
		if (maxErrorCount > -1) {
			xmlElement.setAttribute(XML_MAXERRORCOUNT_ATTRIBUTE,String.valueOf(this.maxErrorCount));
		}
		if (parser.getMetadataRow() > -1) {
			xmlElement.setAttribute(XML_METADATAROW_ATTRIBUTE, String.valueOf(parser.getMetadataRow()));
		}
		if (sheetName != null) {
			xmlElement.setAttribute(XML_SHEETNAME_ATTRIBUTE,this.sheetName);
		}else{
			xmlElement.setAttribute(XML_SHEETNUMBER_ATTRIBUTE,String.valueOf(parser.getSheetNumber()));
		}
		if (parser instanceof JExcelXLSDataParser && 
				((JExcelXLSDataParser)parser).getCharset() != null){
			xmlElement.setAttribute(XML_CHARSET_ATTRIBUTE, ((JExcelXLSDataParser)parser).getCharset());
		}
		
	}

	public void setPolicyType(String strPolicyType) {
        setPolicyType(PolicyType.valueOfIgnoreCase(strPolicyType));
    }
    
    public void setPolicyType(PolicyType policyType) {
        this.policyType = policyType;
        parser.setExceptionHandler(ParserExceptionHandlerFactory.getHandler(policyType));
    }

	public int getStartRow() {
		return startRow;
	}
	
	/**
	 * @param startRow The startRow to set.
	 */
	public void setStartRow(int startRecord) {
		if(startRecord < 1 || (finalRow != -1 && startRecord > finalRow)) {
			throw new InvalidParameterException("Invalid StartRecord parameter.");
		}
		this.startRow = startRecord;
		parser.setFirstRow(startRecord-1);
	}
	
	/**
	 * @return Returns the finalRow.
	 */
	
	public int getFinalRow() {
		return finalRow;
	}
	
	/**
	 * @param finalRow The finalRow to set.
	 */
	public void setFinalRow(int finalRecord) {
		if(finalRecord < 0 || (startRow != -1 && startRow > finalRecord)) {
			throw new InvalidParameterException("Invalid finalRow parameter.");
		}
		this.finalRow = finalRecord;
		parser.setLastRow(finalRow - 1);
	}

	/**
	 * @param finalRow The finalRow to set.
	 */
	public void setMaxErrorCount(int maxErrorCount) {
		if(maxErrorCount < 0) {
			throw new InvalidParameterException("Invalid maxErrorCount parameter.");
		}
		this.maxErrorCount = maxErrorCount;
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.GraphElement#init()
	 */
	@Override
	public void init() throws ComponentNotReadyException {
		super.init();
        if (outPorts.size() < 1) {
            throw new ComponentNotReadyException(getId() + ": at least one output port can be defined!");
        }
		if (sheetName != null){
			parser.setSheetName(sheetName);
		}
		if (sheetNumber != null){
			parser.setSheetNumber(sheetNumber);
		}
		if (metadataRow != 0){
			parser.setMetadataRow(metadataRow-1);
		}
		//set proper mapping type beetwen clover and xls fields
		if (fieldMap != null){
			String[] cloverFields = new String[fieldMap.length];
			String[] xlsFields = new String[fieldMap.length];
			for (int i=0;i<fieldMap.length;i++){
				cloverFields[i] = fieldMap[i][CLOVER_FIELDS];
				if (fieldMap[i].length > 1) {
					xlsFields[i] = fieldMap[i][XLS_FIELDS];
				}else {
					xlsFields[i] = null;
				}
			}
			parser.setCloverFields(cloverFields);
			if (xlsFields[0] != null){
				if (xlsFields[0].startsWith("$")){
					for (int i=0;i<xlsFields.length;i++){
						xlsFields[i] = xlsFields[i].substring(1);
					}
					parser.setMappingType(XLSDataParser.CLOVER_FIELDS_AND_XLS_NUMBERS);
					parser.setXlsFields(xlsFields);
				}else{
					parser.setMappingType(XLSDataParser.CLOVER_FIELDS_AND_XLS_NAMES);
					parser.setXlsFields(xlsFields);
				}
			}else {
				parser.setMappingType(XLSDataParser.ONLY_CLOVER_FIELDS);
			}
		}else if (metadataRow != 0){
			parser.setMappingType(XLSDataParser.MAP_NAMES);
		}else{
			parser.setMappingType(XLSDataParser.NO_METADATA_INFO);
		}
            reader = new MultiFileReader(parser, getGraph() != null ? getGraph().getProjectURL() : null, fileURL);
	        reader.setLogger(logger);
	        reader.setNumRecords(numRecords);
	        try {
	            reader.init(getOutputPort(OUTPUT_PORT).getMetadata());
	        } catch(ComponentNotReadyException e) {
	            e.setAttributeName(XML_FILE_ATTRIBUTE);
	            throw e;
	        }
	}

	private void setSheetName(String sheetName) {
		this.sheetName = sheetName;
	}

	private void setMetadataRow(int metadaRow) {
		this.metadataRow = metadaRow;
	}

	public void setSheetNumber(String sheetNumber) {
		this.sheetNumber = sheetNumber;
	}

	public void setNumRecords(int numRecords) {
		this.numRecords = numRecords;
	}

}
