
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

import java.io.FileInputStream;
import java.io.IOException;
import java.security.InvalidParameterException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.parser.XLSDataParser;
import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ParserExceptionHandlerFactory;
import org.jetel.exception.PolicyType;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.Node;
import org.jetel.graph.TransformationGraph;
import org.jetel.util.ComponentXMLAttributes;
import org.jetel.util.StringUtils;
import org.jetel.util.SynchronizeUtils;
import org.w3c.dom.Element;

/**
 *  <h3>XLS Reader Component</h3>
 *
 * <!-- Parses data from xls file and send the records to out ports. -->
 *
 * <table border="1">
 *  <th>Component:</th>
 * <tr><td><h4><i>Name:</i></h4></td>
 * <td>XLSReader</td></tr>
 * <tr><td><h4><i>Category:</i></h4></td>
 * <td></td></tr>
 * <tr><td><h4><i>Description:</i></h4></td>
 * <td>Parses data from xls file and send the records to out ports.<br>Because 
 * POI currently uses a lot of memory for large sheets, it is impossible to read 
 * large data (over ~4.3MB in xls file - 2.1MB in flat file)</td></tr>
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
 *  <tr><td><b>sheetName</b></td><td>name of sheet for reading data. If it is not set data
 *   are read from first sheet</td>
 *  <tr><td><b>sheetNumber</b></td><td>number of sheet for reading data (starting from 0).
 *   If it is not set data are read from first sheet. If sheetName and sheetNumber are both
 *    set, sheetNumber is ignored</td>
 *  <tr><td><b>metadataRow</b></td><td>number of row where are names of columns</td>
 *  <tr><td><b>fieldMap</b></td><td>Pairs of clover fields and xls columns
 *   (cloverField=xlsColumn) separated by :;| {colon, semicolon, pipe}.
 *  Can be used for mapping clover fields and xls fields or for defining order 
 *  of reading columns from xls sheet. Xls columns can be written as names given 
 *  in row specified by metadataRow attribute or as column's codes preceded 
 *  by $. Xls fields may be missing, then columns are read in order they are 
 *  in xls sheet and are given to proper metadata fields</td>
 *  </tr>
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
	private static final String XML_STARTROW_ATTRIBUTE = "startRow";
	private static final String XML_FINALROW_ATTRIBUTE = "finalRow";
	private static final String XML_MAXERRORCOUNT_ATTRIBUTE = "maxErrorCount";
	private final static String XML_FILE_ATTRIBUTE = "fileURL";
	private final static String XML_DATAPOLICY_ATTRIBUTE = "dataPolicy";
	private final static String XML_SHEETNAME_ATTRIBUTE = "sheetName";
	private final static String XML_SHEETNUMBER_ATTRIBUTE = "sheetNumber";
	private final static String XML_METADATAROW_ATTRIBUTE = "metadataRow";
	private final static String XML_FIELDMAP_ATTRIBUTE = "fieldMap";
	
	private final static String ASSIGMENT_STRING = "=";
	private final static int OUTPUT_PORT = 0;
	private final static int CLOVER_FIELDS = 0;
	private final static int XLS_FIELDS = 1;

	private String fileURL;
	private int startRow = 0;
	private int finalRow = -1;
	private int maxErrorCount = -1;
    
	private XLSDataParser parser;
	private PolicyType policyType = PolicyType.STRICT;
	
	private String sheetName;
	private int sheetNumber = -1;
	private int metadataRow = 0;
	private String[][] fieldMap;

	/**
	 * @param id
	 */
	public XLSReader(String id, String fileURL, String[][] fieldMap) {
		super(id);
		this.fileURL = fileURL;
		this.fieldMap = fieldMap;
		this.parser = new XLSDataParser();
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
		int errorCount = 0;
		int diffRow = (startRow != -1) ? finalRow - startRow +1: finalRow ;
		try{
			while (((record) != null) && runIt) {
				try {
					record = parser.getNext(record);
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
				if(finalRow != -1 && parser.getRecordCount() > diffRow) {
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
//		parser.close();
		broadcastEOF();
		if (runIt) {
			resultMsg = "OK";
		} else {
			resultMsg = "STOPPED";
		}
		resultCode = Node.RESULT_OK;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.jetel.graph.GraphElement#checkConfig()
	 */
	@Override
	public boolean checkConfig() {
		return true;
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
			aXLSReader = new XLSReader(xattribs.getString(Node.XML_ID_ATTRIBUTE),
						xattribs.getString(XML_FILE_ATTRIBUTE),fieldMap);
			aXLSReader.setPolicyType(xattribs.getString(XML_DATAPOLICY_ATTRIBUTE, null));
			aXLSReader.setStartRow(xattribs.getInteger(XML_STARTROW_ATTRIBUTE,1));
			if (xattribs.exists(XML_FINALROW_ATTRIBUTE)){
				aXLSReader.setFinalRow(xattribs.getInteger(XML_FINALROW_ATTRIBUTE));
			}
			if (xattribs.exists(XML_MAXERRORCOUNT_ATTRIBUTE)){
				aXLSReader.setMaxErrorCount(xattribs.getInteger(XML_MAXERRORCOUNT_ATTRIBUTE));
			}
			if (xattribs.exists(XML_SHEETNAME_ATTRIBUTE)){
				aXLSReader.setSheetName(xattribs.getString(XML_SHEETNAME_ATTRIBUTE));
			}else if (xattribs.exists(XML_SHEETNUMBER_ATTRIBUTE)){
				aXLSReader.setSheetNumber(xattribs.getInteger(XML_SHEETNUMBER_ATTRIBUTE));
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
		if(startRecord < 0 || (finalRow != -1 && startRecord > finalRow)) {
			throw new InvalidParameterException("Invalid StartRecord parametr.");
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
        if (outPorts.size() < 1) {
            throw new ComponentNotReadyException(getId() + ": at least one output port can be defined!");
        }
		if (sheetName != null){
			parser.setSheetName(sheetName);
		}
		if (sheetNumber > -1){
			parser.setSheetNumber(sheetNumber);
		}
		if (metadataRow != 0){
			parser.setMetadataRow(metadataRow-1);
		}
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
		try {
			parser.open(new FileInputStream(fileURL), getOutputPort(OUTPUT_PORT).getMetadata());
		} catch (IOException ex) {
			throw new ComponentNotReadyException(getId() + "IOError: " + ex.getMessage());
		}
	}

	private void setSheetName(String sheetName) {
		this.sheetName = sheetName;
	}

	private void setMetadataRow(int metadaRow) {
		this.metadataRow = metadaRow;
	}

	public void setSheetNumber(int sheetNumber) {
		this.sheetNumber = sheetNumber;
	}

}
