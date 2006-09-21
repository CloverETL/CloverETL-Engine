
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
 * @author avackova
 *
 */
public class XLSReader extends Node {

	public final static String COMPONENT_TYPE = "XLS_READER";
    static Log logger = LogFactory.getLog(XLSReader.class);

	/** XML attribute names */
	private static final String XML_STARTROW_ATTRIBUTE = "startRow";
	private static final String XML_FINALROW_ATTRIBUTE = "finalRow";
	private static final String XML_MAXERRORCOUNT_ATTRIBUTE = "maxErrorCount";
	private final static String XML_FILE_ATTRIBUTE = "fileURL";
	private final static String XML_CHARSET_ATTRIBUTE = "charset";
	private final static String XML_DATAPOLICY_ATTRIBUTE = "dataPolicy";
	private final static String XML_SHEETNAME_ATTRIBUTE = "sheetName";
	private final static String XML_METADATAROW_ATTRIBUTE = "metadataRow";
	private final static String XML_CLOVERFIELDS_ATTRIBUTE = "cloverFields";
	private final static String XML_XLSFIELDS_ATTRIBUTE = "xlsFields";

	private final static int OUTPUT_PORT = 0;

	private String fileURL;
	private int startRow = -1;
	private int finalRow = -1;
	private int maxErrorCount = -1;
    
	private XLSDataParser parser;
	private PolicyType policyType = PolicyType.STRICT;
	
	private String sheetName;
	private int metadataRow = -1;
	private String[] cloverFields = null;
	private String[] xlsFields = null;

	/**
	 * @param id
	 */
	public XLSReader(String id, String fileURL) {
		super(id);
		this.fileURL = fileURL;
		parser = new XLSDataParser();
	}

	public XLSReader(String id, String fileURL, String charset) {
		super(id);
		this.fileURL = fileURL;
		parser = new XLSDataParser(charset);
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
			if (xattribs.exists(XML_CHARSET_ATTRIBUTE)) {
				aXLSReader = new XLSReader(xattribs.getString(Node.XML_ID_ATTRIBUTE),
						xattribs.getString(XML_FILE_ATTRIBUTE),
						xattribs.getString(XML_CHARSET_ATTRIBUTE));
			} else {
				aXLSReader = new XLSReader(xattribs.getString(Node.XML_ID_ATTRIBUTE),
						xattribs.getString(XML_FILE_ATTRIBUTE));
			}
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
			}
			if (xattribs.exists(XML_METADATAROW_ATTRIBUTE)){
				aXLSReader.setMetadataRow(xattribs.getInteger(XML_METADATAROW_ATTRIBUTE));
			}
			if (xattribs.exists(XML_CLOVERFIELDS_ATTRIBUTE)){
				aXLSReader.setCloverFields(
					xattribs.getString(XML_CLOVERFIELDS_ATTRIBUTE).split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX));
			}
			if (xattribs.exists(XML_XLSFIELDS_ATTRIBUTE)){
				aXLSReader.setXlsFields(
						xattribs.getString(XML_XLSFIELDS_ATTRIBUTE).split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX));
			}
		} catch (Exception ex) {
		    throw new XMLConfigurationException(COMPONENT_TYPE + ":" + xattribs.getString(XML_ID_ATTRIBUTE," unknown ID ") + ":" + ex.getMessage(),ex);
		}

		return aXLSReader;
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
		parser.setFirstRow(startRecord);
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
		if (cloverFields != null){
			parser.setCloverFields(cloverFields);
		}
		if (xlsFields != null){
			boolean names = xlsFields[0].startsWith("\"") ? true : false ;
			if (names && metadataRow == -1){
				throw new ComponentNotReadyException("Not valid metadataRow = -1 for given field's names");
			}
			if (names){
				for (int i=0;i<xlsFields.length;i++){
					xlsFields[i] = StringUtils.unquote(xlsFields[i]);
				}
			}
			parser.setXlsFields(xlsFields,names);
		}
		if (metadataRow != -1){
			parser.setMetadataRow(metadataRow);
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

	private void setCloverFields(String[] metadataNames) {
		this.cloverFields = metadataNames;
	}

	private void setXlsFields(String[] xlsFields) {
		this.xlsFields = xlsFields;
	}

}
