
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

import java.io.File;
import java.io.IOException;

import org.jetel.data.DataRecord;
import org.jetel.data.formatter.XLSDataFormatter;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.TransformationGraph;
import org.jetel.util.ComponentXMLAttributes;
import org.jetel.util.SynchronizeUtils;
import org.jetel.util.XLSUtils;
import org.w3c.dom.Element;

/**
 * @author avackova
 *
 */
public class XLSWriter extends Node {

	private static final String XML_FILEURL_ATTRIBUTE = "fileURL";
	private static final String XML_SAVENAMES_ATTRIBUTE = "saveNames";
	private static final String XML_SHEETNAME_ATTRIBUTE = "sheetName";
	private static final String XML_SHEETNUMBER_ATTRIBUTE = "sheetNumber";
	private static final String XML_APPEND_ATTRIBUTE = "append";
	private static final String XML_FIRSTDATAROW_ATTRIBUTE = "firstDataRow";
	private static final String XML_FIRSTCOLUMN_ATTRIBUTE = "firstColumn";
	private static final String XML_NAMESROW_ATTRIBUTE = "namesRow";

	public final static String COMPONENT_TYPE = "XLS_WRITER";
	private final static int READ_FROM_PORT = 0;

	private String fileURL;
	private XLSDataFormatter formatter;
	private int namesRow;

	public XLSWriter(String id,String fileURL, boolean saveNames, boolean append){
		super(id);
		this.fileURL = fileURL;
		formatter = new XLSDataFormatter(saveNames,append);
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
		InputPort inPort = getInputPort(READ_FROM_PORT);
		DataRecord record = new DataRecord(inPort.getMetadata());
		record.init();
		while (record != null && runIt) {
			try {
				record = inPort.readRecord(record);
				if (record != null) {
					formatter.write(record);
				}
			}
			catch (IOException ex) {
				resultMsg=ex.getMessage();
				resultCode=Node.RESULT_ERROR;
				closeAllOutputPorts();
				return;
			}
			catch (Exception ex) {
				resultMsg=ex.getClass().getName()+" : "+ ex.getMessage();
				resultCode=Node.RESULT_FATAL_ERROR;
				return;
			}
			SynchronizeUtils.cloverYield();
		}
		formatter.close();
		if (runIt) resultMsg="OK"; else resultMsg="STOPPED";
		resultCode=Node.RESULT_OK;
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
		File out = new File(fileURL);
		try {
			if (!out.exists()){
				out.createNewFile();
			}
			formatter.open(out,getInputPort(READ_FROM_PORT).getMetadata());
		}catch(IOException ex){
			throw new ComponentNotReadyException(ex);
		}
	}

	public static Node fromXML(TransformationGraph graph, Element nodeXML) throws XMLConfigurationException {
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(nodeXML, graph);
		XLSWriter xlsWriter;
		try{
			xlsWriter = new XLSWriter(xattribs.getString(XML_ID_ATTRIBUTE),
					xattribs.getString(XML_FILEURL_ATTRIBUTE),
					xattribs.getBoolean(XML_SAVENAMES_ATTRIBUTE,false),
					xattribs.getBoolean(XML_APPEND_ATTRIBUTE,false));
			if (xattribs.exists(XML_SHEETNAME_ATTRIBUTE)){
				xlsWriter.setSheetName(xattribs.getString(XML_SHEETNAME_ATTRIBUTE));
			}
			else if (xattribs.exists(XML_SHEETNUMBER_ATTRIBUTE)){
				xlsWriter.setSheetNumber(xattribs.getInteger(XML_SHEETNUMBER_ATTRIBUTE));
			}
			xlsWriter.setFirstColumn(xattribs.getString(XML_FIRSTCOLUMN_ATTRIBUTE,"A"));
			xlsWriter.setFirstRow(xattribs.getInteger(XML_FIRSTDATAROW_ATTRIBUTE,0));
			xlsWriter.setNamesRow(xattribs.getInteger(XML_NAMESROW_ATTRIBUTE,0));
			return xlsWriter;
		} catch (Exception ex) {
		    throw new XMLConfigurationException(COMPONENT_TYPE + ":" + xattribs.getString(XML_ID_ATTRIBUTE," unknown ID ") + ":" + ex.getMessage(),ex);
		}
	}

	private void setSheetName(String sheetName) {
		formatter.setSheetName(sheetName);
	}

	private void setSheetNumber(int sheetNumber) {
		formatter.setSheetNumber(sheetNumber);
	}
	
	private void setFirstColumn(String firstColumn){
		formatter.setFirstColumn(firstColumn);
	}
	
	private void setFirstRow(int firstRow){
		formatter.setFirstRow(firstRow-1);
	}
	
	private void setNamesRow(int namesRow){
		formatter.setNamesRow(namesRow-1);
	}
}
