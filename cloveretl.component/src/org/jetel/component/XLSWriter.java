
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

import org.jetel.data.DataRecord;
import org.jetel.data.formatter.Formatter;
import org.jetel.data.formatter.JExcelXLSDataFormatter;
import org.jetel.data.formatter.XLSDataFormatter;
import org.jetel.data.formatter.XLSFormatter;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.util.ComponentXMLAttributes;
import org.jetel.util.StringUtils;
import org.jetel.util.SynchronizeUtils;
import org.w3c.dom.Element;

/**
 *  <h3>XLS Writer Component</h3>
 *
 * <!-- Reads data from input port and writes them to given xls sheet in xls file. -->
 *
 * <table border="1">
 *  <th>Component:</th>
 * <tr><td><h4><i>Name:</i></h4></td>
 * <td>XLSWriter</td></tr>
 * <tr><td><h4><i>Category:</i></h4></td>
 * <td>Writers</td></tr>
 * <tr><td><h4><i>Description:</i></h4></td>
 * <td>Reads data from input port and writes them to given xls sheet in xls file. If 
 *  in one graph you want to write to the same file but to different sheets each XLSWriter
 *  has to have another phase<br>Because POI currently uses a lot of memory for
 *   large sheets, it is impossible to save large data (over ~1.8MB) to xls file</td></tr>
 * <tr><td><h4><i>Inputs:</i></h4></td>
 * <td>one input port defined/connected.</td></tr>
 * <tr><td><h4><i>Outputs:</i></h4></td>
 * <td></tr>
 * <tr><td><h4><i>Comment:</i></h4></td></tr>
 * </table>
 *  <br>
 *  <table border="1">
 *  <th>XML attributes:</th>
 *  <tr><td><b>type</b></td><td>"XLS_WRITER"</td></tr>
 *  <tr><td><b>id</b></td><td>component identification</td>
 *  <tr><td><b>fileURL</b></td><td>path to the output file</td>
 *  <tr><td><b>namesRow</b></td><td>index of row, where to write metadata names</td>
 *  <tr><td><b>firstDataRow</b></td><td>index of row, where to write first data record</td>
 *  <tr><td><b>firstColumn</b></td><td>code of column from which data will be written</td>
 *  <tr><td><b>sheetName</b></td><td>name of sheet for writing data. If it is not set data
 *   new sheet with default name is created</td>
 *  <tr><td><b>sheetNumber</b></td><td>number of sheet for writing data (starting from 0).
 *   If it is not set new sheet with default name is created. If sheetName and sheetNumber 
 *   are both set, sheetNumber is ignored</td>
 *  <tr><td><b>append</b></td><td>indicates if given sheet exist new data are append
 *   to the sheet or old data are deleted and rewritten by new ones (true/false - default 
 *   false)</td>
 *  </tr>
 *  </table>
 *
 *  <h4>Example:</h4>
 *  <pre>&lt;Node fileURL="output/orders.partitioned.xls" firstColumn="f" 
 *   id="XLS_WRITER0" namesRow="2" sheetName="via1" type="XLS_WRITER"/&gt;
 * 
 * <pre>&lt;Node fileURL="output/orders.partitioned.xls" firstDataRow="10" 
 * id="XLS_WRITER1" sheetName="via2" type="XLS_WRITER"/&gt;
 *
 * <pre>&lt;Node append="true" fileURL="output/orders.partitioned.xls" 
 * id="XLS_WRITER2" namesRow="1" firstDataRow="3" sheetName="via3" type="XLS_WRITER"/&gt;
 * 
/**
* @author avackova <agata.vackova@javlinconsulting.cz> ; 
* (c) JavlinConsulting s.r.o.
*	www.javlinconsulting.cz
*	@created October 10, 2006
 */
public class XLSWriter extends Node {

	private static final String XML_FILEURL_ATTRIBUTE = "fileURL";
	private static final String XML_SHEETNAME_ATTRIBUTE = "sheetName";
	private static final String XML_SHEETNUMBER_ATTRIBUTE = "sheetNumber";
	private static final String XML_APPEND_ATTRIBUTE = "append";
	private static final String XML_FIRSTDATAROW_ATTRIBUTE = "firstDataRow";
	private static final String XML_FIRSTCOLUMN_ATTRIBUTE = "firstColumn";
	private static final String XML_NAMESROW_ATTRIBUTE = "namesRow";

	public final static String COMPONENT_TYPE = "XLS_WRITER";
	private final static int READ_FROM_PORT = 0;

	private String fileURL;
	private XLSFormatter formatter;
	
	private boolean usePOI = true;

	/**
	 * Constructor
	 * 
	 * @param id
	 * @param fileURL output file
	 * @param saveNames indicates if save metadata names
	 * @param append indicates if new data are appended or rewrite old data 
	 */
	public XLSWriter(String id,String fileURL, boolean append){
		super(id);
		this.fileURL = fileURL;
		if (usePOI) {
			formatter = new XLSDataFormatter(append);
		}else{		
			formatter = new JExcelXLSDataFormatter(append);
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
		InputPort inPort = getInputPort(READ_FROM_PORT);
		DataRecord record = new DataRecord(inPort.getMetadata());
		record.init();
		formatter.prepareSheet();
		try {
			while (record != null && runIt) {
				record = inPort.readRecord(record);
				if (record != null) {
					formatter.write(record);
				}
				SynchronizeUtils.cloverYield();
			}
		} catch (Exception e) {
			throw e;
		}finally{
			formatter.close();
		}
        return runIt ? Result.FINISHED_OK : Result.ABORTED;
	}

	@Override
	public void free() {
		super.free();
		formatter.close();
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.GraphElement#checkConfig()
	 */
    @Override
    public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		super.checkConfig(status);
		 
		checkInputPorts(status, 1, 1);
        checkOutputPorts(status, 0, 0);

        try {
            init();
            free();
        } catch (ComponentNotReadyException e) {
            ConfigurationProblem problem = new ConfigurationProblem(e.getMessage(), ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL);
            if(!StringUtils.isEmpty(e.getAttributeName())) {
                problem.setAttributeName(e.getAttributeName());
            }
            status.add(problem);
        }
        
        return status;
    }

	/* (non-Javadoc)
	 * @see org.jetel.graph.GraphElement#init()
	 */
	@Override
	public void init() throws ComponentNotReadyException {
		super.init();
		File out = new File(fileURL);
//		try {
//			if (!out.exists()){
//				out.createNewFile();
//			}
			formatter.init(getInputPort(READ_FROM_PORT).getMetadata());
            formatter.setDataTarget(out);
//		}catch(IOException ex){
//			throw new ComponentNotReadyException(ex);
//		}
	}

	public static Node fromXML(TransformationGraph graph, Element nodeXML) throws XMLConfigurationException {
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(nodeXML, graph);
		XLSWriter xlsWriter;
		try{
			xlsWriter = new XLSWriter(xattribs.getString(XML_ID_ATTRIBUTE),
					xattribs.getString(XML_FILEURL_ATTRIBUTE),
					xattribs.getBoolean(XML_APPEND_ATTRIBUTE,false));
			if (xattribs.exists(XML_SHEETNAME_ATTRIBUTE)){
				xlsWriter.setSheetName(xattribs.getString(XML_SHEETNAME_ATTRIBUTE));
			}
			else if (xattribs.exists(XML_SHEETNUMBER_ATTRIBUTE)){
				xlsWriter.setSheetNumber(xattribs.getInteger(XML_SHEETNUMBER_ATTRIBUTE));
			}
			xlsWriter.setFirstColumn(xattribs.getString(XML_FIRSTCOLUMN_ATTRIBUTE,"A"));
			xlsWriter.setFirstRow(xattribs.getInteger(XML_FIRSTDATAROW_ATTRIBUTE,1));
			xlsWriter.setNamesRow(xattribs.getInteger(XML_NAMESROW_ATTRIBUTE,0));
			return xlsWriter;
		} catch (Exception ex) {
		    throw new XMLConfigurationException(COMPONENT_TYPE + ":" + xattribs.getString(XML_ID_ATTRIBUTE," unknown ID ") + ":" + ex.getMessage(),ex);
		}
	}

	/**
	 *  Description of the Method
	 *
	 * @return    Description of the Returned Value
	 * @since     May 21, 2002
	 */
	public void toXML(org.w3c.dom.Element xmlElement) {
		super.toXML(xmlElement);
		xmlElement.setAttribute(XML_FILEURL_ATTRIBUTE,this.fileURL);
		xmlElement.setAttribute(XML_APPEND_ATTRIBUTE, String.valueOf(formatter.isAppend()));
		xmlElement.setAttribute(XML_FIRSTCOLUMN_ATTRIBUTE,String.valueOf(formatter.getFirstColumn()));
		xmlElement.setAttribute(XML_FIRSTDATAROW_ATTRIBUTE, String.valueOf(formatter.getFirstRow()+1));
		xmlElement.setAttribute(XML_NAMESROW_ATTRIBUTE, String.valueOf(formatter.getNamesRow()+1));
		if (formatter.getSheetName() != null) {//TODO can't we obtain it from parser?
			xmlElement.setAttribute(XML_SHEETNAME_ATTRIBUTE,formatter.getSheetName());
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
