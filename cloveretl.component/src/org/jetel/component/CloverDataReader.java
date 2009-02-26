
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
import org.jetel.data.parser.CloverDataParser;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.JetelException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.exception.ConfigurationStatus.Severity;
import org.jetel.graph.Node;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.AutoFilling;
import org.jetel.util.SynchronizeUtils;
import org.jetel.util.file.FileUtils;
import org.jetel.util.property.ComponentXMLAttributes;
import org.w3c.dom.Element;

/**
 *  <h3>Clover Data Reader Component</h3>
 *
 * <!-- Reads data saved in Clover internal format and send the records to out 
 * ports. -->
 *
 * <table border="1">
 *  <th>Component:</th>
 * <tr><td><h4><i>Name:</i></h4></td>
 * <td>CloverReader</td></tr>
 * <tr><td><h4><i>Category:</i></h4></td>
 * <td></td></tr>
 * <tr><td><h4><i>Description:</i></h4></td>
 * <td>Reads data saved in Clover internal format and send the records to out 
 * ports.</td></tr>
 * <tr><td><h4><i>Inputs:</i></h4></td>
 * <td></td></tr>
 * <tr><td><h4><i>Outputs:</i></h4></td>
 * <td>At least one output port defined/connected.</tr>
 * <tr><td><h4><i>Comment:</i></h4></td></tr>
 * </table>
 *  <br>
 *  <table border="1">
 *  <th>XML attributes:</th>
 *  <tr><td><b>type</b></td><td>"CLOVER_READER"</td></tr>
 *  <tr><td><b>id</b></td><td>component identification</td>
 *  <tr><td><b>fileURL</b></td><td>path to the data file. </td>
 *  <tr><td><b>indexFileURL</b><br><i>optional</i></td><td>if index file is not 
 *  in the same directory as data file or has not expected name (fileURL.idx)</td>
 *  <tr><td><b>startRecord</b><br><i>optional</i></td><td>index of first parsed record</td>
 *  <tr><td><b>finalRecord</b><br><i>optional</i></td><td>index of final parsed record</td>
 *  </tr>
 *  </table>
 *
 *  <h4>Example:</h4>
 *  <pre>&lt;Node fileURL="DATA/customers.clv" finalRecord="2" id="CLOVER_READER0" 
 *  startRecord="1" type="CLOVER_READER"/&gt;
 * 
 * <pre>&lt;Node fileURL="customers.clv.zip" id="CLOVER_READER0" type="CLOVER_READER"/&gt;
 * 
 * @author avackova <agata.vackova@javlinconsulting.cz> ; 
 * (c) JavlinConsulting s.r.o.
 *  www.javlinconsulting.cz
 *
 * @since Oct 13, 2006
 * @see CloverDataParser.java
 *
 */


public class CloverDataReader extends Node {

	public final static String COMPONENT_TYPE = "CLOVER_READER";

	/** XML attribute names */
	private final static String XML_FILE_ATTRIBUTE = "fileURL";
	private final static String XML_INDEXFILEURL_ATTRIBUTE = "indexFileURL";
	private static final String XML_STARTRECORD_ATTRIBUTE = "startRecord";
	private static final String XML_FINALRECORD_ATTRIBUTE = "finalRecord";
	private static final String XML_SKIPROWS_ATTRIBUTE = "skipRows";
	private static final String XML_NUMRECORDS_ATTRIBUTE = "numRecords";

	private final static int OUTPUT_PORT = 0;

	private String fileURL;
	private String indexFileURL;
	private CloverDataParser parser;

	private int skipRows;
	private int numRecords = Integer.MAX_VALUE;

    private AutoFilling autoFilling = new AutoFilling();

	/**
	 * @param id
	 * @param fileURL
	 * @param indexFileURL
	 */
	public CloverDataReader(String id, String fileURL, String indexFileURL) {
		super(id);
		this.fileURL = fileURL;
		this.indexFileURL = indexFileURL;
		parser = new CloverDataParser();
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
		int recordCount = 0;
		while ((record = parser.getNext(record))!=null && runIt){
	        //check for index of last returned record
	        if(numRecords == recordCount) {
				break;
	        }
	        autoFilling.setAutoFillingFields(record);
		    writeRecordBroadcast(record);
			SynchronizeUtils.cloverYield();
			System.err.println(recordCount);
		    recordCount++;
		}
		broadcastEOF();
        return runIt ? Result.FINISHED_OK : Result.ABORTED;
	}
	
	
	public static Node fromXML(TransformationGraph graph, Element nodeXML) throws XMLConfigurationException {
		CloverDataReader aDataReader = null;
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(nodeXML, graph);

		try {
			aDataReader = new CloverDataReader(xattribs.getString(Node.XML_ID_ATTRIBUTE),
						xattribs.getString(XML_FILE_ATTRIBUTE),
						xattribs.getString(XML_INDEXFILEURL_ATTRIBUTE,null));
			if (xattribs.exists(XML_STARTRECORD_ATTRIBUTE)){
				aDataReader.setStartRecord(xattribs.getInteger(XML_STARTRECORD_ATTRIBUTE));
			}
			if (xattribs.exists(XML_FINALRECORD_ATTRIBUTE)){
				aDataReader.setFinalRecord(xattribs.getInteger(XML_FINALRECORD_ATTRIBUTE));
			}
			if (xattribs.exists(XML_SKIPROWS_ATTRIBUTE)){
				aDataReader.setSkipRows(xattribs.getInteger(XML_SKIPROWS_ATTRIBUTE));
			}
			if (xattribs.exists(XML_NUMRECORDS_ATTRIBUTE)){
				aDataReader.setNumRecords(xattribs.getInteger(XML_NUMRECORDS_ATTRIBUTE));
			}
		} catch (Exception ex) {
		    throw new XMLConfigurationException(COMPONENT_TYPE + ":" + xattribs.getString(XML_ID_ATTRIBUTE," unknown ID ") + ":" + ex.getMessage(),ex);
		}

		return aDataReader;
	}

	public void toXML(Element xmlElement) {
	    super.toXML(xmlElement);
		xmlElement.setAttribute(XML_FILE_ATTRIBUTE, this.fileURL);
		if (indexFileURL != null){
			xmlElement.setAttribute(XML_INDEXFILEURL_ATTRIBUTE,indexFileURL);
		}
		if (skipRows > 0) {
			xmlElement.setAttribute(XML_SKIPROWS_ATTRIBUTE,String.valueOf(skipRows));
		}
		if (numRecords > 0){
			xmlElement.setAttribute(XML_NUMRECORDS_ATTRIBUTE,String.valueOf(numRecords));
		}
	}
	/* (non-Javadoc)
	 * @see org.jetel.graph.GraphElement#checkConfig()
	 */
    @Override
    public ConfigurationStatus checkConfig(ConfigurationStatus status) {
        super.checkConfig(status);
        
        if(!checkInputPorts(status, 0, 0)
        		|| !checkOutputPorts(status, 1, Integer.MAX_VALUE)) {
        	return status;
        }
        
        checkMetadata(status, getOutMetadata());
        
    	try {
			if (!FileUtils.isServerURL(FileUtils.getInnerAddress(getGraph().getProjectURL(), fileURL)) && 
					!(new File(FileUtils.getFile(getGraph().getProjectURL(), fileURL))).exists()) {
				status.add(new ConfigurationProblem("File " + fileURL + " does not exist.", Severity.WARNING, this, ConfigurationStatus.Priority.NORMAL));
			}
		} catch (Exception e) {
			status.add(new ConfigurationProblem(e.getMessage(), Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL));
		}

//        try {
//            init();
//        } catch (ComponentNotReadyException e) {
//            ConfigurationProblem problem = new ConfigurationProblem(e.getMessage(), ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL);
//            if(!StringUtils.isEmpty(e.getAttributeName())) {
//                problem.setAttributeName(e.getAttributeName());
//            }
//            status.add(problem);
//        } finally {
//        	free();
//        }
        
        return status;
    }

	/* (non-Javadoc)
	 * @see org.jetel.graph.GraphElement#init()
	 */
	@Override
	public void init() throws ComponentNotReadyException {
        if(isInitialized()) return;
		super.init();
		
		//set start record
		if (skipRows > 0) {
			try{
				parser.skip(skipRows);
			}catch (JetelException ex) {}
		}
		DataRecordMetadata metadata = getOutputPort(OUTPUT_PORT).getMetadata();
		parser.init(metadata);
		parser.setProjectURL(getGraph().getProjectURL());
		if (indexFileURL != null) {
			parser.setDataSource(new String[]{fileURL,indexFileURL});
		}else{
			parser.setDataSource(fileURL);
		}
		
    	if (metadata != null) {
    		autoFilling.addAutoFillingFields(metadata);
    	}
    	autoFilling.setFilename(fileURL);
	}
	
	@Override
	public synchronized void free() {
		super.free();
		parser.close();
	}
	
	@Override
	public synchronized void reset() throws ComponentNotReadyException {
		super.reset();
		parser.reset();
		autoFilling.reset();
	}
	
	@Deprecated
	public void setStartRecord(int startRecord){
		setSkipRows(startRecord);
	}

	@Deprecated
	public void setFinalRecord(int finalRecord) {
		setNumRecords(finalRecord-skipRows);
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

}
