
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
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.formatter.CloverDataFormatter;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.metadata.DataRecordMetadataXMLReaderWriter;
import org.jetel.util.ByteBufferUtils;
import org.jetel.util.ComponentXMLAttributes;
import org.jetel.util.StringUtils;
import org.jetel.util.SynchronizeUtils;
import org.w3c.dom.Element;

/**
 *  <h3>Clover Data Writer Component</h3>
 *
 * <!-- Writes data in Clover internal format to binary file. -->
 *
 * <table border="1">
 *  <th>Component:</th>
 * <tr><td><h4><i>Name:</i></h4></td>
 * <td>CloverDataWriter</td></tr>
 * <tr><td><h4><i>Category:</i></h4></td>
 * <td>Writers</td></tr>
 * <tr><td><h4><i>Description:</i></h4></td>
 * <td>Reads data from input port and writes them to binary file in Clover internal
 *  format. With records can be saved indexes of records in binary file (for 
 *  reading not all records afterward) and metadata definition. If compress level
 *   is not set to zero, data are saved in zip file with the structure:<br>DATA/fileName<br>INDEX/fileName.idx<br>
 *   METADATA/fileName.fmt<br>Because POI currently uses a lot of memory for large sheets it impossible to save large data (over ~1.8MB) to xls file</td></tr>
 * <tr><td><h4><i>Inputs:</i></h4></td>
 * <td>one input port defined/connected.</td></tr>
 * <tr><td><h4><i>Outputs:</i></h4></td>
 * <td></tr>
 * <tr><td><h4><i>Comment:</i></h4></td></tr>
 * </table>
 *  <br>
 *  <table border="1">
 *  <th>XML attributes:</th>
 *  <tr><td><b>type</b></td><td>"CLOVER_WRITER"</td></tr>
 *  <tr><td><b>id</b></td><td>component identification</td>
 *  <tr><td><b>fileURL</b></td><td>path to the output file </td>
 *  <tr><td><b>append</b><br><i>optional</i></td><td>whether to append data at
 *   the end if output file exists or replace it (true/false - default true)</td>
 *  <tr><td><b>saveIndex</b><br><i>optional</i></td><td>indicates if indexes to records 
 *  in binary file are saved or not (true/false - default false)</td>
 *  <tr><td><b>saveMetadata</b><br><i>optional</i></td><td>indicates if metadata
 *   definition is saved or not (true/false - default false)</td>
 *  <tr><td><b>compressLevel</b><br><i>optional</i></td><td>Sets the compression level. The default
 *   setting is to compress using default ZIP compression level. 
 *  </tr>
 *  </table>
 *
 *  <h4>Example:</h4>
 *  <pre>&lt;Node compressLevel="0" fileURL="customers.clv" id="CLOVER_WRITER0"
 *   saveIndex="true" saveMetadata="true" type="CLOVER_WRITER"/&gt;
 *  
 *  <pre>&lt;Node fileURL="customers.clv" id="CLOVER_WRITER0"
 *   saveIndex="true" type="CLOVER_WRITER"/&gt;
 *
/**
 * @author avackova (agata.vackova@javlinconsulting.cz) ; 
 * (c) JavlinConsulting s.r.o.
 *  www.javlinconsulting.cz
 *
 * @since Oct 12, 2006
 * @see CloverDataFormater
 *
 */
public class CloverDataWriter extends Node {

	private static final String XML_FILEURL_ATTRIBUTE = "fileURL";
	private static final String XML_APPEND_ATTRIBUTE = "append";
	private static final String XML_SAVEINDEX_ATRRIBUTE = "saveIndex";
	private static final String XML_SAVEMETADATA_ATTRIBUTE = "saveMetadata";
	private static final String XML_COMPRESSLEVEL_ATTRIBUTE = "compressLevel";

	public final static String COMPONENT_TYPE = "CLOVER_WRITER";
	private final static int READ_FROM_PORT = 0;
	
	private String fileURL;
	private boolean append;
	private CloverDataFormatter formatter;
	private boolean saveMetadata;
	private DataRecordMetadata metadata;
	private OutputStream out;
	private InputPort inPort;
	private int compressLevel;
	String fileName;
	
    static Log logger = LogFactory.getLog(CloverDataWriter.class);

 	public CloverDataWriter(String id, String fileURL, boolean saveIndex) {
		super(id);
		this.fileURL = fileURL;
		if (fileURL.toLowerCase().endsWith(".zip")){
			fileName = fileURL.substring(fileURL.lastIndexOf(File.separatorChar)+1,fileURL.lastIndexOf('.'));
		}else{
			fileName = fileURL.substring(fileURL.lastIndexOf(File.separatorChar)+1);
		}
		formatter = new CloverDataFormatter(fileURL,saveIndex);
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.Node#getType()
	 */
	@Override
	public String getType() {
		return COMPONENT_TYPE;
	}

	/**
	 * This method saves metadata definition to METADATA/fileName.fmt or
	 * fileName.zip#METADATA/fileName.fmt
	 * 
	 * @throws IOException
	 */
	private void saveMetadata() throws IOException{
		if (out instanceof FileOutputStream) {
			String dir = fileURL.substring(0,fileURL.lastIndexOf(File.separatorChar)+1);
			FileOutputStream metaFile = new FileOutputStream(
					dir + fileName +".fmt");
			DataRecordMetadataXMLReaderWriter.write(metadata,metaFile);			
		}else{//out is ZipOutputStream
			((ZipOutputStream)out).putNextEntry(new ZipEntry(
					"METADATA" + File.separator + fileName+".fmt"));
			DataRecordMetadataXMLReaderWriter.write(metadata, out);
			((ZipOutputStream)out).closeEntry();
		}
	}
	
	@Override
	public Result execute() throws Exception {
		DataRecord record = new DataRecord(metadata);
		record.init();
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
		if (saveMetadata){
			saveMetadata();
		}
		out.close();
        return runIt ? Result.FINISHED_OK : Result.ABORTED;
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
		inPort = getInputPort(READ_FROM_PORT);
		metadata = inPort.getMetadata();
		try{//create output stream and rewrite existing data
			if (compressLevel != 0) {
				ZipInputStream zipData = null;
//				if append=true existaing file has to be renamed
				File dataOriginal;
				File data = new File(fileURL + ".tmp");
				dataOriginal = new File(fileURL);
				if (append && dataOriginal.exists()) {
					dataOriginal.renameTo(data);
					zipData = new ZipInputStream(new FileInputStream(data));
					//find entry with data
					while (!zipData.getNextEntry().getName().equalsIgnoreCase(
							"DATA" + File.separator + fileName)) {}
				}
				//create new zip file
				out = new ZipOutputStream(new FileOutputStream(fileURL));
				//set compress level if given
				if (compressLevel != -1){
					((ZipOutputStream)out).setLevel(compressLevel);
				}
				((ZipOutputStream)out).putNextEntry(new ZipEntry("DATA" + File.separator + fileName));
				//rewrite existing data to new zip file
				if (append && data.exists()){
					ByteBufferUtils.rewrite(zipData,out);
					zipData.close();
				}
			}else{//compressLevel=0
				String dir = fileURL.substring(0,fileURL.lastIndexOf(File.separatorChar)+1);
				out = new FileOutputStream(dir + fileName, append);
			}
		}catch(IOException ex){
			throw new ComponentNotReadyException(ex);
		}
		formatter.init(metadata);
        formatter.setDataTarget(out);
	}
	/**
	 *  Description of the Method
	 *
	 * @param  nodeXML  Description of Parameter
	 * @return          Description of the Returned Value
	 * @since           May 21, 2002
	 */
	public static Node fromXML(TransformationGraph graph, Element nodeXML) {
		ComponentXMLAttributes xattribs=new ComponentXMLAttributes(nodeXML, graph);
		CloverDataWriter aDataWriter = null;
		
		try{
			aDataWriter = new CloverDataWriter(xattribs.getString(Node.XML_ID_ATTRIBUTE),
					xattribs.getString(XML_FILEURL_ATTRIBUTE),
					xattribs.getBoolean(XML_SAVEINDEX_ATRRIBUTE,false));
			aDataWriter.setAppend(xattribs.getBoolean(XML_APPEND_ATTRIBUTE,false));
			aDataWriter.setSaveMetadata(xattribs.getBoolean(XML_SAVEMETADATA_ATTRIBUTE,false));
			aDataWriter.setCompressLevel(xattribs.getInteger(XML_COMPRESSLEVEL_ATTRIBUTE,-1));
		}catch(Exception ex){
			System.err.println(COMPONENT_TYPE + ":" + xattribs.getString(Node.XML_ID_ATTRIBUTE,"unknown ID") + ":" + ex.getMessage());
			return null;
		}
		
		return aDataWriter;
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.Node#toXML(org.w3c.dom.Element)
	 */
	public void toXML(org.w3c.dom.Element xmlElement) {
		super.toXML(xmlElement);
		xmlElement.setAttribute(XML_FILEURL_ATTRIBUTE,this.fileURL);
		xmlElement.setAttribute(XML_APPEND_ATTRIBUTE,String.valueOf(append));
		xmlElement.setAttribute(XML_SAVEMETADATA_ATTRIBUTE,String.valueOf(saveMetadata));
		xmlElement.setAttribute(XML_SAVEINDEX_ATRRIBUTE,String.valueOf(formatter.isSaveIndex()));
		if (compressLevel > -1){
			xmlElement.setAttribute(XML_COMPRESSLEVEL_ATTRIBUTE,String.valueOf(compressLevel));
		}
	}

	public void setSaveMetadata(boolean saveMetadata) {
		this.saveMetadata = saveMetadata;
	}

	public void setCompressLevel(int compressLevel) {
		this.compressLevel = compressLevel;
	}

	public void setAppend(boolean append) {
		this.append = append;
		formatter.setAppend(append);
	}


}
