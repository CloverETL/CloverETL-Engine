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

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.formatter.CloverDataFormatter;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.metadata.DataRecordMetadataXMLReaderWriter;
import org.jetel.util.SynchronizeUtils;
import org.jetel.util.file.FileURLParser;
import org.jetel.util.file.FileUtils;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.property.RefResFlag;
import org.jetel.util.string.StringUtils;
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
 *  reading not all records afterward) and metadata definition. If compressData 
 *  attribuet is set to "true", data are saved in zip file with the structure:<br>DATA/fileName<br>INDEX/fileName.idx<br>
 *   METADATA/fileName.fmt<br>If compressData attribute is set to "false", all files
 *   are saved in the same directory (as specified in fileURL attribute)</td></tr>
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
 *  <tr><td><b>recordSkip</b></td><td>number of skipped records</td>
 *  <tr><td><b>recordCount</b></td><td>number of written records</td>
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
	private static final String XML_RECORD_SKIP_ATTRIBUTE = "recordSkip";
	private static final String XML_RECORD_COUNT_ATTRIBUTE = "recordCount";
	private static final String XML_MK_DIRS_ATTRIBUTE = "makeDirs";

	public final static String COMPONENT_TYPE = "CLOVER_WRITER";
	private final static int READ_FROM_PORT = 0;

	private String fileURL;
	private boolean append;
	private CloverDataFormatter formatter;
	private boolean saveMetadata;
	private DataRecordMetadata metadata;
	private OutputStream out;//ZipOutputstream or FileOutputStream
	private InputPort inPort;
	private int compressLevel;
	String fileName;
    private int skip;
	private int numRecords = -1;
	private boolean mkDir;
	
    static Log logger = LogFactory.getLog(CloverDataWriter.class);

 	public CloverDataWriter(String id, String fileURL, boolean saveIndex) {
		super(id);
		this.fileURL = fileURL;
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
	 * This method saves metadata definition to fileName.fmt or
	 * fileName.zip#METADATA/fileName.fmt
	 * 
	 * @throws IOException
	 */
	private void saveMetadata() throws IOException{
		if (out instanceof ZipOutputStream) {
			((ZipOutputStream)out).putNextEntry(new ZipEntry(
					CloverDataFormatter.METADATA_DIRECTORY + fileName+ 
					CloverDataFormatter.METADATA_EXTENSION));
			DataRecordMetadataXMLReaderWriter.write(metadata, out);
			((ZipOutputStream)out).closeEntry();
		}else{
			OutputStream metaFile;
			if (fileURL.startsWith("zip:")) {
				metaFile = FileUtils.getOutputStream(getGraph().getRuntimeContext().getContextURL(), 
						fileURL + "#" + CloverDataFormatter.METADATA_DIRECTORY + fileName + CloverDataFormatter.METADATA_EXTENSION, false, -1);
			}
			else {
				metaFile = FileUtils.getOutputStream(getGraph().getRuntimeContext().getContextURL(), 
						fileURL + CloverDataFormatter.METADATA_EXTENSION, false, -1);
			}
			DataRecordMetadataXMLReaderWriter.write(metadata,metaFile);	
			metaFile.close();
		}
	}

	@Override
    public void preExecute() throws ComponentNotReadyException {
    	super.preExecute();
    	
    	if (firstRun()) {//a phase-dependent part of initialization
    		//all has been initialized in init()
    	}
    	else {
    		formatter.reset();
    	}

    	try{//create output stream and rewrite existing data
        	fileName = new File(FileUtils.getFile(getGraph().getRuntimeContext().getContextURL(), fileURL)).getName();
        	if (fileName.toLowerCase().endsWith(".zip")) {
        		fileName = fileName.substring(0,fileName.lastIndexOf('.')); 
        	}
			out = FileUtils.getOutputStream(getGraph().getRuntimeContext().getContextURL(), 
					fileURL.startsWith("zip:") ? fileURL + "#" + CloverDataFormatter.DATA_DIRECTORY + fileName : fileURL, 
					append, compressLevel);
		} catch(IOException e) {
			throw new ComponentNotReadyException(e);
		}

        formatter.setDataTarget(out);
    }
	
	@Override
	public Result execute() throws Exception {
		DataRecord record = DataRecordFactory.newRecord(metadata);
		long iRec = 0;
		int recordTo = numRecords < 0 ? Integer.MAX_VALUE : (skip <= 0 ? numRecords+1 : skip+1 + numRecords);
		record.init();
		while (record != null && runIt) {
			iRec++;
			record = inPort.readRecord(record);
			if (skip >= iRec || recordTo <= iRec) continue;
			if (record != null) {
				formatter.write(record);
			}
			SynchronizeUtils.cloverYield();
		}
        return runIt ? Result.FINISHED_OK : Result.ABORTED;
	}
	
    @Override
    public void postExecute() throws ComponentNotReadyException {
    	super.postExecute();
    	try {
			formatter.finish();
			if (saveMetadata){
				saveMetadata();
			}
		} catch (IOException e) {
			throw new ComponentNotReadyException(e);
		}
  		formatter.close(); //indirectly closes out    		
    }
    
	@Override
	public synchronized void free() {
        if(!isInitialized()) return;
		super.free();
	}
	/* (non-Javadoc)
	 * @see org.jetel.graph.GraphElement#checkConfig()
	 */
    @Override
    public ConfigurationStatus checkConfig(ConfigurationStatus status) {
        super.checkConfig(status);
        
        if(!checkInputPorts(status, 1, 1)
        		|| !checkOutputPorts(status, 0, 0)) {
        	return status;
        }

        if (StringUtils.isEmpty(fileURL)) {
            status.add("Attribute 'fileURL' is required.", ConfigurationStatus.Severity.ERROR, this,
            		ConfigurationStatus.Priority.NORMAL,XML_FILEURL_ATTRIBUTE);
        	return status;
        }
        
        try {
        	FileUtils.canWrite(getContextURL(), fileURL, mkDir);
        } catch (ComponentNotReadyException e) {
            status.add(e,ConfigurationStatus.Severity.ERROR,this,
            		ConfigurationStatus.Priority.NORMAL,XML_FILEURL_ATTRIBUTE);
        }
        
        try {
			if (append && FileURLParser.isArchiveURL(fileURL) && FileURLParser.isServerURL(fileURL)) {
			    status.add("Append true is not supported on remote archive files.", ConfigurationStatus.Severity.WARNING, this,
			    		ConfigurationStatus.Priority.NORMAL, XML_APPEND_ATTRIBUTE);
			}
		} catch (MalformedURLException e) {
            status.add(e.toString(),ConfigurationStatus.Severity.ERROR,this,
            		ConfigurationStatus.Priority.NORMAL,XML_APPEND_ATTRIBUTE);
		}
        
        return status;
    }

	/* (non-Javadoc)
	 * @see org.jetel.graph.GraphElement#init()
	 */
	@Override
	public void init() throws ComponentNotReadyException {
        if(isInitialized()) return;
		super.init();
		
    	// creates necessary directories
        if (mkDir) FileUtils.makeDirs(getGraph().getRuntimeContext().getContextURL(), new File(FileURLParser.getMostInnerAddress(fileURL)).getParent());

		inPort = getInputPort(READ_FROM_PORT);
		metadata = inPort.getMetadata();
		formatter.setProjectURL(getGraph().getRuntimeContext().getContextURL());
		formatter.init(metadata);
	}

	@Override
	public String[] getUsedUrls() {
		return new String[] { fileURL };
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
		ComponentXMLAttributes xattribs=new ComponentXMLAttributes(nodeXML, graph);
		CloverDataWriter aDataWriter = null;
		
		aDataWriter = new CloverDataWriter(xattribs.getString(Node.XML_ID_ATTRIBUTE),
				xattribs.getStringEx(XML_FILEURL_ATTRIBUTE, null, RefResFlag.URL),
				xattribs.getBoolean(XML_SAVEINDEX_ATRRIBUTE,false));
		aDataWriter.setAppend(xattribs.getBoolean(XML_APPEND_ATTRIBUTE,false));
		aDataWriter.setSaveMetadata(xattribs.getBoolean(XML_SAVEMETADATA_ATTRIBUTE,false));
		aDataWriter.setCompressLevel(xattribs.getInteger(XML_COMPRESSLEVEL_ATTRIBUTE,-1));
		if (xattribs.exists(XML_RECORD_SKIP_ATTRIBUTE)){
			aDataWriter.setSkip(Integer.parseInt(xattribs.getString(XML_RECORD_SKIP_ATTRIBUTE)));
		}
		if (xattribs.exists(XML_RECORD_COUNT_ATTRIBUTE)){
			aDataWriter.setNumRecords(Integer.parseInt(xattribs.getString(XML_RECORD_COUNT_ATTRIBUTE)));
		}
		if(xattribs.exists(XML_MK_DIRS_ATTRIBUTE)) {
			aDataWriter.setMkDirs(xattribs.getBoolean(XML_MK_DIRS_ATTRIBUTE));
        }
		
		return aDataWriter;
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

    /**
     * Sets number of skipped records in next call of getNext() method.
     * @param skip
     */
    public void setSkip(int skip) {
        this.skip = skip;
    }

    /**
     * Sets number of written records.
     * @param numRecords
     */
    public void setNumRecords(int numRecords) {
        this.numRecords = numRecords;
    }

	/**
	 * Sets make directory.
	 * @param mkDir - true - creates output directories for output file
	 */
	private void setMkDirs(boolean mkDir) {
		this.mkDir = mkDir;
	}
}
