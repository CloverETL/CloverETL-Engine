
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
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.formatter.DelimitedDataFormatter;
import org.jetel.data.formatter.FixLenDataFormatter;
import org.jetel.data.formatter.Formatter;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.ComponentXMLAttributes;
import org.jetel.util.exec.LoggerDataConsumer;
import org.jetel.util.exec.ProcBox;
import org.w3c.dom.Element;

/**
 * @author avackova (agata.vackova@javlinconsulting.cz) ; 
 * (c) JavlinConsulting s.r.o.
 *  www.javlinconsulting.cz
 *
 * @since Aug 3, 2007
 *
 */
public class Db2DataWriter extends Node {
	
	private enum LoadeMode{
		insert,
		replace,
		restart,
		terminate
	}
	
	private static final String XML_DATABASE_ATTRIBUTE = "database";
	private static final String XML_USERNAME_ATTRIBUTE = "userName";
	private static final String XML_PASSWORD_ATTRIBUTE = "password";	
    private static final String XML_TABLE_ATTRIBUTE = "table";
    private static final String XML_MODE_ATTRIBUTE = "loadMode";
    private static final String XML_FILEURL_ATTRIBUTE = "fileURL";
    private static final String XML_FILEMETYADATA_ATTRIBUTE = "fileMetadata";
    private static final String XML_USEPIPE_ATTRIBUTE = "usePipe";
    private static final String XML_COLUMNDELIMITER_ATTRIBUTE = "columnDelimiter";
	
	public final static String COMPONENT_TYPE = "DB2_DATA_WRITER";

	static Log logger = LogFactory.getLog(Db2DataWriter.class);

	private final String FIXLEN_DATA = "asc";
	private final String DELIMITED_DATA = "del";

	private final String PIPE_NAME = "dataPipe";
	
	private final int READ_FROM_PORT = 0;
	
	private String database;
	private String user;
	private String psw;
	private String fileName;
	private String fileMetadataName;
	private String table;
	private LoadeMode loadMode;
	private boolean usePipe = false;
	private boolean delimitedData;
	private char columnDelimiter = 0;
	
	private String command;
	private Formatter formatter;
	private DataRecordMetadata fileMetadata;
	private Process proc;
	private DataRecordMetadata inMetadata;
	private LoggerDataConsumer consumer;
	private LoggerDataConsumer errConsumer;
	private ProcBox box;
	private InputPort inPort;
	private DataRecord inRecord;

	public Db2DataWriter(String id, String database, String user, String psw, String table, 
			LoadeMode mode,	String fileName, String fileMetadataId) {
		super(id);
		this.database = database;
		this.user = user;
		this.psw = psw;
		this.fileName = fileName;
		this.fileMetadataName = fileMetadataId;
		this.table = table;
		this.loadMode = mode;
	}

	@Override
	public void init() throws ComponentNotReadyException {
		super.init();
		
		if (fileMetadataName != null) {
			fileMetadata = getGraph().getDataRecordMetadata(fileMetadataName);
		}
		if (!getInPorts().isEmpty()) {
			if (usePipe) {//TODO for Windows
				fileName = getGraph().getRuntimeParameters().getTmpDir() + '/' + PIPE_NAME;
			}else{
				fileName = getGraph().getRuntimeParameters().getTmpDir() + '/' + PIPE_NAME + ".txt";
			}
			inMetadata = getInputPort(READ_FROM_PORT).getMetadata();
			delimitedData = inMetadata.getRecType() != DataRecordMetadata.FIXEDLEN_RECORD;
			if (columnDelimiter == 0) {
				columnDelimiter = getColumnDelimiter(inMetadata);
			}
			if (fileMetadata == null) {
				switch (inMetadata.getRecType()) {
				case DataRecordMetadata.DELIMITED_RECORD:
					fileMetadata = inMetadata.duplicate();
					for(int i = 0; i<fileMetadata.getNumFields();i++) {
						fileMetadata.getField(i).setDelimiter(String.valueOf(columnDelimiter));
					}
					break;
				case DataRecordMetadata.FIXEDLEN_RECORD:
					fileMetadata = inMetadata;
					break;
				case DataRecordMetadata.MIXED_RECORD:
					fileMetadata = convertToDelimited(inMetadata);
					break;
				default:
					throw new ComponentNotReadyException("Unknown record type: " + inMetadata.getRecType());
				}
			}
			if (delimitedData) {
				formatter = new DelimitedDataFormatter();//TODO code page
			}else{
				formatter = new FixLenDataFormatter();//TODO code page
			}
			formatter.init(fileMetadata);
			try {
				formatter.setDataTarget(new FileOutputStream(fileName));
			} catch (Exception e) {
				throw new ComponentNotReadyException(this, e);
			}			
		}else{
			if (fileMetadata == null) throw new ComponentNotReadyException(this,"File metadata have to be defined");
			delimitedData = checkMetadata(fileMetadata) == DataRecordMetadata.DELIMITED_RECORD;
		}
		
		command = prepareCommand();
	}
	
	private DataRecordMetadata convertToDelimited(DataRecordMetadata metadata){
		//TODO
		return metadata;
	}
	
	private char getColumnDelimiter(DataRecordMetadata metadata){
		//TODO delimiter z pierwszego pola, ktore jest delimited, maksymalnie 1 znak
		return ',';
	}
	
	private char checkMetadata(DataRecordMetadata metadata) throws ComponentNotReadyException{
		switch (metadata.getRecType()) {
		case DataRecordMetadata.DELIMITED_RECORD:
			if (!metadata.getField(metadata.getNumFields() - 1).getDelimiter().equals("\n")) 
				throw new ComponentNotReadyException(this, "Last field delimiter has to be '\\n'");
			else if (metadata.getNumFields() > 1){
				String fDelimiter = metadata.getField(0).getDelimiter();
				if (fDelimiter.length() != 1) throw new ComponentNotReadyException(this, "Only one char delimiter allowed as a column delimiter");
				columnDelimiter = fDelimiter.charAt(0);
				String fieldDelimiter;
				for (int i = 1; i < metadata.getNumFields() - 1; i++){
					fieldDelimiter = metadata.getField(1).getDelimiter();
					if (!(fieldDelimiter.length() == 1)) {
						throw new ComponentNotReadyException(this, "Only one char delimiter allowed as a column delimiter");
					}
					if (fieldDelimiter.charAt(0) != columnDelimiter) {
						throw new ComponentNotReadyException(this, "Wrong column delimiter for field " + i);
					}
				}
			}
			return DataRecordMetadata.DELIMITED_RECORD;
		case DataRecordMetadata.FIXEDLEN_RECORD:
			return DataRecordMetadata.FIXEDLEN_RECORD;
		default:
			throw new ComponentNotReadyException(this, "Only fixlen or delimited metadata allowed");
		}
	}
	
	private String prepareCommand() {
		StringBuilder command = new StringBuilder("db2 load client from '");
		command.append(fileName);
		command.append("' of ");
		command.append(delimitedData ? DELIMITED_DATA : FIXLEN_DATA);
		command.append(' ');
		command.append(loadMode);
		command.append(" into ");
		command.append(table);
		return command.toString();
	}
	
	private int runWithPipe() throws IOException, InterruptedException, JetelException{
		proc = Runtime.getRuntime().exec(command);
		box = new ProcBox(proc, null, consumer, errConsumer);
		try {
			while (runIt && ((inRecord = inPort.readRecord(inRecord)) != null)) {
				formatter.write(inRecord);
			}
		} catch (Exception e) {
			throw new JetelException("Problem with reading input", e);
		}finally {
			formatter.close();
		}
		
		return box.join();
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.graph.Node#execute()
	 */
	@Override
	public Result execute() throws Exception {
		inPort = getInputPort(READ_FROM_PORT);
		inRecord =null;
		int exitValue = 0;
		
		if (!getInPorts().isEmpty() && usePipe) {//TODO for Windows
			try {
				proc = Runtime.getRuntime().exec("mkfifo " + fileName);
				box = new ProcBox(proc, null, consumer, errConsumer);
				exitValue = box.join();
			} catch (Exception e) {
				proc.destroy();
				proc = Runtime.getRuntime().exec("rm " + fileName);
				if (proc.waitFor() != 0) {
					logger.warn("Pipe was not deleted.");
				}
				throw e;
			}
		}
		
		try {
			proc = Runtime.getRuntime().exec("db2 connect to " + database + " user " + user + " using " + psw);
			exitValue = proc.waitFor();
		} catch (Exception e){
			proc.destroy();
			throw e;
		}

		if (exitValue != 0) {
			logger.error("Connection to database failed");
			logger.error("db2 connect exited with value: " + exitValue);
			throw new JetelException("Process exit value is not 0");
		}
		
		if (inMetadata != null) {
			inRecord = new DataRecord(inMetadata);
			inRecord.init();
		}
		
		consumer = new LoggerDataConsumer(LoggerDataConsumer.LVL_DEBUG, 0);
		errConsumer = new LoggerDataConsumer(LoggerDataConsumer.LVL_ERROR, 0);

		try {
			if (!getInPorts().isEmpty() && usePipe) {//TODO for Windows
				exitValue = runWithPipe();
				File pipe = new File(fileName);
				if (!pipe.delete()){
					logger.warn("Pipe was not deleted.");
				}
			}else {
				if (!getInPorts().isEmpty()) {
					try {
						while (runIt && ((inRecord = inPort.readRecord(inRecord)) != null)) {
							formatter.write(inRecord);
						}
					} catch (Exception e) {
						throw e;
					}finally {
						formatter.close();
					}
				}
				if (runIt) {
					proc = Runtime.getRuntime().exec(command);
					box = new ProcBox(proc, null, consumer, errConsumer);
					exitValue = box.join();
				}						
				if (!getInPorts().isEmpty()) {
					File tmpFile = new File(fileName);
					if (!tmpFile.delete()){
						logger.warn("Tmp file was not deleted.");
					}
				}
			}
		} catch (Exception e) {
			proc.destroy();
			throw e;
		}finally{
			try {
				proc = Runtime.getRuntime().exec("db2 disconnect " + database);
			} catch (Exception e) {
				proc.destroy();
				throw e;
			}
		}

		boolean error = false;
		
		if (exitValue != 0) {
			logger.error("Loading to database failed");
			logger.error("db2 load exited with value: " + exitValue);
			error = true;
		}
		
		exitValue = proc.waitFor();

		if (exitValue != 0) {
			logger.error("Disconnecting to database failed");
			logger.error("db2 disconnect exited with value: " + exitValue);
		}
		
		if (error || exitValue != 0) {
			throw new JetelException("Process exit value is not 0");
		}
		
		return runIt ? Result.FINISHED_OK : Result.ABORTED;
	}

    public static Node fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException {
        ComponentXMLAttributes xattribs = new ComponentXMLAttributes(xmlElement, graph);

        try {
            Db2DataWriter writer = new Db2DataWriter(xattribs.getString(XML_ID_ATTRIBUTE),
                    xattribs.getString(XML_DATABASE_ATTRIBUTE),
                    xattribs.getString(XML_USERNAME_ATTRIBUTE),
                    xattribs.getString(XML_PASSWORD_ATTRIBUTE),
                    xattribs.getString(XML_TABLE_ATTRIBUTE),
                    LoadeMode.valueOf(xattribs.getString(XML_MODE_ATTRIBUTE)),
                    xattribs.getString(XML_FILEURL_ATTRIBUTE, null),
                    xattribs.getString(XML_FILEMETYADATA_ATTRIBUTE, null));
            if(xattribs.exists(XML_USEPIPE_ATTRIBUTE)) {
                writer.setUsePipe(xattribs.getBoolean(XML_USEPIPE_ATTRIBUTE));
            }
            if(xattribs.exists(XML_COLUMNDELIMITER_ATTRIBUTE)) {
                writer.setColumnDelimiter((xattribs.getString(XML_COLUMNDELIMITER_ATTRIBUTE).charAt(0)));
            }
            return writer;
        } catch (Exception ex) {
               throw new XMLConfigurationException(COMPONENT_TYPE + ":" + xattribs.getString(XML_ID_ATTRIBUTE," unknown ID ") + ":" + ex.getMessage(),ex);
        }
    }
	
	
	/* (non-Javadoc)
	 * @see org.jetel.graph.Node#getType()
	 */
	@Override
	public String getType() {
		return COMPONENT_TYPE;
	}

	public char getColumnDelimiter() {
		return columnDelimiter;
	}

	public void setColumnDelimiter(char columnDelimiter) {
		this.columnDelimiter = columnDelimiter;
	}

	public boolean isUsePipe() {
		return usePipe;
	}

	public void setUsePipe(boolean usePipe) {
		this.usePipe = usePipe;
	}

}
