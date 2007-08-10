
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
import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
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
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.ComponentXMLAttributes;
import org.jetel.util.StringUtils;
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
    private static final String XML_USEPIPE_ATTRIBUTE = "useNamedPipe";
    private static final String XML_COLUMNDELIMITER_ATTRIBUTE = "columnDelimiter";
	private static final String XML_INTERPRETER_ATTRIBUTE = "interpreter";
	private static final String XML_FIELDMAP_ATTRIBUTE = "fieldMap";
	private static final String XML_CLOVERFIELDS_ATTRIBUTE = "cloverFields";
	private static final String XML_DBFIELDS_ATTRIBUTE = "dbFields";
	private static final String XML_PARAMETERS_ATTRIBUTE = "parameters";
	private static final String XML_REJECTEDRECORDSURL_ATTRIBUTE = "rejectedURL";//on server
	
    private static final String DEFAULT_COLUMN_DELIMITER = ",";
    private static final char EQUAL_CHAR = '=';

    public static final String DEFAULT_DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
	public static final String DEFAULT_DATE_FORMAT = "yyyy-MM-dd";
	public static final String DEFAULT_TIME_FORMAT = "HH:mm:ss";

	public static final short DATE_FIELD_LENGTH = (short)DEFAULT_DATETIME_FORMAT.length();
    
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
	private String interpreter;
	private String parameters;
	private String rejectedURL;
	
	private Formatter formatter;
	private DataRecordMetadata fileMetadata;
	private Process proc;
	private DataRecordMetadata inMetadata;
	private LoggerDataConsumer consumer;
	private LoggerDataConsumer errConsumer;
	private ProcBox box;
	private InputPort inPort;
	private DataRecord inRecord;
	private File batch;
	private String command;
	private Properties properties;
	private String[] cloverFields;
	private String[] dbFields;

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
		
		properties = new Properties();
		if (parameters != null){
			String[] param = parameters.split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);
			int index;
			for (String string : param) {
				index = string.indexOf(EQUAL_CHAR);
				if (index > -1) {
					properties.put(string.substring(0, index).toLowerCase(), string.substring(index + 1).toLowerCase());
				}else{
					properties.put(string.toLowerCase(), String.valueOf(true));
				}
			}
		}
		
		if (fileMetadataName != null) {
			fileMetadata = getGraph().getDataRecordMetadata(fileMetadataName);
		}
		
		if (columnDelimiter != 0) {
			properties.put("coldel", columnDelimiter);
		}else if (properties.contains("coldel")) {
			columnDelimiter = properties.getProperty("coldel").charAt(0);
		}
		
		if (!getInPorts().isEmpty()) {
			String tmpDir = getGraph().getRuntimeParameters().getTmpDir();
			if (!tmpDir.endsWith(File.separator)) {
				tmpDir = tmpDir.concat(File.separator);
			}
			if (usePipe) {
				if (System.getProperty("os.name").startsWith("Windows")) {
					logger.warn("Pipe transfer not supported on Windows - switching it off");
					usePipe = false;
					fileName = tmpDir + PIPE_NAME + ".txt";
				}else{
					fileName =  tmpDir + PIPE_NAME;
				}
			}else{
				fileName = tmpDir + PIPE_NAME + ".txt";
			}
			inMetadata = getInputPort(READ_FROM_PORT).getMetadata();
			delimitedData = inMetadata.getRecType() != DataRecordMetadata.FIXEDLEN_RECORD;
			if (fileMetadata == null) {
				switch (inMetadata.getRecType()) {
				case DataRecordMetadata.FIXEDLEN_RECORD:
					fileMetadata = setDb2DateFormat(inMetadata);
					break;
				case DataRecordMetadata.DELIMITED_RECORD:
				case DataRecordMetadata.MIXED_RECORD:
					fileMetadata = setDb2DateFormat(convertToDb2Delimited(inMetadata));
					break;
				default:
					throw new ComponentNotReadyException(
							"Unknown record type: " + inMetadata.getRecType());
				}
			}else{
				delimitedData = checkMetadata(fileMetadata) == DataRecordMetadata.DELIMITED_RECORD;
				fileMetadata = setDb2DateFormat(fileMetadata);
			}
			if (delimitedData) {
				formatter = new DelimitedDataFormatter("UTF-8");
			}else{
				formatter = new FixLenDataFormatter("UTF-8");
			}
			properties.put("codepage", "1208");
			formatter.init(fileMetadata);
		}else{
			//TODO nie musza byc dane
			if (fileMetadata == null) throw new ComponentNotReadyException(this,"File metadata have to be defined");
			delimitedData = checkMetadata(fileMetadata) == DataRecordMetadata.DELIMITED_RECORD;
		}
		
		try {
			if (interpreter!=null){
				if (interpreter.contains("${}")){
					command = new String(interpreter.replace("${}",prepareBatch()));
				}else {
					throw new ComponentNotReadyException("Incorect form of "+
							XML_INTERPRETER_ATTRIBUTE + " attribute:" + interpreter +
							"\nUse form:\"interpreter [parameters] ${} [parameters]\"");
				}
			}else{
				command = (System.getProperty("os.name").startsWith("Windows") ? "db2cmd /c /i /w" : "") +
						"db2 -f " + prepareBatch();
			}
		} catch (IOException e) {
			throw new ComponentNotReadyException(this, e);
		}
	}
	
	private DataRecordMetadata setDb2DateFormat(DataRecordMetadata metadata){
		DataRecordMetadata out = metadata.duplicate();
		DataFieldMetadata field;
		short recSize = 0;
		boolean isDate;
		boolean isTime;
		String formatString;
		for (int i=0; i< out.getNumFields(); i++){
			field = out.getField(i);
			if (field.getType() == DataFieldMetadata.DATE_FIELD || field.getType() == DataFieldMetadata.DATETIME_FIELD) {
				formatString = field.getFormatStr();
				isDate = formatString == null || 
						 formatString.contains("G") || formatString.contains("y") || 
						 formatString.contains("M") || formatString.contains("w") || 
						 formatString.contains("W") || formatString.contains("d") ||
						 formatString.contains("D") || formatString.contains("F") ||
						 formatString.contains("E");
				isTime = formatString == null || 
				 		 formatString.contains("a") || formatString.contains("H") || 
						 formatString.contains("h") || formatString.contains("K") || 
						 formatString.contains("k") || formatString.contains("m") ||
						 formatString.contains("s") || formatString.contains("S") ||
						 formatString.contains("z") || formatString.contains("Z");
				if (isDate && isTime) {
					field.setFormatStr(DEFAULT_DATETIME_FORMAT);
				}else if (isDate) {
					field.setFormatStr(DEFAULT_DATE_FORMAT);
				}else{
					field.setFormatStr(DEFAULT_TIME_FORMAT);
				}
				if (out.getRecType() == DataRecordMetadata.FIXEDLEN_RECORD) {
					field = new DataFieldMetadata(field.getName(), field.getType(), DATE_FIELD_LENGTH);
				}
			}
			recSize += field.getSize(); 
		}
		if (out.getRecType() == DataRecordMetadata.FIXEDLEN_RECORD) {
			out.setRecordSize(recSize);
			properties.put("reclen", String.valueOf(recSize));
		}
		properties.put("dateformat", DEFAULT_DATE_FORMAT);
		properties.put("timeformat", DEFAULT_TIME_FORMAT);
		properties.put("timestampformat", DEFAULT_DATETIME_FORMAT);
		return out;
	}
		
	private DataRecordMetadata convertToDb2Delimited(DataRecordMetadata metadata){
		DataRecordMetadata fMetadata = new DataRecordMetadata(metadata.getName() + "_fixlen", DataRecordMetadata.DELIMITED_RECORD);
		boolean delimiterFound = columnDelimiter != 0;
		int delimiterFieldIndex = -1;
		DataFieldMetadata field, newField;
		for (int i=0; i < metadata.getNumFields() - 1; i++){
			field = metadata.getField(i);
			newField = field.duplicate();
			if (delimiterFound){
				newField.setDelimiter(String.valueOf(columnDelimiter));
			}
			if (!delimiterFound && field.isDelimited()){
				if (field.getDelimiter().length() == 1) {
					delimiterFound = true;
					columnDelimiter = field.getDelimiter().charAt(0);
					delimiterFieldIndex = i;
				}
			}
			fMetadata.addField(newField);
		}
		for (int i=0; i< delimiterFieldIndex; i++){
			fMetadata.getField(i).setDelimiter(String.valueOf(columnDelimiter));
		}
		newField = metadata.getField(metadata.getNumFields() - 1);
		newField.setDelimiter("\n");
		fMetadata.addField(newField);
		return fMetadata;
	}
	
	private char checkMetadata(DataRecordMetadata metadata) throws ComponentNotReadyException{
		switch (metadata.getRecType()) {
		case DataRecordMetadata.DELIMITED_RECORD:
			if (!metadata.getField(metadata.getNumFields() - 1).getDelimiter().equals("\n")) 
				throw new ComponentNotReadyException(this, "Last field delimiter has to be '\\n'");
			else if (metadata.getNumFields() > 1){
				String fDelimiter = metadata.getField(0).getDelimiter();
				if (fDelimiter.length() != 1) throw new ComponentNotReadyException(this, "Only one char delimiter allowed as a column delimiter");
				if (columnDelimiter == 0) {
					columnDelimiter = fDelimiter.charAt(0);
				}else if (fDelimiter.charAt(0) != columnDelimiter){
					throw new ComponentNotReadyException(this, "Wrong column delimiter for field 0");
				}
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
	
	private String prepareBatch() throws IOException{
		String tmp;
		batch =  File.createTempFile("tmp",".bat",new File("."));
		FileWriter batchWriter = new FileWriter(batch);

		StringBuilder command = new StringBuilder("connect to ");
		command.append(database);
		command.append(" user ");
		command.append(user);
		command.append(" using ");
		command.append(psw);
		command.append("\n");
		batchWriter.write(command.toString());
		
		command.setLength(0);
		command.append("load client from '");
		command.append(fileName);
		command.append("' of ");
		command.append(delimitedData ? DELIMITED_DATA : FIXLEN_DATA);
		command.append(' ');
//		command.append('\n');
		batchWriter.write(command.toString());
		
		boolean modifiedWritten =false;
		command.setLength(0);
		if (properties.containsKey("anyorder") && properties.getProperty("anyorder").equals("true")) {
			if (!modifiedWritten) {
				command.append("modified by ");
				modifiedWritten = true;
			}
			command.append("anyorder ");
		}
		if (properties.containsKey("generatedignore") && properties.getProperty("generatedignore").equals("true")) {
			if (!modifiedWritten) {
				command.append("modified by ");
				modifiedWritten = true;
			}
			command.append("generatedignore ");
		}
		if (properties.containsKey("generatedmissing") && properties.getProperty("generatedmissing").equals("true")) {
			if (!modifiedWritten) {
				command.append("modified by ");
				modifiedWritten = true;
			}
			command.append("generatedmissing ");
		}
		if (properties.containsKey("generatedoverride") && properties.getProperty("generatedoverride").equals("true")) {
			if (!modifiedWritten) {
				command.append("modified by ");
				modifiedWritten = true;
			}
			command.append("generatedoverride ");
		}
		if (properties.containsKey("identityignore") && properties.getProperty("identityignore").equals("true")) {
			if (!modifiedWritten) {
				command.append("modified by ");
				modifiedWritten = true;
			}
			command.append("identityignore ");
		}
		if (properties.containsKey("identitymissing") && properties.getProperty("identitymissing").equals("true")) {
			if (!modifiedWritten) {
				command.append("modified by ");
				modifiedWritten = true;
			}
			command.append("identitymissing ");
		}
		if (properties.containsKey("identityoverride") && properties.getProperty("identityoverride").equals("true")) {
			if (!modifiedWritten) {
				command.append("modified by ");
				modifiedWritten = true;
			}
			command.append("identityoverride ");
		}
		if (properties.containsKey("indexfreespace")) {
			if (!modifiedWritten) {
				command.append("modified by ");
				modifiedWritten = true;
			}
			command.append("indexfreespace=");
			command.append(properties.getProperty("indexfreespace"));
			command.append(' ');
		}
		if (properties.containsKey("lobsurl")) {
			if (!modifiedWritten) {
				command.append("modified by ");
				modifiedWritten = true;
			}
			command.append("lobsinfile ");
		}
		if (properties.containsKey("noheader") && properties.getProperty("noheader").equals("true")) {
			if (!modifiedWritten) {
				command.append("modified by ");
				modifiedWritten = true;
			}
			command.append("noheader ");
		}
		if (properties.containsKey("norowwarnings") && properties.getProperty("norowwarnings").equals("true")) {
			if (!modifiedWritten) {
				command.append("modified by ");
				modifiedWritten = true;
			}
			command.append("norowwarnings ");
		}
		if (properties.containsKey("pagefreespace")) {
			if (!modifiedWritten) {
				command.append("modified by ");
				modifiedWritten = true;
			}
			command.append("pagefreespace=");
			command.append(properties.getProperty("pagefreespace"));
			command.append(' ');
		}
		if (properties.containsKey("subtableconvert") && properties.getProperty("subtableconvert").equals("true")) {
			if (!modifiedWritten) {
				command.append("modified by ");
				modifiedWritten = true;
			}
			command.append("subtableconvert ");
		}
		if (properties.containsKey("totalfreespace")) {
			if (!modifiedWritten) {
				command.append("modified by ");
				modifiedWritten = true;
			}
			command.append("totalfreespace=");
			command.append(properties.getProperty("totalfreespace"));
			command.append(' ');
		}
		if (properties.containsKey("usedefaults") && properties.getProperty("usedefaults").equals("true")) {
			if (!modifiedWritten) {
				command.append("modified by ");
				modifiedWritten = true;
			}
			command.append("usedefaults ");
		}
		if (properties.containsKey("codepage")) {
			if (!modifiedWritten) {
				command.append("modified by ");
				modifiedWritten = true;
			}
			command.append("codepage=");
			command.append(properties.getProperty("codepage"));
			command.append(' ');
		}
		if (properties.containsKey("dateformat")) {
			if (!modifiedWritten) {
				command.append("modified by ");
				modifiedWritten = true;
			}
			command.append("dateformat=");
			tmp = properties.getProperty("dateformat");
			if (!tmp.startsWith("\"")) {
				command.append('"');
			}			
			command.append(tmp);
			if (!tmp.endsWith("\"")) {
				command.append('"');
			}			
			command.append(' ');
		}
		if (rejectedURL != null || properties.containsKey("dumpfile")) {
			if (!modifiedWritten) {
				command.append("modified by ");
				modifiedWritten = true;
			}
			command.append("dumpfile=");
			if (rejectedURL != null) {
				command.append(rejectedURL);
			}else{
				command.append(properties.getProperty("dumpfile"));
			}
			command.append(' ');
		}
		if (properties.containsKey("dumpfileaccessall") && properties.getProperty("dumpfileaccessall").equals("true")) {
			if (!modifiedWritten) {
				command.append("modified by ");
				modifiedWritten = true;
			}
			command.append("dumpfileaccessall ");
		}
		if (properties.containsKey("fastparse") && properties.getProperty("fastparse").equals("true")) {
			if (!modifiedWritten) {
				command.append("modified by ");
				modifiedWritten = true;
			}
			command.append("fastparse ");
		}
		if (properties.containsKey("implieddecimal") && properties.getProperty("implieddecimal").equals("true")) {
			if (!modifiedWritten) {
				command.append("modified by ");
				modifiedWritten = true;
			}
			command.append("implieddecimal ");
		}
		if (properties.containsKey("timeformat")) {
			if (!modifiedWritten) {
				command.append("modified by ");
				modifiedWritten = true;
			}
			command.append("timeformat=");
			tmp = properties.getProperty("timeformat");
			if (!tmp.startsWith("\"")) {
				command.append('"');
			}			
			command.append(tmp);
			if (!tmp.endsWith("\"")) {
				command.append('"');
			}			
			command.append(' ');
		}
		if (properties.containsKey("timestampformat")) {
			if (!modifiedWritten) {
				command.append("modified by ");
				modifiedWritten = true;
			}
			command.append("timestampformat=");
			tmp = properties.getProperty("timestampformat");
			if (!tmp.startsWith("\"")) {
				command.append('"');
			}			
			command.append(tmp);
			if (!tmp.endsWith("\"")) {
				command.append('"');
			}			
			command.append(' ');
		}
		if (properties.containsKey("noeofchar") && properties.getProperty("noeofchar").equals("true")) {
			if (!modifiedWritten) {
				command.append("modified by ");
				modifiedWritten = true;
			}
			command.append("noeofchar ");
		}
		if (properties.containsKey("usegraphiccodepage") && properties.getProperty("usegraphiccodepage").equals("true")) {
			if (!modifiedWritten) {
				command.append("modified by ");
				modifiedWritten = true;
			}
			command.append("usegraphiccodepage ");
		}
		if (properties.containsKey("binarynumerics") && properties.getProperty("binarynumerics").equals("true")) {
			if (!modifiedWritten) {
				command.append("modified by ");
				modifiedWritten = true;
			}
			command.append("binarynumerics ");
		}
		if (properties.containsKey("nochecklengths") && properties.getProperty("nochecklengths").equals("true")) {
			if (!modifiedWritten) {
				command.append("modified by ");
				modifiedWritten = true;
			}
			command.append("nochecklengths ");
		}
		if (properties.containsKey("nullindchar")) {
			if (!modifiedWritten) {
				command.append("modified by ");
				modifiedWritten = true;
			}
			command.append("nullindchar=");
			command.append(properties.getProperty("nullindchar"));
			command.append(' ');
		}
		if (properties.containsKey("packeddecimal") && properties.getProperty("packeddecimal").equals("true")) {
			if (!modifiedWritten) {
				command.append("modified by ");
				modifiedWritten = true;
			}
			command.append("packeddecimal ");
		}
		if (properties.containsKey("reclen")) {
			if (!modifiedWritten) {
				command.append("modified by ");
				modifiedWritten = true;
			}
			command.append("reclen=");
			command.append(properties.getProperty("reclen"));
			command.append(' ');
		}
		if (properties.containsKey("striptblanks") && properties.getProperty("striptblanks").equals("true")) {
			if (!modifiedWritten) {
				command.append("modified by ");
				modifiedWritten = true;
			}
			command.append("striptblanks ");
		}
		if (properties.containsKey("striptnulls") && properties.getProperty("striptnulls").equals("true")) {
			if (!modifiedWritten) {
				command.append("modified by ");
				modifiedWritten = true;
			}
			command.append("striptnulls ");
		}
		if (properties.containsKey("zoneddecimal") && properties.getProperty("zoneddecimal").equals("true")) {
			if (!modifiedWritten) {
				command.append("modified by ");
				modifiedWritten = true;
			}
			command.append("zoneddecimal ");
		}
		if (properties.containsKey("chardel")) {
			if (!modifiedWritten) {
				command.append("modified by ");
				modifiedWritten = true;
			}
			command.append("chardel=");
			command.append(properties.getProperty("chardel"));
			command.append(' ');
		}
		if (properties.containsKey("coldel")) {
			if (!modifiedWritten) {
				command.append("modified by ");
				modifiedWritten = true;
			}
			command.append("coldel=");
			command.append(properties.getProperty("coldel"));
			command.append(' ');
		}
		if (properties.containsKey("datesiso") && properties.getProperty("datesiso").equals("true")) {
			if (!modifiedWritten) {
				command.append("modified by ");
				modifiedWritten = true;
			}
			command.append("datesiso ");
		}
		if (properties.containsKey("decplusblank") && properties.getProperty("decplusblank").equals("true")) {
			if (!modifiedWritten) {
				command.append("modified by ");
				modifiedWritten = true;
			}
			command.append("decplusblank ");
		}
		if (properties.containsKey("decpt")) {
			if (!modifiedWritten) {
				command.append("modified by ");
				modifiedWritten = true;
			}
			command.append("decpt=");
			command.append(properties.getProperty("decpt"));
			command.append(' ');
		}
		if (properties.containsKey("delprioritychar") && properties.getProperty("delprioritychar").equals("true")) {
			if (!modifiedWritten) {
				command.append("modified by ");
				modifiedWritten = true;
			}
			command.append("delprioritychar ");
		}
		if (properties.containsKey("dldel")) {
			if (!modifiedWritten) {
				command.append("modified by ");
				modifiedWritten = true;
			}
			command.append("dldel=");
			command.append(properties.getProperty("dldel"));
			command.append(' ');
		}
		if (properties.containsKey("keepblanks") && properties.getProperty("keepblanks").equals("true")) {
			if (!modifiedWritten) {
				command.append("modified by ");
				modifiedWritten = true;
			}
			command.append("keepblanks ");
		}
		if (properties.containsKey("nochardel") && properties.getProperty("nochardel").equals("true")) {
			if (!modifiedWritten) {
				command.append("modified by ");
				modifiedWritten = true;
			}
			command.append("nochardel ");
		}
		if (properties.containsKey("nodoubledel") && properties.getProperty("nodoubledel").equals("true")) {
			if (!modifiedWritten) {
				command.append("modified by ");
				modifiedWritten = true;
			}
			command.append("nodoubledel ");
		}
		if (modifiedWritten) {
//			command.append("\n");
			batchWriter.write(command.toString());
		}		
		
		command.setLength(0);
		if (properties.containsKey("lobsurl")) {
			command.append("lobs from ");
			command.append(properties.getProperty("lobsurl"));
			command.append(' ');
		}
		batchWriter.write(command.toString());
		
		command.setLength(0);
		if (cloverFields != null) {
			//TODO
			command.append("lobs from ");
			command.append(properties.getProperty("lobsurl"));
			command.append(' ');
		}
		batchWriter.write(command.toString());
		
		command.setLength(0);
		command.append(loadMode);
		command.append(" into ");
		command.append(table);
		if (dbFields != null) {
			command.append(" (");
			command.append(StringUtils.stringArraytoString(dbFields, ','));
			command.append(')');
		}
		command.append("\n");
		batchWriter.write(command.toString());
		
		command.setLength(0);
		command.append("disconnect ");
		command.append(database);
		command.append("\n");
		batchWriter.write(command.toString());

		batchWriter.close();
		return batch.getName();
	}
	
	private int runWithPipe() throws IOException, InterruptedException, JetelException{
		proc = Runtime.getRuntime().exec(command);
		formatter.setDataTarget(new FileOutputStream(fileName));
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
		if (inMetadata != null) {
			inRecord = new DataRecord(fileMetadata);
			inRecord.init();
		}
		
		consumer = new LoggerDataConsumer(LoggerDataConsumer.LVL_DEBUG, 0);
		errConsumer = new LoggerDataConsumer(LoggerDataConsumer.LVL_ERROR, 0);

		int exitValue = 0;
		
		if (!getInPorts().isEmpty() && usePipe) {
			try {
				proc = Runtime.getRuntime().exec("mkfifo " + fileName);
				box = new ProcBox(proc, null, consumer, errConsumer);
				exitValue = box.join();
			} catch (Exception e) {
				if (proc != null) {
					proc.destroy();
				}				
				File pipe = new File(fileName);
				if (!pipe.delete()){
					logger.warn("Pipe was not deleted.");
				}
				if (!batch.delete()){
					logger.warn("Tmp batch was not deleted.");
				}
				throw e;
			}
		}
		
		try {
			if (!getInPorts().isEmpty() && usePipe) {
				exitValue = runWithPipe();
				File pipe = new File(fileName);
				if (!pipe.delete()){
					logger.warn("Pipe was not deleted.");
				}
			}else {
				if (!getInPorts().isEmpty()) {
					formatter.setDataTarget(new FileOutputStream(fileName));
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
			if (proc != null) {
				proc.destroy();
			}			
			throw e;
		}

		if (exitValue != 0) {
			logger.error("Loading to database failed");
			logger.error("db2 load exited with value: " + exitValue);
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
                    LoadeMode.valueOf(xattribs.getString(XML_MODE_ATTRIBUTE, "insert")),
                    xattribs.getString(XML_FILEURL_ATTRIBUTE, null),
                    xattribs.getString(XML_FILEMETYADATA_ATTRIBUTE, null));
			if (xattribs.exists(XML_FIELDMAP_ATTRIBUTE)){
				String[] pairs = xattribs.getString(XML_FIELDMAP_ATTRIBUTE).split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);
				String[] cloverFields = new String[pairs.length];
				String[] dbFields = new String[pairs.length];
				int equalIndex;
				for (int i=0;i<pairs.length;i++){
					equalIndex = pairs[i].indexOf('=');
					cloverFields[i] = pairs[i].substring(0,equalIndex);
					dbFields[i] = StringUtils.quote(pairs[i].substring(equalIndex +1));
				}
				writer.setCloverFields(cloverFields);
				writer.setDBFields(dbFields);
			}else {
				if (xattribs.exists(XML_DBFIELDS_ATTRIBUTE)) {
					String[] dbFields = xattribs.getString(XML_DBFIELDS_ATTRIBUTE).split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);
					for (String string : dbFields) {
						string = StringUtils.quote(string);
					}
					writer.setDBFields(dbFields);
				}
	
				if (xattribs.exists(XML_CLOVERFIELDS_ATTRIBUTE)) {
					writer.setCloverFields(xattribs.getString(XML_CLOVERFIELDS_ATTRIBUTE).split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX));
				}
			}
            if(xattribs.exists(XML_USEPIPE_ATTRIBUTE)) {
                writer.setUsePipe(xattribs.getBoolean(XML_USEPIPE_ATTRIBUTE));
            }
            if(xattribs.exists(XML_COLUMNDELIMITER_ATTRIBUTE)) {
                writer.setColumnDelimiter((xattribs.getString(XML_COLUMNDELIMITER_ATTRIBUTE).charAt(0)));
            }
            if(xattribs.exists(XML_INTERPRETER_ATTRIBUTE)) {
                writer.setInterpreter((xattribs.getString(XML_INTERPRETER_ATTRIBUTE)));
            }
            if(xattribs.exists(XML_PARAMETERS_ATTRIBUTE)) {
                writer.setParameters((xattribs.getString(XML_PARAMETERS_ATTRIBUTE)));
            }
            if(xattribs.exists(XML_REJECTEDRECORDSURL_ATTRIBUTE)) {
                writer.setRejectedURL((xattribs.getString(XML_REJECTEDRECORDSURL_ATTRIBUTE)));
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

	public String getInterpreter() {
		return interpreter;
	}

	public void setInterpreter(String interpreter) {
		this.interpreter = interpreter;
	}

	public String getParameters() {
		return parameters;
	}

	public void setParameters(String parameters) {
		this.parameters = parameters;
	}

	public String getRejectedURL() {
		return rejectedURL;
	}

	public void setRejectedURL(String rejectedURL) {
		this.rejectedURL = rejectedURL;
	}

	public String[] getCloverFields() {
		return cloverFields;
	}

	public void setCloverFields(String[] cloverFields) {
		this.cloverFields = cloverFields;
	}

	public String[] getDbFields() {
		return dbFields;
	}

	public void setDBFields(String[] dbFields) {
		this.dbFields = dbFields;
	}

}
