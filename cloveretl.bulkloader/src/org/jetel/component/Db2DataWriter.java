
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
	private static final String XML_RECORD_COUNT_ATTRIBUTE = "recordCount";
	private static final String XML_RECORD_SKIP_ATTRIBUTE = "recordSkip";
	private static final String XML_MAXERRORS_ATRIBUTE = "maxErrors";
	
	public static final String LOBS_URL_PARAM = "lobsurl";
	public static final String ANY_ORDER_PARAM = "anyorder";
	public static final String GENERATED_IGNORE_PARAM = "generatedignore";
	public static final String GENERATED_MISSING_PARAM = "generatedmissing";
	public static final String GENERATED_OVERRIDE_PARAM = "generatedoverride";
	public static final String IDENTITY_IGNORE_PARAM = "identityignore";
	public static final String IDENTITY_MISSING_PARAM = "identitymissing";
	public static final String IDENTITY_OVERRIDE_PARAM = "identityoverride";
	public static final String INDEX_FREE_SPACE_PARAM = "indexfreespace";
	public static final String NO_HEADER_PARAM = "noheader";
	public static final String NO_ROW_WARNINGS_PARAM = "norowwarnings";
	public static final String PAGE_FREE_SPACE_PARAM = "pagefreespace";
	public static final String SUBTABLE_CONVERT_PARAM = "subtableconvert";
	public static final String TOTAL_FREE_SPACE_PARAM = "totalfreespace";
	public static final String USE_DEFAULTS_PARAM = "usedefaults";
	public static final String CODE_PAGE_PARAM = "codepage";
	public static final String DATE_FORMAT_PARAM = "dateformat";
	public static final String DUMP_FILE_PARAM = "dumpfile";
	public static final String DUMP_FILE_ACCESS_ALL_PARAM = "dumpfileaccessall";
	public static final String FAST_PARSE_PARAM = "fastparse";
	public static final String TIME_FORMAT_PARAM = "timeformat";
	public static final String IMPLIED_DECIMAL_PARAM = "implieddecimal";
	public static final String TIME_STAMP_FORMAT_PARAM = "timestampformat";
	public static final String NO_EOF_CHAR_PARAM = "noeofchar";
	public static final String USER_GRAPHIC_CODE_PAGE_PARAM = "usegraphiccodepage";
	public static final String BINARY_NUMERICS_PARAM = "binarynumerics";
	public static final String NO_CHECK_LENGTHS_PARAM = "nochecklengths";
	public static final String NULL_IND_CHAR_PARAM = "nullindchar";
	public static final String PACKED_DECIMAL_PARAM = "packeddecimal";
	public static final String REC_LEN_PARAM = "reclen";
	public static final String STRIP_BLANKS_PARAM = "striptblanks";
	public static final String STRIP_NULLS_PARAM = "striptnulls";
	public static final String ZONED_DECIMAL_PARAM = "zoneddecimal";
	public static final String CHAR_DEL_PARAM = "chardel";
	public static final String COL_DEL_PARAM = "coldel";
	public static final String DATES_ISO_PARAM = "datesiso";
	public static final String DEC_PLUS_BLANK_PARAM = "decplusblank";
	public static final String DECPT_PARAM = "decpt";
	public static final String DEL_PRIORYTY_CHAR_PARAM = "delprioritychar";
	public static final String DL_DEL_PARAM = "dldel";
	public static final String KEEP_BLANKS_PARAM = "keepblanks";
	public static final String NO_CHAR_DEL_PARAM = "nochardel";
	public static final String NO_DOUBLE_DEL_PARAM = "nodoubledel";
	public static final String NULL_INDICATORS_PARAM = "nullindicators";
	public static final String SAVE_COUNT_PARAM = "savecount";
	public static final String ROW_COUNT_PARAM = "rowcount";
	public static final String WARNING_COUNT_PARAM = "warningcount";
	public static final String MESSAGES_URL_PARAM = "messagesurl";
	public static final String TMP_URL_PARAM = "tmpURL";
	public static final String DL_LINK_TYPE_PARAM = "dl_link_type";
	public static final String DL_URL_DEFAULT_PREFIX_PARAM = "dl_url_default_prefix";
	public static final String DL_URL_REPLACE_PREFIX_PARAM = "dl_url_replace_prefix";
	public static final String DL_URL_SUFFIX_PARAM = "dl_url_suffix";
	public static final String EXCEPTION_TABLE_PARAM = "exceptiontable";
	public static final String STATISTIC_PARAM = "statistics";
	public static final String COPY_PARAM = "copy";
	public static final String USE_TSM_PARAM = "usetsm";
	public static final String NUM_SESSIONS_PARAM = "numsesions";
	public static final String RECOVERY_LIBRARY_PARAM = "recoverylib";
	public static final String COPY_URL_PARAM = "copyurl";
	public static final String NONRECOVERABLE_PARAM = "nonrecoverable";
	public static final String WITHOUT_PROMPTING_PARAM = "withoutprompting";
	public static final String BUFFER_SIZE_PARAM = "buffersize";
	public static final String SORT_BUFFER_SIZE_PARAM = "sortbuffersize";
	public static final String CPU_NUM_PARAM = "cpunum";
	public static final String DISK_NUM_PARAM = "disknum";
	public static final String INDEXING_MODE_PARAM = "indexingmode";//autoselect, rebuild, incremental, deferred
	public static final String ALLOW_READ_ACCESS_PARAM = "allowreadaccess";
	public static final String INDEX_COPY_TABLE_PARAM = "indexcopytable";
	public static final String CHECK_PENDING_CASCADE_PARAM = "checkpendingcascade";//immediate, deferred
	public static final String LOCK_WITH_FORCE_PARAM = "lockwithforce";
	public static final String PARTITIONED_CONFIG_PARAM = "partitionedConfig";// @see http://publib.boulder.ibm.com/infocenter/db2luw/v8/topic/com.ibm.db2.udb.doc/admin/r0004613.htm
	
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
	private int recordSkip = -1;
	private int skipped = 0;
	
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
	private Properties properties = new Properties();
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
		
		if (parameters != null){
			String[] param = parameters.split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);
			int index;
			for (String string : param) {
				index = string.indexOf(EQUAL_CHAR);
				if (index > -1) {
					properties.setProperty(string.substring(0, index).toLowerCase(), string.substring(index + 1).toLowerCase());
				}else{
					properties.setProperty(string.toLowerCase(), String.valueOf(true));
				}
			}
		}
		
		if (columnDelimiter != 0) {
			properties.put(COL_DEL_PARAM, columnDelimiter);
		}else if (properties.contains(COL_DEL_PARAM)) {
			columnDelimiter = properties.getProperty(COL_DEL_PARAM).charAt(0);
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

			if (fileMetadataName != null) {
				fileMetadata = getGraph().getDataRecordMetadata(fileMetadataName);
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
			properties.put(CODE_PAGE_PARAM, "1208");
			formatter.init(fileMetadata);
		}else{
			//TODO nie musza byc dane metadata
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
			properties.put(REC_LEN_PARAM, String.valueOf(recSize));
		}
		properties.put(DATE_FORMAT_PARAM, DEFAULT_DATE_FORMAT);
		properties.put(TIME_FORMAT_PARAM, DEFAULT_TIME_FORMAT);
		properties.put(TIME_STAMP_FORMAT_PARAM, DEFAULT_DATETIME_FORMAT);
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
	
	private String prepareConnectCommand(){
		return "connect to " + database + " user " + user + " using " + psw + "\n";
	}
	
	private String prepareDisconnectCommand(){
		return "disconnect " + database + "\n";
	}
	
	private String prepareLoadCommand() throws ComponentNotReadyException{
		String tmp;
		String MODIFIED = "modified by ";
		StringBuilder command = new StringBuilder("load client from '");
		command.append(fileName);
		command.append("' of ");
		command.append(delimitedData ? DELIMITED_DATA : FIXLEN_DATA);
		command.append(' ');
//		command.append('\n');
		
		if (properties.containsKey(LOBS_URL_PARAM)) {
			command.append("lobs from ");
			command.append(properties.getProperty(LOBS_URL_PARAM));
			command.append(' ');
		}
		
		boolean modifiedWritten =false;
		if (properties.containsKey(ANY_ORDER_PARAM) && properties.getProperty(ANY_ORDER_PARAM).equals("true")) {
			if (!modifiedWritten) {
				command.append(MODIFIED);
				modifiedWritten = true;
			}
			command.append(ANY_ORDER_PARAM);
			command.append(' ');
		}
		if (properties.containsKey(GENERATED_IGNORE_PARAM) && properties.getProperty(GENERATED_IGNORE_PARAM).equals("true")) {
			if (!modifiedWritten) {
				command.append(MODIFIED);
				modifiedWritten = true;
			}
			command.append(GENERATED_IGNORE_PARAM);
			command.append(' ');
		}
		if (properties.containsKey(GENERATED_MISSING_PARAM) && properties.getProperty(GENERATED_MISSING_PARAM).equals("true")) {
			if (!modifiedWritten) {
				command.append(MODIFIED);
				modifiedWritten = true;
			}
			command.append(GENERATED_MISSING_PARAM);
			command.append(' ');
		}
		if (properties.containsKey(GENERATED_OVERRIDE_PARAM) && properties.getProperty(GENERATED_OVERRIDE_PARAM).equals("true")) {
			if (!modifiedWritten) {
				command.append(MODIFIED);
				modifiedWritten = true;
			}
			command.append(GENERATED_OVERRIDE_PARAM);
			command.append(' ');
		}
		if (properties.containsKey(IDENTITY_IGNORE_PARAM) && properties.getProperty(IDENTITY_IGNORE_PARAM).equals("true")) {
			if (!modifiedWritten) {
				command.append(MODIFIED);
				modifiedWritten = true;
			}
			command.append(IDENTITY_IGNORE_PARAM);
			command.append(' ');
		}
		if (properties.containsKey(IDENTITY_MISSING_PARAM) && properties.getProperty(IDENTITY_MISSING_PARAM).equals("true")) {
			if (!modifiedWritten) {
				command.append(MODIFIED);
				modifiedWritten = true;
			}
			command.append(IDENTITY_MISSING_PARAM);
			command.append(' ');
		}
		if (properties.containsKey(IDENTITY_OVERRIDE_PARAM) && properties.getProperty(IDENTITY_OVERRIDE_PARAM).equals("true")) {
			if (!modifiedWritten) {
				command.append(MODIFIED);
				modifiedWritten = true;
			}
			command.append(IDENTITY_OVERRIDE_PARAM);
			command.append(' ');
		}
		if (properties.containsKey(INDEX_FREE_SPACE_PARAM)) {
			if (!modifiedWritten) {
				command.append(MODIFIED);
				modifiedWritten = true;
			}
			command.append(INDEX_FREE_SPACE_PARAM);
			command.append('=');
			command.append(properties.getProperty(INDEX_FREE_SPACE_PARAM));
			command.append(' ');
		}
		if (properties.containsKey(LOBS_URL_PARAM)) {
			if (!modifiedWritten) {
				command.append(MODIFIED);
				modifiedWritten = true;
			}
			command.append("lobsinfile ");
		}
		if (properties.containsKey(NO_HEADER_PARAM) && properties.getProperty(NO_HEADER_PARAM).equals("true")) {
			if (!modifiedWritten) {
				command.append(MODIFIED);
				modifiedWritten = true;
			}
			command.append(NO_HEADER_PARAM);
			command.append(' ');
		}
		if (properties.containsKey(NO_ROW_WARNINGS_PARAM) && properties.getProperty(NO_ROW_WARNINGS_PARAM).equals("true")) {
			if (!modifiedWritten) {
				command.append(MODIFIED);
				modifiedWritten = true;
			}
			command.append(NO_ROW_WARNINGS_PARAM);
			command.append(' ');
		}
		if (properties.containsKey(PAGE_FREE_SPACE_PARAM)) {
			if (!modifiedWritten) {
				command.append(MODIFIED);
				modifiedWritten = true;
			}
			command.append(PAGE_FREE_SPACE_PARAM);
			command.append('=');
			command.append(properties.getProperty(PAGE_FREE_SPACE_PARAM));
			command.append(' ');
		}
		if (properties.containsKey(SUBTABLE_CONVERT_PARAM) && properties.getProperty(SUBTABLE_CONVERT_PARAM).equals("true")) {
			if (!modifiedWritten) {
				command.append(MODIFIED);
				modifiedWritten = true;
			}
			command.append(SUBTABLE_CONVERT_PARAM);
			command.append(' ');
		}
		if (properties.containsKey(TOTAL_FREE_SPACE_PARAM)) {
			if (!modifiedWritten) {
				command.append(MODIFIED);
				modifiedWritten = true;
			}
			command.append(TOTAL_FREE_SPACE_PARAM);
			command.append('=');
			command.append(properties.getProperty(TOTAL_FREE_SPACE_PARAM));
			command.append(' ');
		}
		if (properties.containsKey(USE_DEFAULTS_PARAM) && properties.getProperty(USE_DEFAULTS_PARAM).equals("true")) {
			if (!modifiedWritten) {
				command.append(MODIFIED);
				modifiedWritten = true;
			}
			command.append(USE_DEFAULTS_PARAM);
			command.append(' ');
		}
		if (properties.containsKey(CODE_PAGE_PARAM)) {
			if (!modifiedWritten) {
				command.append(MODIFIED);
				modifiedWritten = true;
			}
			command.append(CODE_PAGE_PARAM);
			command.append('=');
			command.append(properties.getProperty(CODE_PAGE_PARAM));
			command.append(' ');
		}
		if (properties.containsKey(DATE_FORMAT_PARAM)) {
			if (!modifiedWritten) {
				command.append(MODIFIED);
				modifiedWritten = true;
			}
			command.append(DATE_FORMAT_PARAM);
			command.append('=');
			tmp = properties.getProperty(DATE_FORMAT_PARAM);
			if (!tmp.startsWith("\"")) {
				command.append('"');
			}			
			command.append(tmp);
			if (!tmp.endsWith("\"")) {
				command.append('"');
			}			
			command.append(' ');
		}
		if (rejectedURL != null || properties.containsKey(DUMP_FILE_PARAM)) {
			if (!modifiedWritten) {
				command.append(MODIFIED);
				modifiedWritten = true;
			}
			command.append(DUMP_FILE_PARAM);
			command.append('=');
			if (rejectedURL != null) {
				command.append(rejectedURL);
			}else{
				command.append(properties.getProperty(DUMP_FILE_PARAM));
			}
			command.append(' ');
		}
		if (properties.containsKey(DUMP_FILE_ACCESS_ALL_PARAM) && properties.getProperty(DUMP_FILE_ACCESS_ALL_PARAM).equals("true")) {
			if (!modifiedWritten) {
				command.append(MODIFIED);
				modifiedWritten = true;
			}
			command.append(DUMP_FILE_ACCESS_ALL_PARAM);
			command.append(' ');
		}
		if (properties.containsKey(FAST_PARSE_PARAM) && properties.getProperty(FAST_PARSE_PARAM).equals("true")) {
			if (!modifiedWritten) {
				command.append(MODIFIED);
				modifiedWritten = true;
			}
			command.append(FAST_PARSE_PARAM);
			command.append(' ');
		}
		if (properties.containsKey(IMPLIED_DECIMAL_PARAM) && properties.getProperty(IMPLIED_DECIMAL_PARAM).equals("true")) {
			if (!modifiedWritten) {
				command.append(MODIFIED);
				modifiedWritten = true;
			}
			command.append(IMPLIED_DECIMAL_PARAM);
			command.append(' ');
		}
		if (properties.containsKey(TIME_FORMAT_PARAM)) {
			if (!modifiedWritten) {
				command.append(MODIFIED);
				modifiedWritten = true;
			}
			command.append(TIME_FORMAT_PARAM);
			command.append('=');
			tmp = properties.getProperty(TIME_FORMAT_PARAM);
			if (!tmp.startsWith("\"")) {
				command.append('"');
			}			
			command.append(tmp);
			if (!tmp.endsWith("\"")) {
				command.append('"');
			}			
			command.append(' ');
		}
		if (properties.containsKey(TIME_STAMP_FORMAT_PARAM)) {
			if (!modifiedWritten) {
				command.append(MODIFIED);
				modifiedWritten = true;
			}
			command.append(TIME_STAMP_FORMAT_PARAM);
			command.append('=');
			tmp = properties.getProperty(TIME_STAMP_FORMAT_PARAM);
			if (!tmp.startsWith("\"")) {
				command.append('"');
			}			
			command.append(tmp);
			if (!tmp.endsWith("\"")) {
				command.append('"');
			}			
			command.append(' ');
		}
		if (properties.containsKey(NO_EOF_CHAR_PARAM) && properties.getProperty(NO_EOF_CHAR_PARAM).equals("true")) {
			if (!modifiedWritten) {
				command.append(MODIFIED);
				modifiedWritten = true;
			}
			command.append(NO_EOF_CHAR_PARAM);
			command.append(' ');
		}
		if (properties.containsKey(USER_GRAPHIC_CODE_PAGE_PARAM) && properties.getProperty(USER_GRAPHIC_CODE_PAGE_PARAM).equals("true")) {
			if (!modifiedWritten) {
				command.append(MODIFIED);
				modifiedWritten = true;
			}
			command.append(USER_GRAPHIC_CODE_PAGE_PARAM);
			command.append(' ');
		}
		if (properties.containsKey(BINARY_NUMERICS_PARAM) && properties.getProperty(BINARY_NUMERICS_PARAM).equals("true")) {
			if (!modifiedWritten) {
				command.append(MODIFIED);
				modifiedWritten = true;
			}
			command.append(BINARY_NUMERICS_PARAM);
			command.append(' ');
		}
		if (properties.containsKey(NO_CHECK_LENGTHS_PARAM) && properties.getProperty(NO_CHECK_LENGTHS_PARAM).equals("true")) {
			if (!modifiedWritten) {
				command.append(MODIFIED);
				modifiedWritten = true;
			}
			command.append(NO_CHECK_LENGTHS_PARAM);
			command.append(' ');
		}
		if (properties.containsKey(NULL_IND_CHAR_PARAM)) {
			if (!modifiedWritten) {
				command.append(MODIFIED);
				modifiedWritten = true;
			}
			command.append(NULL_IND_CHAR_PARAM);
			command.append('=');
			command.append(properties.getProperty(NULL_IND_CHAR_PARAM));
			command.append(' ');
		}
		if (properties.containsKey(PACKED_DECIMAL_PARAM) && properties.getProperty(PACKED_DECIMAL_PARAM).equals("true")) {
			if (!modifiedWritten) {
				command.append(MODIFIED);
				modifiedWritten = true;
			}
			command.append(PACKED_DECIMAL_PARAM);
			command.append(' ');
		}
		if (properties.containsKey(REC_LEN_PARAM)) {
			if (!modifiedWritten) {
				command.append(MODIFIED);
				modifiedWritten = true;
			}
			command.append(REC_LEN_PARAM);
			command.append('=');
			command.append(properties.getProperty(REC_LEN_PARAM));
			command.append(' ');
		}
		if (properties.containsKey(STRIP_BLANKS_PARAM) && properties.getProperty(STRIP_BLANKS_PARAM).equals("true")) {
			if (!modifiedWritten) {
				command.append(MODIFIED);
				modifiedWritten = true;
			}
			command.append(STRIP_BLANKS_PARAM);
			command.append(' ');
		}
		if (properties.containsKey(STRIP_NULLS_PARAM) && properties.getProperty(STRIP_NULLS_PARAM).equals("true")) {
			if (!modifiedWritten) {
				command.append(MODIFIED);
				modifiedWritten = true;
			}
			command.append(STRIP_NULLS_PARAM);
			command.append(' ');
		}
		if (properties.containsKey(ZONED_DECIMAL_PARAM) && properties.getProperty(ZONED_DECIMAL_PARAM).equals("true")) {
			if (!modifiedWritten) {
				command.append(MODIFIED);
				modifiedWritten = true;
			}
			command.append(ZONED_DECIMAL_PARAM);
			command.append(' ');
		}
		if (properties.containsKey(CHAR_DEL_PARAM)) {
			if (!modifiedWritten) {
				command.append(MODIFIED);
				modifiedWritten = true;
			}
			command.append(CHAR_DEL_PARAM);
			command.append('=');
			command.append(properties.getProperty(CHAR_DEL_PARAM));
			command.append(' ');
		}
		if (properties.containsKey(COL_DEL_PARAM)) {
			if (!modifiedWritten) {
				command.append(MODIFIED);
				modifiedWritten = true;
			}
			command.append(COL_DEL_PARAM);
			command.append('=');
			command.append(properties.getProperty(COL_DEL_PARAM));
			command.append(' ');
		}
		if (properties.containsKey(DATES_ISO_PARAM) && properties.getProperty(DATES_ISO_PARAM).equals("true")) {
			if (!modifiedWritten) {
				command.append(MODIFIED);
				modifiedWritten = true;
			}
			command.append(DATES_ISO_PARAM);
			command.append(' ');
		}
		if (properties.containsKey(DEC_PLUS_BLANK_PARAM) && properties.getProperty(DEC_PLUS_BLANK_PARAM).equals("true")) {
			if (!modifiedWritten) {
				command.append(MODIFIED);
				modifiedWritten = true;
			}
			command.append(DEC_PLUS_BLANK_PARAM);
			command.append(' ');
		}
		if (properties.containsKey(DECPT_PARAM)) {
			if (!modifiedWritten) {
				command.append(MODIFIED);
				modifiedWritten = true;
			}
			command.append(DECPT_PARAM);
			command.append('=');
			command.append(properties.getProperty(DECPT_PARAM));
			command.append(' ');
		}
		if (properties.containsKey(DEL_PRIORYTY_CHAR_PARAM) && properties.getProperty(DEL_PRIORYTY_CHAR_PARAM).equals("true")) {
			if (!modifiedWritten) {
				command.append(MODIFIED);
				modifiedWritten = true;
			}
			command.append(DEL_PRIORYTY_CHAR_PARAM);
			command.append(' ');
		}
		if (properties.containsKey(DL_DEL_PARAM)) {
			if (!modifiedWritten) {
				command.append(MODIFIED);
				modifiedWritten = true;
			}
			command.append(DL_DEL_PARAM);
			command.append('=');
			command.append(properties.getProperty(DL_DEL_PARAM));
			command.append(' ');
		}
		if (properties.containsKey(KEEP_BLANKS_PARAM) && properties.getProperty(KEEP_BLANKS_PARAM).equals("true")) {
			if (!modifiedWritten) {
				command.append(MODIFIED);
				modifiedWritten = true;
			}
			command.append(KEEP_BLANKS_PARAM);
			command.append(' ');
		}
		if (properties.containsKey(NO_CHAR_DEL_PARAM) && properties.getProperty(NO_CHAR_DEL_PARAM).equals("true")) {
			if (!modifiedWritten) {
				command.append(MODIFIED);
				modifiedWritten = true;
			}
			command.append(NO_CHAR_DEL_PARAM);
			command.append(' ');
		}
		if (properties.containsKey(NO_DOUBLE_DEL_PARAM) && properties.getProperty(NO_DOUBLE_DEL_PARAM).equals("true")) {
			if (!modifiedWritten) {
				command.append(MODIFIED);
				modifiedWritten = true;
			}
			command.append(NO_DOUBLE_DEL_PARAM);
			command.append(' ');
		}
		if (modifiedWritten) {
//			command.append("\n");
		}		
		
		if (cloverFields != null) {
			command.append("method ");
			command.append(delimitedData ? 'P' : 'L');
			command.append(" (");
			int value;
			if (delimitedData) {
				for (int i=0; i<cloverFields.length;i++){
					value  = fileMetadata.getFieldPosition(cloverFields[i]);
					if (value == -1) {
						throw new ComponentNotReadyException(this, "Field " + 
								StringUtils.quote(cloverFields[i]) + " does not exist in metadata " + 
								StringUtils.quote(fileMetadata.getName()));
					}
					command.append(value + 1);
					command.append(',');
				}
				command.setLength(command.length() - 1);
				command.append(") ");
			}else{
				for (int i=0; i<cloverFields.length; i++){
					DataFieldMetadata field = fileMetadata.getField(cloverFields[i]);
					value = fileMetadata.getFieldOffset(cloverFields[i]);
					if (value == -1) {
						throw new ComponentNotReadyException(this, "Field " + 
								StringUtils.quote(cloverFields[i]) + " does not exist in metadata " + 
								StringUtils.quote(fileMetadata.getName()) + " or metadata are not fixlength.");
					}
					command.append(value + 1);
					command.append(' ');
					command.append(value + field.getSize());
					command.append(',');
				}
				command.setLength(command.length() - 1);
				command.append(") ");
				if (properties.containsKey(NULL_INDICATORS_PARAM)) {
					command.append("null indicators (");
					command.append(properties.getProperty(NULL_INDICATORS_PARAM));
					command.append(") ");
				}
				
			}
		}
		
		if (properties.containsKey(SAVE_COUNT_PARAM)) {
			command.append(' ');
			command.append(SAVE_COUNT_PARAM);
			command.append(properties.getProperty(SAVE_COUNT_PARAM));
			command.append(' ');
		}
		
		if (properties.containsKey(ROW_COUNT_PARAM)) {
			command.append(ROW_COUNT_PARAM);
			command.append(' ');
			command.append(properties.getProperty(ROW_COUNT_PARAM));
			command.append(' ');
		}
		
		if (properties.containsKey(WARNING_COUNT_PARAM)) {
			command.append(WARNING_COUNT_PARAM);
			command.append(' ');
			command.append(properties.getProperty(WARNING_COUNT_PARAM));
			command.append(' ');
		}
		
		if (properties.containsKey(MESSAGES_URL_PARAM)) {
			command.append("messages '");
			command.append(properties.getProperty(MESSAGES_URL_PARAM));
			command.append("' ");
		}
		
		if (properties.containsKey(TMP_URL_PARAM)) {
			command.append("tempfiles path '");
			command.append(properties.getProperty(TMP_URL_PARAM));
			command.append("' ");
		}
		
		command.append(loadMode);
		command.append(" into ");
		command.append(table);
		if (dbFields != null) {
			command.append(" (");
			command.append(StringUtils.stringArraytoString(dbFields, ','));
			command.append(") ");
		}

		if (properties.containsKey(EXCEPTION_TABLE_PARAM)) {
			command.append("for exception ");
			command.append(properties.getProperty(EXCEPTION_TABLE_PARAM));
			command.append(" ");
		}
		
		if (properties.containsKey(STATISTIC_PARAM)) {
			command.append("statistics ");
			if (properties.getProperty(STATISTIC_PARAM).equalsIgnoreCase("true")) {
				command.append("use profile");
			}else{
				command.append("no");
			}
			command.append(" ");
		}
		
		if (properties.containsKey(NONRECOVERABLE_PARAM) && (properties.getProperty(STATISTIC_PARAM).equalsIgnoreCase("true"))) {
			command.append("nonrecoverable ");
		}else if (properties.containsKey(COPY_PARAM)) {
			if (!properties.getProperty(COPY_PARAM).equalsIgnoreCase("true")) {
				command.append("copy no ");
			}else{
				command.append("copy yes ");
				if (properties.containsKey(USE_TSM_PARAM)) {
					command.append("use tsm ");
					if (properties.containsKey(NUM_SESSIONS_PARAM)) {
						command.append("open ");
						command.append(properties.getProperty(NUM_SESSIONS_PARAM));
						command.append(" sessions ");
					}
				}
				if (properties.containsKey(COPY_URL_PARAM)) {
					command.append("to '");
					command.append(properties.getProperty(COPY_URL_PARAM));
					command.append("' ");
				}
				if (properties.containsKey(RECOVERY_LIBRARY_PARAM)) {
					command.append("load '");
					command.append(properties.getProperty(RECOVERY_LIBRARY_PARAM));
					command.append("' ");
					if (properties.containsKey(NUM_SESSIONS_PARAM)) {
						command.append("open ");
						command.append(properties.getProperty(NUM_SESSIONS_PARAM));
						command.append(" sessions ");
					}
				}
			}
		}
		
		if (properties.containsKey(WITHOUT_PROMPTING_PARAM) && properties.getProperty(EXCEPTION_TABLE_PARAM).equalsIgnoreCase("true")) {
			command.append("without prompting ");
		}
		
		if (properties.containsKey(BUFFER_SIZE_PARAM)) {
			command.append("data buffer ");
			command.append(properties.getProperty(BUFFER_SIZE_PARAM));
			command.append(" ");
		}

		if (properties.containsKey(SORT_BUFFER_SIZE_PARAM)) {
			command.append("sort buffer ");
			command.append(properties.getProperty(SORT_BUFFER_SIZE_PARAM));
			command.append(" ");
		}

		if (properties.containsKey(CPU_NUM_PARAM)) {
			command.append("cpu_parallelism ");
			command.append(properties.getProperty(CPU_NUM_PARAM));
			command.append(" ");
		}

		if (properties.containsKey(DISK_NUM_PARAM)) {
			command.append("disk_parallelism ");
			command.append(properties.getProperty(DISK_NUM_PARAM));
			command.append(" ");
		}

		if (properties.containsKey(INDEXING_MODE_PARAM)) {
			command.append("indexing mode ");
			command.append(properties.getProperty(INDEXING_MODE_PARAM));
			command.append(" ");
		}

		if (properties.containsKey(ALLOW_READ_ACCESS_PARAM)) {
			if (properties.getProperty(ALLOW_READ_ACCESS_PARAM).equalsIgnoreCase("true")) {
				command.append("allow read access ");
				if (properties.containsKey(INDEX_COPY_TABLE_PARAM)) {
					command.append("use ");
					command.append(properties
							.getProperty(INDEX_COPY_TABLE_PARAM));
					command.append(" ");
				}
			}else{
				command.append("allow no access ");
			}
		}

		if (properties.containsKey(CHECK_PENDING_CASCADE_PARAM)) {
			command.append("check pending cascade ");
			command.append(properties.getProperty(CHECK_PENDING_CASCADE_PARAM));
			command.append(" ");
		}

		if (properties.containsKey(LOCK_WITH_FORCE_PARAM) && properties.getProperty(LOCK_WITH_FORCE_PARAM).equalsIgnoreCase("true")) {
			command.append("lock with force ");
		}
		
		if (properties.containsKey(PARTITIONED_CONFIG_PARAM)) {
			command.append("partitioned db config ");
			command.append(properties.getProperty(PARTITIONED_CONFIG_PARAM));
			command.append(" ");
		}
		
//		if (properties.containsKey(DL_LINK_TYPE_PARAM) || properties.containsKey(DL_URL_DEFAULT_PREFIX_PARAM) ||
//				properties.containsKey(DL_URL_REPLACE_PREFIX_PARAM) || properties.containsKey(DL_URL_SUFFIX_PARAM)) {
//			command.append("datalink specification ");
//			modifiedWritten = false;
//			if (properties.containsKey(DL_LINK_TYPE_PARAM)) {
//				command.append("(");
//				command.append(DL_LINK_TYPE_PARAM);
//				command.append(" url) ");
//				modifiedWritten = true;
//			}
//			if (properties.containsKey(DL_URL_DEFAULT_PREFIX_PARAM)) {
//				if (modifiedWritten) {
//					command.append(", ");
//				}
//				command.append("(");
//				command.append(DL_URL_DEFAULT_PREFIX_PARAM);
//				command.append(' ');
//				tmp = properties.getProperty(DL_URL_DEFAULT_PREFIX_PARAM);
//				if (!tmp.startsWith("\"")) {
//					command.append('"');
//				}
//				command.append(tmp);
//				if (!tmp.endsWith("\"")) {
//					command.append('"');
//				}
//				command.append(") ");
//				modifiedWritten = true;
//			}
//			if (properties.containsKey(DL_URL_REPLACE_PREFIX_PARAM)) {
//				if (modifiedWritten) {
//					command.append(", ");
//				}
//				command.append("(");
//				command.append(DL_URL_REPLACE_PREFIX_PARAM);
//				command.append(' ');
//				tmp = properties.getProperty(DL_URL_REPLACE_PREFIX_PARAM);
//				if (!tmp.startsWith("\"")) {
//					command.append('"');
//				}
//				command.append(tmp);
//				if (!tmp.endsWith("\"")) {
//					command.append('"');
//				}
//				command.append(") ");
//				modifiedWritten = true;
//			}
//			if (properties.containsKey(DL_URL_SUFFIX_PARAM)) {
//				if (modifiedWritten) {
//					command.append(", ");
//				}
//				command.append("(");
//				command.append(DL_URL_SUFFIX_PARAM);
//				command.append(' ');
//				tmp = properties.getProperty(DL_URL_SUFFIX_PARAM);
//				if (!tmp.startsWith("\"")) {
//					command.append('"');
//				}
//				command.append(tmp);
//				if (!tmp.endsWith("\"")) {
//					command.append('"');
//				}
//				command.append(") ");
//			}
//		}
		
		command.append("\n");
		
		return command.toString();
	}
	
	private String prepareBatch() throws IOException, ComponentNotReadyException{
		batch =  File.createTempFile("tmp",".bat",new File("."));
		FileWriter batchWriter = new FileWriter(batch);

		batchWriter.write(prepareConnectCommand());
		batchWriter.write(prepareLoadCommand());
		batchWriter.write(prepareDisconnectCommand());

		batchWriter.close();
		return batch.getName();
	}
	
	private int runWithPipe() throws IOException, InterruptedException, JetelException{
		proc = Runtime.getRuntime().exec(command);
		formatter.setDataTarget(new FileOutputStream(fileName));
		box = new ProcBox(proc, null, consumer, errConsumer);
		try {
			while (runIt && ((inRecord = inPort.readRecord(inRecord)) != null)) {
				if (skipped >= recordSkip) {
					formatter.write(inRecord);
				}else{
					skipped++;
				}
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
							if (skipped >= recordSkip) {
								formatter.write(inRecord);
							}else{
								skipped++;
							}
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
            if (xattribs.exists(XML_RECORD_COUNT_ATTRIBUTE)) {
            	writer.setProperty(ROW_COUNT_PARAM, xattribs.getString(XML_RECORD_COUNT_ATTRIBUTE));
            }
            if (xattribs.exists(XML_MAXERRORS_ATRIBUTE)) {
            	writer.setProperty(WARNING_COUNT_PARAM, xattribs.getString(XML_MAXERRORS_ATRIBUTE));
            }
            if (xattribs.exists(XML_RECORD_SKIP_ATTRIBUTE)) {
            	writer.setRecordSkip(xattribs.getInteger(XML_RECORD_SKIP_ATTRIBUTE));
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

	public void setProperty(String key, String value){
		properties.setProperty(key, value);
	}

	public int getRecordSkip() {
		return recordSkip;
	}

	public void setRecordSkip(int recordSkip) {
		this.recordSkip = recordSkip;
	}
}
