
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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;

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
import org.jetel.util.CommandBuilder;
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
	
	/**
	 * xml attributes for Db2DataWriterComponent
	 */
	private static final String XML_DATABASE_ATTRIBUTE = "database";
	private static final String XML_USERNAME_ATTRIBUTE = "userName";
	private static final String XML_PASSWORD_ATTRIBUTE = "password";	
    private static final String XML_TABLE_ATTRIBUTE = "table";
    private static final String XML_MODE_ATTRIBUTE = "loadMode";
    private static final String XML_FILEURL_ATTRIBUTE = "fileURL";
    private static final String XML_FILEMETADATA_ATTRIBUTE = "fileMetadata";
    private static final String XML_USEPIPE_ATTRIBUTE = "useNamedPipe";
    private static final String XML_COLUMNDELIMITER_ATTRIBUTE = "columnDelimiter";
	private static final String XML_INTERPRETER_ATTRIBUTE = "interpreter";
	private static final String XML_FIELDMAP_ATTRIBUTE = "fieldMap";
	private static final String XML_CLOVERFIELDS_ATTRIBUTE = "cloverFields";
	private static final String XML_DBFIELDS_ATTRIBUTE = "dbFields";
	private static final String XML_PARAMETERS_ATTRIBUTE = "parameters";
	private static final String XML_BATCHURL_ATTRIBUTE = "batchURL";
	private static final String XML_REJECTEDRECORDSURL_ATTRIBUTE = "rejectedURL";//on server
	private static final String XML_RECORD_COUNT_ATTRIBUTE = "recordCount";
	private static final String XML_RECORD_SKIP_ATTRIBUTE = "recordSkip";
	private static final String XML_MAXERRORS_ATRIBUTE = "maxErrors";
	
	/**
	 * available additional parameters
	 */
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
	public static final String DECIMAL_POINT_PARAM = "decpt";
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
	
    private static final char DEFAULT_COLUMN_DELIMITER = ',';
    private static final char EQUAL_CHAR = '=';
    private static final String TRUE = "true";
    public static final String DEFAULT_DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
	public static final String DEFAULT_DATE_FORMAT = "yyyy-MM-dd";
	public static final String DEFAULT_TIME_FORMAT = "HH:mm:ss";

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
	private Db2LoggerDataConsumer consumer;
	private LoggerDataConsumer errConsumer;
	private ProcBox box;
	private InputPort inPort;
	private DataRecord inRecord;
	private File batch;
	private String command;
	private Properties properties = new Properties();
	private String[] cloverFields;
	private String[] dbFields;
	private String batchURL;

	/**
	 * Constructor for Db2DataWriterComponent
	 * 
	 * @param id component identification
	 * @param database name of the database
	 * @param user  database user
	 * @param psw 	 password for database user.
	 * @param table name of table to load data
	 * @param mode load mode 
	 * @param fileName URL to file with extern data
	 * @param fileMetadataId  	 specifies data structure in external file.
	 */
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

	/* (non-Javadoc)
	 * @see org.jetel.graph.Node#init()
	 */
	@Override
	public void init() throws ComponentNotReadyException {
		super.init();
		//read parameters from string to properties
		if (parameters != null){
			String[] param = StringUtils.split(parameters);
			int index;
			for (String string : param) {
				index = string.indexOf(EQUAL_CHAR);
				if (index > -1) {
					properties.setProperty(string.substring(0, index).toLowerCase(), 
							StringUtils.unquote(string.substring(index + 1).toLowerCase()));
				}else{
					properties.setProperty(string.toLowerCase(), String.valueOf(true));
				}
			}
		}
		//set field columnDelimiter and proper property to the same value 
		//(columnDelimiter attribute has the higher priority then delimiter set in parameters
		if (columnDelimiter != 0) {
			if (Character.isWhitespace(columnDelimiter)) 
				throw new ComponentNotReadyException(this, XML_COLUMNDELIMITER_ATTRIBUTE, 
						StringUtils.quote(String.valueOf(columnDelimiter)) + 
						" is not allowed as column delimiter");
			properties.setProperty(COL_DEL_PARAM, String.valueOf(columnDelimiter));
		}else if (properties.contains(COL_DEL_PARAM)) {
			columnDelimiter = properties.getProperty(COL_DEL_PARAM).charAt(0);
		}
		
		if (!getInPorts().isEmpty()) {
			//prepare name for temporary data file/pipe
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
			//prepare metadata for formatting input data
			if (fileMetadataName != null) {
				fileMetadata = getGraph().getDataRecordMetadata(fileMetadataName);
			}
			inMetadata = getInputPort(READ_FROM_PORT).getMetadata();
			delimitedData = fileMetadata != null ? 
					fileMetadata.getRecType() != DataRecordMetadata.FIXEDLEN_RECORD : 
					inMetadata.getRecType() != DataRecordMetadata.FIXEDLEN_RECORD;
			if (fileMetadata == null) {
				//create metadata for formatting from input metadata
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
			}else{//create metadata for formatting from selected metadata
				switch (fileMetadata.getRecType()) {
				case DataRecordMetadata.DELIMITED_RECORD:
					fileMetadata = setDb2DateFormat(convertToDb2Delimited(fileMetadata));
					break;
				case DataRecordMetadata.FIXEDLEN_RECORD:
					fileMetadata = setDb2DateFormat(fileMetadata);
					break;
				default:
					throw new ComponentNotReadyException(this, XML_FILEMETADATA_ATTRIBUTE, 
							"Only fixlen or delimited metadata allowed");
				}
			}
			//create and init formatter
			if (delimitedData) {
				formatter = new DelimitedDataFormatter("UTF-8");
			}else{
				formatter = new FixLenDataFormatter("UTF-8");
				properties.setProperty(STRIP_BLANKS_PARAM, TRUE);
			}
			properties.setProperty(CODE_PAGE_PARAM, "1208");
			formatter.init(fileMetadata);
		}else{//there is not input port connected, data are read from existing file
			if (fileMetadata == null) throw new ComponentNotReadyException(this,
					XML_FILEMETADATA_ATTRIBUTE, "File metadata have to be defined");
			switch (fileMetadata.getRecType()) {
			case DataRecordMetadata.DELIMITED_RECORD:
				delimitedData = true;
				fileMetadata = setDb2DateFormat(convertToDb2Delimited(fileMetadata));
				break;
			case DataRecordMetadata.FIXEDLEN_RECORD:
				delimitedData = false;
				fileMetadata = setDb2DateFormat(fileMetadata);
				break;
			default:
				throw new ComponentNotReadyException(this, XML_FILEMETADATA_ATTRIBUTE, 
						"Only fixlen or delimited metadata allowed");
			}
		}
		//if data are not delimited method L must be specified and all fields listed
		if (cloverFields == null && !delimitedData) {
			cloverFields = new String[fileMetadata.getNumFields()];
			int i = 0;
			for (DataFieldMetadata field : fileMetadata) {
				cloverFields[i++] = field.getName();
			}
		}
		//prepare command for executing script
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
	
	/**
	 * From given metadata creates new metadata but with date/time format defined within 
	 * params or if not defined all dates have to have the same format, all times 
	 * have to have the same format and all timestamps have to have the same format
	 * 
	 * @param metadata pattern metadata
	 * @return new metadata
	 */
	private DataRecordMetadata setDb2DateFormat(DataRecordMetadata metadata){
		//get formats from parameters
		String dateFormat = properties.getProperty(DATE_FORMAT_PARAM);
		String timeFormat = properties.getProperty(TIME_FORMAT_PARAM);
		String timeDateFormat = properties.getProperty(TIME_STAMP_FORMAT_PARAM);
		//prepare ne metadata
		DataRecordMetadata out = metadata.duplicate();
		DataFieldMetadata field;
		boolean isDate;
		boolean isTime;
		String formatString;
		List<Integer[]> fieldsWithNullFormat = new ArrayList<Integer[]>();//fieldNumber, isDate (0,1), isTime(0,1)
		//check all fields
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
					if (timeDateFormat != null) {
						field.setFormatStr(timeDateFormat);
						if (field.isFixed()) {
							field.setSize((short)timeDateFormat.length());
						}
					}else if (formatString != null) {
						logger.warn("Time stamp format set to " + StringUtils.quote(formatString));
						timeDateFormat = formatString;
						properties.setProperty(TIME_STAMP_FORMAT_PARAM, timeDateFormat);
					}else{
						fieldsWithNullFormat.add(new Integer[]{i,1,1});
					}
				}else if (isDate) {
					if (dateFormat != null) {
						field.setFormatStr(dateFormat);
						if (field.isFixed()) {
							field.setSize((short)dateFormat.length());
						}
					}else if (formatString != null) {
						logger.warn("Date format set to " + StringUtils.quote(formatString));
						dateFormat = formatString;
						properties.setProperty(DATE_FORMAT_PARAM, dateFormat);
					}else{
						fieldsWithNullFormat.add(new Integer[]{i,1,0});
					}
				}else{//isTime
					if (timeFormat != null) {
						field.setFormatStr(timeFormat);
						if (field.isFixed()) {
							field.setSize((short)timeFormat.length());
						}
					}else if (formatString != null) {
						logger.warn("Time format set to " + StringUtils.quote(formatString));
						timeFormat = formatString;
						properties.setProperty(TIME_FORMAT_PARAM, timeFormat);
					}else{
						fieldsWithNullFormat.add(new Integer[]{i,0,1});
					}
				}
			}
		}
		//if there are some date/time fields without format, set it
		if (!fieldsWithNullFormat.isEmpty()) {
			if (timeDateFormat == null) {
				logger.warn("Time stamp format set to " + StringUtils.quote(DEFAULT_DATETIME_FORMAT));
				timeDateFormat = DEFAULT_DATETIME_FORMAT;
				properties.setProperty(TIME_STAMP_FORMAT_PARAM, dateFormat);
			}
			if (dateFormat == null) {
				logger.warn("Date format set to " + StringUtils.quote(DEFAULT_DATE_FORMAT));
				dateFormat = DEFAULT_DATE_FORMAT;
				properties.setProperty(DATE_FORMAT_PARAM, dateFormat);
			}
			if (timeFormat == null){
				logger.warn("Time format set to " + StringUtils.quote(DEFAULT_TIME_FORMAT));
				timeFormat = DEFAULT_TIME_FORMAT;
				properties.setProperty(TIME_FORMAT_PARAM, dateFormat);
			}
			for (Integer[] integers : fieldsWithNullFormat) {
				isDate = integers[1] == 1;
				isTime = integers[2] == 1;
				field = out.getField(integers[0]);
				if (isDate && isTime) {
					field.setFormatStr(timeDateFormat);
					if (field.isFixed()) {
						field.setSize((short)timeDateFormat.length());
					}
				}else if (isDate) {
					field.setFormatStr(dateFormat);
					if (field.isFixed()) {
						field.setSize((short)dateFormat.length());
					}
				}else{
					field.setFormatStr(timeFormat);
					if (field.isFixed()) {
						field.setSize((short)timeFormat.length());
					}
				}
			}
		}
//		set propper parameter when using FixLenDataFormatter
		if (out.getRecType() == DataRecordMetadata.FIXEDLEN_RECORD && !inPorts.isEmpty()) {
			properties.put(REC_LEN_PARAM, String.valueOf(out.getRecordSizeStripAutoFilling()));
		}
		return out;
	}
		
	/**
	 * Creates delimited metdata from fixed or delimited and sets the proper field delimiter
	 * 
	 * @param metadata input metadata
	 * @return new metadata
	 */
	private DataRecordMetadata convertToDb2Delimited(DataRecordMetadata metadata){
		DataRecordMetadata fMetadata = new DataRecordMetadata(metadata.getName() + "_delimited", DataRecordMetadata.DELIMITED_RECORD);
		boolean delimiterFound = columnDelimiter != 0;
		int delimiterFieldIndex = -1;
		DataFieldMetadata field, newField;
		//all fields, except last have to have identicall,  one char delimiter
		for (int i=0; i < metadata.getNumFields() - 1; i++){
			field = metadata.getField(i);
			newField = field.duplicate();
			//set defined delimiter
			if (delimiterFound){
				newField.setDelimiter(String.valueOf(columnDelimiter));
			}
			//if found first "good" delimiter set it for all fields
			if (!delimiterFound && field.isDelimited()){
				if (field.getDelimiter().length() == 1 && !Character.isWhitespace(field.getDelimiter().charAt(0))) {
					delimiterFound = true;
					columnDelimiter = field.getDelimiter().charAt(0);
					delimiterFieldIndex = i;
				}
			}
			fMetadata.addField(newField);
		}
		//if column delimiter wasn't set by xml attribute, nor parameters not found in 
		//metadata, set default delimiter
		if (!delimiterFound) {
			columnDelimiter = DEFAULT_COLUMN_DELIMITER;
			delimiterFieldIndex = fMetadata.getNumFields() - 1;
		}
		if (!properties.containsKey(COL_DEL_PARAM)) {
			logger.info(StringUtils.quote(String.valueOf(columnDelimiter)) + " set as column delimiter.");
			properties.setProperty(COL_DEL_PARAM, String.valueOf(columnDelimiter));
		}
		for (int i=0; i< delimiterFieldIndex; i++){
			fMetadata.getField(i).setDelimiter(String.valueOf(columnDelimiter));
		}
		//check last field
		newField = metadata.getField(metadata.getNumFields() - 1);
		newField.setDelimiter("\n");
		fMetadata.addField(newField);
		return fMetadata;
	}
	
	/**
	 * @return "connect to database user using psw"
	 */
	private String prepareConnectCommand(){
		return "connect to " + database + " user " + user + " using " + psw + "\n";
	}
	
	/**
	 * @return "disconnect database"
	 */
	private String prepareDisconnectCommand(){
		return "disconnect " + database + "\n";
	}
	
	/**
	 * Prepares load command with parameters from properties
	 * 
	 * @return requested load command
	 * @throws ComponentNotReadyException
	 */
	private String prepareLoadCommand() throws ComponentNotReadyException{
		String MODIFIED = "modified by";
		CommandBuilder command = new CommandBuilder("load client from '");
		command.setParams(properties);
		
		command.append(fileName);
		command.append("' of ");
		command.append(delimitedData ? DELIMITED_DATA : FIXLEN_DATA);
		
		command.addParameterWithPrefix("lobs from ", LOBS_URL_PARAM);
		
		//write requested "modified by" parameters
		boolean writeModified = true;
		writeModified = !command.addBooleanParameterWithPrefixClauseConditionally(
				MODIFIED, writeModified, ANY_ORDER_PARAM);
		//if modified was written writeModiefien should be false to the end 
		writeModified = !command.addBooleanParameterWithPrefixClauseConditionally(
				MODIFIED, writeModified, GENERATED_IGNORE_PARAM) && writeModified;
		writeModified = !command.addBooleanParameterWithPrefixClauseConditionally(
				MODIFIED, writeModified, GENERATED_MISSING_PARAM) && writeModified;
		writeModified = !command.addBooleanParameterWithPrefixClauseConditionally(
				MODIFIED, writeModified, GENERATED_OVERRIDE_PARAM) && writeModified;
		writeModified = !command.addBooleanParameterWithPrefixClauseConditionally(
				MODIFIED, writeModified, IDENTITY_IGNORE_PARAM) && writeModified;
		writeModified = !command.addBooleanParameterWithPrefixClauseConditionally(
				MODIFIED, writeModified, IDENTITY_MISSING_PARAM) && writeModified;
		writeModified = !command.addBooleanParameterWithPrefixClauseConditionally(
				MODIFIED, writeModified, IDENTITY_OVERRIDE_PARAM) && writeModified;
		writeModified = !command.addParameterWithPrefixClauseConditionally(
				MODIFIED, writeModified, INDEX_FREE_SPACE_PARAM) && writeModified;
		if (properties.containsKey(LOBS_URL_PARAM)) {
			command.append(" lobsinfile");
			writeModified = false;
		}
		writeModified = !command.addBooleanParameterWithPrefixClauseConditionally(
				MODIFIED, writeModified, NO_HEADER_PARAM) && writeModified;
		writeModified = !command.addBooleanParameterWithPrefixClauseConditionally(
				MODIFIED, writeModified, NO_ROW_WARNINGS_PARAM) && writeModified;
		writeModified = !command.addParameterWithPrefixClauseConditionally(
				MODIFIED, writeModified, PAGE_FREE_SPACE_PARAM) && writeModified;
		writeModified = !command.addBooleanParameterWithPrefixClauseConditionally(
				MODIFIED, writeModified, SUBTABLE_CONVERT_PARAM) && writeModified;
		writeModified = !command.addParameterWithPrefixClauseConditionally(
				MODIFIED, writeModified, TOTAL_FREE_SPACE_PARAM) && writeModified;
		writeModified = !command.addBooleanParameterWithPrefixClauseConditionally(
				MODIFIED, writeModified, USE_DEFAULTS_PARAM) && writeModified;
		writeModified = !command.addParameterWithPrefixClauseConditionally(
				MODIFIED, writeModified, CODE_PAGE_PARAM) && writeModified;
		writeModified = !command.addAndQuoteParameterWithPrefixClauseConditionally(
				MODIFIED, writeModified, DATE_FORMAT_PARAM) && writeModified;
		if (rejectedURL != null || properties.containsKey(DUMP_FILE_PARAM)) {
			if (writeModified) {
				command.append(MODIFIED);
				writeModified = false;
			}
			command.append(" " + DUMP_FILE_PARAM + "=");
			if (rejectedURL != null) {
				command.append(rejectedURL);
			}else{
				command.append(properties.getProperty(DUMP_FILE_PARAM));
			}
		}
		//FIXME sprawdzic, czy to boolean czy z wartoscia (dump..=x)
		writeModified = !command.addBooleanParameterWithPrefixClauseConditionally(
				MODIFIED, writeModified, DUMP_FILE_ACCESS_ALL_PARAM) && writeModified;
		writeModified = !command.addBooleanParameterWithPrefixClauseConditionally(
				MODIFIED, writeModified, FAST_PARSE_PARAM) && writeModified;
		writeModified = !command.addBooleanParameterWithPrefixClauseConditionally(
				MODIFIED, writeModified, IMPLIED_DECIMAL_PARAM) && writeModified;
		writeModified = !command.addAndQuoteParameterWithPrefixClauseConditionally(
				MODIFIED, writeModified, TIME_FORMAT_PARAM) && writeModified;
		writeModified = !command.addAndQuoteParameterWithPrefixClauseConditionally(
				MODIFIED, writeModified, TIME_STAMP_FORMAT_PARAM) && writeModified;
		writeModified = !command.addBooleanParameterWithPrefixClauseConditionally(
				MODIFIED, writeModified, NO_EOF_CHAR_PARAM) && writeModified;
		writeModified = !command.addBooleanParameterWithPrefixClauseConditionally(
				MODIFIED, writeModified, USER_GRAPHIC_CODE_PAGE_PARAM) && writeModified;
		writeModified = !command.addBooleanParameterWithPrefixClauseConditionally(
				MODIFIED, writeModified, BINARY_NUMERICS_PARAM) && writeModified;
		writeModified = !command.addBooleanParameterWithPrefixClauseConditionally(
				MODIFIED, writeModified, NO_CHECK_LENGTHS_PARAM) && writeModified;
		writeModified = !command.addParameterWithPrefixClauseConditionally(
				MODIFIED, writeModified, NULL_IND_CHAR_PARAM) && writeModified;
		writeModified = !command.addBooleanParameterWithPrefixClauseConditionally(
				MODIFIED, writeModified, PACKED_DECIMAL_PARAM) && writeModified;
		writeModified = !command.addParameterWithPrefixClauseConditionally(
				MODIFIED, writeModified, REC_LEN_PARAM) && writeModified;
		writeModified = !command.addBooleanParameterWithPrefixClauseConditionally(
				MODIFIED, writeModified, STRIP_BLANKS_PARAM) && writeModified;
		writeModified = !command.addBooleanParameterWithPrefixClauseConditionally(
				MODIFIED, writeModified, STRIP_NULLS_PARAM) && writeModified;
		writeModified = !command.addBooleanParameterWithPrefixClauseConditionally(
				MODIFIED, writeModified, ZONED_DECIMAL_PARAM) && writeModified;
		writeModified = !command.addParameterSpecialWithPrefixClauseConditionally(
				MODIFIED, writeModified, CHAR_DEL_PARAM, "") && writeModified;
		writeModified = !command.addParameterSpecialWithPrefixClauseConditionally(
				MODIFIED, writeModified, COL_DEL_PARAM, "") && writeModified;
		writeModified = !command.addBooleanParameterWithPrefixClauseConditionally(
				MODIFIED, writeModified, DATES_ISO_PARAM) && writeModified;
		writeModified = !command.addBooleanParameterWithPrefixClauseConditionally(
				MODIFIED, writeModified, DEC_PLUS_BLANK_PARAM) && writeModified;
		writeModified = !command.addParameterSpecialWithPrefixClauseConditionally(
				MODIFIED, writeModified, DECIMAL_POINT_PARAM, "") && writeModified;
		writeModified = !command.addBooleanParameterWithPrefixClauseConditionally(
				MODIFIED, writeModified, DEL_PRIORYTY_CHAR_PARAM) && writeModified;
		writeModified = !command.addParameterSpecialWithPrefixClauseConditionally(
				MODIFIED, writeModified, DL_DEL_PARAM, "") && writeModified;
		writeModified = !command.addBooleanParameterWithPrefixClauseConditionally(
				MODIFIED, writeModified, KEEP_BLANKS_PARAM) && writeModified;
		writeModified = !command.addBooleanParameterWithPrefixClauseConditionally(
				MODIFIED, writeModified, NO_CHAR_DEL_PARAM) && writeModified;
		writeModified = !command.addBooleanParameterWithPrefixClauseConditionally(
				MODIFIED, writeModified, NO_DOUBLE_DEL_PARAM);
		//sets "method L/P (columns) null indicators (null_indicators)"
		if (cloverFields != null) {
			command.append(" method ");
			command.append(delimitedData ? "P" : "L");
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
					command.append(String.valueOf(value + 1));
					if (i < cloverFields.length - 1) {
						command.append(",");
					}					
				}
				command.append(")");
			}else{
				for (int i=0; i<cloverFields.length; i++){
					DataFieldMetadata field = fileMetadata.getField(cloverFields[i]);
					value = fileMetadata.getFieldOffset(cloverFields[i]);
					if (value == -1) {
						throw new ComponentNotReadyException(this, "Field " + 
								StringUtils.quote(cloverFields[i]) + " does not exist in metadata " + 
								StringUtils.quote(fileMetadata.getName()) + " or metadata are not fixlength.");
					}
					command.append(String.valueOf(value + 1));
					command.append(" ");
					command.append(String.valueOf(value + field.getSize()));
					if (i < cloverFields.length - 1) {
						command.append(",");
					}					
				}
				command.append(")");
				command.addParameterWithPrefix("null indicators (", NULL_INDICATORS_PARAM);
				if (properties.containsKey(NULL_INDICATORS_PARAM)) {
					command.append(")");
				}
				
			}
		}
		//set rest of modificators
		command.addParameterSpecial(SAVE_COUNT_PARAM, " ");
		command.addParameterSpecial(ROW_COUNT_PARAM, " ");
		command.addParameterSpecial(WARNING_COUNT_PARAM, " ");
		command.addParameterWithPrefix("messages '", MESSAGES_URL_PARAM);
		if (properties.containsKey(MESSAGES_URL_PARAM)) {
			command.append("'");
		}
		command.addParameterWithPrefix("tempfiles path '", TMP_URL_PARAM);
		if (properties.containsKey(TMP_URL_PARAM)) {
			command.append("'");
		}
		
		command.append(" " + String.valueOf(loadMode));
		command.append(" into ");
		command.append(table);
		if (dbFields != null) {
			command.append(" (");
			command.append(StringUtils.stringArraytoString(dbFields, ','));
			command.append(") ");
		}

		command.addParameterWithPrefix("for exception ", EXCEPTION_TABLE_PARAM);
		
		if (properties.containsKey(STATISTIC_PARAM)) {
			command.append(" statistics ");
			if (properties.getProperty(STATISTIC_PARAM).equalsIgnoreCase(TRUE)) {
				command.append("use profile");
			}else{
				command.append("no");
			}
		}
		
		if (properties.containsKey(NONRECOVERABLE_PARAM) && (properties.getProperty(NONRECOVERABLE_PARAM).equalsIgnoreCase(TRUE))) {
			command.append(" nonrecoverable ");
		}else if (properties.containsKey(COPY_PARAM)) {
			if (!properties.getProperty(COPY_PARAM).equalsIgnoreCase(TRUE)) {
				command.append(" copy no ");
			}else{
				command.append(" copy yes ");
				if (properties.containsKey(USE_TSM_PARAM)) {
					command.append("use tsm ");
					if (properties.containsKey(NUM_SESSIONS_PARAM)) {
						command.append(" open ");
						command.append(properties.getProperty(NUM_SESSIONS_PARAM));
						command.append(" sessions ");
					}
				}
				if (properties.containsKey(COPY_URL_PARAM)) {
					command.append(" to '");
					command.append(properties.getProperty(COPY_URL_PARAM));
					command.append("' ");
				}
				if (properties.containsKey(RECOVERY_LIBRARY_PARAM)) {
					command.append(" load '");
					command.append(properties.getProperty(RECOVERY_LIBRARY_PARAM));
					command.append("' ");
					if (properties.containsKey(NUM_SESSIONS_PARAM)) {
						command.append(" open ");
						command.append(properties.getProperty(NUM_SESSIONS_PARAM));
						command.append(" sessions ");
					}
				}
			}
		}
		
		if (properties.containsKey(WITHOUT_PROMPTING_PARAM) && properties.getProperty(WITHOUT_PROMPTING_PARAM).equalsIgnoreCase(TRUE)) {
			command.append(" without prompting ");
		}
		
		command.addParameterWithPrefix("data buffer ", BUFFER_SIZE_PARAM);
		command.addParameterWithPrefix("sort buffer ", SORT_BUFFER_SIZE_PARAM);
		command.addParameterWithPrefix("cpu_parallelism ", CPU_NUM_PARAM);
		command.addParameterWithPrefix("disk_parallelism ", DISK_NUM_PARAM);
		command.addParameterWithPrefix("indexing mode ", INDEXING_MODE_PARAM);

		if (properties.containsKey(ALLOW_READ_ACCESS_PARAM)) {
			if (properties.getProperty(ALLOW_READ_ACCESS_PARAM).equalsIgnoreCase(TRUE)) {
				command.append(" allow read access ");
				if (properties.containsKey(INDEX_COPY_TABLE_PARAM)) {
					command.append(" use ");
					command.append(properties
							.getProperty(INDEX_COPY_TABLE_PARAM));
				}
			}else{
				command.append(" allow no access ");
			}
		}

		command.addParameterWithPrefix("check pending cascade ", CHECK_PENDING_CASCADE_PARAM);

		if (properties.containsKey(LOCK_WITH_FORCE_PARAM) && properties.getProperty(LOCK_WITH_FORCE_PARAM).equalsIgnoreCase(TRUE)) {
			command.append(" lock with force");
		}
		
		command.addParameterWithPrefix("partitioned db config ", PARTITIONED_CONFIG_PARAM);
		
		return command.getCommand();
	}
	
	/**
	 * If script file doesn't exist cretes it
	 * 
	 * @return path to the script file
	 * @throws IOException
	 * @throws ComponentNotReadyException
	 */
	private String prepareBatch() throws IOException, ComponentNotReadyException{
		if (batchURL != null) {
			batch = new File(batchURL);
			if (batch.length() > 0) {
				logger.info("Batch file exist. New batch file will not be created.");
				return batch.getCanonicalPath();
			}
		}
		if (batch == null) {
			batch = File.createTempFile("tmp", ".bat", new File("."));
		}		
		FileWriter batchWriter = new FileWriter(batch);

		batchWriter.write(prepareConnectCommand());
		batchWriter.write(prepareLoadCommand());
		batchWriter.write(prepareDisconnectCommand());

		batchWriter.close();
		return batchURL != null ? batch.getCanonicalPath() : batch.getName();
	}
	
	/**
	 * Reading and writing from/to named pipe
	 * 
	 * @return exit value of loading process
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws JetelException
	 */
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
			//FIXME check me if fileMetadata varies from inMetadata
			inRecord = new DataRecord(fileMetadata);
			inRecord.init();
		}
		
		consumer = new Db2LoggerDataConsumer(LoggerDataConsumer.LVL_DEBUG, 0);
		errConsumer = new LoggerDataConsumer(LoggerDataConsumer.LVL_ERROR, 0);

		int exitValue = 0;
		//create named pipe
		if (!getInPorts().isEmpty() && usePipe) {
			try {
				proc = Runtime.getRuntime().exec("mkfifo " + fileName);
				box = new ProcBox(proc, null, consumer, errConsumer);
				exitValue = box.join();
			} catch (Exception e) {
				cleanup();
				throw e;
			}
		}
		//main part of exec
		try {
			if (!getInPorts().isEmpty() && usePipe) {
				exitValue = runWithPipe();
			}else {
				if (!getInPorts().isEmpty()) {
					//save data in temporary file
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
			}
		} catch (Exception e) {
			cleanup();
			throw e;
		}

		cleanup();
		
		if (exitValue != 0 && exitValue != 2) {
			logger.error("Loading to database failed");
			logger.error("db2 load exited with value: " + exitValue);
			throw new JetelException("Process exit value is not 0");
		}
		
		if (exitValue == 2 || consumer.getLoaded() != consumer.getRead()) {
			if (exitValue == 2) {
				logger.warn("There is at least one warning message in the message file.");
			}
			if (consumer.getLoaded() != consumer.getRead()) {
				logger.warn("Not all records were loaded to database:");
				logger.info("Number of rows read = " + consumer.getRead());
				logger.info("Number of rows loaded = " + consumer.getLoaded());
				logger.info("Number of rows rejected = " + consumer.getRejected());
			}
			for (String message : consumer.getErrors()) {
				logger.warn(message);
			}
		}
		
		return runIt ? Result.FINISHED_OK : Result.ABORTED;
	}

	/**
	 * Deletes temp files
	 */
	private void cleanup() {
		if (proc != null) {
			proc.destroy();
		}			
		if (!getInPorts().isEmpty()) {
			File tmpFile = new File(fileName);
			if (!tmpFile.delete()){
				logger.warn("Tmp file was not deleted.");
			}
		}
		if (batchURL == null && !batch.delete()){
			logger.warn("Tmp batch was not deleted.");
		}
	}
	
    /**
     * Reads node from xml
     * 
     * @param graph
     * @param xmlElement
     * @return
     * @throws XMLConfigurationException
     */
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
                    xattribs.getString(XML_FILEMETADATA_ATTRIBUTE, null));
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
            if (xattribs.exists(XML_BATCHURL_ATTRIBUTE)) {
            	writer.setBatchURL(xattribs.getString(XML_BATCHURL_ATTRIBUTE));
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

	public String getBatchURL() {
		return batchURL;
	}

	public void setBatchURL(String batchURL) {
		this.batchURL = batchURL;
	}

}

/**
 * Class for writing data from process output stream to logger and getting errors and 
 * information about numbers of processed records
 * 
 * @author avackova (agata.vackova@javlinconsulting.cz) ; 
 * (c) JavlinConsulting s.r.o.
 *  www.javlinconsulting.cz
 *
 * @since Aug 17, 2007
 *
 */
class Db2LoggerDataConsumer extends LoggerDataConsumer {
	
	int read;
	int skipped;
	int loaded;
	int rejected;
	int deleted;
	int committed;
	List<String> errors;
	private String errorMessage;
	private boolean partRead = false;
	
	/**
	 * Constructor from superclass
	 * 
	 * @param level
	 * @param maxLines
	 */
	public Db2LoggerDataConsumer(int level, int maxLines) {
		super(level, maxLines);
		errors = new ArrayList<String>();
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.util.exec.LoggerDataConsumer#consume()
	 */
	@Override
	public boolean consume() throws JetelException {
		String line;
		try {
			line = reader.readLine();
		} catch (IOException e) {
			throw new JetelException("Error while reading input data", e);
		}
		if (line == null) {
			return false;
		}
		//remember number of processed records
		if (line.contains("rows read")) {
			read = Integer.parseInt(line.substring(line.indexOf('=') + 1).trim());
		}
		if (line.contains("rows skipped")) {
			skipped = Integer.parseInt(line.substring(line.indexOf('=') + 1).trim());
		}
		if (line.contains("rows loaded")) {
			loaded = Integer.parseInt(line.substring(line.indexOf('=') + 1).trim());
		}
		if (line.contains("rows rejected")) {
			rejected = Integer.parseInt(line.substring(line.indexOf('=') + 1).trim());
		}
		if (line.contains("rows deleted")) {
			deleted = Integer.parseInt(line.substring(line.indexOf('=') + 1).trim());
		}
		if (line.contains("rows committed")) {
			committed = Integer.parseInt(line.substring(line.indexOf('=') + 1).trim());
		}
		//remember errors
		if (line.startsWith("SQL053") || line.startsWith("SQL053") || line.startsWith("SQL0545") || 
				line.startsWith("SQL0659") || line.startsWith("SQL0670") || 
				line.startsWith("SQL0798")  || line.startsWith("SQL1194") || 
				line.startsWith("SQL3101") || line.startsWith("SQL3114") || 
				line.startsWith("SQL3115") || line.startsWith("SQL3116") || 
				line.startsWith("SQL3117") || line.startsWith("SQL3118") || 
				line.startsWith("SQL3119") || line.startsWith("SQL312") || 
				line.startsWith("SQL3130") || line.startsWith("SQL3131") || 
				line.startsWith("SQL3132") || line.startsWith("SQL3137") || 
				line.startsWith("SQL3138") || line.startsWith("SQL3146") || 
				line.startsWith("SQL3148") || line.startsWith("SQL3175") || 
				line.startsWith("SQL3179") || line.startsWith("SQL3186") || 
				line.startsWith("SQL3191") || line.startsWith("SQL3330") || 
				line.startsWith("SQL3411") || line.startsWith("SQL3412") || 
				line.startsWith("SQL3413") || line.startsWith("SQL3415") || 
				line.startsWith("SQL3416") || line.startsWith("SQL3506") || 
				line.startsWith("SQL3511") || line.startsWith("SQL3512") || 
				line.startsWith("SQL3550") || line.startsWith("SQL3602") || 
				line.startsWith("SQL3603") || line.startsWith("SQL8100") || 
				line.startsWith("SQL27972")) {
			errorMessage = line;
			partRead  = true;
		}else if (partRead) {
			partRead = !StringUtils.isBlank(line);
			if (partRead) {
				errorMessage = errorMessage.concat(line);
			}else{
				errors.add(errorMessage);
			}
		}
		
		linesRead++;
		if (maxLines != 0 && linesRead > maxLines) {
			return true;
		}

		switch (level) {
		case LVL_DEBUG:
			logger.debug(line);
			break;
		case LVL_WARN:
			logger.warn(line);
			break;
		case LVL_ERROR:
			logger.error(line);
			break;
		}
		return true;
	}

	public int getCommitted() {
		return committed;
	}

	public int getDeleted() {
		return deleted;
	}

	public int getLoaded() {
		return loaded;
	}

	public int getRead() {
		return read;
	}

	public int getRejected() {
		return rejected;
	}

	public int getSkipped() {
		return skipped;
	}

	public List<String> getErrors() {
		return errors;
	}
	
}