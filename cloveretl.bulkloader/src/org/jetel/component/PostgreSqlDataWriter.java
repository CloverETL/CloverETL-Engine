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
 */

package org.jetel.component;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.formatter.DelimitedDataFormatter;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.JetelException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.exec.LoggerDataConsumer;
import org.jetel.util.exec.ProcBox;
import org.jetel.util.file.FileUtils;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.string.StringUtils;
import org.w3c.dom.Element;

/**
 * <h3>PostgreSQL data writer</h3>
 * 
 * This component loads data to postgre database using the psql utility. It is faster then DBOutputTable. 
 * Load formats data pages directly, while avoiding most of the overhead of individual row processing that inserts incur.<br>
 * There is created psql command (\copy) depending on input parameters. Data are read from given input file or 
 * from the input port and loaded to database.<br>
 * Any generated commands/files can be optionally logged to help diagnose problems.<br>
 * Before you use this component, make sure that postgreSQL client is installed and configured on the machine where CloverETL runs and
 * psql command line tool available.
 * 
 * @author      Miroslav Haupt (Mirek.Haupt@javlinconsulting.cz)
 * (c) Javlin Consulting (www.javlinconsulting.cz)
 * @see         org.jetel.graph.TransformationGraph
 * @see         org.jetel.graph.Node
 * @see         org.jetel.graph.Edge
 * @since 		30.10.2007
 * 
 */
public class PostgreSqlDataWriter extends BulkLoader {

	private static Log logger = LogFactory.getLog(PostgreSqlDataWriter.class);

	/** Description of the Field */
	// attributes for psql client
	private static final String XML_PSQL_PATH_ATTRIBUTE = "psqlPath";
	private static final String XML_DATABASE_ATTRIBUTE = "database";
	private static final String XML_COMMAND_URL_ATTRIBUTE = "commandURL";
	private static final String XML_HOST_ATTRIBUTE = "host";
	private static final String XML_USER_ATTRIBUTE = "username";
	
	// attributes for copy statement	
	private static final String XML_TABLE_ATTRIBUTE = "table";
	private static final String XML_FILE_URL_ATTRIBUTE = "fileURL";
	private static final String XML_COLUMN_DELIMITER_ATTRIBUTE = "columnDelimiter";
	private static final String XML_PARAMETERS_ATTRIBUTE = "parameters";

	// params for psql client
	private static final String PSQL_ECHO_ALL_PARAM = "echoAll";
	private static final String PSQL_ECHO_QUERIES_PARAM = "echoQueries";
	private static final String PSQL_ECHO_HIDDEN_PARAM = "echoHidden";
	private static final String PSQL_LOG_FILE_PARAM = "logFile";
	private static final String PSQL_OUTPUT_PARAM = "output";
	private static final String PSQL_PORT_PARAM = "port";
	private static final String PSQL_PSET_PARAM = "pset";
	private static final String PSQL_QUIET_PARAM = "quiet";
	private static final String PSQL_NO_PSQLRC_PARAM = "noPsqlrc";
	private static final String PSQL_SINGLE_TRANSACTION_PARAM = "singleTransaction";

	// params for copy statement
	private static final String COPY_COLUMNS_PARAM = "columns";
	private static final String COPY_BINARY_PARAM = "binary";
	private static final String COPY_OIDS_PARAM = "oids";
	private static final String COPY_NULL_PARAM = "null";
	private static final String COPY_CSV_PARAM = "csv";
	private static final String COPY_CSV_HEADER_PARAM = "csvHeader";
	private static final String COPY_CSV_QUOTE_PARAM = "csvQuote";
	private static final String COPY_CSV_ESCAPE_PARAM = "csvEscape";
	private static final String COPY_CSV_FORCE_NOT_NULL_PARAM = "csvForceNotNull";
	
	// switches for psql client,these switches have own xml attributes
	private static final String PSQL_DATABASE_SWITCH = "dbname";
	private static final String PSQL_COMMAND_URL_SWITCH = "file";
	private static final String PSQL_HOST_SWITCH = "host";
	private static final String PSQL_USER_SWITCH = "username";

	// switches for psql client
	private static final String PSQL_ECHO_ALL_SWITCH = "echo-all";
	private static final String PSQL_ECHO_QUERIES_SWITCH = "echo-queries";
	private static final String PSQL_ECHO_HIDDEN_SWITCH = "echo-hidden";
	private static final String PSQL_LOG_FILE_SWITCH = "log-file";
	private static final String PSQL_OUTPUT_SWITCH = "output";
	private static final String PSQL_PORT_SWITCH = "port";
	private static final String PSQL_PSET_SWITCH = "pset";
	private static final String PSQL_QUIET_SWITCH = "quiet";
	private static final String PSQL_NO_PSQLRC_SWITCH = "no-psqlrc";
	private static final String PSQL_SINGLE_TRANSACTION_SWITCH = "single-transaction";

	// keywords for copy statement
	private final static String COPY_STDIN_KEYWORD = "pstdin";

	public final static String COMPONENT_TYPE = "POSTGRESQL_DATA_WRITER";

	private final static String POSTGRESQL_FILE_NAME_PREFIX = "postgresql";
	private final static String COMMAND_FILE_NAME_SUFFIX = ".ctl";
	private final static File TMP_DIR = new File(".");
	private final static String DEFAULT_COLUMN_DELIMITER = "\t";
	private final static String DEFAULT_COLUMN_DELIMITER_IN_CSV_MODE = ",";
	
	private final static String DEFAULT_RECORD_DELIMITER = "\n";
	private final static String CHARSET_NAME = "UTF-8";
	private final static String DEFAULT_DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
	private final static String DEFAULT_DATE_FORMAT = "yyyy-MM-dd";
	private final static String DEFAULT_TIME_FORMAT = "HH:mm:ss";
	private final static String DEFAULT_NULL_VALUE = "";
	private final static String DEFAULT_NULL_VALUE_IN_TEXT_MODE = "\\N";

	// variables for psql client
	private String commandURL;
	private String host;

	// variables for copy statement
	private File commandFile;

	private File dataFile = null; // file that is used for exchange data between clover and psql - file from dataURL
	private String[] commandLine; // command line of psql
	private DataRecordMetadata dbMetadata; // it correspond to psql input format

	private boolean csvMode; // true if CSV mode is used for loading data
	
	/**
	 * Constructor for the PostgreSqlDataWriter object
	 * 
	 * @param id Description of the Parameter
	 */
	public PostgreSqlDataWriter(String id, String psqlPath, String database) {
		super(id, psqlPath, database);
	}
	
	/**
	 * Main processing method for the PsqlDataWriter object
	 * 
	 * @since April 4, 2002
	 */
	public Result execute() throws Exception {
		ProcBox box;
		int processExitValue = 0;

		if (isDataReadFromPort) {
			if (dataURL != null) {
				// dataFile is used for exchange data
				formatter.setDataTarget(Channels.newChannel(new FileOutputStream(dataFile)));
				readFromPortAndWriteByFormatter();
				box = createProcBox(null);
			} else {
				Process process = Runtime.getRuntime().exec(commandLine);
				box = createProcBox(process);

				// stdin is used for exchange data - set data target to stdin of process
				OutputStream processIn = new BufferedOutputStream(process.getOutputStream());
				formatter.setDataTarget(Channels.newChannel(processIn));
				readFromPortAndWriteByFormatter();
			}

			processExitValue = box.join();
		} else {
			processExitValue = readDataDirectlyFromFile();
		}

		if (processExitValue != 0) {
			throw new JetelException(getErrorMsg(processExitValue));
		}

		return runIt ? Result.FINISHED_OK : Result.ABORTED;
	}

	/**
	 * Return error message according to error code.
	 * @param exitValue error code of psql utility
	 * @return textual error message
	 */
	private String getErrorMsg(int exitValue) {
		String errorMsg;
		switch (exitValue) {
		case 1:
			errorMsg = "a fatal error of its own (out of memory, file not found) occurs";
			break;
		case 2:
			errorMsg = "the connection to the server went bad and the session was not interactive";
			break;
		case 3:
			errorMsg = "an error occurred in a script and the variable ON_ERROR_STOP was set";
			break;
		default:
			errorMsg = "unknown error";
		}
		
		return "psql utility has failed - " + errorMsg + ".";
	}
	
	/**
	 * This method reads incoming data from port 
	 * and sends them by formatter to psql process.
	 * 
	 * @throws Exception
	 */
	private void readFromPortAndWriteByFormatter() throws Exception {
		InputPort inPort = getInputPort(READ_FROM_PORT);
		DataRecord record = new DataRecord(dbMetadata);
		record.init();

		try {
			while (runIt && ((record = inPort.readRecord(record)) != null)) {
				formatter.write(record);
			}
		} catch (Exception e) {
			throw e;
		} finally {
			formatter.close();
		}
	}

	/**
	 * Call psql process with parameters - psql process reads data directly from file.
	 * 
	 * @return value of finished process
	 * @throws Exception
	 */
	private int readDataDirectlyFromFile() throws Exception {
		ProcBox box = createProcBox(null);
		return box.join();
	}

	/**
	 * Create instance of ProcBox.
	 * 
	 * @param process running process; when process is null, default process is created
	 * @return instance of ProcBox
	 * @throws IOException
	 */
	private ProcBox createProcBox(Process process) throws IOException {
		if (process == null) {
			process = Runtime.getRuntime().exec(commandLine);
		}
		return new ProcBox(process, null, consumer, errConsumer);
	}
	
	/**
	 * Create command line for process, where psql utility is running. 
	 * Example: psql --dbname=testdb --file=/tmp/command.ctl 
	 *  
	 * @return array, first field is name of psql utility and the others fields are parameters
	 * @throws ComponentNotReadyException when command file isn't created
	 */
	private String[] createCommandLineForDbLoader() throws ComponentNotReadyException {
		if (ProcBox.isWindowsPlatform()) {
			loadUtilityPath = StringUtils.backslashToSlash(loadUtilityPath);
		}
		PsqlCommandBuilder command = new PsqlCommandBuilder(loadUtilityPath, properties);

		command.addParam(null, PSQL_DATABASE_SWITCH, database);
		command.addParam(null, PSQL_COMMAND_URL_SWITCH, createCommandFile());
		command.addParam(null, PSQL_HOST_SWITCH, host);
		command.addParam(null, PSQL_USER_SWITCH, user);
		command.addBooleanParam(PSQL_ECHO_ALL_PARAM, PSQL_ECHO_ALL_SWITCH);
		command.addBooleanParam(PSQL_ECHO_QUERIES_PARAM, PSQL_ECHO_QUERIES_SWITCH);
		command.addBooleanParam(PSQL_ECHO_HIDDEN_PARAM, PSQL_ECHO_HIDDEN_SWITCH);
		command.addParam(PSQL_LOG_FILE_PARAM, PSQL_LOG_FILE_SWITCH, null);
		command.addParam(PSQL_OUTPUT_PARAM, PSQL_OUTPUT_SWITCH, null);
		command.addParam(PSQL_PORT_PARAM, PSQL_PORT_SWITCH, null);
		command.addParam(PSQL_PSET_PARAM, PSQL_PSET_SWITCH, null);
		command.addBooleanParam(PSQL_QUIET_PARAM, PSQL_QUIET_SWITCH);
		command.addBooleanParam(PSQL_NO_PSQLRC_PARAM, PSQL_NO_PSQLRC_SWITCH);
		command.addBooleanParam(PSQL_SINGLE_TRANSACTION_PARAM, PSQL_SINGLE_TRANSACTION_SWITCH);

		return command.getCommand();
	}
	
	/**
	 * Create file that contains <i>copy</i> statement and return its name.
	 * 
	 * @return name of the command file
	 * @throws ComponentNotReadyException when command file isn't created
	 */
	String createCommandFile() throws ComponentNotReadyException {
		try {
			if (commandURL != null) {
				commandFile = new File(commandURL);
				if (commandFile.exists()) {
					return commandFile.getCanonicalPath();
				} else {
					commandFile.createNewFile();
				}
			} else {
				commandFile = File.createTempFile(POSTGRESQL_FILE_NAME_PREFIX, 
						COMMAND_FILE_NAME_SUFFIX, TMP_DIR);
			}

			saveCommandToFile(commandFile, getDefaultCommandFileContent());
			return commandFile.getCanonicalPath();
		} catch (IOException ioe) {
			throw new ComponentNotReadyException(this, 
					"Can't create command file for psql utility.", ioe);
		}
	}

	/**
	 * Save <i>copy</i> statement to the file.
	 * @param commandFile file where the copy statement will be saved.
	 * @param command copy statement to save
	 * @throws IOException when error occured
	 */
	private void saveCommandToFile(File commandFile, String command) throws IOException {
		FileWriter commandWriter = new FileWriter(commandFile);
		printCommandToLog(command);
		
		commandWriter.write(command);
		commandWriter.close();
	}
	
	/**
	 * Print <i>copy</i> statement to log.
	 * 
	 * @param command command to be printed
	 */
	private void printCommandToLog(String command) {
		// There is the statement split to two parts, first contains //copy.
		// It's due to converting special chars to string - first part (//copy)
		// already has correct format for printing.
		String[] commandArray = command.split(" ", 2);
		logger.debug("Command file content: " + commandArray[0] + " " +
				StringUtils.specCharToString(commandArray[1]));
	}
	
	/**
	 * Create and return string that contains <i>copy</i> statement.
	 * @return string that contains <i>copy</i> statement
	 * @throws IOException
	 */
	private String getDefaultCommandFileContent() throws IOException {
		CopyCommandBuilder command = new CopyCommandBuilder("\\copy", properties);

		// \copy table [ ( column_list ) ]
		command.append(table);
		if (properties.containsKey(COPY_COLUMNS_PARAM)) {
			command.append("(" + properties.getProperty(COPY_COLUMNS_PARAM) + ")");
		}
		
		// from { filename | pstdin }
		command.append("from");
		if (dataFile != null) {
			command.append(dataFile.getCanonicalPath());
		} else {
			command.append(COPY_STDIN_KEYWORD);
		}
		
		// [ with ] [ binary ] [ oids ] [ delimiter [ as ] 'character' ] [ null [ as ] 'string' ]
		if (isWithKeywordUsed()) {
			command.append("with");
			command.addBooleanParam(COPY_BINARY_PARAM, COPY_BINARY_PARAM);
			command.addBooleanParam(COPY_OIDS_PARAM, COPY_OIDS_PARAM);
			if (isColumnDelimiterUsed()) {
				command.append("delimiter '" + columnDelimiter + "'");
			}
			
			String nullValue = getNullValue();
			if (nullValue != null) {
				command.addSingleQuotedParam(null, COPY_NULL_PARAM, nullValue);

				// warning that user defined value is ignored - default nullValue is used
				if (isDataReadFromPort && properties.containsKey(COPY_NULL_PARAM) &&
						!DEFAULT_NULL_VALUE.equals(properties.getProperty(COPY_NULL_PARAM))) {
							logger.warn("Parameter " + StringUtils.quote(COPY_NULL_PARAM) + " at " + 
								StringUtils.quote(XML_PARAMETERS_ATTRIBUTE) + 
								" attribute is ignored. Default null value is used.");
				}
			}
		}

		// [ csv [ header ] [ quote [ as ] 'character' ] [ escape [ as ] 'character' ] [ force not null column_list ] ]
		if (csvMode) {
			command.append("csv");
			command.addBooleanParam(COPY_CSV_HEADER_PARAM, "header");
			command.addSingleQuotedParam(COPY_CSV_QUOTE_PARAM, "quote");
			command.addSingleQuotedParam(COPY_CSV_ESCAPE_PARAM, "escape");
			command.addParam(COPY_CSV_FORCE_NOT_NULL_PARAM, "force not null");
		}

		return command.toString();
	}
	
	
	/**
	 * Return true if <i>with</i> keyword is used in copy statement.
	 * @return true if <i>with</i> keyword is used in copy statement
	 */
	private boolean isWithKeywordUsed() {
		return csvMode || 
			properties.containsKey(COPY_BINARY_PARAM) ||
			properties.containsKey(COPY_OIDS_PARAM) ||
			isColumnDelimiterUsed() ||
			getNullValue() != null;
	}
	
	/**
	 * Return true if <i>columnDelimiter</i> is used in copy statement.
	 * @return true if <i>columnDelimiter</i> is used in copy statement
	 */
	private boolean isColumnDelimiterUsed() {
		return (csvMode && !columnDelimiter.equals(DEFAULT_COLUMN_DELIMITER_IN_CSV_MODE)) ||
			(!csvMode && !columnDelimiter.equals(DEFAULT_COLUMN_DELIMITER));
	}
	
	/**
	 * If nullValue is used in copy statement then string is returned, else null is returned.
	 * if data is read from port then nullValue == DEFAULT_NULL_VALUE
	 * if data is read directly from flat file then nullValue can be defined by user 
	 * @return string represents nullValuu or null if nullValue isn't used in copy statement.
	 */
	private String getNullValue() {
		if (isDataReadFromPort) {
			return DEFAULT_NULL_VALUE;
		} else {
			if (properties.containsKey(COPY_NULL_PARAM)) {
				String nullValue =  properties.getProperty(COPY_NULL_PARAM);
		
				if ((csvMode && !nullValue.equals(DEFAULT_NULL_VALUE)) ||
						(!csvMode && !nullValue.equals(DEFAULT_NULL_VALUE_IN_TEXT_MODE))) {
					return nullValue;
				}
			}
		}
		
		return null;
	}
	
	/**
	 * Description of the Method
	 * 
	 * @exception ComponentNotReadyException Description of the Exception
	 * @since April 4, 2002
	 */
	public void init() throws ComponentNotReadyException {
		if (isInitialized()) return;
		super.init();

		csvMode = isCsvModeUsed(properties);
		setDefaultColumnDelimiter(csvMode);
		checkParams();

		// prepare (temporary) data file
		if (isDataReadFromPort) {
			if (dataURL != null) {
				dataFile = new File(dataURL);
				dataFile.delete();
			}
		} else {
			try {
				if (!FileUtils.isServerURL(FileUtils.getInnerAddress(dataURL)) && 
						!(new File(FileUtils.getFile(getGraph().getProjectURL(), dataURL))).exists()) {
					free();
					throw new ComponentNotReadyException(this, 
							"Data file " + StringUtils.quote(dataURL) + " not exists.");
				}
			} catch (Exception e) {
				throw new ComponentNotReadyException(this, e);
			}
			dataFile = new File(dataURL);
		}
		
		commandLine = createCommandLineForDbLoader();
		printCommandLineToLog(commandLine);

		if (isDataReadFromPort) {
			InputPort inPort = getInputPort(READ_FROM_PORT);

			dbMetadata = createPsqlMetadata(inPort.getMetadata());

			// init of data formatter
			formatter = new DelimitedDataFormatter(CHARSET_NAME);
			formatter.init(dbMetadata);
		}

		errConsumer = new LoggerDataConsumer(LoggerDataConsumer.LVL_ERROR, 0);
		consumer = new LoggerDataConsumer(LoggerDataConsumer.LVL_DEBUG, 0);
	}
	
	
	/**
	 * Print system command with it's parameters to log. 
	 * @param command
	 */
	private void printCommandLineToLog(String[] command) {
		StringBuilder msg = new StringBuilder("System command: \"");
		msg.append(command[0]).append("\" with parameters:\n");
		for (int idx = 1; idx < command.length; idx++) {
			msg.append(idx).append(": ").append(command[idx]).append("\n");
		}
		logger.debug(msg.toString());
	}

	/**
	 * If no columnDelimiter is set then default column delimiter is set.
	 * @param csvModeUsed
	 */
	private void setDefaultColumnDelimiter(boolean csvModeUsed) {
		if (columnDelimiter == null) {
			if (csvModeUsed) {
				columnDelimiter = DEFAULT_COLUMN_DELIMITER_IN_CSV_MODE;
			} else {
				columnDelimiter = DEFAULT_COLUMN_DELIMITER;
			}
		}
	}
	
	/**
	 * Return true if csv mode is used for loading data.
	 * @param prop properties with info about loading data
	 * @return true if csv mode is used for loading data else false
	 */
	private boolean isCsvModeUsed(Properties prop) {
		return prop.containsKey(COPY_CSV_PARAM) ||
			prop.containsKey(COPY_CSV_HEADER_PARAM) || 
			prop.containsKey(COPY_CSV_QUOTE_PARAM) || 
			prop.containsKey(COPY_CSV_ESCAPE_PARAM) || 
			prop.containsKey(COPY_CSV_FORCE_NOT_NULL_PARAM);
	}

	/**
	 * Checks if mandatory parameters are defined.
	 * And check combination of some parameters.
	 * 
	 * @throws ComponentNotReadyException if any of conditions isn't fulfilled
	 */
	private void checkParams() throws ComponentNotReadyException {
		if (StringUtils.isEmpty(loadUtilityPath)) {
			throw new ComponentNotReadyException(this,
					StringUtils.quote(XML_PSQL_PATH_ATTRIBUTE) + " attribute have to be set.");
		}

		if (StringUtils.isEmpty(database)) {
			throw new ComponentNotReadyException(this, 
					StringUtils.quote(XML_DATABASE_ATTRIBUTE) + " attribute have to be set.");
		}

		if (StringUtils.isEmpty(table) && !fileExists(commandURL)) {
			throw new ComponentNotReadyException(this, 
					StringUtils.quote(XML_TABLE_ATTRIBUTE) + " attribute has to be specified or " +
					StringUtils.quote(XML_COMMAND_URL_ATTRIBUTE) + 
					" attribute has to be specified and file at the URL must exists.");
		}

		if (!isDataReadFromPort && StringUtils.isEmpty(dataURL) && !fileExists(commandURL)) {
			throw new ComponentNotReadyException(this, "Input port or " + 
					StringUtils.quote(XML_FILE_URL_ATTRIBUTE) + " attribute or " +
					StringUtils.quote(XML_COMMAND_URL_ATTRIBUTE) +
					" attribute have to be specified.");
		}

		if (columnDelimiter.length() != 1) {
			throw new ComponentNotReadyException(this, XML_COLUMN_DELIMITER_ATTRIBUTE, 
					"Max. length of column delimiter is one.");
		}
		
		if (properties.containsKey(COPY_BINARY_PARAM) && 
				(isColumnDelimiterUsed() || getNullValue() != null || csvMode)) {
			throw new ComponentNotReadyException(this, "You cannot specify the " +
					StringUtils.quote(XML_COLUMN_DELIMITER_ATTRIBUTE) + " attribute, " +
					StringUtils.quote(COPY_NULL_PARAM) + " param or " + 
					StringUtils.quote("csv") + " param in binary mode (" + 
					StringUtils.quote(COPY_BINARY_PARAM) + ").");
		}
	}

	
	/**
	 * Return true if fileURL exists.
	 * @param fileURL
	 * @return
	 */
	private boolean fileExists(String fileURL) {
		if (StringUtils.isEmpty(fileURL) || !(new File(fileURL).exists())) {
			return false;
		}
		
		return true;
	}
	
	/**
	 * Modify metadata so that they correspond to psql input format. 
	 * Each field is delimited and it has the same delimiter.
	 * Only last field has different delimiter.
	 * 
	 * @param oldMetadata original metadata
	 * @return modified metadata
	 */
	private DataRecordMetadata createPsqlMetadata(DataRecordMetadata originalMetadata) {
		DataRecordMetadata metadata = originalMetadata.duplicate();
		metadata.setRecType(DataRecordMetadata.DELIMITED_RECORD);
		for (int idx = 0; idx < metadata.getNumFields() - 1; idx++) {
			metadata.getField(idx).setDelimiter(columnDelimiter);
			metadata.getField(idx).setSize((short)0);
			setPsqlDateFormat(metadata.getField(idx));
		}
		int lastIndex = metadata.getNumFields() - 1;
		metadata.getField(lastIndex).setDelimiter(DEFAULT_RECORD_DELIMITER);
		metadata.getField(lastIndex).setSize((short)0);
		metadata.setRecordDelimiters("");
		setPsqlDateFormat(metadata.getField(lastIndex));

		return metadata;
	}
	
	/**
	 * If field has format of date or time then default psql format is set.
	 * 
	 * @param field
	 */
	private void setPsqlDateFormat(DataFieldMetadata field) {
		if (field.getType() == DataFieldMetadata.DATE_FIELD || 
				field.getType() == DataFieldMetadata.DATETIME_FIELD) {
			boolean isDate = field.isDateFormat();
			boolean isTime = field.isTimeFormat();

			// if formatStr is undefined then DEFAULT_DATETIME_FORMAT is assigned
			if ((isDate && isTime) || (StringUtils.isEmpty(field.getFormatStr()))) {
				field.setFormatStr(DEFAULT_DATETIME_FORMAT);
			} else if (isDate) {
				field.setFormatStr(DEFAULT_DATE_FORMAT);
			} else {
				field.setFormatStr(DEFAULT_TIME_FORMAT);
			}
		}
	}

	@Override
	public synchronized void free() {
		if(!isInitialized()) return;
		super.free();
		deleteCommandFile();
	}

	/**
	 * If command file isn't temporary then it is deleted.
	 */
	private void deleteCommandFile() {
		if (commandFile == null) {
			return;
		}

		if (commandURL == null) {
			if (!commandFile.delete()) {
				logger.warn("Temp command data file was not deleted.");
			}
		}
	}

	/**
	 * Description of the Method
	 * 
	 * @param nodeXML
	 *            Description of Parameter
	 * @return Description of the Returned Value
	 * @since May 21, 2002
	 */
	public static Node fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException {
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(xmlElement, graph);

		try {
			PostgreSqlDataWriter postgreSQLDataWriter = new PostgreSqlDataWriter(
					xattribs.getString(XML_ID_ATTRIBUTE), 
					xattribs.getString(XML_PSQL_PATH_ATTRIBUTE), 
					xattribs.getString(XML_DATABASE_ATTRIBUTE));

			if (xattribs.exists(XML_COMMAND_URL_ATTRIBUTE)) {
				postgreSQLDataWriter.setCommandURL((xattribs.getString(XML_COMMAND_URL_ATTRIBUTE)));
			}
			if (xattribs.exists(XML_HOST_ATTRIBUTE)) {
				postgreSQLDataWriter.setHost(xattribs.getString(XML_HOST_ATTRIBUTE));
			}
			if (xattribs.exists(XML_USER_ATTRIBUTE)) {
				postgreSQLDataWriter.setUser(xattribs.getString(XML_USER_ATTRIBUTE));
			}
			
			if (xattribs.exists(XML_TABLE_ATTRIBUTE)) {
				postgreSQLDataWriter.setTable(xattribs.getString(XML_TABLE_ATTRIBUTE));
			}
			if (xattribs.exists(XML_FILE_URL_ATTRIBUTE)) {
				postgreSQLDataWriter.setInDataFileName(xattribs.getString(XML_FILE_URL_ATTRIBUTE));
			}
			if (xattribs.exists(XML_COLUMN_DELIMITER_ATTRIBUTE)) {
				postgreSQLDataWriter.setColumnDelimiter(xattribs.getString(XML_COLUMN_DELIMITER_ATTRIBUTE));
			}
			
			if (xattribs.exists(XML_PARAMETERS_ATTRIBUTE)) {
				postgreSQLDataWriter.setParameters(xattribs.getString(XML_PARAMETERS_ATTRIBUTE));
			}

			return postgreSQLDataWriter;
		} catch (Exception ex) {
			throw new XMLConfigurationException(COMPONENT_TYPE + ":" + 
					xattribs.getString(XML_ID_ATTRIBUTE, " unknown ID ") + 
					":" + ex.getMessage(), ex);
		}
	}
	
	@Override
	public void toXML(Element xmlElement) {
		super.toXML(xmlElement);

		xmlElement.setAttribute(XML_PSQL_PATH_ATTRIBUTE, loadUtilityPath);
		xmlElement.setAttribute(XML_DATABASE_ATTRIBUTE, database);
		if (!StringUtils.isEmpty(commandURL)) {
			xmlElement.setAttribute(XML_COMMAND_URL_ATTRIBUTE, commandURL);
		}
		if (!StringUtils.isEmpty(host)) {
			xmlElement.setAttribute(XML_HOST_ATTRIBUTE, host);
		}
		if (!StringUtils.isEmpty(user)) {
			xmlElement.setAttribute(XML_USER_ATTRIBUTE, user);
		}
		if (!StringUtils.isEmpty(table)) {
			xmlElement.setAttribute(XML_TABLE_ATTRIBUTE, table);
		}
		if (!StringUtils.isEmpty(dataURL)) {
			xmlElement.setAttribute(XML_FILE_URL_ATTRIBUTE, dataURL);
		}
		if (!DEFAULT_COLUMN_DELIMITER.equals(columnDelimiter)) {
			xmlElement.setAttribute(XML_COLUMN_DELIMITER_ATTRIBUTE, columnDelimiter);
		}
		
		if (!StringUtils.isEmpty(parameters)) {
			xmlElement.setAttribute(XML_PARAMETERS_ATTRIBUTE, parameters);
		} else if (!properties.isEmpty()) {
			StringBuilder props = new StringBuilder();
			for (Iterator iter = properties.entrySet().iterator(); iter.hasNext();) {
				Entry<String, String> element = (Entry<String, String>) iter.next();
				props.append(element.getKey());
				props.append('=');
				props.append(StringUtils.isQuoted(element.getValue()) ? element.getValue() : StringUtils
						.quote(element.getValue()));
				props.append(';');
			}
			xmlElement.setAttribute(XML_PARAMETERS_ATTRIBUTE, props.toString());
		}
	}

	/** Description of the Method */
	@Override
	public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		super.checkConfig(status);

		if(!checkInputPorts(status, 0, 1)
				|| !checkOutputPorts(status, 0, 0)) {
			return status;
		}

		try {
			init();
		} catch (ComponentNotReadyException e) {
			ConfigurationProblem problem = new ConfigurationProblem(e.getMessage(),
					ConfigurationStatus.Severity.ERROR, this,ConfigurationStatus.Priority.NORMAL);
			if (!StringUtils.isEmpty(e.getAttributeName())) {
				problem.setAttributeName(e.getAttributeName());
			}
			status.add(problem);
        } finally {
        	free();
		}
		
		return status;
	}

	public String getType() {
		return COMPONENT_TYPE;
	}

	private void setColumnDelimiter(String columnDelimiter) {
		this.columnDelimiter = columnDelimiter;
	}

	private void setInDataFileName(String inDataFileName) {
		this.dataURL = inDataFileName;
	}

	private void setHost(String host) {
		this.host = host;
	}

	private void setUser(String user) {
		this.user = user;
	}

	private void setTable(String table) {
		this.table = table;
	}

	private void setParameters(String parameters) {
		this.parameters = parameters;
	}

	public void setCommandURL(String commandURL) {
		this.commandURL = commandURL;
	}

	/**
	 * Helper class for creating command for psql from string pieces and parameters.
	 * Each parameter is one field in array.
	 * 
	 * @see org.jetel.util.exec.ProcBox
	 * @see org.jetel.util.exec.DataConsumer
	 * @author Miroslav Haupt (Mirek.Haupt@javlinconsulting.cz) 
	 * (c) Javlin Consulting (www.javlinconsulting.cz)
	 * @since 5.11.2007
	 */
	private class PsqlCommandBuilder {
		private final static String SWITCH_MARK = "--";
		private Properties params;
		private List<String> cmdList;

		private PsqlCommandBuilder(String command, Properties properties) {
			this.params = properties;
			cmdList = new ArrayList<String>();
			cmdList.add(command);
		}

		/**
		 *  If paramName is in properties and doesn't equal "false" adds: 
		 *  "<i><b>switchMark</b>switchString</i>"<br>
		 *  for exmaple:  --compress
		 * 
		 * @param paramName
		 * @param switchString
		 */
		private void addBooleanParam(String paramName, String switchString) {
			if (params.containsKey(paramName) && !"false".equalsIgnoreCase(params.getProperty(paramName))) {
				cmdList.add(SWITCH_MARK + switchString);
			}
		}

		/**
		 * if paramValue isn't null or paramName is in properties adds:
		 *  "<i><b>switchMark</b>switchString</i>=paramValue"<br>
		 *  for exmaple:  --host=localhost
		 * 
		 * @param paramName
		 * @param switchString
		 * @param paramValue
		 */
		private void addParam(String paramName, String switchString, String paramValue) {
			if (paramValue == null && (paramName == null || !params.containsKey(paramName))) {
				return;
			}

			String param = SWITCH_MARK + switchString + EQUAL_CHAR;

			if (paramValue != null) {
				param += StringUtils.specCharToString(paramValue);
			} else {
				param += StringUtils.specCharToString(params.getProperty(paramName));
			}

			cmdList.add(param);
		}

		
		/**
		 * Return command line where each parameter is one field in array.
		 * @return command line
		 */
		private String[] getCommand() {
			return cmdList.toArray(new String[cmdList.size()]);
		}
	}

	/**
	 * Helper class for creating <i>copy</i> statement.
	 * 
	 * @see org.jetel.util.exec.ProcBox
	 * @see org.jetel.util.exec.DataConsumer
	 * @author Miroslav Haupt (Mirek.Haupt@javlinconsulting.cz) 
	 * (c) Javlin Consulting (www.javlinconsulting.cz)
	 * @since 5.11.2007
	 */
	private class CopyCommandBuilder {
		private StringBuilder command;
		private Properties properties;
		
		public CopyCommandBuilder(String command, Properties properties) {
			this.command = new StringBuilder(command);
			this.properties = properties;
		}

		public StringBuilder append(String str) {
			return command.append(" " + str);
		}

		public String toString() {
			return command.toString();
		}
		
		/**
		 * If paramName is contained in properties and it's value 
		 * doesn't equal "false" then param is added to command.
		 * 
		 * @param paramName name of param in properties
		 * @param param name of param in command
		 */
		public void addBooleanParam(String paramName, String param) {
			if (properties.containsKey(paramName) && 
					!"false".equalsIgnoreCase(properties.getProperty(paramName))) {
				append(param);
			}
		}
		
		/**
		 * Add single quoted param at the end of command.
		 * If <i>paramName</i> is contained in properties then <i>param</i> and 
		 * single quoted value of <i>paramName</i> are added to command.
		 * 
		 * @param paramName name of param in properties
		 * @param param name of param in command
		 */
		public void addSingleQuotedParam(String paramName, String param) {
			addSingleQuotedParam(paramName, param, null);
		}
		
		/**
		 * Add single quoted param at the end of command.
		 * If <i>paramName</i> is contained in properties then <i>param</i> and 
		 * single quoted value of <i>paramName</i> are added to command.
		 * If paramName is null or isn't in properties then default value is 
		 * used instead of value of <i>paramName</i> 
		 * 
		 * @param paramName name of param in properties
		 * @param param name of param in command
		 * @param defaultValue value that is used when paramName isn't in properties 
		 */
		public void addSingleQuotedParam(String paramName, String param, String defaultValue) {
			if (paramName != null && properties.containsKey(paramName)) {
				append(param + " '" + properties.getProperty(paramName) + "'");
				return;
			}
			
			if (defaultValue != null) {
				append(param + " '" + defaultValue + "'");
			}
		}
		
		/**
		 * If <i>paramName</i> is contained in properties then <i>param</i> and 
		 * value of <i>paramName</i> are added to command.
		 * 
		 * @param paramName name of param in properties
		 * @param param name of param in command
		 */
		public void addParam(String paramName, String param) {
			if (properties.containsKey(paramName)) {
				append(param + " " + properties.getProperty(paramName));
			}
		}
	}
}