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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.component.util.CommandBuilder;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.ConfigurationStatus.Priority;
import org.jetel.exception.ConfigurationStatus.Severity;
import org.jetel.exception.JetelException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.Node;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.util.ExceptionUtils;
import org.jetel.util.exec.PlatformUtils;
import org.jetel.util.exec.ProcBox;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.property.RefResFlag;
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
	private static final String XML_COMMAND_URL_ATTRIBUTE = "commandURL";
	private static final String XML_HOST_ATTRIBUTE = "host";
	private static final String XML_FAIL_ON_ERROR_ATTRIBUTE = "failOnError";

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

	private final static String SWITCH_MARK = "--";
	private final static String POSTGRESQL_FILE_NAME_PREFIX = "postgresql";
	private final static String DEFAULT_COLUMN_DELIMITER = "\t";
	private final static String DEFAULT_COLUMN_DELIMITER_IN_CSV_MODE = ",";
	
	private final static String DEFAULT_RECORD_DELIMITER = "\n";
	private final static String DEFAULT_DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
	private final static String DEFAULT_DATE_FORMAT = "yyyy-MM-dd";
	private final static String DEFAULT_TIME_FORMAT = "HH:mm:ss";
	private final static String DEFAULT_NULL_VALUE = "";
	private final static String DEFAULT_NULL_VALUE_IN_TEXT_MODE = "\\N";

	// variables for psql client
	private String commandURL;

	// variables for copy statement
	private File commandFile;

	/** should the component fail in case of any error? */
	private boolean failOnError = true;
	private boolean csvMode; // true if CSV mode is used for loading data
	
	/**
	 * Constructor for the PostgreSqlDataWriter object
	 * 
	 * @param id Description of the Parameter
	 */
	public PostgreSqlDataWriter(String id, String psqlPath, String database) {
		super(id, psqlPath, database);
	}

	public void setFailOnError(boolean failOnError) {
		this.failOnError = failOnError;
	}

	/**
	 * Main processing method for the PsqlDataWriter object
	 * 
	 * @since April 4, 2002
	 */
	@Override
	public Result execute() throws Exception {
		super.execute();
		ProcBox box;
		int processExitValue = 0;

		if (isDataReadFromPort) {
			if (!StringUtils.isEmpty(dataURL)) {
				// dataFile is used for exchange data
				readFromPortAndWriteByFormatter();
				box = createProcBox();
			} else {
				Process process = Runtime.getRuntime().exec(commandLine);
				box = createProcBox(process);

				// stdin is used for exchange data - set data target to stdin of process
				OutputStream processIn = new BufferedOutputStream(process.getOutputStream());
				readFromPortAndWriteByFormatter(processIn);
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
	
	@Override
	protected String[] createCommandLineForLoadUtility() throws ComponentNotReadyException {
		if (PlatformUtils.isWindowsPlatform()) {
			loadUtilityPath = StringUtils.backslashToSlash(loadUtilityPath);
		}

		CommandBuilder cmdBuilder = new CommandBuilder(properties, SWITCH_MARK);

		cmdBuilder.add(loadUtilityPath);
		cmdBuilder.addAttribute(PSQL_DATABASE_SWITCH, database);
		cmdBuilder.addAttribute(PSQL_COMMAND_URL_SWITCH, createCommandFile());
		cmdBuilder.addAttribute(PSQL_HOST_SWITCH, host);
		cmdBuilder.addAttribute(PSQL_USER_SWITCH, user);
		cmdBuilder.addBooleanParam(PSQL_ECHO_ALL_PARAM, PSQL_ECHO_ALL_SWITCH);
		cmdBuilder.addBooleanParam(PSQL_ECHO_QUERIES_PARAM, PSQL_ECHO_QUERIES_SWITCH);
		cmdBuilder.addBooleanParam(PSQL_ECHO_HIDDEN_PARAM, PSQL_ECHO_HIDDEN_SWITCH);
		cmdBuilder.addParam(PSQL_LOG_FILE_PARAM, PSQL_LOG_FILE_SWITCH);
		cmdBuilder.addParam(PSQL_OUTPUT_PARAM, PSQL_OUTPUT_SWITCH);
		cmdBuilder.addParam(PSQL_PORT_PARAM, PSQL_PORT_SWITCH);
		cmdBuilder.addParam(PSQL_PSET_PARAM, PSQL_PSET_SWITCH);
		cmdBuilder.addBooleanParam(PSQL_QUIET_PARAM, PSQL_QUIET_SWITCH);
		cmdBuilder.addBooleanParam(PSQL_NO_PSQLRC_PARAM, PSQL_NO_PSQLRC_SWITCH);
		cmdBuilder.addBooleanParam(PSQL_SINGLE_TRANSACTION_PARAM, PSQL_SINGLE_TRANSACTION_SWITCH);

		return cmdBuilder.getCommand();
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
				commandFile = getFile(commandURL);
				if (commandFile.exists()) {
					return commandFile.getCanonicalPath();
				} else {
					commandFile.createNewFile();
				}
			} else {
				commandFile = createTempFile(POSTGRESQL_FILE_NAME_PREFIX, 
						CONTROL_FILE_NAME_SUFFIX);
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
		CommandBuilder cmdBuilder = new CommandBuilder(properties, SPACE_CHAR);

		if (failOnError) {
    		// The following command ensures that the entire graph fails if the psql utility failed.
    		// Otherwise the graph would succeed even though an error occurred.
    		cmdBuilder.add("\\set ON_ERROR_STOP");
    		cmdBuilder.add(System.getProperty("line.separator"));
		}

		cmdBuilder.add("\\copy");
		// \copy table [ ( column_list ) ]
		cmdBuilder.add(table);
		if (properties.containsKey(COPY_COLUMNS_PARAM)) {
			cmdBuilder.add("(" + properties.getProperty(COPY_COLUMNS_PARAM) + ")");
		}
		
		// from { filename | pstdin }
		cmdBuilder.add("from");
		if (dataFile != null) {
			cmdBuilder.add(dataFile.getCanonicalPath());
		} else {
			cmdBuilder.add(COPY_STDIN_KEYWORD);
		}
		
		// [ with ] [ binary ] [ oids ] [ delimiter [ as ] 'character' ] [ null [ as ] 'string' ]
		if (isWithKeywordUsed()) {
			cmdBuilder.add("with");
			cmdBuilder.addBooleanParam(COPY_BINARY_PARAM, COPY_BINARY_PARAM);
			cmdBuilder.addBooleanParam(COPY_OIDS_PARAM, COPY_OIDS_PARAM);
			if (isColumnDelimiterUsed()) {
				cmdBuilder.addAttribute("delimiter", columnDelimiter, true);
			}
			
			String nullValue = getNullValue();
			if (nullValue != null) {
				// cmdBuilder.addAttribute() method can't be used because nullValue can be "" (empty string)
				cmdBuilder.add(COPY_NULL_PARAM + " '" + StringUtils.specCharToString(nullValue) + "'");

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
			cmdBuilder.add("csv");
			cmdBuilder.addBooleanParam(COPY_CSV_HEADER_PARAM, "header");
			cmdBuilder.addParam(COPY_CSV_QUOTE_PARAM, "quote", true);
			cmdBuilder.addParam(COPY_CSV_ESCAPE_PARAM, "escape", true);
			cmdBuilder.addParam(COPY_CSV_FORCE_NOT_NULL_PARAM, "force not null");
		}
		
		return cmdBuilder.getCommandAsString();
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
	
	@Override
	protected void preInit() throws ComponentNotReadyException {
		csvMode = isCsvModeUsed(properties);
		setDefaultColumnDelimiter(csvMode);
	}

	@Override
	protected void initDataFile() throws ComponentNotReadyException {
		if (isDataReadFromPort) {
			if (dataURL != null) {
				dataFile = getFile(dataURL);
				dataFile.delete();
			}
		} else {
			dataFile = openFile(dataURL);
		}
	}
	
	@Override
	protected String getColumnDelimiter() {
		return columnDelimiter;
	}

	@Override
	protected String getRecordDelimiter() {
		return DEFAULT_RECORD_DELIMITER;
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

	@Override
	protected void checkParams() throws ComponentNotReadyException {
		if (StringUtils.isEmpty(loadUtilityPath)) {
			throw new ComponentNotReadyException(this, StringUtils.quote(XML_PSQL_PATH_ATTRIBUTE)
					+ " attribute have to be set.");
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

		if (!isDataReadFromPort && !fileExists(dataURL) && !fileExists(commandURL)) {
			throw new ComponentNotReadyException(this, "Input port or " + 
					StringUtils.quote(XML_FILE_URL_ATTRIBUTE) + 
					" attribute or " + StringUtils.quote(XML_COMMAND_URL_ATTRIBUTE) +
					" attribute have to be specified and specified file must exist.");
		}

		/*Assumes that if delimiter is null, it will be set right away, since checkParams()
		 *  is run right before preInit() in init() of BulkLoader*/
		if (columnDelimiter != null && columnDelimiter.length() != 1) {
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
	
	@Override
	protected void setLoadUtilityDateFormat(DataFieldMetadata field) {
		setLoadUtilityDateFormat(field, DEFAULT_TIME_FORMAT, 
				DEFAULT_DATE_FORMAT, DEFAULT_DATETIME_FORMAT, null);
	}

	@Override
	protected void deleteTempFiles() {
		super.deleteTempFiles();
		deleteTempFile(commandFile, commandURL);
	}

	/**
	 * Description of the Method
	 * 
	 * @param nodeXML
	 *            Description of Parameter
	 * @return Description of the Returned Value
	 * @throws AttributeNotFoundException 
	 * @since May 21, 2002
	 */
	public static Node fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException, AttributeNotFoundException {
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(xmlElement, graph);

		PostgreSqlDataWriter postgreSQLDataWriter = new PostgreSqlDataWriter(
				xattribs.getString(XML_ID_ATTRIBUTE), 
				xattribs.getStringEx(XML_PSQL_PATH_ATTRIBUTE, RefResFlag.URL), 
				xattribs.getString(XML_DATABASE_ATTRIBUTE));

		if (xattribs.exists(XML_FAIL_ON_ERROR_ATTRIBUTE)) {
			postgreSQLDataWriter.setFailOnError(xattribs.getBoolean(XML_FAIL_ON_ERROR_ATTRIBUTE));
		}

		if (xattribs.exists(XML_COMMAND_URL_ATTRIBUTE)) {
			postgreSQLDataWriter.setCommandURL((xattribs.getStringEx(XML_COMMAND_URL_ATTRIBUTE, RefResFlag.URL)));
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
			postgreSQLDataWriter.setFileUrl(xattribs.getStringEx(XML_FILE_URL_ATTRIBUTE, RefResFlag.URL));
		}
		if (xattribs.exists(XML_COLUMN_DELIMITER_ATTRIBUTE)) {
			postgreSQLDataWriter.setColumnDelimiter(xattribs.getString(XML_COLUMN_DELIMITER_ATTRIBUTE));
		}
		
		if (xattribs.exists(XML_PARAMETERS_ATTRIBUTE)) {
			postgreSQLDataWriter.setParameters(xattribs.getString(XML_PARAMETERS_ATTRIBUTE));
		}

		return postgreSQLDataWriter;
	}
	
	/** Description of the Method */
	@Override
	public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		super.checkConfig(status);

		if(!checkInputPorts(status, 0, 1)
				|| !checkOutputPorts(status, 0, 0)) {
			return status;
		}
		
		isDataReadFromPort = !getInPorts().isEmpty();
		isDataReadDirectlyFromFile = !isDataReadFromPort && !StringUtils.isEmpty(dataURL);
        isDataWrittenToPort = !getOutPorts().isEmpty();
        properties = parseParameters(parameters);
        
        //----Check Parameters        
        if (StringUtils.isEmpty(loadUtilityPath)) {
        	status.add(new ConfigurationProblem(StringUtils.quote(XML_PSQL_PATH_ATTRIBUTE) + " attribute have to be set.",
					Severity.ERROR, this, Priority.HIGH, XML_PSQL_PATH_ATTRIBUTE));
		}		
		if (StringUtils.isEmpty(database)) {
			status.add(new ConfigurationProblem(StringUtils.quote(XML_DATABASE_ATTRIBUTE) + " attribute have to be set.",
					Severity.ERROR, this, Priority.HIGH, XML_DATABASE_ATTRIBUTE));
		}
		try {
			if (!fileExists(commandURL)) {
				if (StringUtils.isEmpty(table)) {
					status.add(new ConfigurationProblem(StringUtils.quote(XML_TABLE_ATTRIBUTE) + " attribute has to be specified or " +
							StringUtils.quote(XML_COMMAND_URL_ATTRIBUTE) + " attribute has to be specified and file at the URL must exists.",
							Severity.ERROR, this, Priority.NORMAL));
				}
				if (!isDataReadFromPort && !fileExists(dataURL)) {
					status.add(new ConfigurationProblem("Input port or " + StringUtils.quote(XML_FILE_URL_ATTRIBUTE) + " attribute or " +
							StringUtils.quote(XML_COMMAND_URL_ATTRIBUTE) + " attribute have to be specified and specified file must exist.",
							Severity.ERROR, this, Priority.NORMAL));
				}
			}			
		} catch (ComponentNotReadyException e) {
			status.add(new ConfigurationProblem(ExceptionUtils.getMessage(e),	ConfigurationStatus.Severity.ERROR, this,ConfigurationStatus.Priority.NORMAL));
		}		
		csvMode = isCsvModeUsed(properties);
		setDefaultColumnDelimiter(csvMode);		
		if (columnDelimiter.length() != 1) {
			status.add(new ConfigurationProblem("Max. length of column delimiter is one.",
					Severity.ERROR, this, Priority.NORMAL, XML_COLUMN_DELIMITER_ATTRIBUTE));
		}
		if (properties.containsKey(COPY_BINARY_PARAM) && (isColumnDelimiterUsed() || getNullValue() != null || csvMode)) {
			status.add(new ConfigurationProblem("You cannot specify the " +	StringUtils.quote(XML_COLUMN_DELIMITER_ATTRIBUTE) + " attribute, " +
					StringUtils.quote(COPY_NULL_PARAM) + " param or " +	StringUtils.quote("csv") + " param in binary mode (" + 
					StringUtils.quote(COPY_BINARY_PARAM) + ").", Severity.ERROR, this, Priority.NORMAL, XML_COLUMN_DELIMITER_ATTRIBUTE));			
		}
		//check creation of data and control file
		try {			
			initDataFile();
			createCommandFile();
		} catch (ComponentNotReadyException e) {
			status.add(new ConfigurationProblem(ExceptionUtils.getMessage(e),	ConfigurationStatus.Severity.ERROR, this,ConfigurationStatus.Priority.NORMAL));
		}
		deleteTempFiles();
		return status;
	}

	public void setCommandURL(String commandURL) {
		this.commandURL = commandURL;
	}
	
	/**
	 * Return list of all adding parameters (parameters attribute).
	 * Deprecated parameters mustn't be used.
	 * It is intended for use in GUI in parameter editor.
	 * @return list of parameters that is viewed in parameters editor
	 */
	public static String[] getAddingParameters() {
		return new String[] {
			// params for psql client
			PSQL_ECHO_ALL_PARAM,
			PSQL_ECHO_QUERIES_PARAM,
			PSQL_ECHO_HIDDEN_PARAM,
			PSQL_LOG_FILE_PARAM,
			PSQL_OUTPUT_PARAM,
			PSQL_PORT_PARAM,
			PSQL_PSET_PARAM,
			PSQL_QUIET_PARAM,
			PSQL_NO_PSQLRC_PARAM,
			PSQL_SINGLE_TRANSACTION_PARAM,

			// params for copy statement
			COPY_COLUMNS_PARAM,
			COPY_BINARY_PARAM,
			COPY_OIDS_PARAM,
			COPY_NULL_PARAM,
			COPY_CSV_PARAM,
			COPY_CSV_HEADER_PARAM,
			COPY_CSV_QUOTE_PARAM,
			COPY_CSV_ESCAPE_PARAM,
			COPY_CSV_FORCE_NOT_NULL_PARAM
		};
	}
}