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
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.component.util.CommandBuilder;
import org.jetel.data.DataRecord;
import org.jetel.data.formatter.DelimitedDataFormatter;
import org.jetel.data.parser.DelimitedDataParser;
import org.jetel.data.parser.Parser;
import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.JetelException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPort;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.SynchronizeUtils;
import org.jetel.util.exec.DataConsumer;
import org.jetel.util.exec.LoggerDataConsumer;
import org.jetel.util.exec.PortDataConsumer;
import org.jetel.util.exec.ProcBox;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.string.StringUtils;
import org.w3c.dom.Element;

/**
 *  <h3>Informix data writer</h3>
 *
 * <!-- All records from input port 0 are loaded into informix database. Connection to database is not through JDBC driver,
 * this component uses the dbload or load2 utility for this purpose. Bad rows send to output port 0.-->
 *
 * <table border="1">
 *  <th>Component:</th>
 * <tr><td><h4><i>Name:</i></h4></td>
 * <td>Informix data writer</td></tr>
 * <tr><td><h4><i>Category:</i></h4></td>
 * <td></td></tr>
 * <tr><td><h4><i>Description:</i></h4></td>
 * <td>This component loads data to Informix database using the dbload or load2 utility. 
 * There is created temporary file with dbload commands depending on input parameters. Data are read from given input file or from the input port and loaded to database. 
 * On Linux/Unix system data transfer can be processed by stdin.<br>
 * Any generated commands/files can be optionally logged to help diagnose problems.<br>
 * CloverETL must run on the same machine as the Informix server with accesed database.
 * Dbload command line tool must be also available (standard part of Informix server).
 * </td></tr>
 * <tr><td><h4><i>Inputs:</i></h4></td>
 * <td>[0] - input records. It can be omitted - then <b>fileURL</b> has to be provided.</td></tr>
 * <tr><td><h4><i>Outputs:</i></h4></td>
 * <td>[0] - optionally one output port defined/connected - rejected records.
 * Metadata on this port must have the same type of field as input metadata. Output metadata 
 * has a additional fields with row number (integer) and error message (string).
 * These two fields are after data fields.
 * Note: when load utility is used then data fields are empty.
 * </td></tr>
 * <tr><td><h4><i>Comment:</i></h4></td>
 * <td></td></tr>
 * </table>
 *  <br>
 *  <table border="1">
 *  <th>XML attributes:</th>
 *  <tr><td><b>type</b></td><td>"INFORMIX_DATA_WRITER"</td></tr>
 *  <tr><td><b>id</b></td><td>component identification</td></tr>
 *  <tr><td><b>dbLoaderPath</b></td><td>path to dbload or load2 utility</td></tr>
 *  <tr><td><b>database</b></td><td>the name of the database to receive the data</td></tr>
 *  <tr><td><b>host</b><br><i>optional</i></td><td>the name of the informix server</td></tr>
 *  <tr><td><b>username</b><br><i>optional</i></td><td>username<br>Note: used only when <i>useLoadUtility</i> = true</td></tr>
 *  <tr><td><b>password</b><br><i>optional</i></td><td>password<br>Note: used only when <i>useLoadUtility</i> = true</td></tr>
 *  <tr><td><b>table</b><br><i>optional</i></td><td>table name, where data are loaded<br/>
 *  Note: table attribute or command attribute must be defined</td></tr>
 *  <tr><td><b>command</b><br><i>optional</i></td><td>a control script for the dbload utility;
 *  	if this parameter is empty default control script is used<br/>
 *  Note: table attribute or command attribute must be defined</td></tr>
 *  <tr><td><b>errorLog</b><br><i>optional</i></td><td>the filename or pathname of an error log file
 *  	if this parameter is empty default errorLog name is used (default = ./error.log)</td></tr</td></tr>
 *  <tr><td><b>maxErrors</b><br><i>optional</i></td><td>the number of bad rows that dbload reads before terminating.
 *   	if this parameter is empty default value is used (default = 10).</td></tr>
 *  <tr><td><b>ignoreRows</b><br><i>optional</i></td><td>the number of rows to ignore in the input file;<br/>
 *  	instructs dbload to read and ignore the specified number of new-line 
 *  characters in the input file before it begins to process.</td></tr>
 *  <tr><td><b>commitInterval</b><br><i>optional</i></td><td>commit interval in number of rows;
 *   	if this parameter is empty default value is used (default = 100)</td></tr>
 *  <tr><td><b>columnDelimiter</b><br><i>optional</i></td><td>char delimiter used for each column in data (default = '|')</br>
 *  Delimiter has length one and value of the delimiter mustn't be contained in data.</td></tr>
 *  <tr><td><b>fileURL</b><br><i>optional</i></td><td>Path to data file to be loaded.<br>
 *  Normally this file is a temporary storage for data to be passed to dbload utility. 
 *  If <i>fileURL</i> is not specified, at Windows platform the file is created in Clover or OS temporary directory and deleted after load finishes.
 *  At Linux/Unix system stdio is used instead of temporary file.<br>
 *  If <i>fileURL</i> is specified, temporary file is created within given path and name and not 
 *  deleted after being loaded. Next graph run overwrites it. 
 *  <br>
 *  There is one more meaning of this parameter. If input port is not specified, this file 
 *  is used only for reading by dbload utility and must already contain data in format expected by 
 *  load. The file is neither deleted nor overwritten.</td></tr>
 *  <tr><td><b>useLoadUtility</b><br><i>optional</i></td><td>Defines if standard informix utility (false) or 
 *  load utility (true) is used to loading data. (default = false).</td></tr>
 *  <tr><td><b>ignoreUniqueKeyVialotion</b><br><i>optional</i></td><td>Defines if ignore unique key violation. (default = false).<br>
 *  Note: used only when <i>useLoadUtility</i> = true</td></tr>
 *  <tr><td><b>useInsertCursor</b><br><i>optional</i></td><td>Use insert cursor. Using insert cursor doubles data transfer performance. (default = true).<br>
 *  Note: used only when <i>useLoadUtility</i> = true</td></tr>
 *  </table>
 *
 *	<h4>Example:</h4>
 *  Reading data from input port (dbload):
 *  <pre>&lt;Node dbLoaderPath="dbload" commitInterval="1000" database="//demo_on/test" 
 *	errorLog="error.log" table="customers" columnDelimiter="|" 
 *	id="INFORMIX_DATA_WRITER0" type="INFORMIX_DATA_WRITER"/&gt;
 *  </pre>
 *  Reading data from flat file (dbload):
 *  <pre>&lt;Node dbLoaderPath="dbload" database="//demo_on/test" 
 *	errorLog="error.log" table="customers" columnDelimiter=";" 
 *	fileURL="/home/student/informix_data/inPlain.txt"
 *	id="INFORMIX_DATA_WRITER0" type="INFORMIX_DATA_WRITER"/&gt;
 *  </pre>
 *  Reading data from input port (load2):
 *  <pre>&lt;Node database="test" dbLoaderPath="${WORKSPACE}/bin/load"
 *  username="informix" password="informix" table="test" useLoadUtility="true"
 *  host="demo_on" id="INFORMIX_DATA_WRITER3" type="INFORMIX_DATA_WRITER"/&gt;
 *  </pre>
 *  Reading data from flat file (load2):
 *  <pre>&lt;Node database="test" dbLoaderPath="${WORKSPACE}/bin/load"
 *  fileURL="${WORKSPACE}/data/delimited/informixFlat.dat" 
 *  host="demo_on" table="test" useLoadUtility="true"
 *  id="INFORMIX_DATA_WRITER2" type="INFORMIX_DATA_WRITER"/&gt;
 *  </pre>
 *
 * @author      Miroslav Haupt (Mirek.Haupt@javlinconsulting.cz)
 *(c) Javlin Consulting (www.javlinconsulting.cz)
 * @see         org.jetel.graph.TransformationGraph
 * @see         org.jetel.graph.Node
 * @see         org.jetel.graph.Edge
 * @since 		20.8.2007
 */
public class InformixDataWriter extends BulkLoader {
	private static Log logger = LogFactory.getLog(InformixDataWriter.class);

    /**  Description of the Field */
	private static final String XML_DB_LOADER_PATH_ATTRIBUTE = "dbLoaderPath";
    private static final String XML_COMMAND_ATTRIBUTE = "command";
    private static final String XML_HOST_ATTRIBUTE = "host";
    private static final String XML_ERROR_LOG_ATTRIBUTE = "errorLog";
    private static final String XML_MAX_ERRORS_ATTRIBUTE = "maxErrors";
    private static final String XML_IGNORE_ROWS_ATTRIBUTE = "ignoreRows";
    private static final String XML_COMMIT_INTERVAL_ATTRIBUTE = "commitInterval";
    private static final String XML_USE_LOAD_UTILITY_ATTRIBUTE = "useLoadUtility";
    private static final String XML_IGNORE_UNIQUE_KEY_VIOLATION_ATTRIBUTE = "ignoreUniqueKeyViolation";
    private static final String XML_USE_INSERT_CUROSOR_ATTRIBUTE = "useInsertCursor";
    
    public final static String COMPONENT_TYPE = "INFORMIX_DATA_WRITER";

    private final static String INFORMIX_COMMAND_PATH_OPTION = "c";
    private final static String INFORMIX_DATABASE_OPTION = "d";
    private final static String INFORMIX_ERROR_LOG_OPTION = "l";
    private final static String INFORMIX_ERRORS_OPTION = "e";
    private final static String INFORMIX_IGNORE_ROWS_OPTION = "i";
    private final static String INFORMIX_COMMIT_INTERVAL_OPTION = "n";
    
    private final static String LOAD_DATABASE_OPTION = "d";
    private final static String LOAD_ERROR_LOG_OPTION = "l";
    private final static String LOAD_ERRORS_OPTION = "i";
    private final static String LOAD_COMMIT_INTERVAL_OPTION = "n";
    private final static String LOAD_HOST_OPTION = "s"; // server
    private final static String LOAD_USER_OPTION = "u";
    private final static String LOAD_PASSWORD_OPTION = "p";
    private final static String LOAD_TABLE_OPTION = "t";
    private final static String LOAD_IGNORE_UNIQUE_KEY_VIOLATION_OPTION = "k";
    private final static String LOAD_USE_INSERT_CURSOR_OPTION = "z";
    
    private final static String SWITCH_MARK = "-";
    private final static String DATA_FILE_NAME_PREFIX = "data";
    private final static String DATA_FILE_NAME_SUFFIX = ".dat";
    private final static String LOADER_FILE_NAME_PREFIX = "loader";
    private final static String DEFAULT_ERROR_FILE = "error.log";
    private final static String DEFAULT_COLUMN_DELIMITER = "|";
    private final static String DEFAULT_RECORD_DELIMITER = "\n";
    private final static String LINE_SEPARATOR = System.getProperty("line.separator");
    private final static String UNIX_STDIN = "/dev/stdin";
    private final static String DEFAULT_DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
    private final static String DEFAULT_DATE_FORMAT = "MM/dd/yyyy"; 
    private final static String DEFAULT_TIME_FORMAT = "HH:mm:ss";
    private final static boolean DEFAULT_USE_LOAD_UTILITY = false;
    private final static boolean DEFAULT_IGNORE_UNIQUE_KEY_VIOLATION = false;
    private final static boolean DEFAULT_USE_INSERT_CURSOR = true;
    
    // variables for dbload's command
    private String command; //contains user-defined control script fot dbload utility
    private String errorLog;
    private int maxErrors = UNUSED_INT;
    private int ignoreRows = UNUSED_INT;
    private int commitInterval = UNUSED_INT;
    private String commandFileName; //file where dbload command is saved
    private boolean useLoadUtility = DEFAULT_USE_LOAD_UTILITY;
    private boolean ignoreUniqueKeyViolation = DEFAULT_IGNORE_UNIQUE_KEY_VIOLATION;
    private boolean useInsertCursor = DEFAULT_USE_INSERT_CURSOR;

    private String[] commandLine; // command line of dbload
    private String tmpDataFileName; // file that is used for exchange data between clover and dbload
    
	/**
     * Constructor for the InformixDataWriter object
     *
     * @param  id  Description of the Parameter
     */
    public InformixDataWriter(String id, String dbLoaderPath, String database) { 
        super(id, dbLoaderPath, database);
        
        columnDelimiter = DEFAULT_COLUMN_DELIMITER;
    }
    
    /**
     *  Main processing method for the InformixDataWriter object
     *
     * @since    April 4, 2002
     */
    public Result execute() throws Exception {
        ProcBox box;
        int processExitValue = 0;

        if (isDataReadFromPort) {
	        if (!StringUtils.isEmpty(dataURL) || (ProcBox.isWindowsPlatform() && !useLoadUtility)) {
	        	// temp file is used for exchange data
	        	readFromPortAndWriteByFormatter(new FileOutputStream(tmpDataFileName));
	        	
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
        	throw new JetelException((useLoadUtility ? "Load" : "Dbload") + " utility has failed.");
		}
        
        return runIt ? Result.FINISHED_OK : Result.ABORTED;
    }
    
	/**
	 * Create instance of ProcBox.
	 * @param process running process; when process is null, default process is created
	 * @return instance of ProcBox
	 * @throws IOException
	 */
	// TODO this method will be deleted from this class after removing commandLine attribute to superclass
	protected ProcBox createProcBox(Process process) throws IOException {
		if (process == null) {
			process = Runtime.getRuntime().exec(commandLine);			
		}
        return new ProcBox(process, null, consumer, errConsumer);
	}
    
	/**
     * Create command line for process, where dbload utility is running.
     * Example: /home/Informix/bin/dbload -d test@pnc -c cmdData.ctl -l data.log
     * 
     * @return array first field is name of laod utility and the others fields are parameters
	 * @throws ComponentNotReadyException when command file isn't created
     */
    private String[] createCommandLineForDbLoader() {
    	CommandBuilder cmdBuilder = new CommandBuilder(properties, SWITCH_MARK, SPACE_CHAR);
    	
		if (useLoadUtility) {
			cmdBuilder.add(loadUtilityPath);
			
			cmdBuilder.addAttribute(LOAD_DATABASE_OPTION, database);
			
			if (!StringUtils.isEmpty(host)) {
				cmdBuilder.addAttribute(LOAD_HOST_OPTION, host);
			}
			if (!StringUtils.isEmpty(user)) {
				cmdBuilder.addAttribute(LOAD_USER_OPTION, user);
			}
			if (!StringUtils.isEmpty(password)) {
				cmdBuilder.addAttribute(LOAD_PASSWORD_OPTION, password);
			}
			cmdBuilder.addAttribute(LOAD_TABLE_OPTION, table);
			
			if (commitInterval != UNUSED_INT) {
				cmdBuilder.addAttribute(LOAD_COMMIT_INTERVAL_OPTION, commitInterval);
			}
			if (ignoreUniqueKeyViolation) {
				cmdBuilder.add(SWITCH_MARK + LOAD_IGNORE_UNIQUE_KEY_VIOLATION_OPTION);
			}
			if (useInsertCursor) {
				cmdBuilder.add(SWITCH_MARK + LOAD_USE_INSERT_CURSOR_OPTION);
			}
			if (maxErrors != UNUSED_INT) {
				cmdBuilder.addAttribute(LOAD_ERRORS_OPTION, maxErrors);
			}
			if (!StringUtils.isEmpty(errorLog)) {
				cmdBuilder.addAttribute(LOAD_ERROR_LOG_OPTION, errorLog);
			}
			if (!isDataReadFromPort || !StringUtils.isEmpty(dataURL)) {
				cmdBuilder.add(tmpDataFileName);
			} // else - when no file is defined stdio is used
		} else {
			cmdBuilder.add(loadUtilityPath);
			
			cmdBuilder.addAttribute(INFORMIX_COMMAND_PATH_OPTION, commandFileName);
			
			if (!StringUtils.isEmpty(host)) {
				database = "//" + host + "/" + database;
			}
			cmdBuilder.addAttribute(INFORMIX_DATABASE_OPTION, database);
			
			cmdBuilder.addAttribute(INFORMIX_ERROR_LOG_OPTION, errorLog);
			
			if (maxErrors != UNUSED_INT) {
				cmdBuilder.addAttribute(INFORMIX_ERRORS_OPTION, maxErrors);
			}
			if (ignoreRows != UNUSED_INT) {
				cmdBuilder.addAttribute(INFORMIX_IGNORE_ROWS_OPTION, ignoreRows);
			}
			if (commitInterval != UNUSED_INT) {
				cmdBuilder.addAttribute(INFORMIX_COMMIT_INTERVAL_OPTION, commitInterval);
			}
		}
		
		return cmdBuilder.getCommand();
    }

    /**
     *  Description of the Method
     *
     * @exception  ComponentNotReadyException  Description of the Exception
     * @since                                  April 4, 2002
     */
    public void init() throws ComponentNotReadyException {
        if (isInitialized()) return;
		super.init();

		isDataReadFromPort = !getInPorts().isEmpty() && StringUtils.isEmpty(command);

		try {
			checkParams();
		} catch (ComponentNotReadyException cnre) {
			free();
			throw new ComponentNotReadyException(cnre);
		}

		// prepare name for temporary file
		try {
			if (!useLoadUtility) {
	            commandFileName = File.createTempFile(LOADER_FILE_NAME_PREFIX, 
	            		CONTROL_FILE_NAME_SUFFIX, TMP_DIR).getCanonicalPath();
	            
	            if (errorLog == null) {
	            	errorLog = new File(TMP_DIR, DEFAULT_ERROR_FILE).getCanonicalPath();
	            }
	            
	            if (isDataReadFromPort) {
		        	if (ProcBox.isWindowsPlatform() || dataURL != null) {
		        		if (dataURL != null) {
		        			tmpDataFileName = new File(dataURL).getCanonicalPath();
		        		} else {
		        			tmpDataFileName = File.createTempFile(DATA_FILE_NAME_PREFIX, 
			            			DATA_FILE_NAME_SUFFIX, TMP_DIR).getCanonicalPath();
		        		}
		            } else {
		            	tmpDataFileName = UNIX_STDIN;
		            }
	            } else { // loadUtility
	            	tmpDataFileName = new File(dataURL).getCanonicalPath();
	            }
	
		        createCommandFile();
			} else {
				if (dataURL != null || !isDataReadFromPort) {
	    			tmpDataFileName = new File(dataURL).getCanonicalPath();
	    		}
			}
		} catch (IOException ioe) {
    		free();
            throw new ComponentNotReadyException(this, "Some of the temporary files cannot be created.");
		}
		
		commandLine = createCommandLineForDbLoader();
		printCommandLineToLog(commandLine, logger);
		
        if (isDataReadFromPort) {
	        dbMetadata = createLoadUtilityMetadata(getColumnDelimiter(), DEFAULT_RECORD_DELIMITER);
	        
	        // init of data formatter
	        formatter = new DelimitedDataFormatter(CHARSET_NAME);
	        formatter.init(dbMetadata);
        }
        
        try {
	        if (isDataWrittenToPort) {
	        	if (useLoadUtility) {
	        		consumer = new LoggerDataConsumer(LoggerDataConsumer.LVL_DEBUG, 0);
	        		errConsumer = new InformixPortDataConsumer(getOutputPort(WRITE_TO_PORT), LoggerDataConsumer.LVL_ERROR);
	        	} else { // dbload
	        		consumer = new InformixPortDataConsumer(getOutputPort(WRITE_TO_PORT), LoggerDataConsumer.LVL_DEBUG);
	        		errConsumer = new LoggerDataConsumer(LoggerDataConsumer.LVL_ERROR, 0);
	        	}
	        } else {
        		consumer = new LoggerDataConsumer(LoggerDataConsumer.LVL_DEBUG, 0);
        		errConsumer = new LoggerDataConsumer(LoggerDataConsumer.LVL_ERROR, 0);
	        }
        } catch (ComponentNotReadyException cnre) {
        	free();
        	throw new ComponentNotReadyException(this, "Error during initialization of InformixPortDataConsumer.", cnre);
        }
    }

    @Override
	protected void setLoadUtilityDateFormat(DataFieldMetadata field) {
    	setLoadUtilityDateFormat(field, DEFAULT_TIME_FORMAT, 
				DEFAULT_DATE_FORMAT, DEFAULT_DATETIME_FORMAT, null);
	}
    
    private String getColumnDelimiter() {
		if (useLoadUtility) {
			return DEFAULT_COLUMN_DELIMITER;
		}
		
		return columnDelimiter;
    }
    
    /**
	 * Checks if mandatory attributes are defined.
	 * And check combination of some parameters.
	 * 
	 * @throws ComponentNotReadyException if any of conditions isn't fulfilled
	 */
	private void checkParams() throws ComponentNotReadyException {
		if (StringUtils.isEmpty(loadUtilityPath)) {
			throw new ComponentNotReadyException(this, StringUtils.quote(XML_DB_LOADER_PATH_ATTRIBUTE)
					+ " attribute have to be set.");
		}

		if (StringUtils.isEmpty(database)) {
			throw new ComponentNotReadyException(this, 
					StringUtils.quote(XML_DATABASE_ATTRIBUTE) + " attribute have to be set.");
		}
		
    	if (columnDelimiter != null && columnDelimiter.length() != 1) {
			throw new ComponentNotReadyException(this, XML_COLUMN_DELIMITER_ATTRIBUTE, "Max. length of column delimiter is one.");
		}
    	if (maxErrors != UNUSED_INT && maxErrors < 0) {
    		throw new ComponentNotReadyException(this, XML_MAX_ERRORS_ATTRIBUTE + " mustn't be less than 0.");
    	}
    	if (ignoreRows != UNUSED_INT && ignoreRows < 0) {
    		throw new ComponentNotReadyException(this, XML_IGNORE_ROWS_ATTRIBUTE + " mustn't be less than 0.");
    	}
		if (commitInterval != UNUSED_INT && commitInterval < 0) {
    		throw new ComponentNotReadyException(this, XML_COMMIT_INTERVAL_ATTRIBUTE + " mustn't be less than 0.");
		}
		
		// check if each of mandatory attributes is set
		if (StringUtils.isEmpty(command) && StringUtils.isEmpty(table)) {
			throw new ComponentNotReadyException(this, 
					StringUtils.quote(XML_TABLE_ATTRIBUTE) + " attribute has to be specified or " +
					StringUtils.quote(XML_COMMAND_ATTRIBUTE) + 
					" attribute has to be specified.");
		}
		
		if (!isDataReadFromPort) {
			if (StringUtils.isEmpty(dataURL)) {
        		throw new ComponentNotReadyException(this, "There is neither input port nor " 
        				+ StringUtils.quote(XML_FILE_URL_ATTRIBUTE) + " attribute specified.");
        	}
			if (!fileExists(dataURL)) {
				throw new ComponentNotReadyException(this, "Data file "
							+ StringUtils.quote(dataURL) + " not exists.");
			}
		}
		
		// report on ignoring some attributes
		List<String> ignoredFields = new ArrayList<String>();
		if (useLoadUtility) {
			if (!StringUtils.isEmpty(command)) {
				ignoredFields.add(XML_COMMAND_ATTRIBUTE);
			}
			if (ignoreRows != UNUSED_INT) {
				ignoredFields.add(XML_IGNORE_ROWS_ATTRIBUTE);
			}
			if (!DEFAULT_COLUMN_DELIMITER.equals(columnDelimiter)) {
				ignoredFields.add(XML_COLUMN_DELIMITER_ATTRIBUTE);
			}
		} else { // dbload
			if (!StringUtils.isEmpty(user)) {
				ignoredFields.add(XML_USER_ATTRIBUTE);
			}
			if (!StringUtils.isEmpty(password)) {
				ignoredFields.add(XML_PASSWORD_ATTRIBUTE);
			}
			if (ignoreUniqueKeyViolation != DEFAULT_IGNORE_UNIQUE_KEY_VIOLATION) {
				ignoredFields.add(XML_IGNORE_UNIQUE_KEY_VIOLATION_ATTRIBUTE);
			}
			if (useInsertCursor != DEFAULT_USE_INSERT_CURSOR) {
				ignoredFields.add(XML_USE_INSERT_CUROSOR_ATTRIBUTE);
			}
		}
		
		if (!ignoredFields.isEmpty()) {
			StringBuilder fields = new StringBuilder("(");
			for (String field : ignoredFields) {
				fields.append(StringUtils.quote(field));
				fields.append(", ");
			}
			fields.replace(fields.length() - 2, fields.length(), ")");
			
			logger.warn("Attributes " + fields + " are ignored. " +
					"They are used only when '" +
					(useLoadUtility ? "dbload" : "load") + "' utility is used.");
		}
    }
    
    @Override
	public synchronized void free() {
        if(!isInitialized()) return;
		super.free();
		deleteFile(commandFileName, logger);
		deleteDataFile();
	}

	/**
     * Create new temp file with control script for dbload utility.
     * @throws ComponentNotReadyException
     */
    private void createCommandFile() throws ComponentNotReadyException {
    	File commandFile = new File(commandFileName);
        FileWriter commandWriter;
        try {
        	commandFile.createNewFile();
        	commandWriter = new FileWriter(commandFile);
        	String content = null;
        	if (command == null) {
        		if (isDataReadFromPort) {
        			content = getDefaultControlFileContent(table, getInputPort(READ_FROM_PORT).getMetadata().getNumFields(),
        						columnDelimiter, tmpDataFileName);
        		} else if (isDataWrittenToPort) {
        			content = getDefaultControlFileContent(table, 
        					getOutputPort(READ_FROM_PORT).getMetadata().getNumFields() - InformixPortDataConsumer.NUMBER_OF_ADDED_FIELDS,
        						columnDelimiter, dataURL);
        		} else { // data is read from file and bad rows isn't written to any port
        			content = getDefaultControlFileContent(table, getNumFieldsFromInFile(),
    						columnDelimiter, dataURL);
        		}
        		logger.debug("Control file content: " + content);
        	} else {
        		content = command;
        	}

            commandWriter.write(content);
            commandWriter.close();
        } catch(IOException ex) {
            throw new ComponentNotReadyException(this, "Can't create temp control file for dbload utility.", ex);
        }
    }

    /**
     * @return return number of field in one row of inDataFileName file
     * @throws ComponentNotReadyException
     */
    private int getNumFieldsFromInFile() throws ComponentNotReadyException {
		BufferedReader reader = null;
		String line = null;
		try {
			reader = new BufferedReader(new FileReader(new File(dataURL)));
			if (ignoreRows != UNUSED_INT) {
				// skip ignore rows
				for (int i = ignoreRows; i < ignoreRows; i++) {
					line = reader.readLine();
				}
			}
			line = reader.readLine();
		} catch (IOException ioe) {
			throw new ComponentNotReadyException(this, "Error during opening or reading file ." 
					+ StringUtils.quote(dataURL), ioe);
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e) {
					logger.warn("File" + StringUtils.quote(dataURL) + " wasn't closed.");
				}
			}
		}

		if (line == null) {
			throw new ComponentNotReadyException(this, "Error during reading file ." 
					+ StringUtils.quote(dataURL));
		}
		return line.split(columnDelimiter).length;
    }
    
    /**
     * Generates default control script.
     * 
     * Default script:
     * 
     * FILE <file> DELIMITER '<delimiter>' <numFields>;
     * INSERT INTO <tableName>;
     * 
     * @param tableName
     * @param numFields
     * @param delimiter
     * @param fileName
     * @return control script
     */
    private String getDefaultControlFileContent(String tableName, int numFields, String delimiter, String fileName) throws ComponentNotReadyException {
        return "FILE " + StringUtils.quote(fileName) + " DELIMITER '" + delimiter + "' " +
        	numFields + ";" + LINE_SEPARATOR +
        	"INSERT INTO " + tableName + ";";
    }
    
    /**
     * Deletes data file which was used for exchange data.
     */
    private void deleteDataFile() {
    	if (StringUtils.isEmpty(tmpDataFileName)) {
    		return;
    	}
    	
    	if (isDataReadFromPort && !UNIX_STDIN.equals(tmpDataFileName) && dataURL == null) {
    		File dataFile = new File(tmpDataFileName);
    		if (!dataFile.delete()) {
    			logger.warn("Temp data file was not deleted.");
    		}
    	}
    }

    /**
     *  Description of the Method
     *
     * @param  nodeXML  Description of Parameter
     * @return          Description of the Returned Value
     * @since           May 21, 2002
     */
    public static Node fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException {
        ComponentXMLAttributes xattribs = new ComponentXMLAttributes(xmlElement, graph);

        try {
        	InformixDataWriter informixDataWriter = new InformixDataWriter(
        			xattribs.getString(XML_ID_ATTRIBUTE),
                    xattribs.getString(XML_DB_LOADER_PATH_ATTRIBUTE),
                    xattribs.getString(XML_DATABASE_ATTRIBUTE));
        	if (xattribs.exists(XML_TABLE_ATTRIBUTE)) {
        		informixDataWriter.setTable(xattribs.getString(XML_TABLE_ATTRIBUTE));
        	}
        	if (xattribs.exists(XML_COMMAND_ATTRIBUTE)) {
        		informixDataWriter.setCommand(xattribs.getString(XML_COMMAND_ATTRIBUTE));
        	}
        	if (xattribs.exists(XML_ERROR_LOG_ATTRIBUTE)) {
        		informixDataWriter.setErrorLog(xattribs.getString(XML_ERROR_LOG_ATTRIBUTE));
        	}
        	if (xattribs.exists(XML_MAX_ERRORS_ATTRIBUTE)) {
        		informixDataWriter.setMaxErrors(xattribs.getInteger(XML_MAX_ERRORS_ATTRIBUTE));
        	}
        	if (xattribs.exists(XML_IGNORE_ROWS_ATTRIBUTE)) {
        		informixDataWriter.setIgnoreRows(xattribs.getInteger(XML_IGNORE_ROWS_ATTRIBUTE));
        	}
        	if (xattribs.exists(XML_COMMIT_INTERVAL_ATTRIBUTE)) {
        		informixDataWriter.setCommitInterval(xattribs.getInteger(XML_COMMIT_INTERVAL_ATTRIBUTE));
        	}
        	if (xattribs.exists(XML_COLUMN_DELIMITER_ATTRIBUTE)) {
        		informixDataWriter.setColumnDelimiter(xattribs.getString(XML_COLUMN_DELIMITER_ATTRIBUTE));
        	}
        	if (xattribs.exists(XML_FILE_URL_ATTRIBUTE)) {
        		informixDataWriter.setFileUrl(xattribs.getString(XML_FILE_URL_ATTRIBUTE));
        	}
        	if (xattribs.exists(XML_HOST_ATTRIBUTE)) {
        		informixDataWriter.setHost(xattribs.getString(XML_HOST_ATTRIBUTE));
        	}
        	if (xattribs.exists(XML_USE_LOAD_UTILITY_ATTRIBUTE)) {
        		informixDataWriter.setUseLoadUtility(xattribs.getBoolean(XML_USE_LOAD_UTILITY_ATTRIBUTE));
        	}
        	if (xattribs.exists(XML_USER_ATTRIBUTE)) {
        		informixDataWriter.setUser(xattribs.getString(XML_USER_ATTRIBUTE));
        	}
        	if (xattribs.exists(XML_PASSWORD_ATTRIBUTE)) {
        		informixDataWriter.setPassword(xattribs.getString(XML_PASSWORD_ATTRIBUTE));
        	}
        	if (xattribs.exists(XML_IGNORE_UNIQUE_KEY_VIOLATION_ATTRIBUTE)) {
        		informixDataWriter.setIgnoreUniqueKeyViolation(xattribs.getBoolean(XML_IGNORE_UNIQUE_KEY_VIOLATION_ATTRIBUTE));
        	}
        	if (xattribs.exists(XML_USE_INSERT_CUROSOR_ATTRIBUTE)) {
        		informixDataWriter.setUseInsertCursor(xattribs.getBoolean(XML_USE_INSERT_CUROSOR_ATTRIBUTE));
        	}
            return informixDataWriter;
        } catch (Exception ex) {
               throw new XMLConfigurationException(COMPONENT_TYPE + ":" + 
            		   xattribs.getString(XML_ID_ATTRIBUTE," unknown ID ") + ":" + ex.getMessage(),ex);
        }
    }
    
    @Override
	public void toXML(Element xmlElement) {
		super.toXML(xmlElement);
		
		xmlElement.setAttribute(XML_DB_LOADER_PATH_ATTRIBUTE, loadUtilityPath);

		if (!StringUtils.isEmpty(command)) {
			xmlElement.setAttribute(XML_COMMIT_INTERVAL_ATTRIBUTE, command);
		}
		if (!StringUtils.isEmpty(errorLog)) {
			xmlElement.setAttribute(XML_ERROR_LOG_ATTRIBUTE, errorLog);
		}
		if (maxErrors != UNUSED_INT) {
			xmlElement.setAttribute(XML_MAX_ERRORS_ATTRIBUTE, String.valueOf(maxErrors));
		}
		if (ignoreRows != UNUSED_INT) {
			xmlElement.setAttribute(XML_IGNORE_ROWS_ATTRIBUTE, String.valueOf(ignoreRows));
		}
		if (commitInterval != UNUSED_INT) {
			xmlElement.setAttribute(XML_COMMIT_INTERVAL_ATTRIBUTE, String.valueOf(commitInterval));
		}
		if (!DEFAULT_COLUMN_DELIMITER.equals(columnDelimiter)) {
			xmlElement.setAttribute(XML_COLUMN_DELIMITER_ATTRIBUTE, columnDelimiter);
		}
		if (!StringUtils.isEmpty(host)) {
			xmlElement.setAttribute(XML_HOST_ATTRIBUTE, host);
		}
		if (useLoadUtility != DEFAULT_USE_LOAD_UTILITY) {
			xmlElement.setAttribute(XML_USE_LOAD_UTILITY_ATTRIBUTE, String.valueOf(useLoadUtility));
		}
		if (ignoreUniqueKeyViolation != DEFAULT_IGNORE_UNIQUE_KEY_VIOLATION) {
			xmlElement.setAttribute(XML_IGNORE_UNIQUE_KEY_VIOLATION_ATTRIBUTE, String.valueOf(ignoreUniqueKeyViolation));
		}
		if (useInsertCursor != DEFAULT_USE_INSERT_CURSOR) {
			xmlElement.setAttribute(XML_USE_INSERT_CUROSOR_ATTRIBUTE, String.valueOf(useInsertCursor));
		}
	}

	/**  Description of the Method */
    @Override
    public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		super.checkConfig(status);
		 
		if(!checkInputPorts(status, 0, 1)
				|| !checkOutputPorts(status, 0, 1)) {
			return status;
		}

        try {
            init();
        } catch (ComponentNotReadyException e) {
            ConfigurationProblem problem = new ConfigurationProblem(e.getMessage(), ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL);
            if(!StringUtils.isEmpty(e.getAttributeName())) {
                problem.setAttributeName(e.getAttributeName());
            }
            status.add(problem);
        } finally {
        	free();
        }
        
        return status;
    }
    
    public String getType(){
        return COMPONENT_TYPE;
    }

    private void setCommand(String command) {
		this.command = command;
	}
    
    private void setErrorLog(String errorLog) {
    	this.errorLog = errorLog;
	}
    
    private void setMaxErrors(int maxErrors) {
    	this.maxErrors = maxErrors;
	}
    
    private void setIgnoreRows(int ignoreRows) {
    	this.ignoreRows = ignoreRows;
	}
    
    private void setCommitInterval(int commitInterval) {
    	this.commitInterval = commitInterval;
	}
    
    private void setUseLoadUtility(boolean useLoadUtility) {
		this.useLoadUtility = useLoadUtility;
	}

	private void setIgnoreUniqueKeyViolation(boolean ignoreUniqueKeyViolation) {
		this.ignoreUniqueKeyViolation = ignoreUniqueKeyViolation;
	}
	
	private void setUseInsertCursor(boolean useInsertCursor) {
		this.useInsertCursor = useInsertCursor;
	}
    
    /**
     * Class for reading and parsing data from input stream, which is supposed to be connected to process' output,
     * and sends them to specified output port.
     * 
     * @see 		org.jetel.util.exec.ProcBox
     * @see 		org.jetel.util.exec.DataConsumer
     * @author      Miroslav Haupt (Mirek.Haupt@javlinconsulting.cz)
     * 				(c) Javlin Consulting (www.javlinconsulting.cz)
     * @since 		20.8.2007
     */
    private class InformixPortDataConsumer implements DataConsumer {
    	private DataRecord dbRecord;				// record from informix db output
    	private DataRecordMetadata dbOutMetadata;	// format as informix db output
    	private BufferedReader reader;				// read from input stream (=output stream of dbload process)
    	private Parser dbParser;					// parse record from informix db output
    	private DataRecord errRecord = null;
    	private OutputPort errPort = null;
    	private int logLevel;
    	
    	private DataRecordMetadata errMetadata;		// format as output port
    	
    	private Log logger = LogFactory.getLog(PortDataConsumer.class);
    	
    	// pattern for output from dbload utility
    	private String strBadRowPattern = "Row number (\\d+) is bad";
    	
    	// pattern for output from load utility
    	private String loadStrBadRowPattern = "ERROR:Line (\\d+): (.+)";

    	private Matcher badRowMatcher;
    	
    	private int rowNumberFieldNo; // last field -2
    	private int errMsgFieldNo; // last field
    	private final static int NUMBER_OF_ADDED_FIELDS = 2; // number of addded fields in errPortMetadata against dbIn(Out)Metadata

    	/**
    	 * @param port Output port receiving consumed data.
    	 * @throws ComponentNotReadyException 
    	 */
    	public InformixPortDataConsumer(OutputPort errPort, int logLevel) throws ComponentNotReadyException {
    		if (errPort == null) {
        		throw new ComponentNotReadyException("Output port wasn't found.");
    		}

    		this.errPort = errPort;
    		this.logLevel = logLevel;
    		
    		errMetadata = errPort.getMetadata();
    		if (errMetadata == null) {
        		throw new ComponentNotReadyException("Output port hasn't assigned metadata.");
        	}
    		
    		getNumberOfAddedFields();
    		checkErrPortMetadata();
    		
    		dbParser = new DelimitedDataParser(CHARSET_NAME);
    		this.dbOutMetadata = createDbOutMetadata();

    		dbRecord = new DataRecord(dbOutMetadata);
    		dbRecord.init();
    		
			errRecord = new DataRecord(errMetadata);
			errRecord.init();
    		
			Pattern badRowPattern;
			if (useLoadUtility) {
				badRowPattern = Pattern.compile(loadStrBadRowPattern);
			} else {
				badRowPattern = Pattern.compile(strBadRowPattern);
			}
			badRowMatcher = badRowPattern.matcher("");
			
   			dbParser.init(dbOutMetadata);
    	}
    	
    	/**
    	 * Gets index of added fields (rowNumber and errMsg).
    	 */
    	private void getNumberOfAddedFields() {
    		int numFields = errMetadata.getNumFields();
    		rowNumberFieldNo = numFields - 2;
        	errMsgFieldNo = numFields - 1;
    	}
    	
    	/**
         * Create metadata so that they correspond to format of informix db output
         * 
         * @return modified metadata
    	 * @throws ComponentNotReadyException 
         */
        private DataRecordMetadata createDbOutMetadata() {
        	DataRecordMetadata metadata = errMetadata.duplicate();
        	metadata.setRecType(DataRecordMetadata.DELIMITED_RECORD);
        	// delete first and second field
        	for (int i = 0; i < NUMBER_OF_ADDED_FIELDS; i++) {
        		metadata.delField(metadata.getNumFields() - 1);
        	}
        	
        	for (DataFieldMetadata fieldMetadata: metadata) {
        		fieldMetadata.setDelimiter(columnDelimiter);
        	}

        	return metadata;
        }
    	
    	/**
    	 * @see org.jetel.util.exec.DataConsumer
    	 */
    	public void setInput(InputStream stream) {
    		reader = new BufferedReader(new InputStreamReader(stream));
    	}

    	/**
    	 * @see org.jetel.util.exec.DataConsumer
    	 */
    	public boolean consume() throws JetelException {
    		if (useLoadUtility) {
    			return consumeLoad();
    		} else {
    			return consumeDbload();
    		}
    	}
    	
    	/**
    	 * Example of bad rows in stream:
		 * ERROR:Line 4: not an integer format (-1213): 7S
		 * ERROR:Line 5: not a date format (-1206): 09/33/2007
		 * 
    	 * @return
    	 * @throws JetelException
    	 */
    	private boolean consumeLoad() throws JetelException {
    		try {
				String line;
				if ((line = readLine()) == null) {
					return false;
				}

				badRowMatcher.reset(line);
				if (badRowMatcher.find()) {
        			int rowNumber = Integer.valueOf(badRowMatcher.group(1));
        			String errMsg = badRowMatcher.group(2); 
        			
					setErrRecord(errRecord, rowNumber, errMsg);
					errPort.writeRecord(errRecord);
        		}
    		} catch (Exception e) {
				throw new JetelException("Error while writing output record", e);
			}
    		
    		SynchronizeUtils.cloverYield();
    		return true;
    	}
    	
    	/**
    	 * Example of bad row in stream:
		 * In INSERT statement number 1 of raw data file /home/informix_data/data21672.dat.
	     * Row number 2 is bad.
	     * abc|def|ghi|
	     * Invalid month in date
	     * 
    	 * @return
    	 * @throws JetelException
    	 */
    	private boolean consumeDbload() throws JetelException {
    		try {
				String line;
				if ((line = readLine()) == null) {
					return false;
				}

				badRowMatcher.reset(line);
				if (badRowMatcher.find()) {
        			int rowNumber = Integer.valueOf(badRowMatcher.group(1));
        			if ((line = readLine()) == null) {
        				return false;
        			}
        			dbParser.setDataSource(getInputStream(line, CHARSET_NAME));

        			// read empty line(s) and error message, first not empty line is errMsg
        			String errMsg;
        			errMsg = readLine();
        			while (StringUtils.isEmpty(errMsg) || StringUtils.isBlank(errMsg)) {
        				if (errMsg == null) {
        					return false;
        				}
        				errMsg = readLine();
        			}
        			
					try {
						if (dbParser.getNext(dbRecord) != null) {
							setErrRecord(dbRecord, errRecord, rowNumber, errMsg);
							errPort.writeRecord(errRecord);
    					}
					} catch (BadDataFormatException e) {
						logger.warn("Bad row - it couldn't be parsed and sent to out port. Line: " + line);
					}
        		}
    		} catch (Exception e) {
				throw new JetelException("Error while writing output record", e);
			}
    		
    		SynchronizeUtils.cloverYield();
    		return true;
    	}
    	
    	/**
    	 * Read line by reader and write it by logger and return it.
    	 * @return read line
    	 * @throws IOException
    	 */
    	private String readLine() throws IOException {
    		String line = reader.readLine();
    		if (!StringUtils.isEmpty(line)) {
    			switch (logLevel) {
	    			case LoggerDataConsumer.LVL_ERROR:
	    				logger.error(line);
	    				break;
	    			case LoggerDataConsumer.LVL_DEBUG:
	    			default:
	    				logger.debug(line);
    			}
    		}
    		return line;
    	}
    	
    	/**
    	 * Set value in errRecord. In last two field is set row number and error message
    	 * and other fields are copies from dbRecord
    	 * @param dbRecord source record
    	 * @param errRecord destination record
    	 * @param rowNumber number of bad row
    	 * @param errMsg errorMsg
    	 * @return destination record
    	 */
    	private DataRecord setErrRecord(DataRecord dbRecord, DataRecord errRecord, int rowNumber, String errMsg) {
    		errRecord = setErrRecord(errRecord, rowNumber, errMsg);
    		for (int dbFieldNum = 0; dbFieldNum < dbRecord.getNumFields(); dbFieldNum++) {
    			errRecord.getField(dbFieldNum).setValue(dbRecord.getField(dbFieldNum));
    		}
    		return errRecord;
    	}
    	
    	/**
    	 * Set value in errRecord. In last two field is set row number and error message.
    	 * @param dbRecord source record
    	 * @param errRecord destination record
    	 * @param rowNumber number of bad row
    	 * @param errMsg errorMsg
    	 * @return destination record
    	 */
    	private DataRecord setErrRecord(DataRecord errRecord, int rowNumber, String errMsg) {
    		errRecord.reset();
    		errRecord.getField(rowNumberFieldNo).setValue(rowNumber);
    		errRecord.getField(errMsgFieldNo).setValue(errMsg);

    		return errRecord;
    	}
    	
    	/**
    	 * It create and return InputStream from string
    	 * @param str string, returned InputStream contains this string  
    	 * @param charsetName
    	 * @return InputStream created from string
    	 */
    	private InputStream getInputStream(String str, String charsetName) throws UnsupportedEncodingException {
        	return new ByteArrayInputStream(str.getBytes(charsetName));
        }
    	
    	/**
    	 * check metadata at error port against metadata at input port
    	 * if metadata isn't correct then throws ComponentNotReadyException
    	 * @throws ComponentNotReadyException when metadata isn't correct
    	 */
    	private void checkErrPortMetadata() throws ComponentNotReadyException {
			// check if last - 1  field of errMetadata is integer - rowNumber
			if (errMetadata.getFieldType(rowNumberFieldNo) != DataFieldMetadata.INTEGER_FIELD) {
				throw new ComponentNotReadyException("Last but one field of " +  StringUtils.quote(errMetadata.getName()) +  
						" has different type from integer.");
			}
			
			// check if last field of errMetadata is string - errMsg
			if (errMetadata.getFieldType(errMsgFieldNo) != DataFieldMetadata.STRING_FIELD) {
				throw new ComponentNotReadyException("Last field of " +  StringUtils.quote(errMetadata.getName()) +  
						" has different type from string.");
			}
    		
    		if (dbMetadata == null) {
    			return;
    		}
    		
    		// check number of fields; if inNumFields == outNumFields + NUMBER_OF_ADDED_FIELDS
			if (errMetadata.getNumFields() != dbMetadata.getNumFields() + NUMBER_OF_ADDED_FIELDS) {
				throw new ComponentNotReadyException("Number of fields of " +  StringUtils.quote(errMetadata.getName()) +  
						" isn't equal number of fields of " +  StringUtils.quote(dbMetadata.getName()) + " + " + NUMBER_OF_ADDED_FIELDS + ".");
			}
			
			// check if other fields' type of errMetadata are equals as dbMetadat
			int count = 0;
			for (DataFieldMetadata dbFieldMetadata: dbMetadata){
				if (!dbFieldMetadata.equals(errMetadata.getField(count++))) {
					throw new ComponentNotReadyException("Field "
							+ StringUtils.quote(errMetadata.getField(count - 1).getName()) + " in " 
							+ StringUtils.quote(errMetadata.getName()) + " has different type from field " 
							+ StringUtils.quote(dbFieldMetadata.getName()) + " in " + StringUtils.quote(dbMetadata.getName()) + ".");
				}
			}
    	}
    	
    	/**
    	 * @see org.jetel.util.exec.DataConsumer
    	 */
    	public void close() {
    		if (dbParser != null) {
    			dbParser.close();
    		}
    		
    		try {
				if (reader != null) {
					reader.close();
				}
			} catch (IOException ioe) {
				logger.warn("Reader wasn't closed.", ioe);
			}
    		
    		try {
        		if (errPort != null) {
        			errPort.eof();
        		}
			} catch (Exception ie) {
				logger.warn("Out port wasn't closed.", ie);
			}
    	}
    }
}
