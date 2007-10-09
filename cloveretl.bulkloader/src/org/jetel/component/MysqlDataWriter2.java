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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.util.Iterator;
import java.util.Properties;
import java.util.Random;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.formatter.DataFormatter;
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
import org.jetel.util.CommandBuilder;
import org.jetel.util.ComponentXMLAttributes;
import org.jetel.util.StringUtils;
import org.jetel.util.exec.DataConsumer;
import org.jetel.util.exec.LoggerDataConsumer;
import org.jetel.util.exec.ProcBox;
import org.w3c.dom.Element;

/**
 *  <h3>Mysql data writer</h3>
 *
 * <!-- All records from input port 0 are loaded into mysql database. Connection to database is not through JDBC driver,
 * this component uses the mysqlimport utility for this purpose.-->
 *
 * <table border="1">
 *  <th>Component:</th>
 * <tr><td><h4><i>Name:</i></h4></td>
 * <td>Mysql data writer</td></tr>
 * <tr><td><h4><i>Category:</i></h4></td>
 * <td></td></tr>
 * <tr><td><h4><i>Description:</i></h4></td>
 * <td>This component loads data to mysql database using the mysqlimport utility. 
 * There is created mysqlimport command depending on input parameters. Data are read from given input file or from the input port and loaded to database.<br>
 * Any generated scripts/commands can be optionally logged to help diagnose problems.<br>
 * Before you use this component, make sure that mysql client is installed and configured on the machine where CloverETL runs and mysqlimport command line tool available.
 * </td></tr>
 * <tr><td><h4><i>Inputs:</i></h4></td>
 * <td>[0] - input records. It can be omitted - then <b>fileURL</b> has to be provided.</td></tr>
 * <tr><td><h4><i>Outputs:</i></h4></td>
 * <td>
 * </td></tr>
 * <tr><td><h4><i>Comment:</i></h4></td>
 * <td></td></tr>
 * </table>
 *  <br>
 *  <table border="1">
 *  <th>XML attributes:</th>
 *  <tr><td><b>type</b></td><td>"MYSQL_DATA_WRITER2"</td></tr>
 *  <tr><td><b>id</b></td><td>component identification</td></tr>
 *  <tr><td><b>mysqlImportPath</b></td><td>path to mysqlimport utility</td></tr>
 *  <tr><td><b>database</b></td><td>the name of the database to receive the data</td></tr>
 *  <tr><td><b>table</b></td><td>table name, where data are loaded</td></tr>
 *  <tr><td><b>columnDelimiter</b><br><i>optional</i></td><td>delimiter used for each column in data (default = '\t')</br>
 *  Value of the delimiter mustn't be contained in data.</td></tr>
 *  <tr><td><b>fileURL</b><br><i>optional</i></td><td>Path to data file to be loaded.<br>
 *  Normally this file is a temporary storage for data to be passed to mysqlimport utility.
 *  If file URL is not specified, the file is created in Clover or OS temporary directory and deleted after load finishes.<br>
 *  If file URL is specified, temporary file is created within given path and name and not 
 *  deleted after being loaded. Next graph run overwrites it.<br>
 *  There is one more meaning of this parameter. If input port is not specified, this file 
 *  is used only for reading by mysqlimport utility and must already contain data in format expected by 
 *  load. The file is neither deleted nor overwritten.<br>
 *  For higher performance is recommanded that <i>fileURL</i> attribute has the same base name as <i>table</i> attribute. For example: table="test", fileURL="test.dat"</td></tr>
 *  <tr><td><b>host</b><br><i>optional</i></td><td>Import data to the MySQL server on the given host. The default host is localhost.</td></tr>
 *  <tr><td><b>user</b><br><i>optional</i></td><td>The MySQL username to use when connecting to the server.</td></tr>
 *  <tr><td><b>password</b><br><i>optional</i></td><td>The password to use when connecting to the server.</td></tr>
 *  <tr><td><b>parameters</b><br><i>optional</i></td><td>All possible additional parameters 
 *  which can be passed on to mysqlimport utility (See <a href="http://www.mysql.org/doc/refman/5.1/en/mysqlimport.html">
 *  mysqlimport utility</a>). Parameters, in form <i>key=value</i> (or <i>key</i> - 
 *  interpreted as <i>key=true</i>, if possible value are only "true" or "false") has to be 
 *  separated by :;| {colon, semicolon, pipe}. If in parameter value occurs one of :;|, value 
 *  has to be double quoted.<br><b>Load parameters</b><br><table>
 *  <tr><td>characterSetsDir</td><td>The directory where character sets are installed.
 *  See <a href="http://www.mysql.org/doc/refman/5.1/en/character-sets.html">
 *  The Character Set Used for Data and Sorting</a>.</td></tr>
 *  <tr><td>columns</td><td>This option takes a comma-separated list of column names as its value. The order of the column names indicates how to match data file columns with table columns.<br>
 *  </td></tr>
 *  <tr><td>compress</td><td>Compress all information sent between the client and the server if both support compression.</td></tr>
 *  <tr><td>defaultCharacterSet</td><td>Use <i>defaultCharacterSet</i> as the default character set. See <a href="http://www.mysql.org/doc/refman/5.1/en/character-sets.html"> The Character Set Used for Data and Sorting</a>.</td></tr>
 *  <tr><td>delete</td><td>Empty the table before importing the text file.</td></tr>
 *  <tr><td>fieldsEnclosedBy</td><td>It is used for enclosing each filed in data by char.
 *  <br>When data is read from input port this attribute is ignored.</td></tr>
    <tr><td>fieldsIsOptionallyEnclosed</td><td>It decide if <i>fieldsEnclosedBy</i> is used for each columns or not.
    <br>It can be used only with <i>fieldsEnclosedBy</i> attribute.
    <br>When data is read from input port this attribute is ignored.</td></tr>
 *  <tr><td>fieldsEscapedBy</td><td>It is used form escaping fields by char.
 *  <br>When data is read from input port this attribute is ignored.</td></tr>
 *  <tr><td>force</td><td>Ignore errors.</td></tr>
 *  <tr><td>ignore</td><td>See the description for the <i>replace</i> attribute.</td></tr>
 *  <tr><td>replace</td><td>The <i>replace</i> attribute and <i>ignore</i> attribute control handling of input rows that duplicate existing rows on unique key values. If you specify <i>replace</i> attribute, new rows replace existing rows that have the same unique key value. If you specify <i>ignore</i> attribute, input rows that duplicate an existing row on a unique key value are skipped. If you do not specify either option, an error occurs when a duplicate key value is found, and the rest of the text file is ignored.</td></tr>
 *  <tr><td>ignoreRows</td><td>Ignore the first N lines of the data file.</td></tr>
 *  <tr><td>recordDelimiter</td><td>Specifies the record delimiter. (default = '\n' (newline character)).</td></tr>
 *  <tr><td>local</td><td>Read input files locally from the client host.<br>
 *  If <i>local</i> attribute isn't defined, default value is true.</td></tr>
 *  <tr><td>lockTables</td><td>Lock all tables for writing before processing any text files. This ensures that all tables are synchronized on the server.</td></tr>
 *  <tr><td>lowPriority</td><td>Use LOW_PRIORITY when loading the table. This affects only storage engines that use only table-level locking (MyISAM, MEMORY, MERGE).</td></tr>
 *  <tr><td>port</td><td>The TCP/IP port number to use for the connection.</td></tr>
 *  <tr><td>protocol</td><td>The connection protocol to use. One of {TCP|SOCKET|PIPE|MEMORY}.</td></tr>
 *  <tr><td>silent</td><td>Silent mode. Produce output only when errors occur.</td></tr>
 *  <tr><td>socket</td><td>For connections to localhost, the Unix socket file to use, or, on Windows, the name of the named pipe to use.</td></tr>
 *  <tr><td>ssl</td><td>Options that begin with <i>ssl</i> attribute specify whether to connect to the server via SSL and indicate where to find SSL keys and certificates. See <a href="http://www.mysql.org/doc/refman/5.1/en/ssl-options.html"> SSL Command Options</a>.</td></tr>
 *  
 *  </table></td></tr>
 *  </table>
 *
 *
 *	<h4>Example:</h4>
 *  Reading data from input port:
 *  <pre>&lt;Node
 *  mysqlImportPath="mysqlimport" 
 *	database="testdb"
 *	id="MYSQL_DATA_WRITER21"
 *	table="test" 
 *	type="MYSQL_DATA_WRITER2"
 *  /&gt;
 *  </pre>
 *  Reading data from flat file:
 *  <pre>&lt;Node
 *  mysqlImportPath="mysqlimport" 
 *	database="testdb"
 *	table="test" 
 *	fileURL="${WORKSPACE}data/delimited/mysql.dat"
 *	columnDelimiter=","
 *	parameters="fieldsEnclosedBy=*|fieldsIsOptionallyEnclosed"
 *	id="MYSQL_DATA_WRITER20"
 *	type="MYSQL_DATA_WRITER2"
 *  /&gt;
 *
 * @author      Miroslav Haupt (Mirek.Haupt@javlinconsulting.cz)
 *(c) Javlin Consulting (www.javlinconsulting.cz)
 * @see         org.jetel.graph.TransformationGraph
 * @see         org.jetel.graph.Node
 * @see         org.jetel.graph.Edge
 * @since 		24.9.2007
 */
public class MysqlDataWriter2 extends Node {

	private static Log logger = LogFactory.getLog(MysqlDataWriter2.class);

    /**  Description of the Field */
	private static final String XML_MYSQL_IMPORT_PATH_ATTRIBUTE = "mysqlImportPath";
    private static final String XML_DATABASE_ATTRIBUTE = "database";
    private static final String XML_TABLE_ATTRIBUTE = "table";
    private static final String XML_FILE_URL_ATTRIBUTE = "fileURL";
    private static final String XML_COLUMN_DELIMITER_ATTRIBUTE = "columnDelimiter";
    private static final String XML_HOST_ATTRIBUTE = "host";
    private static final String XML_USER_ATTRIBUTE = "user";
    private static final String XML_PASSWORD_ATTRIBUTE = "password";
    private final static String XML_PARAMETERS_ATTRIBUTE = "parameters";
    
    private static final String MYSQL_CHARACTER_SETS_DIR_PARAM = "characterSetsDir";
    private static final String MYSQL_COLUMNS_PARAM = "columns";
    private static final String MYSQL_COMPRESS_PARAM = "compress";
    private static final String MYSQL_DEFAULT_CHARACTER_SET_PARAM = "defaultCharacterSet";
    private static final String MYSQL_DELETE_PARAM = "delete";
    private static final String MYSQL_FIELDS_ENCLOSED_BY_PARAM = "fieldsEnclosedBy";
    private static final String MYSQL_FIELDS_IS_OPTIONALLY_ENCLOSED_PARAM = "fieldsIsOptionallyEnclosed";
    private static final String MYSQL_FIELDS_ESCAPED_BY_PARAM = "fieldsEscapedBy";
    private static final String MYSQL_FORCE_PARAM = "force";
    private static final String MYSQL_REPLACE_PARAM = "replace";
    private static final String MYSQL_IGNORE_PARAM = "ignore";
    private static final String MYSQL_IGNORE_ROWS_PARAM = "ignoreRows";
    private static final String MYSQL_RECORD_DELIMITER_PARAM = "recordDelimiter";
    private static final String MYSQL_LOCAL_PARAM = "local";
    private static final String MYSQL_LOCK_TABLES_PARAM = "lockTables";
    private static final String MYSQL_LOW_PRIORITY_PARAM = "lowPriority";
    private static final String MYSQL_PORT_PARAM = "port";
    private static final String MYSQL_PROTOCOL_PARAM = "protocol";
    private static final String MYSQL_SILENT_PARAM = "silent";
    private static final String MYSQL_SOCKET_PARAM = "socket";
    private static final String MYSQL_SSL_PARAM = "ssl";
    
    private static final String MYSQL_COLUMN_DELIMITER_SWITCH = "fields-terminated-by";
    private static final String MYSQL_HOST_SWITCH = "host";
    private static final String MYSQL_USER_SWITCH = "user";
    private static final String MYSQL_PASSWORD_SWITCH = "password";
    private static final String MYSQL_CHARACTER_SETS_DIR_SWITCH = "character-sets-dir";
    private static final String MYSQL_COLUMNS_SWITCH = "columns";
    private static final String MYSQL_COMPRESS_SWITCH = "compress";
    private static final String MYSQL_DEFAULT_CHARACTER_SET_SWITCH = "default-character-set";
    private static final String MYSQL_DELETE_SWITCH = "delete";
    private static final String MYSQL_FIELDS_ENCLOSED_BY_SWITCH = "fields-enclosed-by";
    private static final String MYSQL_FIELDS_OPTIONALLY_ENCLOSED_BY_SWITCH = "fields-optionally-enclosed-by";
    private static final String MYSQL_FIELDS_ESCAPED_BY_SWITCH = "fields-escaped-by";
    private static final String MYSQL_FORCE_SWITCH = "force";
    private static final String MYSQL_REPLACE_SWITCH = "replace";
    private static final String MYSQL_IGNORE_SWITCH = "ignore";
    private static final String MYSQL_IGNORE_ROWS_SWITCH = "ignore-lines";
    private static final String MYSQL_RECORD_DELIMITER_SWITCH = "lines-terminated-by";
    private static final String MYSQL_LOCAL_SWITCH = "local";
    private static final String MYSQL_LOCK_TABLES_SWITCH = "lock-tables";
    private static final String MYSQL_LOW_PRIORITY_SWITCH = "low-priority";
    private static final String MYSQL_PORT_SWITCH = "port";
    private static final String MYSQL_PROTOCOL_SWITCH = "protocol";
    private static final String MYSQL_SILENT_SWITCH = "silent";
    private static final String MYSQL_SOCKET_SWITCH = "socket";
    private static final String MYSQL_SSL_SWITCH = "ssl*";
    
    
    public final static String COMPONENT_TYPE = "MYSQL_DATA_WRITER2";
    private final static int READ_FROM_PORT = 0;
    private final static char EQUAL_CHAR = '=';

    private final static String COMMAND_LINE_SWITCH_MARK = "--";
    private final static File TMP_DIR = new File(".");
    private final static String DEFAULT_COLUMN_DELIMITER = "\t";
    private final static String DEFAULT_RECORD_DELIMITER = "\n";
    private final static String CHARSET_NAME = "UTF-8";
    private final static String DEFAULT_DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
    private final static String DEFAULT_DATE_FORMAT = "yyyy-MM-dd"; 
    private final static String DEFAULT_TIME_FORMAT = "HH:mm:ss";
    private final static String DEFAULT_YEAR_FORMAT = "yyyy";
    
    // variables for dbload's command
	private String mysqlImportPath;
    private String database;
    private String table;
    private String host;
    private String user;
    private String password;
    private String columnDelimiter = DEFAULT_COLUMN_DELIMITER;
    private String dataURL; // fileUrl from XML - data file that is used when no input port is connected
    private String parameters;
    
    private Properties properties;
    private File dataFile; // file that is used for exchange data between clover and mysqlimport - file from dataURL
    private String commandLine; // command line of mysqlimport
    private DataRecordMetadata dbMetadata; // it correspond to mysqlImport input format
    private DataFormatter formatter; // format data to mysqlimport format and write them to dataFileName 
    private DataConsumer consumer = null; // consume data from out stream of mysqlimport
    private DataConsumer errConsumer; // consume data from err stream of mysqlimport - write them to by logger
    private boolean isDataFileRenamed;
    
    /**
     * true - dataURL is in correct form from mysqlimport utility ->
     * 		  dataURL == [path]table[.extension]
     * false - dataURL == null, empty or dataURL isn't in correct form
     */
    private boolean isDataURLCorrectlyNamed;

    /**
     * true - data is read from in port;
     * false - data is read from file directly by mysqlimport utility
     */
    private boolean isDataReadFromPort;
    
    
    /**
     * Constructor for the MysqlDataWriter object
     *
     * @param  id  Description of the Parameter
     */
    public MysqlDataWriter2(String id, String mysqlImportPath, String database, String table) { 
        super(id);
        this.mysqlImportPath = mysqlImportPath;
        this.database = database;
        this.table = table;
    }
    
    /**
     *  Main processing method for the MysqlDataWriter object
     *
     * @since    April 4, 2002
     */
    public Result execute() throws Exception {
        ProcBox box;
        int processExitValue = 0;

        if (isDataReadFromPort) {
        	// dataFile is used for exchange data
        	formatter.setDataTarget(Channels.newChannel(new FileOutputStream(dataFile)));
        	readFromPortAndWriteByFormatter();
            box = createProcBox();
   		
    		processExitValue = box.join();
        } else {
        	processExitValue = readDataDirectlyFromFile();
        }
        
        if (processExitValue != 0) {
        	throw new JetelException("Mysqlimport utility has failed.");
		}

        return runIt ? Result.FINISHED_OK : Result.ABORTED;
    }

    /**
     * This method reads incoming data from port and sends them by formatter to mysqlimport process.
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
	 * Call mysqlimport process with parameters - mysqlimport process reads data directly from file.  
	 * @return value of finished process
	 * @throws Exception
	 */
	private int readDataDirectlyFromFile() throws Exception {
		if (!isDataURLCorrectlyNamed) {
			renameDataFileToCorrectNamedFile();
		}
        ProcBox box = createProcBox();
        return box.join();
	}

	/**
	 * Rename file define by user (dataURL) to file with right base name 
	 * for mysqlimport (dataFile). If renaming isn't success then dataURL
	 * file is copied to dataFile. 
	 * 
	 * @throws JetelException if renaming and copying isn't success simultaneously
	 */
	private void renameDataFileToCorrectNamedFile() throws JetelException {
		
		isDataFileRenamed = renameFile(new File(dataURL), dataFile);
		if (!isDataFileRenamed) {
			logger.warn("System copying " + StringUtils.quote(new File(dataURL).getAbsolutePath()) + 
					" to " + StringUtils.quote(dataFile.getAbsolutePath()) + ".\n It very lower performance. " +
					"To avoid slowdown base name of data file (" + 
					StringUtils.quote(new File(dataURL).getAbsolutePath()) + ") has to be same as table name " +
					"or Clover has to have write access to this file.");			
			try {
				FileUtils.copyFile(new File(dataURL), dataFile);
			} catch (IOException e) {
				throw new JetelException("Copying data file has failed.");
			}
		}
	}
	
	/**
	 * Create instance of ProcBox.
	 * @return instance of ProcBox
	 * @throws IOException
	 */
	private ProcBox createProcBox() throws IOException {
		Process process = Runtime.getRuntime().exec(commandLine);			
        ProcBox box = new ProcBox(process, null, consumer, errConsumer);
		return box;
	}
    
    /**
     * Create command line for process, where mysqlimport utility is running.
     * Example: mysqlimport --host=localhost --user=root --password --local testdb test.txt
     * @return
     * @throws ComponentNotReadyException 
     */
    private String createCommandLineForDbLoader() throws ComponentNotReadyException {
    	CommandBuilder command = new CommandBuilder(mysqlImportPath, COMMAND_LINE_SWITCH_MARK);
    	command.setParams(properties);

    	if (columnDelimiter != DEFAULT_COLUMN_DELIMITER) {
    		command.addParameterSwitchWithEqualChar(null, MYSQL_COLUMN_DELIMITER_SWITCH, columnDelimiter);
    	}
    	command.addParameterSwitchWithEqualChar(null, MYSQL_HOST_SWITCH, host);
    	command.addParameterSwitchWithEqualChar(null, MYSQL_USER_SWITCH, user);
    	command.addParameterSwitchWithEqualChar(null, MYSQL_PASSWORD_SWITCH, password);
    	
    	command.addParameterSwitchWithEqualChar(MYSQL_CHARACTER_SETS_DIR_PARAM, MYSQL_CHARACTER_SETS_DIR_SWITCH, null);
    	command.addParameterSwitchWithEqualChar(MYSQL_COLUMNS_PARAM, MYSQL_COLUMNS_SWITCH, null);
    	command.addParameterBooleanSwitch(MYSQL_COMPRESS_PARAM, MYSQL_COMPRESS_SWITCH);
    	command.addParameterSwitchWithEqualChar(MYSQL_DEFAULT_CHARACTER_SET_PARAM, MYSQL_DEFAULT_CHARACTER_SET_SWITCH, null);
    	command.addParameterBooleanSwitch(MYSQL_DELETE_PARAM, MYSQL_DELETE_SWITCH);
    	
    	// ignore parameters; those paramaters are used only when data is read directly from file
    	if (!isDataReadFromPort) {
    		if (properties.containsKey(MYSQL_FIELDS_IS_OPTIONALLY_ENCLOSED_PARAM) && 
        			!"false".equalsIgnoreCase(properties.getProperty(MYSQL_FIELDS_IS_OPTIONALLY_ENCLOSED_PARAM))) {
        		command.addParameterSwitchWithEqualChar(MYSQL_FIELDS_ENCLOSED_BY_PARAM, MYSQL_FIELDS_OPTIONALLY_ENCLOSED_BY_SWITCH, null);
        	} else {
        		command.addParameterSwitchWithEqualChar(MYSQL_FIELDS_ENCLOSED_BY_PARAM, MYSQL_FIELDS_ENCLOSED_BY_SWITCH, null);
        	}
        	
        	command.addParameterSwitchWithEqualChar(MYSQL_FIELDS_ESCAPED_BY_PARAM, MYSQL_FIELDS_ESCAPED_BY_SWITCH, null);
    	}
    	
    	command.addParameterBooleanSwitch(MYSQL_FORCE_PARAM, MYSQL_FORCE_SWITCH);
    	command.addParameterBooleanSwitch(MYSQL_REPLACE_PARAM, MYSQL_REPLACE_SWITCH);
    	command.addParameterBooleanSwitch(MYSQL_IGNORE_PARAM, MYSQL_IGNORE_SWITCH);
    	command.addParameterSwitchWithEqualChar(MYSQL_IGNORE_ROWS_PARAM, MYSQL_IGNORE_ROWS_SWITCH, null);
    	command.addParameterSwitchWithEqualChar(MYSQL_RECORD_DELIMITER_PARAM, MYSQL_RECORD_DELIMITER_SWITCH, null);
    	
    	// MYSQL_LOCAL_SWITCH is default used
    	if (!properties.containsKey(MYSQL_LOCAL_PARAM) || !"false".equalsIgnoreCase(properties.getProperty(MYSQL_LOCAL_PARAM))) {
    		command.append(" " + COMMAND_LINE_SWITCH_MARK + MYSQL_LOCAL_SWITCH);
    	}
    	
    	command.addParameterBooleanSwitch(MYSQL_LOCK_TABLES_PARAM, MYSQL_LOCK_TABLES_SWITCH);
    	command.addParameterBooleanSwitch(MYSQL_LOW_PRIORITY_PARAM, MYSQL_LOW_PRIORITY_SWITCH);
    	command.addParameterSwitchWithEqualChar(MYSQL_PORT_PARAM, MYSQL_PORT_SWITCH, null);
    	command.addParameterSwitchWithEqualChar(MYSQL_PROTOCOL_PARAM, MYSQL_PROTOCOL_SWITCH, null);
    	command.addParameterBooleanSwitch(MYSQL_SILENT_PARAM, MYSQL_SILENT_SWITCH);
    	command.addParameterSwitchWithEqualChar(MYSQL_SOCKET_PARAM, MYSQL_SOCKET_SWITCH, null);
    	command.addParameterBooleanSwitch(MYSQL_SSL_PARAM, MYSQL_SSL_SWITCH);
		
	    command.append(" " + database);
	    try {
			command.append(" " + dataFile.getCanonicalPath());
		} catch (IOException ioe) {
			throw new ComponentNotReadyException(this, ioe);
		}
		
		return command.getCommand();
    }

    /**
     *  Description of the Method
     *
     * @exception  ComponentNotReadyException  Description of the Exception
     * @since                                  April 4, 2002
     */
    public void init() throws ComponentNotReadyException {
		super.init();

		isDataReadFromPort = !getInPorts().isEmpty();
		
		properties = parseParameters(parameters);
		checkParams();

		if (table.equals(FilenameUtils.getBaseName(dataURL))) {
			isDataURLCorrectlyNamed = true;
		} else {
			isDataURLCorrectlyNamed = false;
		}

		// prepare (temporary) data file
		try {
            if (isDataReadFromPort) {
            	if (dataURL != null) {
            		dataFile = getCorrectlyNamedFile(dataURL, false);
            	} else {
            		dataFile = getCorrectlyNamedFile(dataURL, true);
            	}
        		dataFile.delete();
            } else {
            	if (!new File(dataURL).exists()) {
            		free();
            		throw new ComponentNotReadyException(this, "Data file " + StringUtils.quote(dataURL) + " not exists."); 
            	}
            	dataFile = getCorrectlyNamedFile(dataURL, false);
            }
        } catch (IOException e) {
        	free();
            throw new ComponentNotReadyException(this, "Temp data file cannot be created.");
        }

        commandLine = createCommandLineForDbLoader();
		logger.info("System command: " + commandLine);
        
        if (isDataReadFromPort) {
	        InputPort inPort = getInputPort(READ_FROM_PORT);
	
	        dbMetadata = createMysqlMetadata(inPort.getMetadata());
	        
	        // init of data formatter
	        formatter = new DataFormatter(CHARSET_NAME);
	        formatter.init(dbMetadata);
        }
        
      	consumer = new LoggerDataConsumer(LoggerDataConsumer.LVL_DEBUG, 0);
      	errConsumer = new LoggerDataConsumer(LoggerDataConsumer.LVL_ERROR, 0);
    }
    
    /**
     * Get correctly named unique file based on dataURL.
     * If dataURL isn't correctly named then its base name is changed.
     * Uniqueness of the returned file is assured.
     * 
     * @param unique true - always check uniqueness; 
     * 				 false - check uniqueness only if dataURL isn't correctly named 
     * @param dataURL base for name of file; if equals null then temp is used
     * @return correctly named unique file based on dataURL
     * @throws ComponentNotReadyException
     * @throws IOException
     */
    private File getCorrectlyNamedFile(String dataURL, boolean unique) throws ComponentNotReadyException, IOException {
		if (!isDataURLCorrectlyNamed) {
			String correctFileName;
			if (dataURL != null) {
				// change original base name to correct name (name of db table)
				correctFileName = new File(dataURL).getParentFile().getCanonicalPath();
			} else {
				correctFileName = TMP_DIR.getCanonicalPath();
			}
			correctFileName += File.separator + table;
			
			return FileUtils.createUniqueFile(correctFileName);
		} else {
			if (unique) {
				return FileUtils.createUniqueFile(dataURL);
			} else {
				return new File(dataURL);
			}
		}
    }
    
    /**
     * Create instance of Properties from String.
     * Parse parameters from string "parameteres" and fill properties by them.
     * 
     * @param parameters string that contains parameters
     * @return instance of Properties created by parsing string
     */
    private Properties parseParameters(String parameters) {
    	Properties properties = new Properties();
    	
    	if (parameters != null) {
			String[] param = StringUtils.split(parameters);
			int index;
			for (String string : param) {
				index = string.indexOf(EQUAL_CHAR);
				if (index > -1) {
					properties.setProperty(string.substring(0, index), 
							StringUtils.unquote(string.substring(index + 1)));
				} else {
					properties.setProperty(string, String.valueOf(true));
				}
			}
		}
    	
    	return properties;
    }
    
    /**
     * Checks if mandatory parameters are defined.
     * And check combination of some parameters.
     * 
     * @throws ComponentNotReadyException if any of mandatory parameters is empty
     */
    private void checkParams() throws ComponentNotReadyException {
    	if (StringUtils.isEmpty(mysqlImportPath)) {
    		throw new ComponentNotReadyException(this, StringUtils.quote(XML_MYSQL_IMPORT_PATH_ATTRIBUTE) 
    				+ " attribute have to be set.");
		}
    	
    	if (StringUtils.isEmpty(database)) {
			throw new ComponentNotReadyException(this, StringUtils.quote(XML_DATABASE_ATTRIBUTE)
					+ " attribute have to be set.");
		}
    	
    	if (StringUtils.isEmpty(table)) {
			throw new ComponentNotReadyException(this, StringUtils.quote(XML_TABLE_ATTRIBUTE)
					+ " attribute have to be set.");
		}
    	
    	if (!isDataReadFromPort && StringUtils.isEmpty(dataURL)) {
    		throw new ComponentNotReadyException(this, "There is neither input port nor " 
    				+ StringUtils.quote(XML_FILE_URL_ATTRIBUTE) + " attribute specified.");
		}
    	
    	// check combination
    	if (properties.containsKey(MYSQL_FIELDS_IS_OPTIONALLY_ENCLOSED_PARAM) &&
    			!properties.containsKey(MYSQL_FIELDS_ENCLOSED_BY_PARAM)) {
    		logger.warn("Attribute" + StringUtils.quote(MYSQL_FIELDS_IS_OPTIONALLY_ENCLOSED_PARAM) + 
    				" is ignored because it has to be used in combination with " +
    				StringUtils.quote(MYSQL_FIELDS_ENCLOSED_BY_PARAM) + " attribute.");
    	}
    	
    	// report on ignoring some attributes
    	if (isDataReadFromPort) {
    		if (properties.containsKey(MYSQL_FIELDS_ENCLOSED_BY_PARAM)) {
    			logger.warn("Attribute " + StringUtils.quote(MYSQL_FIELDS_ENCLOSED_BY_PARAM) + 
        				" is ignored because it is used only when data is read directly from file.");
    		}
    		if (properties.containsKey(MYSQL_FIELDS_IS_OPTIONALLY_ENCLOSED_PARAM)) {
    			logger.warn("Attribute " + StringUtils.quote(MYSQL_FIELDS_IS_OPTIONALLY_ENCLOSED_PARAM) + 
				" is ignored because it is used only when data is read directly from file.");
    		}
    		if (properties.containsKey(MYSQL_FIELDS_ESCAPED_BY_PARAM)) {
    			logger.warn("Attribute " + StringUtils.quote(MYSQL_FIELDS_ESCAPED_BY_PARAM) + 
				" is ignored because it is used only when data is read directly from file.");
    		}
    	}
    }
    
    /**
     * Modify metadata so that they correspond to mysqlimport input format. 
     * Each field is delimited and it has the same delimiter.
     * Only last field has different delimiter.
     *
     * @param oldMetadata original metadata
     * @return modified metadata
     */
    private DataRecordMetadata createMysqlMetadata(DataRecordMetadata originalMetadata) {
    	DataRecordMetadata metadata = originalMetadata.duplicate();
		metadata.setRecType(DataRecordMetadata.DELIMITED_RECORD);
		for (int idx = 0; idx < metadata.getNumFields() - 1; idx++) {
			metadata.getField(idx).setDelimiter(columnDelimiter);
			setMysqlDateFormat(metadata.getField(idx));
		}
		if (properties.containsKey(MYSQL_RECORD_DELIMITER_PARAM)) {
			metadata.getField(metadata.getNumFields() - 1).setDelimiter(
					(String)properties.get(MYSQL_RECORD_DELIMITER_PARAM));
		} else {
			metadata.getField(metadata.getNumFields() - 1).setDelimiter(DEFAULT_RECORD_DELIMITER);
		}
		setMysqlDateFormat(metadata.getField(metadata.getNumFields() - 1));
	
		return metadata;
    }

    /**
     * If field has format of date or time then default informix format is set.
     * @param field 
     */
    private void setMysqlDateFormat(DataFieldMetadata field) {
		if (field.getType() == DataFieldMetadata.DATE_FIELD ||
				field.getType() == DataFieldMetadata.DATETIME_FIELD) {
			boolean isDate = field.isDateFormat();
			boolean isTime = field.isTimeFormat();
			boolean isOnlyYearFormat = isDate && field.getFormatStr().matches("(y|Y)*");

			// if formatStr is undefined then DEFAULT_DATETIME_FORMAT is assigned
			if (isOnlyYearFormat) {
				field.setFormatStr(DEFAULT_YEAR_FORMAT);
			} else if ((isDate && isTime) || (StringUtils.isEmpty(field.getFormatStr()))) {
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
		super.free();
		deleteDataFile();
	}
    
    /**
     * Deletes (renames or nothing) data file which was used for exchange data.
     */
    private void deleteDataFile() {
    	if (dataFile == null) {
    		return;
    	}
    	
    	if (isDataReadFromPort) {
    		if (dataURL != null) {
    			if (!isDataURLCorrectlyNamed) {
    				if (!renameFile(dataFile, new File(dataURL))) {
						logger.warn("Temp data file was not saved into file " + StringUtils.quote(dataURL) + 
           					" but into file " + StringUtils.quote(dataFile.toString()) + ".");
        			}
    			}
    		} else {
    			if (!dataFile.delete()) {
   					logger.warn("Temp data file was not deleted.");
        		}
    		}
        } else {
        	if (!isDataURLCorrectlyNamed) {
        		if (isDataFileRenamed) {
        			if (!renameFile(dataFile, new File(dataURL))) {
        				logger.warn("Data file " + StringUtils.quote(dataURL) + 
            					" was renamed to " + StringUtils.quote(dataFile.toString()) + ".");
        			}
        		} else {
        			if (!dataFile.delete()) {
           				logger.warn("Temp data file was not deleted.");
            		}
        		}
    		}
        }
    }
    
    /**
     * Rename oldFile to newFile.
     * 
     * @param oldFile 
     * @param newFile 
     * @return true if success
     */
    private boolean renameFile(File oldFile, File newFile) {
    	try {
			newFile.delete();
			if (!(oldFile.renameTo(newFile))) {
				return false;
			}
			logger.debug("File " + StringUtils.quote(oldFile.getAbsolutePath()) + " is been renaming to " + 
					StringUtils.quote(newFile.getAbsolutePath()) + ".");
		} catch (SecurityException se) {
			return false;
		}
		
		return true;
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
        	MysqlDataWriter2 mysqlDataWriter = new MysqlDataWriter2(
        			xattribs.getString(XML_ID_ATTRIBUTE),
                    xattribs.getString(XML_MYSQL_IMPORT_PATH_ATTRIBUTE),
                    xattribs.getString(XML_DATABASE_ATTRIBUTE),
                    xattribs.getString(XML_TABLE_ATTRIBUTE));
        	
        	if (xattribs.exists(XML_FILE_URL_ATTRIBUTE)) {
        		mysqlDataWriter.setInDataFileName(xattribs.getString(XML_FILE_URL_ATTRIBUTE));
        	}
        	if (xattribs.exists(XML_COLUMN_DELIMITER_ATTRIBUTE)) {
        		mysqlDataWriter.setColumnDelimiter(xattribs.getString(XML_COLUMN_DELIMITER_ATTRIBUTE));
        	}
        	if (xattribs.exists(XML_HOST_ATTRIBUTE)) {
        		mysqlDataWriter.setHost(xattribs.getString(XML_HOST_ATTRIBUTE));
        	}
        	if (xattribs.exists(XML_USER_ATTRIBUTE)) {
        		mysqlDataWriter.setUser(xattribs.getString(XML_USER_ATTRIBUTE));
        	}
        	if (xattribs.exists(XML_PASSWORD_ATTRIBUTE)) {
        		mysqlDataWriter.setPassword(xattribs.getString(XML_PASSWORD_ATTRIBUTE));
        	}
        	if (xattribs.exists(XML_PARAMETERS_ATTRIBUTE)) {
        		mysqlDataWriter.setParameters(xattribs.getString(XML_PARAMETERS_ATTRIBUTE));
        	}

            return mysqlDataWriter;
        } catch (Exception ex) {
               throw new XMLConfigurationException(COMPONENT_TYPE + ":" + 
            		   xattribs.getString(XML_ID_ATTRIBUTE," unknown ID ") + ":" + ex.getMessage(),ex);
        }
    }
    
    @Override
	public void toXML(Element xmlElement) {
		super.toXML(xmlElement);
		
		xmlElement.setAttribute(XML_MYSQL_IMPORT_PATH_ATTRIBUTE, mysqlImportPath);
		xmlElement.setAttribute(XML_DATABASE_ATTRIBUTE, database);
		xmlElement.setAttribute(XML_TABLE_ATTRIBUTE, table);

		if (!StringUtils.isEmpty(dataURL)) {
			xmlElement.setAttribute(XML_FILE_URL_ATTRIBUTE, dataURL);
		}
		if (!DEFAULT_COLUMN_DELIMITER.equals(columnDelimiter)) {
			xmlElement.setAttribute(XML_COLUMN_DELIMITER_ATTRIBUTE, columnDelimiter);
		}
		if (!StringUtils.isEmpty(host)) {
			xmlElement.setAttribute(XML_HOST_ATTRIBUTE, host);
		}
		if (!StringUtils.isEmpty(user)) {
			xmlElement.setAttribute(XML_USER_ATTRIBUTE, user);
		}
		if (!StringUtils.isEmpty(password)) {
			xmlElement.setAttribute(XML_PASSWORD_ATTRIBUTE, password);
		}
		if (!StringUtils.isEmpty(parameters)) {
			xmlElement.setAttribute(XML_PARAMETERS_ATTRIBUTE, parameters);
		} else if (!properties.isEmpty()) {
			StringBuilder props = new StringBuilder();
			for (Iterator iter = properties.entrySet().iterator(); iter.hasNext();) {
				Entry<String, String> element = (Entry<String, String>) iter.next();
				props.append(element.getKey());
				props.append('=');
				props.append(StringUtils.isQuoted(element.getValue()) ? element.getValue() : 
					StringUtils.quote(element.getValue()));
				props.append(';');
			} 
			xmlElement.setAttribute(XML_PARAMETERS_ATTRIBUTE, props.toString());
		}
	}

	/**  Description of the Method */
    @Override
    public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		super.checkConfig(status);
		 
		checkInputPorts(status, 0, 1);
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
    
    public String getType(){
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

	private void setPassword(String password) {
		this.password = password;
	}
    
    private void setParameters(String parameters) {
    	this.parameters = parameters;
	}
    
    
    /**
     * General file manipulation utilities.
     */
    private static class FileUtils {
    	private static final int DEFAULT_BUFFER_SIZE = 1024 * 4;
    	private static final char EXTENSION_SEPARATOR = '.';
    	
    	private FileUtils() {
    		
    	}
    	
    	/**
         * Copies a file to a new location.
         * <p>
         * This method copies the contents of the specified source file
         * to the specified destination file.
         * The directory holding the destination file is created if it does not exist.
         * If the destination file exists, then this method will overwrite it.
         *
         * @param srcFile  an existing file to copy, must not be <code>null</code>
         * @param destFile  the new file, must not be <code>null</code>
         * @param preserveFileDate  true if the file date of the copy
         *  should be the same as the original
         *
         * @throws NullPointerException if source or destination is <code>null</code>
         * @throws IOException if source or destination is invalid
         * @throws IOException if an IO error occurs during copying
         */
        public static void copyFile(File srcFile, File destFile) throws IOException {
            if (srcFile == null) {
                throw new NullPointerException("Source must not be null");
            }
            if (destFile == null) {
                throw new NullPointerException("Destination must not be null");
            }
            if (srcFile.exists() == false) {
                throw new FileNotFoundException("Source '" + srcFile + "' does not exist");
            }
            if (srcFile.isDirectory()) {
                throw new IOException("Source '" + srcFile + "' exists but is a directory");
            }
            if (srcFile.getCanonicalPath().equals(destFile.getCanonicalPath())) {
                throw new IOException("Source '" + srcFile + "' and destination '" + destFile + "' are the same");
            }
            if (destFile.getParentFile() != null && destFile.getParentFile().exists() == false) {
                if (destFile.getParentFile().mkdirs() == false) {
                    throw new IOException("Destination '" + destFile + "' directory cannot be created");
                }
            }
            if (destFile.exists() && destFile.canWrite() == false) {
                throw new IOException("Destination '" + destFile + "' exists but is read-only");
            }
            doCopyFile(srcFile, destFile, true);
        }

        /**
         * Internal copy file method.
         * 
         * @param srcFile  the validated source file, must not be <code>null</code>
         * @param destFile  the validated destination file, must not be <code>null</code>
         * @param preserveFileDate  whether to preserve the file date
         * @throws IOException if an error occurs
         */
        private static void doCopyFile(File srcFile, File destFile, boolean preserveFileDate) throws IOException {
            if (destFile.exists() && destFile.isDirectory()) {
                throw new IOException("Destination '" + destFile + "' exists but is a directory");
            }

            FileInputStream input = new FileInputStream(srcFile);
            try {
                FileOutputStream output = new FileOutputStream(destFile);
                try {
                    copy(input, output);
                } finally {
                    closeQuietly(output);
                }
            } finally {
                closeQuietly(input);
            }

            if (srcFile.length() != destFile.length()) {
                throw new IOException("Failed to copy full contents from '" +
                        srcFile + "' to '" + destFile + "'");
            }
            if (preserveFileDate) {
                destFile.setLastModified(srcFile.lastModified());
            }
        }
        
        /**
         * Copy bytes from an <code>InputStream</code> to an
         * <code>OutputStream</code>.
         * <p>
         * This method buffers the input internally, so there is no need to use a
         * <code>BufferedInputStream</code>.
         * <p>
         * Large streams (over 2GB) will return a bytes copied value of
         * <code>-1</code> after the copy has completed since the correct
         * number of bytes cannot be returned as an int. For large streams
         * use the <code>copyLarge(InputStream, OutputStream)</code> method.
         * 
         * @param input  the <code>InputStream</code> to read from
         * @param output  the <code>OutputStream</code> to write to
         * @return the number of bytes copied
         * @throws NullPointerException if the input or output is null
         * @throws IOException if an I/O error occurs
         * @throws ArithmeticException if the byte count is too large
         * @since Commons IO 1.1
         */
        private static int copy(InputStream input, OutputStream output) throws IOException {
            long count = copyLarge(input, output);
            if (count > Integer.MAX_VALUE) {
                return -1;
            }
            return (int) count;
        }

        /**
         * Copy bytes from a large (over 2GB) <code>InputStream</code> to an
         * <code>OutputStream</code>.
         * <p>
         * This method buffers the input internally, so there is no need to use a
         * <code>BufferedInputStream</code>.
         * 
         * @param input  the <code>InputStream</code> to read from
         * @param output  the <code>OutputStream</code> to write to
         * @return the number of bytes copied
         * @throws NullPointerException if the input or output is null
         * @throws IOException if an I/O error occurs
         * @since Commons IO 1.3
         */
        private static long copyLarge(InputStream input, OutputStream output)
                throws IOException {
            byte[] buffer = new byte[DEFAULT_BUFFER_SIZE];
            long count = 0;
            int n = 0;
            while (-1 != (n = input.read(buffer))) {
                output.write(buffer, 0, n);
                count += n;
            }
            return count;
        }

        /**
         * Unconditionally close an <code>InputStream</code>.
         * <p>
         * Equivalent to {@link InputStream#close()}, except any exceptions will be ignored.
         * This is typically used in finally blocks.
         *
         * @param input  the InputStream to close, may be null or already closed
         */
        private static void closeQuietly(InputStream input) {
            try {
                if (input != null) {
                    input.close();
                }
            } catch (IOException ioe) {
                // ignore
            }
        }
        
        /**
         * Unconditionally close an <code>OutputStream</code>.
         * <p>
         * Equivalent to {@link OutputStream#close()}, except any exceptions will be ignored.
         * This is typically used in finally blocks.
         *
         * @param output  the OutputStream to close, may be null or already closed
         */
        private static void closeQuietly(OutputStream output) {
            try {
                if (output != null) {
                    output.close();
                }
            } catch (IOException ioe) {
                // ignore
            }
        }
        
        /**
         * Gets the fileName and create instance of File from it.
         * If file with <i>fileName</i> name exists then extension is added so that 
         * file is unique.
         *
         * @param filename the filename of the file that should be unique
         * @return the file that is unique
         */
        public static File createUniqueFile(String fileName) throws IOException {
    		File file = new File(fileName);
        	String tmpFileName = fileName;
    		while (file.exists()) {
    			fileName = tmpFileName + EXTENSION_SEPARATOR + new Random().nextInt(999);
    			file = new File(fileName);
    		}
    		
    		file.createNewFile();
    		return file;
        }
    }
    
    /**
     * General filename and filepath manipulation utilities.
     */
    private static class FilenameUtils {
    	private static final char EXTENSION_SEPARATOR = '.';
        private static final char UNIX_SEPARATOR = '/';
        private static final char WINDOWS_SEPARATOR = '\\';
    	
    	/**
         * Gets the base name, minus the full path and extension, from a full filename.
         * <pre>
         * a/b/c.txt --> c
         * a.txt     --> a
         * a/b/c     --> c
         * a/b/c/    --> ""
         * </pre>
         *
         * @param filename  the filename to query, null returns null
         * @return the name of the file without the path, or an empty string if none exists
         */
        public static String getBaseName(String filename) {
            return removeExtension(getName(filename));
        }
        
        /**
         * Gets the name minus the path from a full filename.
         * <p>
         * The text after the last forward or backslash is returned.
         * <pre>
         * a/b/c.txt --> c.txt
         * a.txt     --> a.txt
         * a/b/c     --> c
         * a/b/c/    --> ""
         * </pre>
         * <p>
         *
         * @param filename  the filename to query, null returns null
         * @return the name of the file without the path, or an empty string if none exists
         */
        private static String getName(String filename) {
            if (filename == null) {
                return null;
            }
            int index = indexOfLastSeparator(filename);
            return filename.substring(index + 1);
        }
        
        /**
         * Removes the extension from a filename.
         * <p>
         * This method returns the textual part of the filename before the last dot.
         * There must be no directory separator after the dot.
         * <pre>
         * foo.txt    --> foo
         * a\b\c.jpg  --> a\b\c
         * a\b\c      --> a\b\c
         * a.b\c      --> a.b\c
         * </pre>
         * <p>
         * The output will be the same irrespective of the machine that the code is running on.
         *
         * @param filename  the filename to query, null returns null
         * @return the filename minus the extension
         */
        private static String removeExtension(String filename) {
            if (filename == null) {
                return null;
            }
            int index = indexOfExtension(filename);
            if (index == -1) {
                return filename;
            } else {
                return filename.substring(0, index);
            }
        }
        
        /**
         * Returns the index of the last extension separator character, which is a dot.
         * <p>
         * This method also checks that there is no directory separator after the last dot.
         * To do this it uses {@link #indexOfLastSeparator(String)} which will
         * handle a file in either Unix or Windows format.
         * <p>
         * 
         * @param filename  the filename to find the last path separator in, null returns -1
         * @return the index of the last separator character, or -1 if there
         * is no such character
         */
        private static int indexOfExtension(String filename) {
            if (filename == null) {
                return -1;
            }
            int extensionPos = filename.lastIndexOf(EXTENSION_SEPARATOR);
            int lastSeparator = indexOfLastSeparator(filename);
            return (lastSeparator > extensionPos ? -1 : extensionPos);
        }
        
        /**
         * Returns the index of the last directory separator character.
         * 
         * @param filename  the filename to find the last path separator in, null returns -1
         * @return the index of the last separator character, or -1 if there
         * is no such character
         */
        private static int indexOfLastSeparator(String filename) {
            if (filename == null) {
                return -1;
            }
            int lastUnixPos = filename.lastIndexOf(UNIX_SEPARATOR);
            int lastWindowsPos = filename.lastIndexOf(WINDOWS_SEPARATOR);
            return Math.max(lastUnixPos, lastWindowsPos);
        }
    }
}
