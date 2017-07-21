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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.component.util.CommandBuilder;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.ConfigurationStatus.Priority;
import org.jetel.exception.ConfigurationStatus.Severity;
import org.jetel.exception.JetelException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPort;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.ExceptionUtils;
import org.jetel.util.SynchronizeUtils;
import org.jetel.util.exec.DataConsumer;
import org.jetel.util.exec.LoggerDataConsumer;
import org.jetel.util.exec.PlatformUtils;
import org.jetel.util.exec.ProcBox;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.property.RefResFlag;
import org.jetel.util.string.StringUtils;
import org.w3c.dom.Element;

/**
 * <h3>Mysql data writer</h3>
 * 
 * <!-- All records from input port 0 are loaded into mysql database. Connection to database is not through JDBC driver, this
 * component uses the mysql utility for this purpose. Bad rows' description is sent to output port 0.-->
 * 
 * <table border="1">
 * <th>Component:</th>
 * <tr>
 * <td>
 * <h4><i>Name:</i></h4>
 * </td>
 * <td>Mysql data writer</td>
 * </tr>
 * <tr>
 * <td>
 * <h4><i>Category:</i></h4>
 * </td>
 * <td></td>
 * </tr>
 * <tr>
 * <td>
 * <h4><i>Description:</i></h4>
 * </td>
 * <td>This component loads data to mysql database using the mysql utility. It is faster then DBOutputTable. 
 * Load formats data pages directly, while avoiding most of the overhead of individual row processing that inserts incur.<br>
 * There is created mysql command (LOAD DATA INFILE) depending on input parameters. Data are read from given input file or 
 * from the input port and loaded to database.<br>
 * Any generated commands/files can be optionally logged to help diagnose problems.<br>
 * Before you use this component, make sure that mysql client is installed and configured on the machine where CloverETL runs and
 * mysql command line tool available. </td>
 * </tr>
 * <tr>
 * <td>
 * <h4><i>Inputs:</i></h4>
 * </td>
 * <td>[0] - input records. It can be omitted - then <b>fileURL</b> has to be provided.</td>
 * </tr>
 * <tr><td><h4><i>Outputs:</i></h4></td>
 * <td>[0] - optionally one output port defined/connected - info about rejected records.
 * Output metadata contains three fields with row number (integer), column name (string) and error message (string).
 * </td></tr>
 * 
 * <h4><i>Comment:</i></h4>
 * </td>
 * <td></td>
 * </tr>
 * </table> <br>
 * <table border="1">
 * <th>XML attributes:</th>
 * <tr>
 * <td><b>type</b></td>
 * <td>"MYSQL_DATA_WRITER"</td>
 * </tr>
 * <tr>
 * <td><b>id</b></td>
 * <td>component identification</td>
 * </tr>
 * <tr>
 * <td><b>mysqlPath</b></td>
 * <td>path to mysql utility</td>
 * </tr>
 * <tr>
 * <td><b>database</b></td>
 * <td>the name of the database to receive the data</td>
 * </tr>
 * <tr>
 * <td><b>table</b></td>
 * <td>table name, where data are loaded</td>
 * </tr>
 * <tr>
 * <td><b>columnDelimiter</b><br>
 * <i>optional</i></td>
 * <td>delimiter used for each column in data (default = '\t')</br> Value of the delimiter mustn't be contained in data.</td>
 * </tr>
 * <tr>
 * <td><b>fileURL</b><br>
 * <i>optional</i></td>
 * <td>Path to data file to be loaded.<br>
 * Normally this file is a temporary storage for data to be passed to mysql utility.
 * If <i>fileURL</i> is not specified, at Windows platform the file is created in Clover or OS temporary directory and deleted after load finishes.
 * At Linux/Unix system named pipe is used instead of temporary file.
 * If <i>fileURL</i> is specified, temporary file is created within given path and name and not deleted after being loaded. Next graph
 * run overwrites it.<br>
 * There is one more meaning of this parameter. If input port is not specified, this file is used only for reading by mysql
 * utility and must already contain data in format expected by load. The file is neither deleted nor overwritten.</td>
 * </tr>
 * <tr>
 * <td><b>commandURL</b><br>
 * <i>optional</i></td>
 * <td>Path to command file where LOAD DATA INFILE statement is stored.<br>
 * If <i>commandURL</i> is not specified, the command file is created in Clover temporary directory and deleted after load finishes.<br>
 * If <i>commandURL</i> is specified and this file doesn't exist, temporary command file is created within given path and name and not deleted after being loaded. Default command file is stored here.<br>
 * If <i>commandURL</i> is specified and this file exist, this file is used instead of command file created by Clover.
 * </td>
 * </tr>
 * <tr>
 * <td><b>host</b><br>
 * <i>optional</i></td>
 * <td>Import data to the MySQL server on the given host. The default host is localhost.</td>
 * </tr>
 * <tr>
 * <td><b>username</b><br>
 * <i>optional</i></td>
 * <td>The MySQL username to use when connecting to the server.</td>
 * </tr>
 * <tr>
 * <td><b>password</b><br>
 * <i>optional</i></td>
 * <td>The password to use when connecting to the server.</td>
 * </tr>
 * <tr>
 * <td>lockTable</td>
 * <td>A flag specifying whether to lock the table in order to ensure exclusive access and possibly faster loading.</td>
 * </tr>
 * <tr>
 * <td>ignoreRows</td>
 * <td>Ignore the first N lines of the data file.</td>
 * </tr>
 * <tr>
 * <td><b>parameters</b><br>
 * <i>optional</i></td>
 * <td>All possible additional parameters which can be passed on to 
 * <a href="http://dev.mysql.com/doc/refman/5.1/en/mysql-command-options.html"> mysql utility</a> or to 
 * <a href="http://dev.mysql.com/doc/refman/5.1/en/load-data.html"> LOAD DATA INFILE statement</a>.<br>
 * Parameters, in form <i>key=value</i>
 * (or <i>key</i> - interpreted as <i>key=true</i>, if possible value are only "true" or "false") has to be separated by :;|
 * {colon, semicolon, pipe}. If in parameter value occurs one of :;|, value has to be double quoted.<br>
 * <b>Load parameters</b><br>
 * <b><i>Parameters for mysql utility</i></b><br>
 * <table> 
 * <tr>
 * <td>skipAutoRehash</td>
 * <td>Disable automatic rehashing. Disables table and column name completion. That causes mysql to start faster.<br>
 * If <i>skipAutoRehash</i> attribute isn't defined, default value is true.</td>
 * </tr>
 * <tr>
 * <td>characterSetsDir</td>
 * <td>The directory where character sets are installed. See <a
 * href="http://www.mysql.org/doc/refman/5.1/en/character-sets.html"> The Character Set Used for Data and Sorting</a>.</td>
 * </tr>
 * <tr>
 * <td>compress</td>
 * <td>Compress all information sent between the client and the server if both support compression.</td>
 * </tr>
 * <tr>
 * <td>defaultCharacterSet</td>
 * <td>Use <i>defaultCharacterSet</i> as the default character set. See <a
 * href="http://www.mysql.org/doc/refman/5.1/en/character-sets.html"> The Character Set Used for Data and Sorting</a>.</td>
 * </tr>
 * <tr>
 * <td>force</td>
 * <td>Continue even if an SQL error occurs.</td>
 * </tr>
 * <tr>
 * <td>noBeep</td>
 * <td>Do not beep when errors occur.</td>
 * </tr>
 * <tr>
 * <td>port</td>
 * <td>The TCP/IP port number to use for the connection.</td>
 * </tr>
 * <tr>
 * <td>protocol</td>
 * <td>The connection protocol to use. One of {TCP|SOCKET|PIPE|MEMORY}.</td>
 * </tr>
 * <tr>
 * <td>reconnect</td>
 * <td>If the connection to the server is lost, automatically try to reconnect. A single reconnect attempt is made each time the connection is lost.</td>
 * </tr>
 * <tr>
 * <td>secureAuth</td>
 * <td>Do not send passwords to the server in old (pre-4.1.1) format. This prevents connections except for servers that use the newer password format.</td>
 * </tr>
 * <tr>
 * <td>showWarnings</td>
 * <td>Cause warnings to be shown after each statement if there are any.
 * If <i>showWarnings</i> attribute isn't defined, default value is true.<br>
 * If any output port is connected, this parameter must be used.</td>
 * </tr>
 * <tr>
 * <td>silent</td>
 * <td>Silent mode. Produce output only when errors occur.</td>
 * </tr>
 * <tr>
 * <td>socket</td>
 * <td>For connections to localhost, the Unix socket file to use, or, on Windows, the name of the named pipe to use.</td>
 * </tr>
 * <tr>
 * <td>ssl</td>
 * <td>Options that begin with <i>ssl</i> attribute specify whether to connect to the server via SSL and indicate where to find
 * SSL keys and certificates. See <a href="http://www.mysql.org/doc/refman/5.1/en/ssl-options.html"> SSL Command Options</a>.</td>
 * </tr>
 * </table>
 * <b><i>Parameters for LOAD DATA INFILE statement</i></b><br>
 * <table>
 * <tr>
 * <td>uniqueChecks</td>
 * <td>Used to set the unique_checks session variable before the LOAD DATA INFILE statement is executed. If set to 0 (default is 1), it may speed up the execution.</td>
 * </tr>
 * <tr>
 * <td>foreignKeyChecks</td>
 * <td>Used to set foreign_key_checks session variable before the LOAD DATA INFILE statement is executed. If set to 0 (default is 1), it may speed up the execution.</td>
 * </tr>
 * <tr>
 * <td>sqlLogBin</td>
 * <td>Used to set the sql_log_bin session variable before the LOAD DATA INFILE statement is executed. If set to 0 (default is 1), it may speed up the execution.</td>
 * </tr>
 * <tr>
 * <td>local</td>
 * <td>Read input files locally from the client host.<br>
 * If <i>local</i> attribute isn't defined, default value is true.</td>
 * </tr>
 * <tr>
 * <td>lowPriority</td>
 * <td>If you use <i>lowPriority</i>, execution of the LOAD DATA statement is delayed until no other clients are reading from the table.
 * This affects only storage engines that use only table-level locking (MyISAM, MEMORY, MERGE).</td>
 * </tr>
 * <tr>
 * <td>concurrent</td>
 * <td>If you specify <i>concurrent</i> with a MyISAM table that satisfies the condition for concurrent inserts 
 * (that is, it contains no free blocks in the middle), other threads can retrieve data from the table while LOAD DATA is executing. 
 * Using this option affects the performance of LOAD DATA a bit, even if no other thread is using the table at the same time.</td>
 * </tr>
 * <tr>
 * <td>ignore</td>
 * <td>See the description for the <i>replace</i> attribute.</td>
 * </tr>
 * <tr>
 * <td>replace</td>
 * <td>The <i>replace</i> and <i>ignore</i> parameter control handling of input rows that duplicate existing rows on
 * unique key values. If you specify <i>replace</i> parameter, input rows replace existing rows. 
 * In other words, rows that have the same value for a primary key or unique index as an existing row. See <a href="http://dev.mysql.com/doc/refman/5.1/en/replace.html">REPLACE Syntax</a>.<br>
 * If you specify <i>ignore</i> parameter, input rows that duplicate an existing row on a unique key value are skipped. 
 * If you do not specify either option, the behavior depends on whether the <i>local</i> parameter is specified. 
 * Without <i>local</i>, an error occurs when a duplicate key value is found, and the rest of the text file is ignored. 
 * With <i>local</i>, the default behavior is the same as if <i>ignore</i> is specified; 
 * this is because the server has no way to stop transmission of the file in the middle of the operation.</td>
 * </tr>
 * <tr>
 * <td>fieldsEnclosedBy</td>
 * <td>It is used for enclosing each filed in data by char. <br>
 * When data is read from input port this attribute is ignored.</td>
 * </tr>
 * <tr>
 * <td>fieldsIsOptionallyEnclosed</td>
 * <td>It decide if <i>fieldsEnclosedBy</i> is used for each columns or not. <br>
 * It can be used only with <i>fieldsEnclosedBy</i> attribute. <br>
 * When data is read from input port this attribute is ignored.</td>
 * </tr>
 * <tr>
 * <td>fieldsEscapedBy</td>
 * <td>It is used form escaping fields by char. <br>
 * When data is read from input port this attribute is ignored.</td>
 * </tr>
 * <tr>
 * <td>linesStartingBy</td>
 * <td>If all the lines you want to read in have a common prefix that you want to ignore, 
 * you can use <i>linesStartingBy</i> to skip over the prefix, and anything before it. 
 * If a line does not include the prefix, the entire line is skipped.</td>
 * </tr>
 * <tr>
 * <td>recordDelimiter</td>
 * <td>Specifies the record delimiter. (default = '\n' (newline character)).</td>
 * </tr>
 * <tr>
 * <td>columns</td>
 * <td>This option takes a comma-separated list of column names as its value. The order of the column names indicates how to
 * match data file columns with table columns.<br>
 * </td>
 * </tr>
 * </table></td>
 * </tr>
 * </table>
 * 
 * <h4>Example:</h4>
 * Reading data from input port:
 * 
 * <pre>
 * &lt;Node
 *  mysqlPath="mysql"
 *  database="testdb"
 *  table="test"
 *  id="MYSQL_DATA_WRITER1"
 *  type="MYSQL_DATA_WRITER"
 *  /&gt;
 * </pre>
 * 
 * Reading data from flat file:
 * 
 * <pre>
 * &lt;Node
 *  mysqlPath="mysql"
 *  database="testdb" 
 *  table="test"
 *  columnDelimiter="," 
 *  fileURL="${WORKSPACE}/data/delimited/mysqlFlat.dat" 
 *  parameters="fieldsEnclosedBy=*|fieldsIsOptionallyEnclosed"
 *  id="MYSQL_DATA_WRITER0"
 *  type="MYSQL_DATA_WRITER"/>
 *  /&gt;
 *  
 * @author      Miroslav Haupt (Mirek.Haupt@javlinconsulting.cz)
 * (c) Javlin Consulting (www.javlinconsulting.cz)
 * @see         org.jetel.graph.TransformationGraph
 * @see         org.jetel.graph.Node
 * @see         org.jetel.graph.Edge
 * @since 		24.9.2007
 * 
 */
public class MysqlDataWriter extends BulkLoader {

	private static Log logger = LogFactory.getLog(MysqlDataWriter.class);

	/** Description of the Field */
	private static final String XML_MYSQL_PATH_ATTRIBUTE = "mysqlPath";
	private static final String XML_HOST_ATTRIBUTE = "host";
	private static final String XML_COMMAND_URL_ATTRIBUTE = "commandURL";
	private static final String XML_LOCK_TABLE_ATTRIBUTE = "lockTable";
	private static final String XML_IGNORE_ROWS_ATTRIBUTE = "ignoreRows";

	// params for mysql client
	private static final String MYSQL_SKIP_AUTO_REHASH_PARAM = "skipAutoRehash";
	private static final String MYSQL_CHARACTER_SETS_DIR_PARAM = "characterSetsDir";
	private static final String MYSQL_COMPRESS_PARAM = "compress";
	private static final String MYSQL_DEFAULT_CHARACTER_SET_PARAM = "defaultCharacterSet";
	private static final String MYSQL_FORCE_PARAM = "force";
	private static final String MYSQL_NO_BEEP_PARAM = "noBeep";
	private static final String MYSQL_PORT_PARAM = "port";
	private static final String MYSQL_PROTOCOL_PARAM = "protocol";
	private static final String MYSQL_RECONNECT_PARAM = "reconnect";
	private static final String MYSQL_SECURE_AUTH_PARAM = "secureAuth";
	private static final String MYSQL_SHOW_WARNINGS_PARAM = "showWarnings";
	private static final String MYSQL_SILENT_PARAM = "silent";
	private static final String MYSQL_SOCKET_PARAM = "socket";
	private static final String MYSQL_SSL_PARAM = "ssl";

    // params (in fact session variables) for LOAD DATA INFILE statement
    private static final String LOAD_UNIQUE_CHECKS_PARAM = "uniqueChecks";
    private static final String LOAD_FOREIGN_KEY_CHECKS_PARAM = "foreignKeyChecks";
    private static final String LOAD_SQL_LOG_BIN_PARAM = "sqlLogBin";

	// params for LOAD DATA INFILE statement
	private static final String LOAD_LOCAL_PARAM = "local";
	private static final String LOAD_LOW_PRIORITY_PARAM = "lowPriority";
	private static final String LOAD_CONCURRENT_PARAM = "concurrent";
	private static final String LOAD_REPLACE_PARAM = "replace";
	private static final String LOAD_IGNORE_PARAM = "ignore";
	private static final String LOAD_FIELDS_ENCLOSED_BY_PARAM = "fieldsEnclosedBy";
	private static final String LOAD_FIELDS_IS_OPTIONALLY_ENCLOSED_PARAM = "fieldsIsOptionallyEnclosed";
	private static final String LOAD_FIELDS_ESCAPED_BY_PARAM = "fieldsEscapedBy";
	private static final String LOAD_LINES_STARTING_BY_PARAM = "linesStartingBy";
	private static final String LOAD_RECORD_DELIMITER_PARAM = "recordDelimiter";
	private static final String LOAD_COLUMNS_PARAM = "columns";

	// switches for mysql client,these switches have own xml attributes
	private static final String MYSQL_DATABASE_SWITCH = "database";
	private static final String MYSQL_HOST_SWITCH = "host";
	private static final String MYSQL_USER_SWITCH = "user";
	private static final String MYSQL_PASSWORD_SWITCH = "password";

	// switches for mysql client
	private static final String MYSQL_SKIP_AUTO_REHASH_SWITCH = "skip-auto-rehash";
	private static final String MYSQL_CHARACTER_SETS_DIR_SWITCH = "character-sets-dir";
	private static final String MYSQL_COMPRESS_SWITCH = "compress";
	private static final String MYSQL_DEFAULT_CHARACTER_SET_SWITCH = "default-character-set";
	private static final String MYSQL_EXECUTE_SWITCH = "execute";
	private static final String MYSQL_FORCE_SWITCH = "force";
	private static final String MYSQL_LOCAL_INFILE_SWITCH = "local-infile";
	private static final String MYSQL_NO_BEEP_SWITCH = "no-beep";
	private static final String MYSQL_PORT_SWITCH = "port";
	private static final String MYSQL_PROTOCOL_SWITCH = "protocol";
	private static final String MYSQL_RECONNECT_SWITCH = "reconnect";
	private static final String MYSQL_SECURE_AUTH_SWITCH = "secure-auth";
	private static final String MYSQL_SHOW_WARNINGS_SWITCH = "show-warnings";
	private static final String MYSQL_SILENT_SWITCH = "silent";
	private static final String MYSQL_SOCKET_SWITCH = "socket";
	private static final String MYSQL_SSL_SWITCH = "ssl*";

    // names of session variables that affect the LOAD DATA INFILE statement
    private static final String LOAD_UNIQUE_CHECKS_VAR = "unique_checks";
    private static final String LOAD_FOREIGN_KEY_CHECKS_VAR = "foreign_key_checks";
    private static final String LOAD_SQL_LOG_BIN_VAR = "sql_log_bin";

	// keywords for LOAD DATA INFILE statement
	private static final String LOAD_LOCAL_KEYWORD = "LOCAL";
	private static final String LOAD_LOW_PRIORITY_KEYWORD = "LOW_PRIORITY";
	private static final String LOAD_CONCURRENT_KEYWORD = "CONCURRENT";
	private static final String LOAD_REPLACE_KEYWORD = "REPLACE";
	private static final String LOAD_IGNORE_KEYWORD = "IGNORE";
	private static final String LOAD_FIELDS_ENCLOSED_BY_KEYWORD = "ENCLOSED BY";
	private static final String LOAD_FIELDS_OPTIONALLY_ENCLOSED_KEYWORD = "OPTIONALLY";
	private static final String LOAD_FIELDS_ESCAPED_BY_KEYWORD = "ESCAPED BY";
	private static final String LOAD_LINES_STARTING_BY_KEYWORD = "STARTING BY";
	private static final String LOAD_RECORD_DELIMITER_KEYWORD = "TERMINATED BY";

	public final static String COMPONENT_TYPE = "MYSQL_DATA_WRITER";

	private final static String LINE_SEPARATOR = System.getProperty("line.separator");
    private final static String COMMAND_SEPARATOR = ";" + LINE_SEPARATOR;
	private final static String SWITCH_MARK = "--";

	private final static String EXCHANGE_FILE_PREFIX = "mysqlExchange";
	private final static String MYSQL_FILE_NAME_PREFIX = "mysql";
	private final static String DEFAULT_COLUMN_DELIMITER = "\t";
	private final static String DEFAULT_RECORD_DELIMITER = "\n";
	private final static String DEFAULT_DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
	private final static String DEFAULT_DATE_FORMAT = "yyyy-MM-dd";
	private final static String DEFAULT_TIME_FORMAT = "HH:mm:ss";
	private final static String DEFAULT_YEAR_FORMAT = "yyyy";

	// variables for dbload's command
	private boolean lockTable = false;
	private int ignoreRows = UNUSED_INT;
	private String commandURL;
	private File commandFile;

	/**
	 * Constructor for the MysqlDataWriter object
	 * 
	 * @param id Description of the Parameter
	 */
	public MysqlDataWriter(String id, String mysqlPath, String database, String table) {
		super(id, mysqlPath, database);
		this.table = table;
		
		columnDelimiter = DEFAULT_COLUMN_DELIMITER;
	}

	/**
	 * Main processing method for the MysqlDataWriter object
	 * 
	 * @since April 4, 2002
	 */
	@Override
	public Result execute() throws Exception {
		super.execute();
		ProcBox box;
		int processExitValue = 0;

		if (isDataReadFromPort) {
			if (PlatformUtils.isWindowsPlatform() || !StringUtils.isEmpty(dataURL)) {
				// dataFile is used for exchange data
				readFromPortAndWriteByFormatter();
				box = createProcBox();
				processExitValue = box.join();
			} else {  // data is send to process through named pipe
				processExitValue = runWithPipe();
			}
		} else {
			processExitValue = readDataDirectlyFromFile();
		}

		if (processExitValue != 0) {
			throw new JetelException("Mysql utility has failed.");
		}

		return runIt ? Result.FINISHED_OK : Result.ABORTED;
	}

	@Override
	protected String[] createCommandLineForLoadUtility() throws ComponentNotReadyException {
		if (PlatformUtils.isWindowsPlatform()) {
			loadUtilityPath = StringUtils.backslashToSlash(loadUtilityPath);
		}
		CommandBuilder cmdBuilder = new CommandBuilder(properties, SWITCH_MARK);

		cmdBuilder.add(loadUtilityPath);
		cmdBuilder.addBooleanParam(MYSQL_SKIP_AUTO_REHASH_PARAM, MYSQL_SKIP_AUTO_REHASH_SWITCH, true);
		cmdBuilder.addAttribute(MYSQL_HOST_SWITCH, host);
		cmdBuilder.addAttribute(MYSQL_USER_SWITCH, user);
		cmdBuilder.addAttribute(MYSQL_PASSWORD_SWITCH, password);
		cmdBuilder.addAttribute(MYSQL_DATABASE_SWITCH, database);

		cmdBuilder.addParam(MYSQL_CHARACTER_SETS_DIR_PARAM, MYSQL_CHARACTER_SETS_DIR_SWITCH);
		cmdBuilder.addBooleanParam(MYSQL_COMPRESS_PARAM, MYSQL_COMPRESS_SWITCH);
		cmdBuilder.addParam(MYSQL_DEFAULT_CHARACTER_SET_PARAM, MYSQL_DEFAULT_CHARACTER_SET_SWITCH);

		String commandFileName = createCommandFile();
		if (PlatformUtils.isWindowsPlatform()) {
			commandFileName = StringUtils.backslashToSlash(commandFileName);
		}
		cmdBuilder.addAttribute(MYSQL_EXECUTE_SWITCH, "source " + commandFileName);
		cmdBuilder.addBooleanParam(MYSQL_FORCE_PARAM, MYSQL_FORCE_SWITCH);
		cmdBuilder.addBooleanParam("", MYSQL_LOCAL_INFILE_SWITCH, true);
		cmdBuilder.addBooleanParam(MYSQL_NO_BEEP_PARAM, MYSQL_NO_BEEP_SWITCH);

		cmdBuilder.addParam(MYSQL_PORT_PARAM, MYSQL_PORT_SWITCH);
		cmdBuilder.addParam(MYSQL_PROTOCOL_PARAM, MYSQL_PROTOCOL_SWITCH);
		cmdBuilder.addBooleanParam(MYSQL_RECONNECT_PARAM, MYSQL_RECONNECT_SWITCH);
		cmdBuilder.addBooleanParam(MYSQL_SECURE_AUTH_PARAM, MYSQL_SECURE_AUTH_SWITCH);
		cmdBuilder.addBooleanParam(MYSQL_SHOW_WARNINGS_PARAM, MYSQL_SHOW_WARNINGS_SWITCH, true);
		cmdBuilder.addBooleanParam(MYSQL_SILENT_PARAM, MYSQL_SILENT_SWITCH);
		cmdBuilder.addParam(MYSQL_SOCKET_PARAM, MYSQL_SOCKET_SWITCH);
		cmdBuilder.addBooleanParam(MYSQL_SSL_PARAM, MYSQL_SSL_SWITCH);

		return cmdBuilder.getCommand();
	}

	/**
	 * Create file that contains LOAD DATA INFILE command and return its name.
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
				commandFile = createTempFile(MYSQL_FILE_NAME_PREFIX, CONTROL_FILE_NAME_SUFFIX);
			}

			saveCommandFile(commandFile);
			return commandFile.getCanonicalPath();
		} catch (IOException ioe) {
			throw new ComponentNotReadyException(this, 
					"Can't create command file for mysql utility.", ioe);
		}
	}

	/**
	 * Save default LOAD DATA INFILE command to the file.
	 * @throws ComponentNotReadyException 
	 * @throws IOException 
	 * 
	 * @throws IOException when error occured
	 */
	private void saveCommandFile(File commandFile) throws ComponentNotReadyException, IOException {
		FileWriter commandWriter = new FileWriter(commandFile);
		String command = getDefaultCommandFileContent();
		logger.debug("Command file content: " + command);

		commandWriter.write(command);
		commandWriter.close();
	}
	
	/**
	 * Create and return string that contains LOAD DATA INFILE command.
	 * @return string that contains LOAD DATA INFILE command
	 * @throws ComponentNotReadyException 
	 */
	private String getDefaultCommandFileContent() throws ComponentNotReadyException {
		CommandBuilder cmdBuilder = new CommandBuilder(properties, SPACE_CHAR);

		// set session variables before the LOAD DATA INFILE statement is executed
		if (properties.containsKey(LOAD_UNIQUE_CHECKS_PARAM)) {
	        cmdBuilder.add("SET SESSION " + LOAD_UNIQUE_CHECKS_VAR + " = "
	                + properties.getProperty(LOAD_UNIQUE_CHECKS_PARAM) + COMMAND_SEPARATOR);
		}
		if (properties.containsKey(LOAD_FOREIGN_KEY_CHECKS_PARAM)) {
            cmdBuilder.add("SET SESSION " + LOAD_FOREIGN_KEY_CHECKS_VAR + " = "
                    + properties.getProperty(LOAD_FOREIGN_KEY_CHECKS_PARAM) + COMMAND_SEPARATOR);
		}
		if (properties.containsKey(LOAD_SQL_LOG_BIN_PARAM)) {
            cmdBuilder.add("SET SESSION " + LOAD_SQL_LOG_BIN_VAR + " = "
                    + properties.getProperty(LOAD_SQL_LOG_BIN_PARAM) + COMMAND_SEPARATOR);
		}

		// LOAD DATA [LOW_PRIORITY | CONCURRENT] [LOCAL] INFILE 'file_name'
		cmdBuilder.add("LOAD DATA");
		cmdBuilder.addBooleanParam(LOAD_LOW_PRIORITY_PARAM, LOAD_LOW_PRIORITY_KEYWORD);
		cmdBuilder.addBooleanParam(LOAD_CONCURRENT_PARAM, LOAD_CONCURRENT_KEYWORD);
		cmdBuilder.addBooleanParam(LOAD_LOCAL_PARAM, LOAD_LOCAL_KEYWORD, true);
		
		cmdBuilder.add("INFILE " + StringUtils.quote(getDataFilePath()) + LINE_SEPARATOR);

		// [REPLACE | IGNORE]
		if (cmdBuilder.addBooleanParam(LOAD_REPLACE_PARAM, LOAD_REPLACE_KEYWORD)
				|| cmdBuilder.addBooleanParam(LOAD_IGNORE_PARAM, LOAD_IGNORE_KEYWORD)) {
			cmdBuilder.add(LINE_SEPARATOR);
		}

		// INTO TABLE tbl_name
		cmdBuilder.add("INTO TABLE " + table + LINE_SEPARATOR);

		// [FIELDS
		// [TERMINATED BY 'string']
		// [[OPTIONALLY] ENCLOSED BY 'char']
		// [ESCAPED BY 'char']
		// ]
		if (!columnDelimiter.equals(DEFAULT_COLUMN_DELIMITER) || 
				properties.containsKey(LOAD_FIELDS_ENCLOSED_BY_PARAM) ||
				properties.containsKey(LOAD_FIELDS_ESCAPED_BY_PARAM)) {
			cmdBuilder.add("FIELDS" + LINE_SEPARATOR);
			cmdBuilder.add("TERMINATED BY '" + columnDelimiter + "'" + LINE_SEPARATOR);

			cmdBuilder.addBooleanParam(LOAD_FIELDS_IS_OPTIONALLY_ENCLOSED_PARAM,
							LOAD_FIELDS_OPTIONALLY_ENCLOSED_KEYWORD);
			if (cmdBuilder.addParam(LOAD_FIELDS_ENCLOSED_BY_PARAM, LOAD_FIELDS_ENCLOSED_BY_KEYWORD, true)) {
				cmdBuilder.add(LINE_SEPARATOR);
			}
			if (cmdBuilder.addParam(LOAD_FIELDS_ESCAPED_BY_PARAM, LOAD_FIELDS_ESCAPED_BY_KEYWORD, true)) {
				cmdBuilder.add(LINE_SEPARATOR);
			}
		}

		// [LINES
		// [STARTING BY 'string']
		// [TERMINATED BY 'string']
		// ]
		if (properties.containsKey(LOAD_LINES_STARTING_BY_PARAM) ||
				properties.containsKey(LOAD_RECORD_DELIMITER_PARAM)) {
			cmdBuilder.add("LINES" + LINE_SEPARATOR);

			if (cmdBuilder.addParam(LOAD_LINES_STARTING_BY_PARAM, LOAD_LINES_STARTING_BY_KEYWORD, true)) {
				cmdBuilder.add(LINE_SEPARATOR);
			}
			if (cmdBuilder.addParam(LOAD_RECORD_DELIMITER_PARAM, LOAD_RECORD_DELIMITER_KEYWORD, true)) {
				cmdBuilder.add(LINE_SEPARATOR);
			}
		}

		// [IGNORE number LINES]
		if (ignoreRows != UNUSED_INT) {
			cmdBuilder.add("IGNORE " + ignoreRows + " LINES" + LINE_SEPARATOR);
		}

		// [(col_name_or_user_var,...)]
		if (properties.containsKey(LOAD_COLUMNS_PARAM)) {
			cmdBuilder.add("'" + properties.getProperty(LOAD_COLUMNS_PARAM) + "'");
		}

		if (!lockTable) {
		    return cmdBuilder.getCommandAsString();
		}

		// wrap the command(s) constructed so far between LOCK TABLES and UNLOCK TABLES commands
		StringBuilder commandWithLock = new StringBuilder();
		commandWithLock.append("LOCK TABLES ").append(table).append(" WRITE").append(COMMAND_SEPARATOR);
		commandWithLock.append(cmdBuilder.getCommandAsString().trim()).append(COMMAND_SEPARATOR);
        commandWithLock.append("UNLOCK TABLES").append(COMMAND_SEPARATOR);

		return commandWithLock.toString();
	}
	
	private String getDataFilePath() throws ComponentNotReadyException {
	    if (PlatformUtils.isWindowsPlatform() || "false".equalsIgnoreCase(properties.getProperty(LOAD_LOCAL_PARAM))) {
			// convert "C:\examples\xxx.dat" to "C:/examples/xxx.dat"
			return StringUtils.backslashToSlash(getFilePath(dataFile));
		}
		return getFilePath(dataFile);
	}

	@Override
	protected void initDataFile() throws ComponentNotReadyException {
		// data is read directly from file -> file isn't used for exchange
    	if (isDataReadDirectlyFromFile) {
            if (!fileUrlExists(dataURL)) {
                free();
                throw new ComponentNotReadyException(this, "Data file " + StringUtils.quote(dataURL) + " doesn't exist.");
            }

            dataFile = getFile(dataURL);
    	} else {
    		defaultCreateFileForExchange(EXCHANGE_FILE_PREFIX);
    	}
	}
	
	@Override
	protected void createConsumers() throws ComponentNotReadyException {
		errConsumer = new LoggerDataConsumer(LoggerDataConsumer.LVL_ERROR, 0);

		if (isDataWrittenToPort) {
			try {
				// create data consumer and check metadata
				consumer = new MysqlPortDataConsumer(getOutputPort(WRITE_TO_PORT));
			} catch (ComponentNotReadyException cnre) {
				free();
				throw new ComponentNotReadyException(this, "Error during initialization of MysqlPortDataConsumer.", cnre);
			}
		} else {
			consumer = new LoggerDataConsumer(LoggerDataConsumer.LVL_DEBUG, 0);
		}
	}

	@Override
	protected String getColumnDelimiter() {
		return columnDelimiter;
	}

	@Override
	protected String getRecordDelimiter() {
		if (properties.containsKey(LOAD_RECORD_DELIMITER_PARAM)) {
			return (String) properties.get(LOAD_RECORD_DELIMITER_PARAM);
		} else {
			return DEFAULT_RECORD_DELIMITER;
		}
	}
	
	@Override
	protected void checkParams() throws ComponentNotReadyException {
		if (StringUtils.isEmpty(loadUtilityPath)) {
			throw new ComponentNotReadyException(this, StringUtils.quote(XML_MYSQL_PATH_ATTRIBUTE)
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
		
		if (!isDataReadFromPort && !fileUrlExists(dataURL) && !fileExists(commandURL)) {
			throw new ComponentNotReadyException(this, "Input port or " + 
					StringUtils.quote(XML_FILE_URL_ATTRIBUTE) + 
					" attribute or " + StringUtils.quote(XML_COMMAND_URL_ATTRIBUTE) +
					" attribute have to be specified and specified file must exist.");
		}

		// If commandURL points to a file that does not exist and dataURL is empty, the component won't run correctly
		// because it doesn't know what data file is specified in the command script
		if (commandURL != null && !fileExists(commandURL) && StringUtils.isEmpty(dataURL)) {
            throw new ComponentNotReadyException(this, "The " + StringUtils.quote(XML_FILE_URL_ATTRIBUTE) + " attribute "
                    + "has to be set because the " + StringUtils.quote(XML_COMMAND_URL_ATTRIBUTE) + " attribute is set!");
		}

		if (ignoreRows != UNUSED_INT && ignoreRows < 0) {
			throw new ComponentNotReadyException(this,
					XML_IGNORE_ROWS_ATTRIBUTE + " mustn't be less than 0.");
		}
		
		// check combination
		if (properties.containsKey(LOAD_FIELDS_IS_OPTIONALLY_ENCLOSED_PARAM)
				&& !properties.containsKey(LOAD_FIELDS_ENCLOSED_BY_PARAM)) {
			logger.warn("Attribute " + StringUtils.quote(LOAD_FIELDS_IS_OPTIONALLY_ENCLOSED_PARAM)
					+ " is ignored because it has to be used in combination with "
					+ StringUtils.quote(LOAD_FIELDS_ENCLOSED_BY_PARAM) + " attribute.");
		}

		// if any output port is connected, MYSQL_SHOW_WARNINGS_SWITCH parameter must be used
		// MYSQL_SHOW_WARNINGS_SWITCH is used when MYSQL_SHOW_WARNINGS_PARAM isn't defined
		// or when MYSQL_SHOW_WARNINGS_PARAM=true
		if (isDataWrittenToPort) {
			if (properties.containsKey(MYSQL_SHOW_WARNINGS_PARAM) &&
					"false".equalsIgnoreCase(properties.getProperty(MYSQL_SHOW_WARNINGS_PARAM))) {
				properties.setProperty(MYSQL_SHOW_WARNINGS_PARAM, "true");
				logger.warn("If any output port is connected, " + 
						StringUtils.quote(MYSQL_SHOW_WARNINGS_PARAM) + 
						" parameter mustn't equals false. " +
						StringUtils.quote(MYSQL_SHOW_WARNINGS_PARAM) + " parameters was set to true.");
						
			}
		}

		// report on ignoring some attributes
		if (isDataReadFromPort) {
			if (properties.containsKey(LOAD_FIELDS_ENCLOSED_BY_PARAM)) {
				logger.warn("Attribute " + StringUtils.quote(LOAD_FIELDS_ENCLOSED_BY_PARAM)
						+ " is ignored because it is used only when data is read directly from file.");
			}
			if (properties.containsKey(LOAD_FIELDS_IS_OPTIONALLY_ENCLOSED_PARAM)) {
				logger.warn("Attribute " + StringUtils.quote(LOAD_FIELDS_IS_OPTIONALLY_ENCLOSED_PARAM)
						+ " is ignored because it is used only when data is read directly from file.");
			}
			if (properties.containsKey(LOAD_FIELDS_ESCAPED_BY_PARAM)) {
				logger.warn("Attribute " + StringUtils.quote(LOAD_FIELDS_ESCAPED_BY_PARAM)
						+ " is ignored because it is used only when data is read directly from file.");
			}
		}
	}

	private boolean fileUrlExists(String fileUrl) throws ComponentNotReadyException {
	    // If the data file is located on server, just check if its URL is not empty. Proper checking is made during
	    // the execution of the component. If desired, it should also be placed here.
	    if ("false".equalsIgnoreCase(properties.getProperty(LOAD_LOCAL_PARAM))) {
	        return !StringUtils.isEmpty(fileUrl);
	    }

	    return fileExists(fileUrl);
	}

	@Override
	protected void setLoadUtilityDateFormat(DataFieldMetadata field) {
		setLoadUtilityDateFormat(field, DEFAULT_TIME_FORMAT, DEFAULT_DATE_FORMAT, 
				DEFAULT_DATETIME_FORMAT, DEFAULT_YEAR_FORMAT);
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

		MysqlDataWriter mysqlDataWriter = new MysqlDataWriter(
				xattribs.getString(XML_ID_ATTRIBUTE), 
				xattribs.getString(XML_MYSQL_PATH_ATTRIBUTE), 
				xattribs.getString(XML_DATABASE_ATTRIBUTE), 
				xattribs.getString(XML_TABLE_ATTRIBUTE));

		if (xattribs.exists(XML_FILE_URL_ATTRIBUTE)) {
			mysqlDataWriter.setFileUrl(xattribs.getStringEx(XML_FILE_URL_ATTRIBUTE,RefResFlag.SPEC_CHARACTERS_OFF));
		}
		if (xattribs.exists(XML_COLUMN_DELIMITER_ATTRIBUTE)) {
			mysqlDataWriter.setColumnDelimiter(xattribs.getStringEx(XML_COLUMN_DELIMITER_ATTRIBUTE, RefResFlag.SPEC_CHARACTERS_OFF));
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
		if (xattribs.exists(XML_COMMAND_URL_ATTRIBUTE)) {
			mysqlDataWriter.setCommandURL((xattribs.getStringEx(XML_COMMAND_URL_ATTRIBUTE,RefResFlag.SPEC_CHARACTERS_OFF)));
		}
		if (xattribs.exists(XML_LOCK_TABLE_ATTRIBUTE)) {
		    mysqlDataWriter.setLockTable(xattribs.getBoolean(XML_LOCK_TABLE_ATTRIBUTE));
		}
		if (xattribs.exists(XML_IGNORE_ROWS_ATTRIBUTE)) {
			mysqlDataWriter.setIgnoreRows(xattribs.getInteger(XML_IGNORE_ROWS_ATTRIBUTE));
		}
		if (xattribs.exists(XML_PARAMETERS_ATTRIBUTE)) {
			mysqlDataWriter.setParameters(xattribs.getString(XML_PARAMETERS_ATTRIBUTE));
		}

		return mysqlDataWriter;
	}

	/** Description of the Method */
	@Override
	public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		super.checkConfig(status);

		if (!checkInputPorts(status, 0, 1) || !checkOutputPorts(status, 0, 1)) {
            return status;
        }
		
		isDataReadFromPort = !getInPorts().isEmpty();
		isDataReadDirectlyFromFile = !isDataReadFromPort && !StringUtils.isEmpty(dataURL);
        isDataWrittenToPort = !getOutPorts().isEmpty();
        properties = parseParameters(parameters);

        //---CheckParams
        
        if (StringUtils.isEmpty(loadUtilityPath)) {
        	status.add(new ConfigurationProblem(StringUtils.quote(XML_MYSQL_PATH_ATTRIBUTE)	+ " attribute have to be set.",
					Severity.ERROR, this, Priority.HIGH, XML_MYSQL_PATH_ATTRIBUTE));
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
				if (!isDataReadFromPort && !fileUrlExists(dataURL)) {
					status.add(new ConfigurationProblem("Input port or " + StringUtils.quote(XML_FILE_URL_ATTRIBUTE) + " attribute or " +
							StringUtils.quote(XML_COMMAND_URL_ATTRIBUTE) + " attribute have to be specified and specified file must exist.",
							Severity.ERROR, this, Priority.NORMAL));
				}
				// If commandURL points to a file that does not exist and dataURL is empty, the component won't run correctly
				// because it doesn't know what data file is specified in the command script
				if (commandURL != null && StringUtils.isEmpty(dataURL)) {
					status.add(new ConfigurationProblem("The " + StringUtils.quote(XML_FILE_URL_ATTRIBUTE) + " attribute "
				            + "has to be set because the " + StringUtils.quote(XML_COMMAND_URL_ATTRIBUTE) + " attribute is set!",
							Severity.ERROR, this, Priority.NORMAL, XML_FILE_URL_ATTRIBUTE));
				}
			}
			
		} catch (ComponentNotReadyException e) {
			status.add(new ConfigurationProblem(ExceptionUtils.getMessage(e),	Severity.ERROR, this, Priority.NORMAL));
		}
		if (ignoreRows != UNUSED_INT && ignoreRows < 0) {
			status.add(new ConfigurationProblem(XML_IGNORE_ROWS_ATTRIBUTE + " mustn't be less than 0.",	Severity.ERROR,
					this, Priority.NORMAL, XML_IGNORE_ROWS_ATTRIBUTE));
		}
		
		// check combination
		if (properties.containsKey(LOAD_FIELDS_IS_OPTIONALLY_ENCLOSED_PARAM) && !properties.containsKey(LOAD_FIELDS_ENCLOSED_BY_PARAM)) {
			status.add(new ConfigurationProblem("Attribute " + StringUtils.quote(LOAD_FIELDS_IS_OPTIONALLY_ENCLOSED_PARAM)
					+ " is ignored because it has to be used in combination with " + StringUtils.quote(LOAD_FIELDS_ENCLOSED_BY_PARAM) +
					" attribute.",	Severity.WARNING, this, Priority.NORMAL, LOAD_FIELDS_IS_OPTIONALLY_ENCLOSED_PARAM));
		}

		// if any output port is connected, MYSQL_SHOW_WARNINGS_SWITCH parameter must be used
		// MYSQL_SHOW_WARNINGS_SWITCH is used when MYSQL_SHOW_WARNINGS_PARAM isn't defined
		// or when MYSQL_SHOW_WARNINGS_PARAM=true
		if (isDataWrittenToPort) {
			if (properties.containsKey(MYSQL_SHOW_WARNINGS_PARAM) &&
					"false".equalsIgnoreCase(properties.getProperty(MYSQL_SHOW_WARNINGS_PARAM))) {
				status.add(new ConfigurationProblem("If any output port is connected, " + StringUtils.quote(MYSQL_SHOW_WARNINGS_PARAM) + 
						" parameter mustn't equals false.",	Severity.WARNING, this, Priority.NORMAL, MYSQL_SHOW_WARNINGS_PARAM));
			}
		}

		// report on ignoring some attributes
		if (isDataReadFromPort) {
			if (properties.containsKey(LOAD_FIELDS_ENCLOSED_BY_PARAM)) {
				status.add(new ConfigurationProblem("Attribute " + StringUtils.quote(LOAD_FIELDS_ENCLOSED_BY_PARAM)
						+ " is ignored because it is used only when data is read directly from file.",	Severity.WARNING,
						this, Priority.NORMAL, LOAD_FIELDS_ENCLOSED_BY_PARAM));
			}
			if (properties.containsKey(LOAD_FIELDS_IS_OPTIONALLY_ENCLOSED_PARAM)) {
				status.add(new ConfigurationProblem("Attribute " + StringUtils.quote(LOAD_FIELDS_IS_OPTIONALLY_ENCLOSED_PARAM)
						+ " is ignored because it is used only when data is read directly from file.",	Severity.WARNING,
						this, Priority.NORMAL, LOAD_FIELDS_IS_OPTIONALLY_ENCLOSED_PARAM));
			}
			if (properties.containsKey(LOAD_FIELDS_ESCAPED_BY_PARAM)) {
				status.add(new ConfigurationProblem("Attribute " + StringUtils.quote(LOAD_FIELDS_ESCAPED_BY_PARAM)
						+ " is ignored because it is used only when data is read directly from file.",	Severity.WARNING,
						this, Priority.NORMAL, LOAD_FIELDS_ESCAPED_BY_PARAM));
			}
		}
		
		//Check creation of control and data file
		try {
			initDataFile();
			createCommandFile();
		} catch (ComponentNotReadyException e) {
			status.add(new ConfigurationProblem(ExceptionUtils.getMessage(e),	Severity.ERROR, this, Priority.NORMAL));
		}
		deleteTempFiles();
		return status;
	}

	@Override
	public String getType() {
		return COMPONENT_TYPE;
	}

    public void setLockTable(boolean lockTable) {
        this.lockTable = lockTable;
    }

    public void setIgnoreRows(int ignoreRows) {
		this.ignoreRows = ignoreRows;
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
			// params for mysql client
			MYSQL_SKIP_AUTO_REHASH_PARAM,
			MYSQL_CHARACTER_SETS_DIR_PARAM,
			MYSQL_COMPRESS_PARAM,
			MYSQL_DEFAULT_CHARACTER_SET_PARAM,
			MYSQL_FORCE_PARAM,
			MYSQL_NO_BEEP_PARAM,
			MYSQL_PORT_PARAM,
			MYSQL_PROTOCOL_PARAM,
			MYSQL_RECONNECT_PARAM,
			MYSQL_SECURE_AUTH_PARAM,
			MYSQL_SHOW_WARNINGS_PARAM,
			MYSQL_SILENT_PARAM,
			MYSQL_SOCKET_PARAM,
			MYSQL_SSL_PARAM,
	
            // params (in fact session variables) for LOAD DATA INFILE statement
            LOAD_UNIQUE_CHECKS_PARAM,
            LOAD_FOREIGN_KEY_CHECKS_PARAM,
            LOAD_SQL_LOG_BIN_PARAM,

            // params for LOAD DATA INFILE statement
			LOAD_LOCAL_PARAM,
			LOAD_LOW_PRIORITY_PARAM,
			LOAD_CONCURRENT_PARAM,
			LOAD_REPLACE_PARAM,
			LOAD_IGNORE_PARAM,
			LOAD_FIELDS_ENCLOSED_BY_PARAM,
			LOAD_FIELDS_IS_OPTIONALLY_ENCLOSED_PARAM,
			LOAD_FIELDS_ESCAPED_BY_PARAM,
			LOAD_LINES_STARTING_BY_PARAM,
			LOAD_RECORD_DELIMITER_PARAM,
			LOAD_COLUMNS_PARAM
		};
	}

	/**
	 * Class for reading and parsing data from input stream, which is supposed 
	 * to be connected to process' output, and sends them to specified output port.
	 * 
	 * @see org.jetel.util.exec.ProcBox
	 * @see org.jetel.util.exec.DataConsumer
	 * @author Miroslav Haupt (Mirek.Haupt@javlinconsulting.cz) 
	 * (c) Javlin Consulting (www.javlinconsulting.cz)
	 * @since 17.10.2007
	 */
	private class MysqlPortDataConsumer implements DataConsumer {
		private BufferedReader reader; // read from input stream (=output stream of mysql process)
		private DataRecord errRecord = null;
		private OutputPort errPort = null;
		private DataRecordMetadata errMetadata; // format as output port
		private Log logger = LogFactory.getLog(MysqlPortDataConsumer.class);

		private final static int ROW_NUMBER_FIELD_NO = 0;
		private final static int COLUMN_NAME_FIELD_NO = 1;
		private final static int ERR_MSG_FIELD_NO = 2;
		private final static int NUMBER_OF_FIELDS = 3;

		private String strBadRowPattern = "(.+) for column '(\\w+)' at row (\\d+)";
		private Matcher badRowMatcher;

		/**
		 * @param port Output port receiving consumed data.
		 * @throws ComponentNotReadyException
		 */
		public MysqlPortDataConsumer(OutputPort errPort) throws ComponentNotReadyException {
			if (errPort == null) {
				throw new ComponentNotReadyException("Output port wasn't found.");
			}

			this.errPort = errPort;

			errMetadata = errPort.getMetadata();
			if (errMetadata == null) {
				throw new ComponentNotReadyException("Output port hasn't assigned metadata.");
			}

			checkErrPortMetadata();

			errRecord = DataRecordFactory.newRecord(errMetadata);
			errRecord.init();

			Pattern badRowPattern = Pattern.compile(strBadRowPattern);
			badRowMatcher = badRowPattern.matcher("");
		}

		/**
		 * @see org.jetel.util.exec.DataConsumer
		 */
		@Override
		public void setInput(InputStream stream) {
			reader = new BufferedReader(new InputStreamReader(stream));
		}

		/**
		 * Example of bad rows in stream: 
		 * Warning (Code 1264): Out of range value adjusted for column 'datetimeT' at row 1
		 * Warning (Code 1366): Incorrect integer value: 's' for column 'smallT' at row 2
		 * Note (Code 1265): Data truncated for column 'decT' at row 3
		 * 
		 * @see org.jetel.util.exec.DataConsumer
		 */
		@Override
		public boolean consume() throws JetelException {
			try {
				String line;
				if ((line = readLine()) == null) {
					return false;
				}

				badRowMatcher.reset(line);
				if (badRowMatcher.find()) {
					int rowNumber = Integer.valueOf(badRowMatcher.group(3));
					String columnName = badRowMatcher.group(2);
					String errMsg = badRowMatcher.group(1);

					setErrRecord(errRecord, rowNumber, columnName, errMsg);
					errPort.writeRecord(errRecord);
				}
			} catch (Exception e) {
				close();
				throw new JetelException("Error while writing output record", e);
			}

			SynchronizeUtils.cloverYield();
			return true;
		}

		/**
		 * Read line by reader and write it by logger and return it.
		 * 
		 * @return read line
		 * @throws IOException
		 */
		private String readLine() throws IOException {
			String line = reader.readLine();
			if (!StringUtils.isEmpty(line)) {
				logger.debug(line);
			}
			return line;
		}

		/**
		 * Set value in errRecord.
		 * 
		 * @param errRecord destination record
		 * @param rowNumber number of bad row
		 * @param columnName column's name of bad row
		 * @param errMsg error message
		 * @return destination record
		 */
		private DataRecord setErrRecord(DataRecord errRecord, int rowNumber, String columnName, String errMsg) {
			errRecord.reset();
			errRecord.getField(ROW_NUMBER_FIELD_NO).setValue(rowNumber);
			errRecord.getField(COLUMN_NAME_FIELD_NO).setValue(columnName);
			errRecord.getField(ERR_MSG_FIELD_NO).setValue(errMsg);

			return errRecord;
		}

		/**
		 * check metadata at error port if metadata isn't correct then throws ComponentNotReadyException
		 * 
		 * @throws ComponentNotReadyException when metadata isn't correct
		 */
		private void checkErrPortMetadata() throws ComponentNotReadyException {
			// check number of fields
			if (errMetadata.getNumFields() != NUMBER_OF_FIELDS) {
				throw new ComponentNotReadyException("Number of fields of " + 
						StringUtils.quote(errMetadata.getName()) +
						" isn't equal " + NUMBER_OF_FIELDS + ".");
			}

			// check if first field of errMetadata is integer - rowNumber
			if (errMetadata.getFieldType(ROW_NUMBER_FIELD_NO) != DataFieldMetadata.INTEGER_FIELD) {
				throw new ComponentNotReadyException("First field of " +  StringUtils.quote(errMetadata.getName()) +  
						" has different type from integer.");
			}
			
			// check if second field of errMetadata is string - columnName
			if (errMetadata.getFieldType(COLUMN_NAME_FIELD_NO) != DataFieldMetadata.STRING_FIELD) {
				throw new ComponentNotReadyException("Second field of " +  StringUtils.quote(errMetadata.getName()) +  
						" has different type from string.");
			}
			
			// check if third field of errMetadata is string - errMsg
			if (errMetadata.getFieldType(ERR_MSG_FIELD_NO) != DataFieldMetadata.STRING_FIELD) {
				throw new ComponentNotReadyException("Third field of " +  StringUtils.quote(errMetadata.getName()) +  
						" has different type from string.");
			}
		}

		/**
		 * @see org.jetel.util.exec.DataConsumer
		 */
		@Override
		public void close() {
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