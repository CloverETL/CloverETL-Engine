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

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.channels.Channels;
import java.util.Iterator;
import java.util.Properties;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.formatter.DataFormatter;
import org.jetel.data.parser.DelimitedDataParser;
import org.jetel.data.parser.Parser;
import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.JetelException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPort;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.CommandBuilder;
import org.jetel.util.ComponentXMLAttributes;
import org.jetel.util.StringUtils;
import org.jetel.util.exec.DataConsumer;
import org.jetel.util.exec.LoggerDataConsumer;
import org.jetel.util.exec.PortDataConsumer;
import org.jetel.util.exec.ProcBox;
import org.w3c.dom.Element;

/**
 *  <h3>MsSql data writer</h3>
 *
 * <!-- All records from input port:0 are loaded into mssql database. Connection to database is not through JDBC driver,
 * this component uses the bcp utility for this purpose. Bad rows send to output port 0.-->
 *
 * <table border="1">
 *  <th>Component:</th>
 * <tr><td><h4><i>Name:</i></h4></td>
 * <td>MsSql data writer</td></tr>
 * <tr><td><h4><i>Category:</i></h4></td>
 * <td></td></tr>
 * <tr><td><h4><i>Description:</i></h4></td>
 * <td>This component loads data to an MsSql database using the bcp utility. 
 * It creates a temporary file with bcp commands depending on input parameters. Data are read from given 
 * input file or from the input port and loaded to database.<br>
 * To use this component MsSql client must be installed and configured on the local host.
 * </td></tr>
 * <tr><td><h4><i>Inputs:</i></h4></td>
 * <td>[0] - input records, optional</td></tr>
 * <tr><td><h4><i>Outputs:</i></h4></td>
 * <td>[0] - optionally one output port defined/connected - rejected records.
 * Metadata on this port must have the same type of field as input metadata, except otput metadata has a additional fields with row number, column number and error message.
 * First field is row number (integer), second is column number (integer); third is error message (string) and other field is shift.
 * </td></tr>
 * <tr><td><h4><i>Comment:</i></h4></td>
 * <td></td></tr>
 * </table>
 *  <br>
 *  <table border="1">
 *  <th>XML attributes:</th>
 *  <tr><td><b>type</b></td><td>"MS_SQL_DATA_WRITER"</td></tr>
 *  <tr><td><b>id</b></td><td>component identification</td></tr>
 *  <tr><td><b>dbLoaderPath</b></td><td>path to bcp utility</td></tr>
 *  <tr><td><b>database</b><br><i>optional</i></td><td>Is the name of the database in which the specified table or view resides. 
 *  If not specified, this is the default database for the user.<br/>
 *  example: //server_name/directory_on_server/database_name</td></tr>
 *  <tr><td><b>owner</b><br><i>optional</i></td><td>Is the name of the owner of the table or view. 
 *  owner is optional if the user performing the operation owns the specified table or view. 
 *  If owner is not specified and the user performing the operation does not own the specified table or view, 
 *  SQL Server 2005 returns an error message, and the operation is canceled.</td></tr>
 *  <tr><td><b>table</b><br><i>optional</i></td><td>Is the name of the destination table</br>Note: table or view must be set</td></tr>
 *  <tr><td><b>view</b><br><i>optional</i></td><td>Is the name of the destination view. 
 *  Only views in which all columns refer to the same table can be used as destination views. 
 *  </br>Note: table or view must be set</td></tr>
 *    <tr><td><b>fileURL</b><br><i>optional</i></td><td>path to the data input file. If there is not connected 
 *  input port data have to be in external file. If there is connected input port this attribute is ignored.</br>
 *  The path can have from 1 through 255 characters. The data file can contain a maximum of 2,147,483,647 rows.</td></tr>
 *  <tr><td><b>parameters</b><br><i>optional</i></td><td>All possible additional parameters 
 *  which can be passed on to bcp utility (See  <a href="http://technet.microsoft.com/en-us/library/ms162802.aspx">
 * bcp utility</a>). Parameters, in form <i>key=value</i> (or <i>key</i> - 
 * interpreted as <i>key=true</i>, if possible value are only "true" or "false") has to be 
 * separated by :;| {colon, semicolon, pipe}. If in parameter value occurs one of :;|, value 
 * has to be double quoted.<br><b>Load parameters</b><table>
 * <tr><td>maxErrors</td><td>Specifies the maximum number of syntax errors that can occur before the bcp operation is canceled. 
 * A syntax error implies a data conversion error to the target data type. 
 * The max_errors total excludes any errors that can be detected only at the server, such as constraint violations.
 * A row that cannot be copied by the bcp utility is ignored and is counted as one error. If this option is not included, the default is 10.</td></tr>
 * <tr><td>formatFile</td><td>Specifies the full path of a format file. The meaning of this option depends on the environment in which it is used, as follows:
* If -f is used with the format option, the specified format_file is created for the specified table or view. 
* To create an XML format file, also specify the -x option. 
* For more information, see  <a href="http://technet.microsoft.com/en-us/library/ms191516.aspx"> Creating a Format File</a>.</td></tr>
 * <tr><td>generateXmlFormatFile</td><td>Used with the format and -f format_file options, generates an XML-based format file instead of the default non-XML format file. 
 * The -x does not work when importing or exporting data. 
 * It generates an error if used without both format and -f format_file.</td></tr>
 * <tr><td>errFile</td><td>Specifies the full path of an error file used to store any rows that the bcp utility cannot transfer from the file to the database. 
 * Error messages from the bcp command go to the workstation of the user. 
 * If this option is not used, an error file is not created.</td></tr>
 * <tr><td>firstRow</td><td>Specifies the number of the first row to export from a table or import from a data file. 
 * This parameter requires a value greater than (>) 0 but less than (<) or equal to (=) the total number rows. 
 * In the absence of this parameter, the default is the first row of the file.</td></tr>
 * <tr><td>lastRow</td><td>Specifies the number of the last row to export from a table or import from a data file. 
 * This parameter requires a value greater than (>) 0 but less than (<) or equal to (=) the number of the last row. 
 * In the absence of this parameter, the default is the last row of the file.</td></tr>
 * <tr><td>batchSize</td><td>Specifies the number of rows per batch of imported data. 
 * Each batch is imported and logged as a separate transaction that imports the whole batch before being committed. 
 * By default, all the rows in the data file are imported as one batch. 
 * To distribute the rows among multiple batches, specify a batch_size that is smaller than the number of rows in the data file. 
 * If the transaction for any batch fails, only insertions from the current batch are rolled back. 
 * Batches already imported by committed transactions are unaffected by a later failure.</br>
 * Do not use this option in conjunction with the h"ROWS_PER_BATCH = bb" option.</br>
 * For more information, see <a href="http://technet.microsoft.com/en-us/library/ms188267.aspx"> Managing Batches for Bulk Import</a>.</td></tr>
 * <tr><td>nativeType</td><td>Performs the bulk-copy operation using the native (database) data types of the data. 
 * This option does not prompt for each field; it uses the native values.</br>
 * For more information, see <a href="http://technet.microsoft.com/en-us/library/ms191232.aspx">Using Native Format to Import or Export Data</a>.</td></tr>
 * <tr><td>characterType</td><td>Performs the operation using a character data type. 
 * This option does not prompt for each field; it uses char as the storage type, without prefixes and with \t (tab character) as the field separator and \r\n (newline character) as the row terminator.</br>
 * For more information, see <a href="http://technet.microsoft.com/en-us/library/ms190919.aspx">Using Character Format to Import or Export Data</a>.</td></tr>
 * <tr><td>keepNonTextNative</td><td>Performs the bulk-copy operation using the native (database) data types of the data for noncharacter data, and Unicode characters for character data. 
 * This option offers a higher performance alternative to the -w option, and is intended for transferring data from one instance of SQL Server to another using a data file. 
 * It does not prompt for each field. Use this option when you are transferring data that contains ANSI extended characters and you want to take advantage of the performance of native mode. 
 * -N cannot be used with SQL Server 6.5 or earlier versions.</br>
 *  For more information, see <a href="http://technet.microsoft.com/en-us/library/ms189941.aspx">Using Unicode Native Format to Import or Export Data</a>.</td></tr>
 * <tr><td>wideCharacterType</td><td>Performs the bulk copy operation using Unicode characters. 
 * This option does not prompt for each field; it uses nchar as the storage type, no prefixes, \t (tab character) as the field separator, and \n (newline character) as the row terminator. 
 * This option cannot be used with SQL Server 6.5 or earlier versions.</br>
 * For more information, see <a href="http://technet.microsoft.com/en-us/library/ms188289.aspx">Using Unicode Character Format to Import or Export Data</a>.</td></tr>
 * <tr><td>fileFormatVersion</td><td>Performs the bulk-copy operation using data types from an earlier version of SQL Server. 
 * This option does not prompt for each field; it uses the default values. 
 * For example, to bulk copy date formats supported by the bcp utility provided with SQL Server 6.5 (but no longer supported by ODBC) into SQL Server 2005, use the -V 65 parameter.</br>
 * Important:</br>
 * When data is bulk exported from SQL Server into a data file, the bcp utility does not generate SQL Server 6.0 or SQL Server 6.5 date formats for any datetime or smalldatetime data, even if -V is specified. 
 * Dates are always written in ODBC format. Additionally, null values in bit columns are written as the value 0 because SQL Server 6.5 and earlier versions do not support nullable bit data.</td></tr>
 * <tr><td>quotedIdentifier</td><td>Executes the SET QUOTED_IDENTIFIERS ON statement in the connection between the bcp utility and an instance of SQL Server. 
 * Use this option to specify a database, owner, table, or view name that contains a space or a single quotation mark. 
 * Enclose the entire three-part table or view name in quotation marks ("").</br>
 * To specify a database name that contains a space or single quotation mark, you must use the q option.</br>
 * For more information, see Remarks later in this topic.</td></tr>
 * <tr><td>codePageSpecifier</td><td>Supported for compatibility with early versions of SQL Server. 
 * For SQL Server 7.0 and later, Microsoft recommends that you specify a collation name for each column in a format file.
 * Specifies the code page of the data in the data file. code_page is relevant only if the data contains char, varchar, or text columns with character values greater than 127 or less than 32.</br>
 * <table border="1"><tr><td><b>Code page value</b></td><td><b>description</b></td></tr>
 * <tr><td>ACP</td><td>ANSI/MicrosoftWindows (ISO 1252).</td></tr>
 * <tr><td>OEM</td><td>Default code page used by the client. This is the default code page used if -C is not specified.</td></tr>
 * <tr><td>RAW</td><td>No conversion from one code page to another occurs. This is the fastest option because no conversion occurs.</td></tr>
 * <tr><td><i>code_page</i></td><td>Specific code page number; for example, 850.</td></tr>
 * </table>
 * For more information, see <a href="http://technet.microsoft.com/en-us/library/ms190657.aspx">Copying Data Between Different Collations</a>.</td></tr>
 * <tr><td>fieldTerminator</td><td>Specifies the field terminator. The default is \t (tab character). 
 * Use this parameter to override the default field terminator. 
 * For more information, see <a href="http://technet.microsoft.com/en-us/library/ms191485.aspx">Specifying Field and Row Terminators</a>.</td></tr>
 * <tr><td>rowTerminator</td><td>Specifies the row terminator. The default is \n (newline character). 
 * Use this parameter to override the default row terminator. 
 * For more information, see <a href="http://technet.microsoft.com/en-us/library/ms191485.aspx">Specifying Field and Row Terminators</a>.</td></tr>
 * <tr><td>inputFile</td><td>Specifies the name of a response file, containing the responses to the command prompt questions for each data field when 
 * a bulk copy is being performed using interactive mode (-n, -c, -w, -6, or -N not specified).</td></tr>
 * <tr><td>outputFile</td><td>Specifies the name of a file that receives output redirected from the command prompt.</td></tr>
 * <tr><td>packetSize</td><td>Specifies the number of bytes, per network packet, sent to and from the server. 
 * A server configuration option can be set by using SQL Server Management Studio (or the sp_configure system stored procedure). However, the server 
 * configuration option can be overridden on an individual basis by using this option. packet_size can be from 4096 to 65535 bytes; the default is 4096.</br>
 * Increased packet size can enhance performance of bulk-copy operations. 
 * If a larger packet is requested but cannot be granted, the default is used. 
 * The performance statistics generated by the bcp utility show the packet size used.</td></tr>
 * <tr><td>serverName</td><td>Specifies the instance of SQL Server to which to connect. If no server is specified, the bcp utility connects to the default instance of SQL Server on the local computer. 
 * This option is required when a bcp command is run from a remote computer on the network or a local named instance. 
 * To connect to the default instance of SQL Server on a server, specify only server_name. 
 * To connect to a named instance of SQL Server 2005, specify server_name\instance_name.</td></tr>
 * <tr><td>userName</td><td>Specifies the login ID used to connect to SQL Server.</br>
 * Security Note:</br>
 * When the bcp utility is connecting to SQL Server with a trusted connection using integrated security, use the -T option (trusted connection) instead of the user name and password combination.</td></tr>
 * <tr><td>password</td><td>Specifies the password for the login ID. If this option is not used, the bcp command prompts for a password. 
 * If this option is used at the end of the command prompt without a password, bcp uses the default password (NULL).</td></tr>
 * <tr><td>trustedConnection</td><td>Specifies that the bcp utility connects to SQL Server with a trusted connection using integrated security. 
 * The security credentials of the network user, login_id, and password are not required. If T is not specified, you need to specify U and P to successfully log in.</td></tr>
 * <tr><td>version</td><td>Reports the bcp utility version number and copyright.</td></tr>
 * <tr><td>regionalEnable</td><td>Specifies that currency, date, and time data is bulk copied into SQL Server using the regional format defined for the locale setting of the client computer. 
 * By default, regional settings are ignored.</td></tr>
 * <tr><td>keepNullValues</td><td>Specifies that empty columns should retain a null value during the operation, rather than have any default values for the columns inserted. 
 * For more information, see <a href="http://technet.microsoft.com/en-us/library/ms187887.aspx">Keeping Nulls or Using Default Values During Bulk Import</a>.</td></tr>
 * <tr><td>keepIdentityValues</td><td>Specifies that identity value or values in the imported data file are to be used for the identity column. 
 * If -E is not given, the identity values for this column in the data file being imported are ignored, and SQL Server 2005 automatically 
 * assigns unique values based on the seed and increment values specified during table creation.</br>
 * If the data file does not contain values for the identity column in the table or view, use a format file to specify that the identity column in the table or view should be skipped when importing data; 
 * SQL Server 2005 automatically assigns unique values for the column. 
 * For more information, see <a href="http://technet.microsoft.com/en-us/library/ms176057.aspx">DBCC CHECKIDENT (Transact-SQL)</a>.</br>
 * The -E option has a special permissions requirement. For more information, see "Remarks" later in this topic.</br>
 * For more information, see about keeping identify values see <a href="http://technet.microsoft.com/en-us/library/ms186335.aspx">Keeping Identity Values When Bulk Importing Data</a>.</td></tr>
 * <tr><td>hint</td><td>Specifies the hint or hints to be used during a bulk import of data into a table or view. 
 * This option cannot be used when bulk copying data into SQL Server 6.x or earlier.</td></tr>
 * </table></td></tr>
 *  </table>
 *
 *	<h4>Example:</h4>
 *  Reading data from file:
 *  <pre>&lt;Node 
 *	dbLoaderPath="bcp" 
 *	database="test"
 *	owner="dbo" 
 *	table="test" 
 *	fileURL="C:\MSSQL_data\graph\in1.bcp" 
 *	parameters="characterType|trustedConnection|codePageSpecifier=ACP|errFile=C&quot;:&quot;\MSSQL_data\graph\err.bcp"
 *	id="MS_SQL_DATA_WRITER0"
 *	type="MS_SQL_DATA_WRITER"
 *  /&gt;
 *  </pre>
 *  
 * @author      Miroslav Haupt (Mirek.Haupt@javlinconsulting.cz)<br>
 *				(c) Javlin Consulting (www.javlinconsulting.cz)
 * @see         org.jetel.graph.TransformationGraph
 * @see         org.jetel.graph.Node
 * @see         org.jetel.graph.Edge
 * @since 		29.8.2007
 */
public class MsSqlDataWriter extends Node {

	private static Log logger = LogFactory.getLog(MsSqlDataWriter.class);

    /**  Description of the Field */
	private final static String XML_DB_LOADER_PATH_ATTRIBUTE = "dbLoaderPath";
    private final static String XML_DATABASE_ATTRIBUTE = "database";
    private final static String XML_OWNER_ATTRIBUTE = "owner";
    private final static String XML_TABLE_ATTRIBUTE = "table";
    private final static String XML_VIEW_ATTRIBUTE = "view";
    private final static String XML_FILE_URL_ATTRIBUTE = "fileURL";
    private final static String XML_PARAMETERS_ATTRIBUTE = "parameters";
    
    private final static String MS_SQL_MAX_ERRORS_PARAM = "maxErrors";
    private final static char MS_SQL_MAX_ERRORS_SWITCH = 'm';
    private final static String MS_SQL_FORMAT_FILE_PARAM = "formatFile";
    private final static char MS_SQL_FORMAT_FILE_SWITCH = 'f';
    private final static String MS_SQL_GENERATE_XML_FORMAT_FILE_PARAM = "generateXmlFormatFile";
    private final static char MS_SQL_GENERATE_XML_FORMAT_FILE_SWITCH = 'x';
    private final static String MS_SQL_ERR_FILE_PARAM = "errFile";
    private final static char MS_SQL_ERR_FILE_SWITCH = 'e';
    private final static String MS_SQL_FIRST_ROW_PARAM = "firstRow";
    private final static char MS_SQL_FIRST_ROW_SWITCH = 'F';
    private final static String MS_SQL_LAST_ROW_PARAM = "lastRow";
    private final static char MS_SQL_LAST_ROW_SWITCH = 'L';
    private final static String MS_SQL_BATCH_SIZE_PARAM = "batchSize";
    private final static char MS_SQL_BATCH_SIZE_SWITCH = 'b';
    private final static String MS_SQL_NATIVE_TYPE_PARAM = "nativeType";
    private final static char MS_SQL_NATIVE_TYPE_SWITCH = 'n';
    private final static String MS_SQL_CHARACTER_TYPE_PARAM = "characterType";
    private final static char MS_SQL_CHARACTER_TYPE_SWITCH = 'c';
    private final static String MS_SQL_WIDE_CHARACTER_TYPE_PARAM = "wideCharacterType";
    private final static char MS_SQL_WIDE_CHARACTER_TYPE_SWITCH = 'w';
    private final static String MS_SQL_KEEP_NON_TEXT_NATIVE_PARAM = "keepNonTextNative";
    private final static char MS_SQL_KEEP_NON_TEXT_NATIVE_SWITCH = 'N';
    private final static String MS_SQL_FILE_FORMAT_VERSION_PARAM = "fileFormatVersion";
    private final static char MS_SQL_FILE_FORMAT_VERSION_SWITCH = 'V';
    private final static String MS_SQL_QUOTED_IDENTIFIER_PARAM = "quotedIdentifier";
    private final static char MS_SQL_QUOTED_IDENTIFIER_SWITCH = 'q';
    private final static String MS_SQL_CODE_PAGE_SPECIFIER_PARAM = "codePageSpecifier";
    private final static char MS_SQL_CODE_PAGE_SPECIFIER_SWITCH = 'C';
    private final static String MS_SQL_FIELD_TERMINATOR_PARAM = "fieldTerminator";
    private final static char MS_SQL_FIELD_TERMINATOR_SWITCH = 't';
    private final static String MS_SQL_ROW_TERMINATOR_PARAM = "rowTerminator";
    private final static char MS_SQL_ROW_TERMINATOR_SWITCH = 'r';
    private final static String MS_SQL_INPUT_FILE_PARAM = "inputFile";
    private final static char MS_SQL_INPUT_FILE_SWITCH = 'i';
    private final static String MS_SQL_OUTPUT_FILE_PARAM = "outputFile";
    private final static char MS_SQL_OUTPUT_FILE_SWITCH = 'o';
    private final static String MS_SQL_PACKET_SIZE_PARAM = "packetSize";
    private final static char MS_SQL_PACKET_SIZE_SWITCH = 'a';
    private final static String MS_SQL_SERVER_NAME_PARAM = "serverName";
    private final static char MS_SQL_SERVER_NAME_SWITCH = 'S';
    private final static String MS_SQL_USER_NAME_PARAM = "userName";
    private final static char MS_SQL_USER_NAME_SWITCH = 'U';
    private final static String MS_SQL_PASSWORD_PARAM = "password";
    private final static char MS_SQL_PASSWORD_SWITCH = 'P';
    private final static String MS_SQL_TRUSTED_CONNECTION_PARAM = "trustedConnection";
    private final static char MS_SQL_TRUSTED_CONNECTION_SWITCH = 'T';
    private final static String MS_SQL_VERSION_PARAM = "version";
    private final static char MS_SQL_VERSION_SWITCH = 'v';
    private final static String MS_SQL_REGIONAL_ENABLE_PARAM = "regionalEnable";
    private final static char MS_SQL_REGIONAL_ENABLE_SWITCH = 'R';
    private final static String MS_SQL_KEEP_NULL_VALUES_PARAM = "keepNullValues";
    private final static char MS_SQL_KEEP_NULL_VALUES_SWITCH = 'k';
    private final static String MS_SQL_KEEP_IDENTITY_VALUES_PARAM = "keepIdentityValues";
    private final static char MS_SQL_KEEP_IDENTITY_VALUES_SWITCH = 'E';
    private final static String MS_SQL_LOAD_HINTS_PARAM = "loadHints";
    private final static char MS_SQL_LOAD_HINTS_SWITCH = 'h';
    
    public final static String COMPONENT_TYPE = "MS_SQL_DATA_WRITER";
    private final static int READ_FROM_PORT = 0;
    private final static int WRITE_TO_PORT = 0;	//port for write bad record
    private final static char EQUAL_CHAR = '=';
    
    private final static String DATA_FILE_NAME_PREFIX = "data";
    private final static String DATA_FILE_NAME_SUFFIX = ".dat";
    private final static String ERROR_FILE_NAME_PREFIX = "error";
    private final static String ERROR_FILE_NAME_SUFFIX = ".log";
    private final static File TMP_DIR = new File(".");
    private final static String CHARSET_NAME = "UTF-8";
    private final static String DEFAULT_FIELD_DELIMITER = "\t"; // according bcp
    private final static String DEFAULT_ROW_DELIMITER = "\n"; // according bcp
    
    private final static String DEFAULT_DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";
    private final static String DEFAULT_DATE_FORMAT = "MM/dd/yyyy"; 
    private final static String DEFAULT_TIME_FORMAT = DEFAULT_DATETIME_FORMAT;

    // variables for bcp's command
	private String dbLoaderPath;
    private String database;
    private String owner;
    private String table;
    private String view;
    private String inDataFileName; // fileUrl from XML - data file that is used when no input port is connected
    private String parameters;
    private String errFileName = null; // errFile insert by user or tmpErrFile for parsing bad rows
    private boolean isErrFileFromUser; // true if errFile was inserted by user; false when errFile is tmpErrFile
    
    private Properties properties = new Properties();
    private DataConsumer consumer; // consume data from out stream of bcp
    private DataConsumer errConsumer; // consume data from err stream of bcp - write them to by logger
    private MsSqlBadRowReaderWriter badRowReaderWriter;
    
    private String tmpDataFileName; // file that is used for exchange data between clover and bcp
    private DataRecordMetadata dbMetadata; // it correspond to bcp input format
    private DataFormatter formatter; // format data to bcp format and write them to dataFileName 
    private String commandLine; // command line of bcp 
  
    /**
     * true - data is read from port;
     * false - data is read from file directly by bcp utility
     */
    private boolean isDataReadFromPort;
    
    /**
     * true - bad rows is written to out port;
     * false - bad rows isn't written to anywhere
     */
    private boolean isDataWrittenToPort;
    
    
    /**
     * Constructor for the MsSqlDataWriter object
     *
     * @param  id  Description of the Parameter
     */
    public MsSqlDataWriter(String id, String dbLoaderPath, String database) { 
        super(id);
        this.dbLoaderPath = dbLoaderPath;
        this.database = database;
    }
    
    /**
     *  Main processing method for the MsSqlDataWriter object
     *
     * @since    April 4, 2002
     */
    public Result execute() throws Exception {
        ProcBox box;
        int processExitValue = 0;

        if (isDataReadFromPort) {
        	// temp file is used for exchange data
        	formatter.setDataTarget(Channels.newChannel(new FileOutputStream(tmpDataFileName)));
        	readFromPortAndWriteByFormatter();
        	
            box = createProcBox(null);
    		
    		processExitValue = box.join();
        } else {
        	processExitValue = readDataDirectlyFromFile();
        }
        
        if (processExitValue != 0) {
        	throw new JetelException("bcp utility has failed.");
		}
        
        if (isDataWrittenToPort) {
        	badRowReaderWriter.run();
        }
        
        return runIt ? Result.FINISHED_OK : Result.ABORTED;
    }
    
    /**
     * This method reads incoming data from port and sends them by formatter to bcp process.
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
	 * Call bcp process with parameters - bcp process reads data directly from file.  
	 * @return value of finished process
	 * @throws Exception
	 */
	private int readDataDirectlyFromFile() throws Exception {
        ProcBox box = createProcBox(null);
        return box.join();
	}

	/**
	 * Create instance of ProcBox.
	 * @param process running process; when process is null, default process is created
	 * @return instance of ProcBox
	 * @throws IOException
	 */
	private ProcBox createProcBox(Process process) throws IOException {
		if (process == null) {
			process = Runtime.getRuntime().exec(commandLine);			
		}
        ProcBox box = new ProcBox(process, null, consumer, errConsumer);
		return box;
	}
    
    /**
     * Create command line for process, where bcp utility is running.
     * Example: bcp dbName.tableName in data.dat -T -c
     * @return
     * @throws ComponentNotReadyException 
     */
    private String createCommandLineForDbLoader() throws ComponentNotReadyException {
    	CommandBuilder command = new CommandBuilder(dbLoaderPath + " ");
		command.setParams(properties);
    	
		if (!StringUtils.isEmpty(database)) {
			command.append(database + ".");
		}
		if (!StringUtils.isEmpty(owner)) {
			command.append(owner + ".");
		}
		if (!StringUtils.isEmpty(table)) {
			command.append(table);
		} else {
			command.append(view);
		}
		command.append(" in ");
		if (isDataReadFromPort) {
			command.append(StringUtils.quote(tmpDataFileName));
		} else {
			command.append(StringUtils.quote(inDataFileName));
		}

		command.addParameterSwitch(MS_SQL_MAX_ERRORS_PARAM, MS_SQL_MAX_ERRORS_SWITCH);
	    command.addParameterSwitch(MS_SQL_FORMAT_FILE_PARAM, MS_SQL_FORMAT_FILE_SWITCH);
	    command.addParameterBooleanSwitch(MS_SQL_GENERATE_XML_FORMAT_FILE_PARAM, MS_SQL_GENERATE_XML_FORMAT_FILE_SWITCH);
	    command.addSwitch(MS_SQL_ERR_FILE_SWITCH, errFileName);
	    command.addParameterSwitch(MS_SQL_FIRST_ROW_PARAM, MS_SQL_FIRST_ROW_SWITCH);
	    command.addParameterSwitch(MS_SQL_LAST_ROW_PARAM, MS_SQL_LAST_ROW_SWITCH);
	    command.addParameterSwitch(MS_SQL_BATCH_SIZE_PARAM, MS_SQL_BATCH_SIZE_SWITCH);
	    command.addParameterBooleanSwitch(MS_SQL_NATIVE_TYPE_PARAM, MS_SQL_NATIVE_TYPE_SWITCH);
	    command.addParameterBooleanSwitch(MS_SQL_CHARACTER_TYPE_PARAM, MS_SQL_CHARACTER_TYPE_SWITCH);
	    command.addParameterBooleanSwitch(MS_SQL_WIDE_CHARACTER_TYPE_PARAM, MS_SQL_WIDE_CHARACTER_TYPE_SWITCH);
	    command.addParameterBooleanSwitch(MS_SQL_KEEP_NON_TEXT_NATIVE_PARAM, MS_SQL_KEEP_NON_TEXT_NATIVE_SWITCH);
	    command.addParameterSwitch(MS_SQL_FILE_FORMAT_VERSION_PARAM, MS_SQL_FILE_FORMAT_VERSION_SWITCH);
	    command.addParameterBooleanSwitch(MS_SQL_QUOTED_IDENTIFIER_PARAM, MS_SQL_QUOTED_IDENTIFIER_SWITCH);
	    command.addParameterSwitch(MS_SQL_CODE_PAGE_SPECIFIER_PARAM, MS_SQL_CODE_PAGE_SPECIFIER_SWITCH);
	    command.addParameterSwitch(MS_SQL_FIELD_TERMINATOR_PARAM, MS_SQL_FIELD_TERMINATOR_SWITCH);
	    command.addParameterSwitch(MS_SQL_ROW_TERMINATOR_PARAM, MS_SQL_ROW_TERMINATOR_SWITCH);
	    command.addParameterSwitch(MS_SQL_INPUT_FILE_PARAM, MS_SQL_INPUT_FILE_SWITCH);
	    command.addParameterSwitch(MS_SQL_OUTPUT_FILE_PARAM, MS_SQL_OUTPUT_FILE_SWITCH);
	    command.addParameterSwitch(MS_SQL_PACKET_SIZE_PARAM, MS_SQL_PACKET_SIZE_SWITCH);
	    command.addParameterSwitch(MS_SQL_SERVER_NAME_PARAM, MS_SQL_SERVER_NAME_SWITCH);
	    command.addParameterSwitch(MS_SQL_USER_NAME_PARAM, MS_SQL_USER_NAME_SWITCH);
	    command.addParameterSwitch(MS_SQL_PASSWORD_PARAM, MS_SQL_PASSWORD_SWITCH);
	    command.addParameterBooleanSwitch(MS_SQL_TRUSTED_CONNECTION_PARAM, MS_SQL_TRUSTED_CONNECTION_SWITCH);
	    command.addParameterBooleanSwitch(MS_SQL_VERSION_PARAM, MS_SQL_VERSION_SWITCH);
	    command.addParameterBooleanSwitch(MS_SQL_REGIONAL_ENABLE_PARAM, MS_SQL_REGIONAL_ENABLE_SWITCH);
	    command.addParameterBooleanSwitch(MS_SQL_KEEP_NULL_VALUES_PARAM, MS_SQL_KEEP_NULL_VALUES_SWITCH);
	    command.addParameterBooleanSwitch(MS_SQL_KEEP_IDENTITY_VALUES_PARAM, MS_SQL_KEEP_IDENTITY_VALUES_SWITCH);
	    command.addParameterSwitch(MS_SQL_LOAD_HINTS_PARAM, MS_SQL_LOAD_HINTS_SWITCH);
		
		return command.getCommand();
    }

    /**
     * if any of mandatory parameters is empty then throw ComponentNotReadyException 
     * @throws ComponentNotReadyException
     */
    private void checkParams() throws ComponentNotReadyException {
    	if (StringUtils.isEmpty(dbLoaderPath)) {
    		throw new ComponentNotReadyException(this, StringUtils.quote(XML_DB_LOADER_PATH_ATTRIBUTE) 
    				+ " attribute must be set.");
		}
    	
    	if (StringUtils.isEmpty(table) && StringUtils.isEmpty(view)) {
			throw new ComponentNotReadyException(this, StringUtils.quote(XML_TABLE_ATTRIBUTE)
					+ " attribute or "
					+ StringUtils.quote(XML_VIEW_ATTRIBUTE) + " attribute must be set.");
		}
    }
    
    /**
     *  Description of the Method
     *
     * @exception  ComponentNotReadyException  Description of the Exception
     * @since                                  April 4, 2002
     */
    public void init() throws ComponentNotReadyException {
		super.init();

		parseParameters();
		checkParams();

		isDataReadFromPort = !getInPorts().isEmpty();
		isDataWrittenToPort = !getOutPorts().isEmpty();
		isErrFileFromUser = properties.containsKey(MS_SQL_ERR_FILE_PARAM);
		
		// prepare name for temporary data file
		try {
            if (isDataReadFromPort) {
        		tmpDataFileName = File.createTempFile(DATA_FILE_NAME_PREFIX, 
            			DATA_FILE_NAME_SUFFIX, TMP_DIR).getCanonicalPath();
            }
            
            if (isErrFileFromUser) {
            	errFileName = properties.getProperty(MS_SQL_ERR_FILE_PARAM);
            	errFileName = errFileName.replace("\"", "");
            } else if (isDataWrittenToPort) {
            	errFileName = File.createTempFile(ERROR_FILE_NAME_PREFIX, 
            			ERROR_FILE_NAME_SUFFIX, TMP_DIR).getCanonicalPath();
            }
            
        } catch(IOException e) {
        	free();
            throw new ComponentNotReadyException(this, "Some of the log files cannot be created.");
        }
		
		commandLine = createCommandLineForDbLoader();
		logger.info("System command: " + commandLine);
		
        if (isDataReadFromPort) {
	        InputPort inPort = getInputPort(READ_FROM_PORT);
	
	        dbMetadata = createMsSqlMetadata(inPort.getMetadata());
	        
	        // init of data formatter
	        formatter = new DataFormatter(CHARSET_NAME);
	        formatter.init(dbMetadata);
        }

        errConsumer = new LoggerDataConsumer(LoggerDataConsumer.LVL_ERROR, 0);
        consumer = new LoggerDataConsumer(LoggerDataConsumer.LVL_DEBUG, 0);
        
        if (isDataWrittenToPort) {
        	badRowReaderWriter = new MsSqlBadRowReaderWriter(getOutputPort(WRITE_TO_PORT));
        }
    }
    
    /**
     * parse parameters from string and save them to properties
     */
    private void parseParameters() {
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
    }
    
    /**
     * Modify metadata so that they correspond to bcp input format. 
     * Each field is delimited and it has the same delimiter.
     * Only last field is delimited by '\n'.
     *
     * @param oldMetadata original metadata
     * @return modified metadata
     */
    private DataRecordMetadata createMsSqlMetadata(DataRecordMetadata originalMetadata) {
    	DataRecordMetadata metadata = originalMetadata.duplicate();
		metadata.setRecType(DataRecordMetadata.DELIMITED_RECORD);
		for (int idx = 0; idx < metadata.getNumFields() - 1; idx++) {
			metadata.getField(idx).setDelimiter(properties.getProperty(
					MS_SQL_FIELD_TERMINATOR_PARAM, DEFAULT_FIELD_DELIMITER));
			setMsSqlDateFormat(metadata.getField(idx), idx);
		}
		int lastIndex = metadata.getNumFields() - 1;
		metadata.getField(lastIndex).setDelimiter(properties.getProperty(
				MS_SQL_ROW_TERMINATOR_PARAM, DEFAULT_ROW_DELIMITER));
		setMsSqlDateFormat(metadata.getField(lastIndex), lastIndex);
	
		return metadata;
    }

    /**
     * If field has format of date or time then default mssql format is set.
     * @param field 
     */
    private void setMsSqlDateFormat(DataFieldMetadata field, int fieldNum) {
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
		super.free();
		deleteDataFile();
		deleteErrFile();
	}

    /**
     * Deletes data file which was used for exchange data.
     */
    private void deleteDataFile() {
    	if (StringUtils.isEmpty(tmpDataFileName)) {
    		return;
    	}
    	
   		File dataFile = new File(tmpDataFileName);
   		dataFile.delete();
    }
    
    /**
     * If errFile wasn't apply by user (it's tmp file) then deletes err file.
     */
    private void deleteErrFile() {
    	if (StringUtils.isEmpty(errFileName) || isErrFileFromUser) {
    		return;
    	}
    	
   		File dataFile = new File(errFileName);
   		dataFile.delete();
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
        	MsSqlDataWriter msSqlDataWriter = new MsSqlDataWriter(
        			xattribs.getString(XML_ID_ATTRIBUTE),
                    xattribs.getString(XML_DB_LOADER_PATH_ATTRIBUTE),
                    xattribs.getString(XML_DATABASE_ATTRIBUTE));
        	
        	if (xattribs.exists(XML_TABLE_ATTRIBUTE)) {
        		msSqlDataWriter.setTable(xattribs.getString(XML_TABLE_ATTRIBUTE));
        	}
        	if (xattribs.exists(XML_FILE_URL_ATTRIBUTE)) {
        		msSqlDataWriter.setFileUrl(xattribs.getString(XML_FILE_URL_ATTRIBUTE));
        	}
        	if (xattribs.exists(XML_OWNER_ATTRIBUTE)) {
        		msSqlDataWriter.setOwner(xattribs.getString(XML_OWNER_ATTRIBUTE));
        	}
        	if (xattribs.exists(XML_VIEW_ATTRIBUTE)) {
        		msSqlDataWriter.setView(xattribs.getString(XML_VIEW_ATTRIBUTE));
        	}
        	if (xattribs.exists(XML_PARAMETERS_ATTRIBUTE)) {
        		msSqlDataWriter.setParameters(xattribs.getString(XML_PARAMETERS_ATTRIBUTE));
        	}

            return msSqlDataWriter;
        } catch (Exception ex) {
               throw new XMLConfigurationException(COMPONENT_TYPE + ":" + 
            		   xattribs.getString(XML_ID_ATTRIBUTE," unknown ID ") + ":" + ex.getMessage(),ex);
        }
    }
    
    @Override
	public void toXML(Element xmlElement) {
		super.toXML(xmlElement);
		
		xmlElement.setAttribute(XML_DB_LOADER_PATH_ATTRIBUTE, dbLoaderPath);
		xmlElement.setAttribute(XML_DATABASE_ATTRIBUTE, database);
		xmlElement.setAttribute(XML_FILE_URL_ATTRIBUTE, inDataFileName);

		if (!StringUtils.isEmpty(table)) {
			xmlElement.setAttribute(XML_TABLE_ATTRIBUTE, table);
		}
		if (!StringUtils.isEmpty(owner)) {
			xmlElement.setAttribute(XML_OWNER_ATTRIBUTE, owner);
		}
		if (!StringUtils.isEmpty(view)) {
			xmlElement.setAttribute(XML_VIEW_ATTRIBUTE, view);
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
        checkOutputPorts(status, 0, 1);

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
   
    private void setTable(String table) {
		this.table = table;
	}
    
    private void setOwner(String owner) {
		this.owner = owner;
	}
    
    private void setView(String view) {
    	this.view = view;
	}
    
    private void setParameters(String parameters) {
    	this.parameters = parameters;
	}
    
    private void setFileUrl(String dataFile) {
    	this.inDataFileName = dataFile;
    }
    
    /**
     * Class for reading and parsing data from input file,
     * and sends them to specified output port.
     * 
     * @see 		org.jetel.util.exec.ProcBox
     * @see 		org.jetel.util.exec.DataConsumer
     * @author      Miroslav Haupt (Mirek.Haupt@javlinconsulting.cz)
     * 				(c) Javlin Consulting (www.javlinconsulting.cz)
     * @since 		20.8.2007
     */
    private class MsSqlBadRowReaderWriter {
    	private DataRecord dbOutRecord;				// record from bcp error file
    	private DataRecordMetadata dbOutMetadata;	// format as bcp error file
    	private Parser dbParser;					// parse record from bcp error file
    	private BufferedReader reader;				// read from input stream (error file of bcp)
    	private DataRecord errRecord = null;
    	private OutputPort errPort = null;

    	// #@ dek 2, Sloupec 3: Neplatn hodnota znaku pro uren pevodu (CAST). @#
    	private String strBadRowPattern = "\\D+(\\d+)\\D+(\\d+): (.+)";
    	private Matcher badRowMatcher;
    	
    	private Log logger = LogFactory.getLog(PortDataConsumer.class);
    	
    	private final static int ROW_NUBMER_FIELD_NO = 0;
    	private final static int COLUMN_NUBMER_FIELD_NO = 1;
    	private final static int ERR_MSG_FIELD_NO = 2;
    	private final static int NUMBER_OF_ADDED_FIELDS = 3; // number of addded fields in errPortMetadata against dbIn(Out)Metadata
    	
    	MsSqlBadRowReaderWriter(OutputPort errPort) throws ComponentNotReadyException {
    		if (errPort == null) {
        		throw new ComponentNotReadyException("No output port was found.");
    		}

    		this.errPort = errPort;
    		checkErrPortMetadata();
    		
    		errRecord = new DataRecord(errPort.getMetadata());
			errRecord.init();
			
    		this.dbOutMetadata = createDbOutMetadata();
    		
    		dbOutRecord = new DataRecord(dbOutMetadata);
    		dbOutRecord.init();
    		
    		dbParser = new DelimitedDataParser(Defaults.DataParser.DEFAULT_CHARSET_DECODER);
			dbParser.init(dbOutMetadata);
			
			Pattern badRowPattern = Pattern.compile(strBadRowPattern);
			badRowMatcher = badRowPattern.matcher("");
    	}
    	
    	/**
         * Create metadata so that they correspond to format of bcp error file
         * 
         * @return modified metadata
    	 * @throws ComponentNotReadyException 
         */
        private DataRecordMetadata createDbOutMetadata() {
        	DataRecordMetadata metadata = errPort.getMetadata().duplicate();
        	metadata.setRecType(DataRecordMetadata.DELIMITED_RECORD);
        	// delete first, second and third field
        	for (int i = 0; i < NUMBER_OF_ADDED_FIELDS; i++) {
        		metadata.delField(0);
        	}
        	
        	for (DataFieldMetadata fieldMetadata: metadata) {
        		fieldMetadata.setDelimiter(DEFAULT_FIELD_DELIMITER);
        		
        		if (fieldMetadata.getType() == DataFieldMetadata.DATE_FIELD ||
        				fieldMetadata.getType() == DataFieldMetadata.DATETIME_FIELD) {
        			fieldMetadata.setFormatStr(DEFAULT_DATETIME_FORMAT);
        		}
        	}
        	// re-set last delimiter
        	metadata.getField(metadata.getNumFields() - 1).setDelimiter(DEFAULT_ROW_DELIMITER);
        	
        	return metadata;
        }
    	
    	/**
    	 * check metadata at error port against metadata at input port
    	 * if metadata isn't correct then throws ComponentNotReadyException
    	 * @throws ComponentNotReadyException when metadata isn't correct
    	 */
    	private void checkErrPortMetadata() throws ComponentNotReadyException {
    		DataRecordMetadata errMetadata = errPort.getMetadata();
    		if (errMetadata == null) {
        		throw new ComponentNotReadyException("Output port hasn't assigned metadata.");
        	}
   		
    		if (dbMetadata == null) {
    			return;
    		}
    		
    		// check number of fields; if inNumFields == outNumFields + NUMBER_OF_ADDED_FIELDS
			if (errMetadata.getNumFields() != dbMetadata.getNumFields() + NUMBER_OF_ADDED_FIELDS) {
				throw new ComponentNotReadyException("Number of fields of " +  StringUtils.quote(errMetadata.getName()) +  
						" isn't equal number of fields of " +  StringUtils.quote(dbMetadata.getName()) + " + " + NUMBER_OF_ADDED_FIELDS + ".");
			}

			// check if first field of errMetadata is integer - rowNumber
			if (errMetadata.getFieldType(ROW_NUBMER_FIELD_NO) != DataFieldMetadata.INTEGER_FIELD) {
				throw new ComponentNotReadyException("First field of " +  StringUtils.quote(errMetadata.getName()) +  
						" has different type from integer.");
			}
			
			// check if second field of errMetadata is integer - columnNumber
			if (errMetadata.getFieldType(COLUMN_NUBMER_FIELD_NO) != DataFieldMetadata.INTEGER_FIELD) {
				throw new ComponentNotReadyException("Second field of " +  StringUtils.quote(errMetadata.getName()) +  
						" has different type from integer.");
			}
			
			// check if third field of errMetadata is string - errMsg
			if (errMetadata.getFieldType(ERR_MSG_FIELD_NO) != DataFieldMetadata.STRING_FIELD) {
				throw new ComponentNotReadyException("Second field of " +  StringUtils.quote(errMetadata.getName()) +  
						" has different type from string.");
			}
			
			// check if other fields' type of errMetadata are equals as dbMetadata
			int count = NUMBER_OF_ADDED_FIELDS;
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
    	 * Example of bad row in stream:
		 * #@ dek 2, Sloupec 3: Neplatn hodnota znaku pro uren pevodu (CAST). @#
		 * a	s	32s	32.00	1970-01-01 12:34:45.000	fsd
    	 * @throws JetelException 
	     *
    	 * @see org.jetel.util.exec.DataConsumer
    	 */
    	public void run() throws JetelException {
    		try {
	    		reader = getReader();

	    		String line;
    			while ((line = reader.readLine()) != null) {
					badRowMatcher.reset(line);
					if (badRowMatcher.find()) {
	        			int rowNumber = Integer.valueOf(badRowMatcher.group(1));
	        			int columnNumber = Integer.valueOf(badRowMatcher.group(2));
	        			String errMsg = badRowMatcher.group(3);

	        			// read bad row
	        			if ((line = reader.readLine()) != null) {
	        				line = line + DEFAULT_ROW_DELIMITER;
		        			dbParser.setDataSource(getInputStream(line));
		        			try {
								if (dbParser.getNext(dbOutRecord) != null) {
									setErrRecord(dbOutRecord, errRecord, rowNumber, columnNumber, errMsg);
									errPort.writeRecord(errRecord);
		    					}
							} catch (BadDataFormatException e) {
								logger.warn("Bad row - it couldn't be parsed and sent to out port. Line: " + line);
							}
	        			}
					}
				}
			} catch (Exception e) {
				throw new JetelException("Error while writing output record", e);
			} finally {
				close();
			}
    	}
    	
    	/**
    	 * Set value in errRecord. In first field is set row number and other fields are copies from dbRecord
    	 * @param dbRecord source record
    	 * @param errRecord destination record
    	 * @param rowNumber number of bad row
    	 * @param columnNumber number of bad column
    	 * @param errMsg errMsg
    	 * @return destination record
    	 */
    	private DataRecord setErrRecord(DataRecord dbRecord, DataRecord errRecord, 
    			int rowNumber, int columnNumber, String errMsg) {
    		errRecord.reset();
    		errRecord.getField(ROW_NUBMER_FIELD_NO).setValue(rowNumber);
    		errRecord.getField(COLUMN_NUBMER_FIELD_NO).setValue(columnNumber);
    		errRecord.getField(ERR_MSG_FIELD_NO).setValue(errMsg);
    		for (int dbFieldNum = 0; dbFieldNum < dbRecord.getNumFields(); dbFieldNum++) {
    			errRecord.getField(dbFieldNum + NUMBER_OF_ADDED_FIELDS).setValue(dbRecord.getField(dbFieldNum));
    		}
    		return errRecord;
    	}
    	
    	/**
    	 * It create and return InputStream from string
    	 * @param str string, returned InputStream contains this string  
    	 * @return InputStream created from string
    	 */
    	private InputStream getInputStream(String str) throws UnsupportedEncodingException {
        	return new ByteArrayInputStream(str.getBytes(Defaults.DataParser.DEFAULT_CHARSET_DECODER));
        }
    	
    	private BufferedReader getReader() throws FileNotFoundException {
			return new BufferedReader(new FileReader(errFileName));
    	}
    	
    	private void close() {
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
    		} catch (InterruptedException ie) {
    			logger.warn("Out port wasn't closed.", ie);
    		}
    	}
    }
}
