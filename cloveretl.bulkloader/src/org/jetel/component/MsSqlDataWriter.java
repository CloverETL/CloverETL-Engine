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
import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.component.util.CommandBuilder;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.Defaults;
import org.jetel.data.parser.DelimitedDataParser;
import org.jetel.data.parser.Parser;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.BadDataFormatException;
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
import org.jetel.util.exec.PortDataConsumer;
import org.jetel.util.exec.ProcBox;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.property.RefResFlag;
import org.jetel.util.string.StringUtils;
import org.w3c.dom.Element;

/**
 * <h3>MsSql data writer</h3>
 * 
 * <!-- All records from input port:0 are loaded into mssql database. Connection to database is not through JDBC driver, this
 * component uses the bcp utility for this purpose. Bad rows send to output port 0.-->
 * 
 * <table border="1">
 * <th>Component:</th>
 * <tr>
 * <td>
 * <h4><i>Name:</i></h4>
 * </td>
 * <td>MsSql data writer</td>
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
 * <td>This component loads data to MsSql database using the bcp utility. There is created bcp command depending on input
 * parameters. Data are read from given input file or from input port and loaded to database.<br>
 * Any generated temporary files can be optionally logged to help diagnose problems.<br>
 * If data is read from input port, one of these values (nativeType, characterType, wideCharacterType, keepNonTextNative,
 * formatFile, inputFile) in parameters attribute has to be used. serverName option in parameters attribute is required when a bcp
 * command is run from a remote computer on the network or a local named instance.<br>
 * Before you use this component, make sure that SQL Server Client Connectivity Components are installed and configured on the
 * machine where CloverETL runs and bcp.exe command line tool available (part of SQL Server Management Tools, also included in SQL
 * Server Express Edition). </td>
 * </tr>
 * <tr>
 * <td>
 * <h4><i>Inputs:</i></h4>
 * </td>
 * <td>[0] - input records. It can be omitted - then <b>fileURL</b> has to be provided.</td>
 * </tr>
 * <tr>
 * <td>
 * <h4><i>Outputs:</i></h4>
 * </td>
 * <td>[0] - optionally one output port defined/connected - rejected records. Metadata on this port must have the same type of
 * field as input metadata. Output metadata has a additional fields with row number (integer), column number (integer) and error
 * message (string). These three fields are after data fields. </td>
 * </tr>
 * <tr>
 * <td>
 * <h4><i>Comment:</i></h4>
 * </td>
 * <td></td>
 * </tr>
 * </table> <br>
 * <table border="1">
 * <th>XML attributes:</th>
 * <tr>
 * <td><b>type</b></td>
 * <td>"MS_SQL_DATA_WRITER"</td>
 * </tr>
 * <tr>
 * <td><b>id</b></td>
 * <td>component identification</td>
 * </tr>
 * <tr>
 * <td><b>bcpUtilityPath</b></td>
 * <td>path to bcp utility</td>
 * </tr>
 * <tr>
 * <td><b>database</b><br>
 * <i>optional</i></td>
 * <td>Is the name of the database in which the specified table or view resides. If not specified, this is the default database
 * for the user.</td>
 * </tr>
 * <tr>
 * <td><b>owner</b><br>
 * <i>optional</i></td>
 * <td>Is the name of the owner of the table or view. owner is optional if the user performing the operation owns the specified
 * table or view. If owner is not specified and the user performing the operation does not own the specified table or view, SQL
 * Server 2005 returns an error message, and the operation is canceled.</td>
 * </tr>
 * <tr>
 * <td><b>table</b><br>
 * <i>optional</i></td>
 * <td>Is the name of the destination table</br>Note: table or view must be set</td>
 * </tr>
 * <tr>
 * <td><b>view</b><br>
 * <i>optional</i></td>
 * <td>Is the name of the destination view. Only views in which all columns refer to the same table can be used as destination
 * views. </br>Note: table or view must be set</td>
 * </tr>
 * <tr>
 * <td><b>fileURL</b><br>
 * <i>optional</i></td>
 * <td>Path to data file to be loaded.<br>
 * Normally this file is a temporary storage for data to be passed to bcp utility. If file URL is not specified, the file is
 * created in Clover or OS temporary directory and deleted after load finishes.<br>
 * If file URL is specified, temporary file is created within given path and name and not deleted after being loaded. Next graph
 * run overwrites it.<br>
 * There is one more meaning of this parameter. If input port is not specified, this file is used only for reading by bcp utility
 * and must already contain data in format expected by load. The file is neither deleted nor overwritten.</br> The path can have
 * from 1 through 255 characters. The data file can contain a maximum of 2,147,483,647 rows.</td>
 * </tr>
 * <tr>
 * <td><b>username</b><br>
 * <i>optional</i></td>
 * <td>Specifies the login ID used to connect to SQL Server.<br>
 * This attribute has the same meaning as parameter <i>userName</i> in attribute <i>parameters</i>. When this attribute is
 * defined, this attribute is used and <i>userName</i> parameter is ignored.</td>
 * </tr>
 * <tr>
 * <td><b>password</b><br>
 * <i>optional</i></td>
 * <td>Specifies the password for the login ID.<br>
 * This attribute has the same meaning as parameter <i>password</i> in attribute <i>parameters</i>. When this attribute is
 * defined, this attribute is used and <i>password</i> parameter is ignored.</td>
 * </tr>
 * <td><b>columnDelimiter</b><br>
 * <i>optional</i></td>
 * <td>Specifies the field terminator. The default is <b>\t</b> (tab character). Use this parameter to override the default
 * field terminator. For more information, see <a href="http://technet.microsoft.com/en-us/library/ms191485.aspx">Specifying Field
 * and Row Terminators</a>.<br>
 * This attribute has the same meaning as parameter <i>fieldTerminator</i> in attribute <i>parameters</i>. When this attribute is
 * defined, this attribute is used and <i>fieldTerminator</i> parameter is ignored.</td>
 * </tr>
 * <td><b>serverName</b><br>
 * <i>optional</i></td>
 * <td>Specifies the instance of SQL Server to which to connect. If no server is specified, the <b>bcp</b> utility connects to
 * the default instance of SQL Server on the local computer. This option is required when a <b>bcp</b> command is run from a
 * remote computer on the network or a local named instance. To connect to the default instance of SQL Server on a server, specify
 * only server_name. To connect to a named instance of SQL Server 2005, specify server_name\instance_name.<br>
 * This attribute has the same meaning as parameter <i>serverName</i> in attribute <i>parameters</i>. When this attribute is
 * defined, this attribute is used and <i>serverName</i> parameter is ignored.</td>
 * </tr>
 * <tr>
 * <td><b>parameters</b><br>
 * <i>optional</i></td>
 * <td>All possible additional parameters which can be passed on to bcp utility (See <a
 * href="http://technet.microsoft.com/en-us/library/ms162802.aspx"> bcp utility</a>). Parameters, in form <i>key=value</i> (or
 * <i>key</i> - interpreted as <i>key=true</i>, if possible value are only "true" or "false") has to be separated by :;| {colon,
 * semicolon, pipe}. If in parameter value occurs one of :;|, value has to be double quoted.<br>
 * <b>Load parameters</b><table>
 * <tr>
 * <td>maxErrors</td>
 * <td>Specifies the maximum number of syntax errors that can occur before the bcp operation is canceled. A syntax error implies
 * a data conversion error to the target data type. The max_errors total excludes any errors that can be detected only at the
 * server, such as constraint violations. A row that cannot be copied by the bcp utility is ignored and is counted as one error.
 * If this option is not included, the default is 10.</td>
 * </tr>
 * <tr>
 * <td>formatFile</td>
 * <td>Specifies the full path of a format file. <br>
 * For more information, see <a href="http://technet.microsoft.com/en-us/library/ms191516.aspx"> Creating a Format File</a>.</td>
 * </tr>
 * <tr>
 * <td>errFile</td>
 * <td>Specifies the full path of an error file used to store any rows that the bcp utility cannot transfer from the file to the
 * database. Error messages from the bcp command go to the workstation of the user. If this option is not used, an error file is
 * not created.</td>
 * </tr>
 * <tr>
 * <td>firstRow</td>
 * <td>Specifies the number of the first row to export from a table or import from a data file. This parameter requires a value
 * greater than (>) 0 but less than (<) or equal to (=) the total number rows. In the absence of this parameter, the default is
 * the first row of the file.</td>
 * </tr>
 * <tr>
 * <td>lastRow</td>
 * <td>Specifies the number of the last row to export from a table or import from a data file. This parameter requires a value
 * greater than (>) 0 but less than (<) or equal to (=) the number of the last row. In the absence of this parameter, the default
 * is the last row of the file.</td>
 * </tr>
 * <tr>
 * <td>batchSize</td>
 * <td>Specifies the number of rows per batch of imported data. Each batch is imported and logged as a separate transaction that
 * imports the whole batch before being committed. By default, all the rows in the data file are imported as one batch. To
 * distribute the rows among multiple batches, specify a batch_size that is smaller than the number of rows in the data file. If
 * the transaction for any batch fails, only insertions from the current batch are rolled back. Batches already imported by
 * committed transactions are unaffected by a later failure.</br> Do not use this option in conjunction with the <i>hint</i>="ROWS_PER_BATCH =
 * bb" option.</br> For more information, see <a href="http://technet.microsoft.com/en-us/library/ms188267.aspx"> Managing
 * Batches for Bulk Import</a>.</td>
 * </tr>
 * <tr>
 * <td>nativeType</td>
 * <td>Performs the bulk-copy operation using the native (database) data types of the data. This option does not prompt for each
 * field; it uses the native values.</br> For more information, see <a
 * href="http://technet.microsoft.com/en-us/library/ms191232.aspx">Using Native Format to Import or Export Data</a>.</td>
 * </tr>
 * <tr>
 * <td>characterType</td>
 * <td>Performs the operation using a character data type. This option does not prompt for each field; it uses char as the
 * storage type, without prefixes and with <b>\t</b> (tab character) as the field separator and <b>\r\n</b> (newline character)
 * as the row terminator.</br> For more information, see <a href="http://technet.microsoft.com/en-us/library/ms190919.aspx">Using
 * Character Format to Import or Export Data</a>.</td>
 * </tr>
 * <tr>
 * <td>keepNonTextNative</td>
 * <td>Performs the bulk-copy operation using the native (database) data types of the data for noncharacter data, and Unicode
 * characters for character data. This option offers a higher performance alternative to the <i>wideCharacterType</i> option, and
 * is intended for transferring data from one instance of SQL Server to another using a data file. It does not prompt for each
 * field. Use this option when you are transferring data that contains ANSI extended characters and you want to take advantage of
 * the performance of native mode. <i>keepNonTextNative</i> cannot be used with SQL Server 6.5 or earlier versions.</br> For
 * more information, see <a href="http://technet.microsoft.com/en-us/library/ms189941.aspx">Using Unicode Native Format to Import
 * or Export Data</a>.</td>
 * </tr>
 * <tr>
 * <td>wideCharacterType</td>
 * <td>Performs the bulk copy operation using Unicode characters. This option does not prompt for each field; it uses <b>nchar</b>
 * as the storage type, no prefixes, <b>\t</b> (tab character) as the field separator, and <b>\n</b> (newline character) as the
 * row terminator. This option cannot be used with SQL Server 6.5 or earlier versions.</br> For more information, see <a
 * href="http://technet.microsoft.com/en-us/library/ms188289.aspx">Using Unicode Character Format to Import or Export Data</a>.</td>
 * </tr>
 * <tr>
 * <td>fileFormatVersion</td>
 * <td>Performs the bulk-copy operation using data types from an earlier version of SQL Server. This option does not prompt for
 * each field; it uses the default values. For example, to bulk copy date formats supported by the <b>bcp</b> utility provided
 * with SQL Server 6.5 (but no longer supported by ODBC) into SQL Server 2005, use the <i>fileFormatVersion</i> 65 parameter.</br>
 * <b>Important:</b> When data is bulk exported from SQL Server into a data file, the <b>bcp</b> utility does not generate SQL
 * Server 6.0 or SQL Server 6.5 date formats for any <b>datetime</b> or <b>smalldatetime</b> data, even if <i>fileFormatVersion</i>
 * is specified. Dates are always written in ODBC format. Additionally, null values in bit columns are written as the value 0
 * because SQL Server 6.5 and earlier versions do not support nullable <b>bit</b> data.<br>
 * For more information, see <a href="http://technet.microsoft.com/en-us/library/ms191212.aspx">Importing Native and Character
 * Format Data from Earlier Versions of SQL Server</a>.</td>
 * </tr>
 * <tr>
 * <td>quotedIdentifier</td>
 * <td>Executes the SET QUOTED_IDENTIFIERS ON statement in the connection between the <b>bcp</b> utility and an instance of SQL
 * Server. Use this option to specify a database, owner, table, or view name that contains a space or a single quotation mark.
 * Enclose the entire three-part table or view name in quotation marks ("").</br> To specify a database name that contains a
 * space or single quotation mark, you must use the <i>quotedIdentifier</i> option.</td>
 * </tr>
 * <tr>
 * <td>codePageSpecifier</td>
 * <td>Supported for compatibility with early versions of SQL Server. For SQL Server 7.0 and later, Microsoft recommends that you
 * specify a collation name for each column in a format file. Specifies the code page of the data in the data file. code_page is
 * relevant only if the data contains <b>char</b>, <b>varchar</b>, or <b>text</b> columns with character values greater than
 * 127 or less than 32.</br> <table border="1">
 * <tr>
 * <td><b>Code page value</b></td>
 * <td><b>description</b></td>
 * </tr>
 * <tr>
 * <td>ACP</td>
 * <td>ANSI/MicrosoftWindows (ISO 1252).</td>
 * </tr>
 * <tr>
 * <td>OEM</td>
 * <td>Default code page used by the client. This is the default code page used if -C is not specified.</td>
 * </tr>
 * <tr>
 * <td>RAW</td>
 * <td>No conversion from one code page to another occurs. This is the fastest option because no conversion occurs.</td>
 * </tr>
 * <tr>
 * <td><i>code_page</i></td>
 * <td>Specific code page number; for example, 850.</td>
 * </tr>
 * </table> For more information, see <a href="http://technet.microsoft.com/en-us/library/ms190657.aspx">Copying Data Between
 * Different Collations</a>.</td>
 * </tr>
 * <tr>
 * <td>fieldTerminator</td>
 * <td>Specifies the field terminator. The default is <b>\t</b> (tab character). Use this parameter to override the default
 * field terminator. For more information, see <a href="http://technet.microsoft.com/en-us/library/ms191485.aspx">Specifying Field
 * and Row Terminators</a>.</td>
 * </tr>
 * <tr>
 * <td>rowTerminator</td>
 * <td>Specifies the row terminator. The default is <b>\n</b> (newline character). Use this parameter to override the default
 * row terminator. For more information, see <a href="http://technet.microsoft.com/en-us/library/ms191485.aspx">Specifying Field
 * and Row Terminators</a>.<br>
 * When both <i>rowTerminator</i> and <i>recordDelimiter</i> are defined then <i>rowTerminator</i> is ignored.
 * </td>
 * </tr>
 * <td>recordDelimiter</td>
 * <td>This is an alias for rowTerminator.
 * When both <i>rowTerminator</i> and <i>recordDelimiter</i> are used defined <i>rowTerminator</i> is ignored.</td>
 * </tr>
 * <tr>
 * <td>inputFile</td>
 * <td>Specifies the name of a response file, containing the responses to the command prompt questions for each data field when a
 * bulk copy is being performed using interactive mode (<i>nativeType</i>, <i>characterType</i>, <i>wideCharacterType</i>, or
 * <i>keepNonTextNative</i> not specified).</td>
 * </tr>
 * <tr>
 * <td>outputFile</td>
 * <td>Specifies the name of a file that receives output redirected from the command prompt.</td>
 * </tr>
 * <tr>
 * <td>packetSize</td>
 * <td>Specifies the number of bytes, per network packet, sent to and from the server. A server configuration option can be set
 * by using SQL Server Management Studio (or the <b>sp_configure</b> system stored procedure). However, the server configuration
 * option can be overridden on an individual basis by using this option. packet_size can be from 4096 to 65535 bytes; the default
 * is 4096.</br> Increased packet size can enhance performance of bulk-copy operations. If a larger packet is requested but
 * cannot be granted, the default is used. The performance statistics generated by the <b>bcp</b> utility show the packet size
 * used.</td>
 * </tr>
 * <tr>
 * <td>serverName</td>
 * <td>Specifies the instance of SQL Server to which to connect. If no server is specified, the <b>bcp</b> utility connects to
 * the default instance of SQL Server on the local computer. This option is required when a <b>bcp</b> command is run from a
 * remote computer on the network or a local named instance. To connect to the default instance of SQL Server on a server, specify
 * only server_name. To connect to a named instance of SQL Server 2005, specify server_name\instance_name.</td>
 * </tr>
 * <tr>
 * <td>userName</td>
 * <td>Specifies the login ID used to connect to SQL Server.</br> Security Note:</br> When the bcp utility is connecting to SQL
 * Server with a trusted connection using integrated security, use the <i>trustedConnection</i> option (trusted connection)
 * instead of the user name and password combination. This parameter has the same meaning as attribute <i>password</i>. When
 * attribute <i>password</i> is defined, this parameter is ignored.</td>
 * </tr>
 * <tr>
 * <td>password</td>
 * <td>Specifies the password for the login ID.<br>
 * This parameter has the same meaning as attribute <i>username</i>. When attribute <i>username</i> is defined, this parameter
 * is ignored.</td>
 * </tr>
 * <tr>
 * <td>trustedConnection</td>
 * <td>Specifies that the <b>bcp</b> utility connects to SQL Server with a trusted connection using integrated security. The
 * security credentials of the network user, <i>userName</i>, and <i>password</i> are not required. If <i>trustedConnection</i>
 * is not specified, you need to specify <i>userName</i> and <i>password</i> to successfully log in.</td>
 * </tr>
 * <tr>
 * <td>regionalEnable</td>
 * <td>Specifies that currency, date, and time data is bulk copied into SQL Server using the regional format defined for the
 * locale setting of the client computer. By default, regional settings are ignored.</td>
 * </tr>
 * <tr>
 * <td>keepNullValues</td>
 * <td>Specifies that empty columns should retain a null value during the operation, rather than have any default values for the
 * columns inserted. For more information, see <a href="http://technet.microsoft.com/en-us/library/ms187887.aspx">Keeping Nulls or
 * Using Default Values During Bulk Import</a>.</td>
 * </tr>
 * <tr>
 * <td>keepIdentityValues</td>
 * <td>Specifies that identity value or values in the imported data file are to be used for the identity column. If
 * <i>keepIdentityValues</i> is not given, the identity values for this column in the data file being imported are ignored, and
 * SQL Server 2005 automatically assigns unique values based on the seed and increment values specified during table creation.</br>
 * If the data file does not contain values for the identity column in the table or view, use a format file to specify that the
 * identity column in the table or view should be skipped when importing data; SQL Server 2005 automatically assigns unique values
 * for the column. For more information, see <a href="http://technet.microsoft.com/en-us/library/ms176057.aspx">DBCC CHECKIDENT
 * (Transact-SQL)</a>.</br> The <i>keepIdentityValues</i> option has a special permissions requirement. For more information,
 * see "Remarks" later in this topic.</br> For more information, see about keeping identify values see <a
 * href="http://technet.microsoft.com/en-us/library/ms186335.aspx">Keeping Identity Values When Bulk Importing Data</a>.</td>
 * </tr>
 * <tr>
 * <td>hint</td>
 * <td>Specifies the hint or hints to be used during a bulk import of data into a table or view. This option cannot be used when
 * bulk copying data into SQL Server 6.x or earlier.<br>
 * <b>ORDER</b>(column [ASC | DESC] [,...n])<br>
 * The sort order of the data in the data file. Bulk import performance is improved if the data being imported is sorted according
 * to the clustered index on the table, if any. If the data file is sorted in a different order, that is other than the order of a
 * clustered index key, or if there is no clustered index on the table, the ORDER clause is ignored. The column names supplied
 * must be valid column names in the destination table. By default, <b>bcp</b> assumes the data file is unordered. For optimized
 * bulk import, SQL Server also validates that the imported data is sorted.<br>
 * For more information, see <a href="http://technet.microsoft.com/en-us/library/ms177468.aspx">Controlling the Sort Order When
 * Bulk Importing Data</a>.<br>
 * <b>ROWS_PER_BATCH</b> = bb<br>
 * Number of rows of data per batch (as bb). Used when <i>batchSize</i> is not specified, resulting in the entire data file being
 * sent to the server as a single transaction. The server optimizes the bulk load according to the value bb. By default,
 * ROWS_PER_BATCH is unknown.<br>
 * For more information, see <a href="http://technet.microsoft.com/en-us/library/ms188267.aspx">Managing Batches for Bulk Import</a>.<br>
 * <b>KILOBYTES_PER_BATCH</b> = cc<br>
 * Approximate number of kilobytes of data per batch (as cc). By default, KILOBYTES_PER_BATCH is unknown.<br>
 * For more information, see <a href="http://technet.microsoft.com/en-us/library/ms188267.aspx">Managing Batches for Bulk Import</a>.<br>
 * <b>TABLOCK</b><br>
 * Specifies that a bulk update table-level lock is acquired for the duration of the bulk load operation; otherwise, a row-level
 * lock is acquired. This hint significantly improves performance because holding a lock for the duration of the bulk-copy
 * operation reduces lock contention on the table. A table can be loaded concurrently by multiple clients if the table has no
 * indexes and <b>TABLOCK</b> is specified. By default, locking behavior is determined by the table option <b>table lock on bulk
 * load</b>.<br>
 * For more information, see <a href="http://technet.microsoft.com/en-us/library/ms180876.aspx">Controlling the Locking Behavior
 * for Bulk Import</a>.<br>
 * <b>CHECK_CONSTRAINTS</b><br>
 * Specifies that all constraints on the target table or view must be checked during the bulk-import operation. Without the
 * CHECK_CONSTRAINTS hint, any CHECK and FOREIGN KEY constraints are ignored, and after the operation the constraint on the table
 * is marked as not-trusted.<br>
 * Note: UNIQUE, PRIMARY KEY, and NOT NULL constraints are always enforced.<br>
 * At some point, you will need to check the constraints on the entire table. If the table was nonempty before the bulk import
 * operation, the cost of revalidating the constraint may exceed the cost of applying CHECK constraints to the incremental data.
 * Therefore, we recommend that normally you enable constraint checking during an incremental bulk import.<br>
 * A situation in which you might want constraints disabled (the default behavior) is if the input data contains rows that violate
 * constraints. With CHECK constraints disabled, you can import the data and then use Transact-SQL statements to remove data that
 * is not valid.<br>
 * Note: In SQL Server 2005, <b>bcp</b> enforces new data validation and data checks that might cause existing scripts to fail
 * when they are executed on invalid data in a data file.<br>
 * Note: The maxErrors switch does not apply to constraint checking.<br>
 * For more information, see <a href="http://technet.microsoft.com/en-us/library/ms186247.aspx">Controlling Constraint Checking by
 * Bulk Import Operations</a>.<br>
 * <b>FIRE_TRIGGERS</b><br>
 * Specified with the in argument, any insert triggers defined on the destination table will run during the bulk-copy operation.
 * If FIRE_TRIGGERS is not specified, no insert triggers will run.<br>
 * For more information, see <a href="http://technet.microsoft.com/en-us/library/ms187640.aspx">Controlling Trigger Execution When
 * Bulk Importing Data</a>. </td>
 * </tr>
 * </table></td>
 * </tr>
 * </table>
 * 
 * <h4>Example:</h4>
 * Reading data from flat file:
 * 
 * <pre>
 * &lt;Node 
 * bcpUtilityPath=&quot;bcp&quot; 
 * database=&quot;test&quot;
 * owner=&quot;dbo&quot; 
 * table=&quot;test&quot; 
 * fileURL=&quot;${WORKSPACE}\in1.bcp&quot;
 * username="test"
 * password="test" 
 * parameters=&quot;characterType|codePageSpecifier=ACP|errFile=&quot;${WORKSPACE}\err.bcp&quot;|fieldTerminator=&quot;;&quot;&quot;
 * id=&quot;MS_SQL_DATA_WRITER0&quot;
 * type=&quot;MS_SQL_DATA_WRITER&quot;
 *  /&gt;
 * </pre>
 * 
 * Reading data from input port:
 * 
 * <pre>
 * &lt;Node 
 * bcpUtilityPath=&quot;bcp&quot; 
 * database=&quot;test&quot;
 * owner=&quot;dbo&quot; 
 * table=&quot;test&quot;
 * username="test"
 * password="test" 
 * parameters=&quot;characterType|codePageSpecifier=ACP|fieldTerminator=&quot;|&quot;&quot;
 * id=&quot;MS_SQL_DATA_WRITER0&quot;
 * type=&quot;MS_SQL_DATA_WRITER&quot;
 *  /&gt;
 * </pre>
 * 
 * @author Miroslav Haupt (Mirek.Haupt@javlinconsulting.cz)<br>
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 * @see org.jetel.graph.TransformationGraph
 * @see org.jetel.graph.Node
 * @see org.jetel.graph.Edge
 * @since 29.8.2007
 */
public class MsSqlDataWriter extends BulkLoader {

	private static Log logger = LogFactory.getLog(MsSqlDataWriter.class);

	/** Description of the Field */
	private final static String XML_BCP_UTILITY_PATH_ATTRIBUTE = "bcpUtilityPath";
	private final static String XML_OWNER_ATTRIBUTE = "owner";
	private final static String XML_VIEW_ATTRIBUTE = "view";
	public static final String XML_SERVER_NAME_ATTRIBUTE = "serverName";

	private final static String MS_SQL_MAX_ERRORS_PARAM = "maxErrors";
	private final static String MS_SQL_MAX_ERRORS_SWITCH = "m";
	private final static String MS_SQL_FORMAT_FILE_PARAM = "formatFile";
	private final static String MS_SQL_FORMAT_FILE_SWITCH = "f";
	private final static String MS_SQL_ERR_FILE_PARAM = "errFile";
	private final static String MS_SQL_ERR_FILE_SWITCH = "e";
	private final static String MS_SQL_FIRST_ROW_PARAM = "firstRow";
	private final static String MS_SQL_FIRST_ROW_SWITCH = "F";
	private final static String MS_SQL_LAST_ROW_PARAM = "lastRow";
	private final static String MS_SQL_LAST_ROW_SWITCH = "L";
	private final static String MS_SQL_BATCH_SIZE_PARAM = "batchSize";
	private final static String MS_SQL_BATCH_SIZE_SWITCH = "b";
	private final static String MS_SQL_NATIVE_TYPE_PARAM = "nativeType";
	private final static String MS_SQL_NATIVE_TYPE_SWITCH = "n";
	private final static String MS_SQL_CHARACTER_TYPE_PARAM = "characterType";
	private final static String MS_SQL_CHARACTER_TYPE_SWITCH = "c";
	private final static String MS_SQL_WIDE_CHARACTER_TYPE_PARAM = "wideCharacterType";
	private final static String MS_SQL_WIDE_CHARACTER_TYPE_SWITCH = "w";
	private final static String MS_SQL_KEEP_NON_TEXT_NATIVE_PARAM = "keepNonTextNative";
	private final static String MS_SQL_KEEP_NON_TEXT_NATIVE_SWITCH = "N";
	private final static String MS_SQL_FILE_FORMAT_VERSION_PARAM = "fileFormatVersion";
	private final static String MS_SQL_FILE_FORMAT_VERSION_SWITCH = "V";
	private final static String MS_SQL_QUOTED_IDENTIFIER_PARAM = "quotedIdentifier";
	private final static String MS_SQL_QUOTED_IDENTIFIER_SWITCH = "q";
	private final static String MS_SQL_CODE_PAGE_SPECIFIER_PARAM = "codePageSpecifier";
	private final static String MS_SQL_CODE_PAGE_SPECIFIER_SWITCH = "C";
	private final static String MS_SQL_COLUMN_DELIMITER_PARAM = "fieldTerminator";
	private final static String MS_SQL_COLUMN_DELIMITER_SWITCH = "t";
	private final static String MS_SQL_RECORD_DELIMITER_ALIAS_PARAM = "rowTerminator";
	private final static String MS_SQL_RECORD_DELIMITER_PARAM = "recordDelimiter";
	private final static String MS_SQL_RECORD_DELIMITER_SWITCH = "r";
	private final static String MS_SQL_INPUT_FILE_PARAM = "inputFile";
	private final static String MS_SQL_INPUT_FILE_SWITCH = "i";
	private final static String MS_SQL_OUTPUT_FILE_PARAM = "outputFile";
	private final static String MS_SQL_OUTPUT_FILE_SWITCH = "o";
	private final static String MS_SQL_PACKET_SIZE_PARAM = "packetSize";
	private final static String MS_SQL_PACKET_SIZE_SWITCH = "a";
	private final static String MS_SQL_SERVER_NAME_PARAM = "serverName";
	private final static String MS_SQL_SERVER_NAME_SWITCH = "S";
	private final static String MS_SQL_USER_NAME_PARAM = "userName";
	private final static String MS_SQL_USER_NAME_SWITCH = "U";
	private final static String MS_SQL_PASSWORD_PARAM = "password";
	private final static String MS_SQL_PASSWORD_SWITCH = "P";
	private final static String MS_SQL_TRUSTED_CONNECTION_PARAM = "trustedConnection";
	private final static String MS_SQL_TRUSTED_CONNECTION_SWITCH = "T";
	private final static String MS_SQL_REGIONAL_ENABLE_PARAM = "regionalEnable";
	private final static String MS_SQL_REGIONAL_ENABLE_SWITCH = "R";
	private final static String MS_SQL_KEEP_NULL_VALUES_PARAM = "keepNullValues";
	private final static String MS_SQL_KEEP_NULL_VALUES_SWITCH = "k";
	private final static String MS_SQL_KEEP_IDENTITY_VALUES_PARAM = "keepIdentityValues";
	private final static String MS_SQL_KEEP_IDENTITY_VALUES_SWITCH = "E";
	private final static String MS_SQL_LOAD_HINTS_PARAM = "loadHints";
	private final static String MS_SQL_LOAD_HINTS_SWITCH = "h";

	public final static String COMPONENT_TYPE = "MS_SQL_DATA_WRITER";

	private final static String EXCHANGE_FILE_PREFIX = "mssqlExchange";
	private final static String ERROR_FILE_NAME_PREFIX = "error";
	private final static String DEFAULT_COLUMN_DELIMITER = "\t"; // according bcp
	private final static String SWITCH_MARK = "-";
	
	/**
	 * when Character Format is used to Import Data then data file can't 
	 * contain only "\n" as a record delimiter - it isn't allowed by bcp
	 * bcp utility behave unusually:
	 * in data file: default record delimiter is "\r\n"
	 * in command line as parameter: default record delimiter is "\n"
	 */
	private final static String DEFAULT_RECORD_DELIMITER = "\r\n"; // according bcp
	
	private final static String DEFAULT_RECORD_DELIMITER_WIDE = "\n"; // according bcp
	

	private final static String DEFAULT_DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";
	private final static String DEFAULT_DATE_FORMAT = "MM/dd/yyyy";
	private final static String DEFAULT_TIME_FORMAT = DEFAULT_DATETIME_FORMAT;

	// variables for bcp's command
	private String owner;
	private String view;
	private String errFileName = null; // errFile insert by user or tmpErrFile for parsing bad rows
	private boolean isErrFileFromUser; // true if errFile was inserted by user; false when errFile is tmpErrFile
	private String serverName;

	private MsSqlBadRowReaderWriter badRowReaderWriter;

	/**
	 * Constructor for the MsSqlDataWriter object
	 * 
	 * @param id Description of the Parameter
	 */
	public MsSqlDataWriter(String id, String bcpUtilityPath, String database) {
		super(id, bcpUtilityPath, database);
	}

	/**
	 * Main processing method for the MsSqlDataWriter object
	 * 
	 * @since April 4, 2002
	 */
	@Override
	public Result execute() throws Exception {
		super.execute();
		ProcBox box;
		int processExitValue = 0;

		if (isDataReadFromPort) {
			// temp file is used for exchange data
			readFromPortAndWriteByFormatter();

			box = createProcBox();

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

	@Override
	protected String[] createCommandLineForLoadUtility() throws ComponentNotReadyException {
		CommandBuilder cmdBuilder =	new CommandBuilder(properties, SWITCH_MARK, "");
		
		cmdBuilder.add(loadUtilityPath);
		cmdBuilder.add(getDbTable());
		cmdBuilder.add("in");
		cmdBuilder.add(getData());
		cmdBuilder.addParam(MS_SQL_MAX_ERRORS_PARAM, MS_SQL_MAX_ERRORS_SWITCH);
		cmdBuilder.addParam(MS_SQL_FORMAT_FILE_PARAM, MS_SQL_FORMAT_FILE_SWITCH);
		cmdBuilder.addAttribute(MS_SQL_ERR_FILE_SWITCH, errFileName);
		cmdBuilder.addParam(MS_SQL_FIRST_ROW_PARAM, MS_SQL_FIRST_ROW_SWITCH);
		cmdBuilder.addParam(MS_SQL_LAST_ROW_PARAM, MS_SQL_LAST_ROW_SWITCH);
		cmdBuilder.addParam(MS_SQL_BATCH_SIZE_PARAM, MS_SQL_BATCH_SIZE_SWITCH);
		cmdBuilder.addBooleanParam(MS_SQL_NATIVE_TYPE_PARAM, MS_SQL_NATIVE_TYPE_SWITCH);
		cmdBuilder.addBooleanParam(MS_SQL_CHARACTER_TYPE_PARAM, MS_SQL_CHARACTER_TYPE_SWITCH);
		cmdBuilder.addBooleanParam(MS_SQL_WIDE_CHARACTER_TYPE_PARAM, MS_SQL_WIDE_CHARACTER_TYPE_SWITCH);
		cmdBuilder.addBooleanParam(MS_SQL_KEEP_NON_TEXT_NATIVE_PARAM, MS_SQL_KEEP_NON_TEXT_NATIVE_SWITCH);
		cmdBuilder.addParam(MS_SQL_FILE_FORMAT_VERSION_PARAM, MS_SQL_FILE_FORMAT_VERSION_SWITCH);
		cmdBuilder.addBooleanParam(MS_SQL_QUOTED_IDENTIFIER_PARAM, MS_SQL_QUOTED_IDENTIFIER_SWITCH);
		cmdBuilder.addParam(MS_SQL_CODE_PAGE_SPECIFIER_PARAM, MS_SQL_CODE_PAGE_SPECIFIER_SWITCH);
		cmdBuilder.addAttribute(MS_SQL_COLUMN_DELIMITER_SWITCH, getColumnDelimiter(true));
		cmdBuilder.addAttribute(MS_SQL_RECORD_DELIMITER_SWITCH, getRecordDelimiter(true));
		cmdBuilder.addParam(MS_SQL_INPUT_FILE_PARAM, MS_SQL_INPUT_FILE_SWITCH);
		cmdBuilder.addParam(MS_SQL_OUTPUT_FILE_PARAM, MS_SQL_OUTPUT_FILE_SWITCH);
		cmdBuilder.addParam(MS_SQL_PACKET_SIZE_PARAM, MS_SQL_PACKET_SIZE_SWITCH);
		//HOTFIX: CL-1932 @see CommandBuilder.addAttributeDirect() javadoc
		cmdBuilder.addAttributeDirect(MS_SQL_SERVER_NAME_SWITCH, getServerName());
		cmdBuilder.addAttribute(MS_SQL_USER_NAME_SWITCH, user);
		cmdBuilder.addAttribute(MS_SQL_PASSWORD_SWITCH, password);
		cmdBuilder.addBooleanParam(MS_SQL_TRUSTED_CONNECTION_PARAM, MS_SQL_TRUSTED_CONNECTION_SWITCH);
		cmdBuilder.addBooleanParam(MS_SQL_REGIONAL_ENABLE_PARAM, MS_SQL_REGIONAL_ENABLE_SWITCH);
		cmdBuilder.addBooleanParam(MS_SQL_KEEP_NULL_VALUES_PARAM, MS_SQL_KEEP_NULL_VALUES_SWITCH);
		cmdBuilder.addBooleanParam(MS_SQL_KEEP_IDENTITY_VALUES_PARAM, MS_SQL_KEEP_IDENTITY_VALUES_SWITCH);
		cmdBuilder.addParam(MS_SQL_LOAD_HINTS_PARAM, MS_SQL_LOAD_HINTS_SWITCH);
		
		return cmdBuilder.getCommand();
	}

	private String getDbTable() {
		StringBuilder dbTable = new StringBuilder(); 
		if (!StringUtils.isEmpty(database)) {
			dbTable.append(database + ".");
		}
		if (!StringUtils.isEmpty(owner)) {
			dbTable.append(owner + ".");
		}
		if (!StringUtils.isEmpty(table)) {
			dbTable.append(table);
		} else {
			dbTable.append(view);
		}
		return dbTable.toString();
	}
	
	private String getData() {
		try {
			return StringUtils.quote(dataFile.getCanonicalPath());
		} catch (IOException ioe) {
			return StringUtils.quote(dataFile.getAbsolutePath());
		}
	}

	@Override
	protected void checkParams() throws ComponentNotReadyException {
		if (StringUtils.isEmpty(loadUtilityPath)) {
			throw new ComponentNotReadyException(this, StringUtils.quote(XML_BCP_UTILITY_PATH_ATTRIBUTE)
					+ " attribute have to be set.");
		}

		if (StringUtils.isEmpty(table) && StringUtils.isEmpty(view)) {
			throw new ComponentNotReadyException(this, StringUtils.quote(XML_TABLE_ATTRIBUTE) + " attribute or "
					+ StringUtils.quote(XML_VIEW_ATTRIBUTE) + " attribute must be set.");
		}
		
		if (isUsedDataTypeForImportData() > 1) {
			throw new ComponentNotReadyException(this, "Only one parameter from these " + 
					StringUtils.quote(MS_SQL_NATIVE_TYPE_PARAM) + ", " +
					StringUtils.quote(MS_SQL_CHARACTER_TYPE_PARAM) + ", " +
					StringUtils.quote(MS_SQL_WIDE_CHARACTER_TYPE_PARAM) + ", " +
					StringUtils.quote(MS_SQL_KEEP_NON_TEXT_NATIVE_PARAM) +
					" can be used simultaneously.");
		}
		
		if (isUsedDataTypeForImportData() < 1 && 
				!properties.containsKey(MS_SQL_FORMAT_FILE_PARAM) &&
				!properties.containsKey(MS_SQL_INPUT_FILE_PARAM)) {
			throw new ComponentNotReadyException(this, "One of these parameters has to be set: " + 
					StringUtils.quote(MS_SQL_NATIVE_TYPE_PARAM) + ", " +
					StringUtils.quote(MS_SQL_CHARACTER_TYPE_PARAM) + ", " +
					StringUtils.quote(MS_SQL_WIDE_CHARACTER_TYPE_PARAM) + ", " +
					StringUtils.quote(MS_SQL_KEEP_NON_TEXT_NATIVE_PARAM) + ", " +
					StringUtils.quote(MS_SQL_FORMAT_FILE_PARAM) + ", " +
					StringUtils.quote(MS_SQL_INPUT_FILE_PARAM) + ".\n" +
					"For most cases (when input port is connected) " + 
					StringUtils.quote(MS_SQL_CHARACTER_TYPE_PARAM) + " should be used.");
		}

		// report that some problem can occur
		if (StringUtils.specCharToString("\r\n").equals(getRecordDelimiter(true)) && 
				isCharTypeUseToLoad()) {
			logger.warn("When " + StringUtils.quote(MS_SQL_RECORD_DELIMITER_PARAM +
					"=" + StringUtils.specCharToString("\r\n")) + 
					" then problem with loading data can occur.");
		}
		
		// report on ignoring some attributes
		if (columnDelimiter != null && properties.containsKey(MS_SQL_COLUMN_DELIMITER_PARAM)) {
			logger.warn("Parameter " + StringUtils.quote(MS_SQL_COLUMN_DELIMITER_PARAM) + " in attribute "
					+ StringUtils.quote(XML_PARAMETERS_ATTRIBUTE) + " is ignored. Attribute "
					+ StringUtils.quote(XML_COLUMN_DELIMITER_ATTRIBUTE) + " is used.");
		}
		
		if (properties.containsKey(MS_SQL_RECORD_DELIMITER_PARAM) && 
				properties.containsKey(MS_SQL_RECORD_DELIMITER_ALIAS_PARAM) ) {
			logger.warn("Parameter " + StringUtils.quote(MS_SQL_RECORD_DELIMITER_ALIAS_PARAM) + " in attribute "
					+ StringUtils.quote(XML_PARAMETERS_ATTRIBUTE) + " is ignored. Parameter "
					+ StringUtils.quote(MS_SQL_RECORD_DELIMITER_PARAM) + " is used.");
		}
		
		if (serverName != null && properties.containsKey(MS_SQL_SERVER_NAME_PARAM)) {
			logger.warn("Parameter " + StringUtils.quote(MS_SQL_SERVER_NAME_PARAM) + " in attribute "
					+ StringUtils.quote(XML_PARAMETERS_ATTRIBUTE) + " is ignored. Attribute "
					+ StringUtils.quote(XML_SERVER_NAME_ATTRIBUTE) + " is used.");
		}
		
		checkServerName();
	}
	
	/**
	 * nativeType, characterType, wideCharacterType, keepNonTextNative
	 * If it uses all of under types then returns 4
	 * @return number of data type import definition
	 */
	private int isUsedDataTypeForImportData() {
		int dataTypeDefinition = 0;
		if (isNativeTypeUseToLoad()) {
			dataTypeDefinition++;
		}
		if (isCharTypeUseToLoad()) {
			dataTypeDefinition++;
		} 
		if (isWideCharTypeUseToLoad()) {
			dataTypeDefinition++;
		}
		if (isKeepNonTextTypeUseToLoad()) {
			dataTypeDefinition++;
		}
		return dataTypeDefinition;
	}
	
	private void checkServerName() {
		String sn = getServerName();
		if (sn != null && sn.contains(":")) {
			logger.warn("If attribute " + StringUtils.quote(XML_SERVER_NAME_ATTRIBUTE) 
					+ " contains port number then 'bcp' utility can failed - " + 
					StringUtils.quote("Connection string is not valid") + ":.");
		}
	}

	/**
	 * Description of the Method
	 * 
	 * @exception ComponentNotReadyException Description of the Exception
	 * @since April 4, 2002
	 */
	@Override
	public void init() throws ComponentNotReadyException {
		if (isInitialized()) return;
		super.init();

		if (isDataWrittenToPort) {
			badRowReaderWriter = new MsSqlBadRowReaderWriter(getOutputPort(WRITE_TO_PORT));
		}
	}
	
	@Override
	protected void preInit() throws ComponentNotReadyException {
		getUserAndPassword();

		isErrFileFromUser = properties.containsKey(MS_SQL_ERR_FILE_PARAM);
	}

	@Override
	protected void initDataFile() throws ComponentNotReadyException {
		try {
			if (isDataReadFromPort) {
				if (dataURL != null) {
					dataFile = getFile(dataURL);
				} else {
					dataFile = createTempFile(EXCHANGE_FILE_PREFIX);
				}
				dataFile.delete();
			} else {
				if (dataURL == null) {
					throw new ComponentNotReadyException(this, "There is neither input port nor "
							+ StringUtils.quote(XML_FILE_URL_ATTRIBUTE) + " attribute specified.");
				}
				dataFile = openFile(dataURL);
			}

			if (isErrFileFromUser) {
				errFileName = getFilePath(properties.getProperty(MS_SQL_ERR_FILE_PARAM));
			} else if (isDataWrittenToPort) {
				errFileName = createTempFile(ERROR_FILE_NAME_PREFIX).getCanonicalPath();
			}

		} catch (IOException e) {
			free();
			throw new ComponentNotReadyException(this, "Some of the temp files can't be created.");
		}		
	}
	
	@Override
	protected String getColumnDelimiter() {
		return getColumnDelimiter(false);
	}

	@Override
	protected String getRecordDelimiter() {
		return getRecordDelimiter(false);
	}

	/**
	 * Get column delimiter.
	 */
	private String getColumnDelimiter(boolean inCommand) {
		String colDel;
		if (columnDelimiter != null) {
			colDel = columnDelimiter;
		} else if (properties.containsKey(MS_SQL_COLUMN_DELIMITER_PARAM)) {
			colDel = properties.getProperty(MS_SQL_COLUMN_DELIMITER_PARAM);
		} else {
			if (inCommand) {
				return null; // default value is used
			} else {
				return DEFAULT_COLUMN_DELIMITER;
			}
		}
		
		return colDel;
	}
	
	/**
	 * Get record delimiter.
	 */
	private String getRecordDelimiter(boolean inCommand) {
		String recDel;
		if (properties.containsKey(MS_SQL_RECORD_DELIMITER_PARAM)) {
			recDel = properties.getProperty(MS_SQL_RECORD_DELIMITER_PARAM);
		} else if (properties.containsKey(MS_SQL_RECORD_DELIMITER_ALIAS_PARAM)) {
			recDel = properties.getProperty(MS_SQL_RECORD_DELIMITER_ALIAS_PARAM);
		} else if (inCommand){
			return null; // default value is used
		} else {
			if (isCharTypeUseToLoad()) {
				return DEFAULT_RECORD_DELIMITER;
			} else if (isWideCharTypeUseToLoad()) {
				return DEFAULT_RECORD_DELIMITER_WIDE;
			} else {
				return DEFAULT_RECORD_DELIMITER; // shouldn't be occur
			}
		}
		
		if (inCommand) {
			return recDel;
		} else {
			// when Character Format is used to Import Data then in data file 
			// can't contain only "\n" as a record delimiter - it isn't allowed by bcp
			if ("\n".equals(recDel) && isCharTypeUseToLoad()) {
				recDel = DEFAULT_RECORD_DELIMITER;
			}
			return recDel;
		}
	}
	
	private boolean isCharTypeUseToLoad() {
		return properties.containsKey(MS_SQL_CHARACTER_TYPE_PARAM);
	}
	
	private boolean isWideCharTypeUseToLoad() {
		return properties.containsKey(MS_SQL_WIDE_CHARACTER_TYPE_PARAM);
	}
	
	private boolean isNativeTypeUseToLoad() {
		return properties.containsKey(MS_SQL_NATIVE_TYPE_PARAM);
	}
	
	private boolean isKeepNonTextTypeUseToLoad() {
		return properties.containsKey(MS_SQL_KEEP_NON_TEXT_NATIVE_PARAM);
	}
	
	/**
	 * Get server name.
	 */
	private String getServerName() {
		if (serverName != null) {
			return serverName;
		}
		
		if (properties.containsKey(MS_SQL_SERVER_NAME_PARAM)) {
			return properties.getProperty(MS_SQL_SERVER_NAME_PARAM);
		}
		
		return null;
	}
	
	/**
	 * Get user and password from properties and save 
	 * them to "user" and "password" attribute.
	 * When both attribute username and parameter userName are defined
	 * then warning message is printed to log. (As well for password.)
	 */
	private void getUserAndPassword() {
		if (properties.containsKey(MS_SQL_USER_NAME_PARAM)) {
			if (user == null) {
				user = properties.getProperty(MS_SQL_USER_NAME_PARAM);
			} else {
				logger.warn("Parameter " + StringUtils.quote(MS_SQL_USER_NAME_PARAM) + " in attribute "
						+ StringUtils.quote(XML_PARAMETERS_ATTRIBUTE) + " is ignored. Attribute "
						+ StringUtils.quote(XML_USER_ATTRIBUTE) + " is used.");
			}
		}

		if (properties.containsKey(MS_SQL_PASSWORD_PARAM)) {
			if (password == null) {
				password = properties.getProperty(MS_SQL_PASSWORD_PARAM);
			} else {
				logger.warn("Parameter " + StringUtils.quote(MS_SQL_PASSWORD_PARAM) + " in attribute "
						+ StringUtils.quote(XML_PARAMETERS_ATTRIBUTE) + " is ignored. Attribute "
						+ StringUtils.quote(XML_PASSWORD_ATTRIBUTE) + "is used.");
			}
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
		deleteErrFile();
	}

	/**
	 * If errFile wasn't apply by user (it's tmp file) then deletes err file.
	 */
	private void deleteErrFile() {
		if (StringUtils.isEmpty(errFileName) || isErrFileFromUser) {
			return;
		}

		deleteFile(errFileName);
	}

	/**
	 * Description of the Method
	 * 
	 * @param nodeXML Description of Parameter
	 * @return Description of the Returned Value
	 * @throws AttributeNotFoundException 
	 * @since May 21, 2002
	 */
	public static Node fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException, AttributeNotFoundException {
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(xmlElement, graph);

		MsSqlDataWriter msSqlDataWriter = new MsSqlDataWriter(
				xattribs.getString(XML_ID_ATTRIBUTE), 
				xattribs.getStringEx(XML_BCP_UTILITY_PATH_ATTRIBUTE, RefResFlag.URL),
				xattribs.getString(XML_DATABASE_ATTRIBUTE));

		if (xattribs.exists(XML_TABLE_ATTRIBUTE)) {
			msSqlDataWriter.setTable(xattribs.getString(XML_TABLE_ATTRIBUTE));
		}
		if (xattribs.exists(XML_FILE_URL_ATTRIBUTE)) {
			msSqlDataWriter.setFileUrl(xattribs.getStringEx(XML_FILE_URL_ATTRIBUTE, RefResFlag.URL));
		}
		if (xattribs.exists(XML_OWNER_ATTRIBUTE)) {
			msSqlDataWriter.setOwner(xattribs.getString(XML_OWNER_ATTRIBUTE));
		}
		if (xattribs.exists(XML_VIEW_ATTRIBUTE)) {
			msSqlDataWriter.setView(xattribs.getString(XML_VIEW_ATTRIBUTE));
		}
		if (xattribs.exists(XML_USER_ATTRIBUTE)) {
			msSqlDataWriter.setUser(xattribs.getString(XML_USER_ATTRIBUTE));
		}
		if (xattribs.exists(XML_PASSWORD_ATTRIBUTE)) {
			msSqlDataWriter.setPassword(xattribs.getStringEx(XML_PASSWORD_ATTRIBUTE, RefResFlag.SECURE_PARAMATERS));
		}
		if (xattribs.exists(XML_COLUMN_DELIMITER_ATTRIBUTE)) {
    		msSqlDataWriter.setColumnDelimiter(xattribs.getString(XML_COLUMN_DELIMITER_ATTRIBUTE));
		}
		if (xattribs.exists(XML_SERVER_NAME_ATTRIBUTE)) {
    		msSqlDataWriter.setServerName(xattribs.getStringEx(XML_SERVER_NAME_ATTRIBUTE, RefResFlag.SPEC_CHARACTERS_OFF));
		}
		if (xattribs.exists(XML_PARAMETERS_ATTRIBUTE)) {
			msSqlDataWriter.setParameters(xattribs.getString(XML_PARAMETERS_ATTRIBUTE));
		}

		return msSqlDataWriter;
	}

	/** Description of the Method */
	@Override
	public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		super.checkConfig(status);

		if(!checkInputPorts(status, 0, 1)
				|| !checkOutputPorts(status, 0, 1)) {
			return status;
		}
		
		isDataReadFromPort = !getInPorts().isEmpty();
		isDataReadDirectlyFromFile = !isDataReadFromPort && !StringUtils.isEmpty(dataURL);
        isDataWrittenToPort = !getOutPorts().isEmpty();
        properties = parseParameters(parameters);
        
		//---checkParams
		if (StringUtils.isEmpty(loadUtilityPath)) {
			status.add(new ConfigurationProblem(StringUtils.quote(XML_BCP_UTILITY_PATH_ATTRIBUTE) + " attribute have to be set.",
					Severity.ERROR, this, Priority.HIGH, XML_BCP_UTILITY_PATH_ATTRIBUTE));			
		}
		if (StringUtils.isEmpty(table) && StringUtils.isEmpty(view)) {
			status.add(new ConfigurationProblem(StringUtils.quote(XML_TABLE_ATTRIBUTE) + " attribute or " +
					StringUtils.quote(XML_VIEW_ATTRIBUTE) + " attribute must be set.", Severity.ERROR, this, Priority.HIGH));
		}		
		if (isUsedDataTypeForImportData() > 1) {
			status.add(new ConfigurationProblem("Only one parameter from these " +	StringUtils.quote(MS_SQL_NATIVE_TYPE_PARAM) + ", " +
					StringUtils.quote(MS_SQL_CHARACTER_TYPE_PARAM) + ", " +	StringUtils.quote(MS_SQL_WIDE_CHARACTER_TYPE_PARAM) + ", " +
					StringUtils.quote(MS_SQL_KEEP_NON_TEXT_NATIVE_PARAM) + " can be used simultaneously.",
					Severity.ERROR, this, Priority.NORMAL));
		} else if (isUsedDataTypeForImportData() < 1 && !properties.containsKey(MS_SQL_FORMAT_FILE_PARAM)
				&& !properties.containsKey(MS_SQL_INPUT_FILE_PARAM)) {
			status.add(new ConfigurationProblem("One of these parameters has to be set: " + StringUtils.quote(MS_SQL_NATIVE_TYPE_PARAM) + ", " +
					StringUtils.quote(MS_SQL_CHARACTER_TYPE_PARAM) + ", " +	StringUtils.quote(MS_SQL_WIDE_CHARACTER_TYPE_PARAM) + ", " +
					StringUtils.quote(MS_SQL_KEEP_NON_TEXT_NATIVE_PARAM) + ", " + StringUtils.quote(MS_SQL_FORMAT_FILE_PARAM) + ", " +
					StringUtils.quote(MS_SQL_INPUT_FILE_PARAM) + ".\n" + "For most cases (when input port is connected) " + 
					StringUtils.quote(MS_SQL_CHARACTER_TYPE_PARAM) + " should be used.", Severity.ERROR, this, Priority.NORMAL));
		}
		// report that some problem can occur
		if (StringUtils.specCharToString("\r\n").equals(getRecordDelimiter(true)) && isCharTypeUseToLoad()) {
			status.add(new ConfigurationProblem("When " + StringUtils.quote(MS_SQL_RECORD_DELIMITER_PARAM +	"=" +
					StringUtils.specCharToString("\r\n")) + " then problem with loading data can occur.",
					Severity.WARNING, this, Priority.NORMAL, MS_SQL_RECORD_DELIMITER_PARAM));
		}
		
		// report on ignoring some attributes
		if (columnDelimiter != null && properties.containsKey(MS_SQL_COLUMN_DELIMITER_PARAM)) {
			status.add(new ConfigurationProblem("Parameter " + StringUtils.quote(MS_SQL_COLUMN_DELIMITER_PARAM) + " in attribute "
					+ StringUtils.quote(XML_PARAMETERS_ATTRIBUTE) + " is ignored. Attribute " + StringUtils.quote(XML_COLUMN_DELIMITER_ATTRIBUTE) +
					" is used.", Severity.WARNING, this, Priority.NORMAL, MS_SQL_COLUMN_DELIMITER_PARAM));
		}		
		if (properties.containsKey(MS_SQL_RECORD_DELIMITER_PARAM) && properties.containsKey(MS_SQL_RECORD_DELIMITER_ALIAS_PARAM) ) {
			status.add(new ConfigurationProblem("Parameter " + StringUtils.quote(MS_SQL_RECORD_DELIMITER_ALIAS_PARAM) + " in attribute "
					+ StringUtils.quote(XML_PARAMETERS_ATTRIBUTE) + " is ignored. Parameter " + StringUtils.quote(MS_SQL_RECORD_DELIMITER_PARAM) +
					" is used.", Severity.WARNING, this, Priority.NORMAL, MS_SQL_RECORD_DELIMITER_ALIAS_PARAM));
		}		
		if (serverName != null && properties.containsKey(MS_SQL_SERVER_NAME_PARAM)) {
			status.add(new ConfigurationProblem("Parameter " + StringUtils.quote(MS_SQL_SERVER_NAME_PARAM) + " in attribute "
					+ StringUtils.quote(XML_PARAMETERS_ATTRIBUTE) + " is ignored. Attribute " + StringUtils.quote(XML_SERVER_NAME_ATTRIBUTE) +
					" is used.", Severity.WARNING, this, Priority.NORMAL, MS_SQL_SERVER_NAME_PARAM));
		}		
		String sn = getServerName();
		if (sn != null && sn.contains(":")) {
			status.add(new ConfigurationProblem("If attribute " + StringUtils.quote(XML_SERVER_NAME_ATTRIBUTE) 
					+ " contains port number then 'bcp' utility can fail - " + StringUtils.quote("Connection string is not valid") + ":.",
					Severity.WARNING, this, Priority.NORMAL, XML_SERVER_NAME_ATTRIBUTE));			
		}
		//---End of checkParams
		try {
			initDataFile();
		} catch (ComponentNotReadyException e) {
			status.add(new ConfigurationProblem(ExceptionUtils.getMessage(e),	ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL));
		}
		deleteTempFiles();
		return status;
	}

	@Override
	public String getType() {
		return COMPONENT_TYPE;
	}

	private void setOwner(String owner) {
		this.owner = owner;
	}

	private void setView(String view) {
		this.view = view;
	}

	private void setServerName(String serverName) {
		this.serverName = serverName;
	}

	/**
	 * Return list of all adding parameters (parameters attribute).
	 * Deprecated parameters mustn't be used.
	 * It is intended for use in GUI in parameter editor.
	 * @return list of parameters that is viewed in parameters editor
	 */
	public static String[] getAddingParameters() {
		return new String[] {
			MS_SQL_MAX_ERRORS_PARAM,
			MS_SQL_FORMAT_FILE_PARAM,
			MS_SQL_ERR_FILE_PARAM,
			MS_SQL_FIRST_ROW_PARAM,
			MS_SQL_LAST_ROW_PARAM,
			MS_SQL_BATCH_SIZE_PARAM,
			MS_SQL_NATIVE_TYPE_PARAM,
			MS_SQL_CHARACTER_TYPE_PARAM,
			MS_SQL_WIDE_CHARACTER_TYPE_PARAM,
			MS_SQL_KEEP_NON_TEXT_NATIVE_PARAM,
			MS_SQL_FILE_FORMAT_VERSION_PARAM,
			MS_SQL_QUOTED_IDENTIFIER_PARAM,
			MS_SQL_CODE_PAGE_SPECIFIER_PARAM,
			// MS_SQL_COLUMN_DELIMITER_PARAM, // deprecated
			// MS_SQL_RECORD_DELIMITER_ALIAS_PARAM, // deprecated
			MS_SQL_RECORD_DELIMITER_PARAM,
			MS_SQL_INPUT_FILE_PARAM,
			MS_SQL_OUTPUT_FILE_PARAM,
			MS_SQL_PACKET_SIZE_PARAM,
			MS_SQL_SERVER_NAME_PARAM, // deprecated
			// MS_SQL_USER_NAME_PARAM, // deprecated
			// MS_SQL_PASSWORD_PARAM, // deprecated
			MS_SQL_TRUSTED_CONNECTION_PARAM,
			MS_SQL_REGIONAL_ENABLE_PARAM,
			MS_SQL_KEEP_NULL_VALUES_PARAM,
			MS_SQL_KEEP_IDENTITY_VALUES_PARAM,
			MS_SQL_LOAD_HINTS_PARAM
		};
	}
	
	/**
	 * Class for reading and parsing data from input file, 
	 * and sends them to specified output port.
	 * 
	 * @see 		org.jetel.util.exec.ProcBox
	 * @see 		org.jetel.util.exec.DataConsumer
	 * @author 		Miroslav Haupt (Mirek.Haupt@javlinconsulting.cz)
	 *				(c) Javlin Consulting (www.javlinconsulting.cz)
	 * @since 20.8.2007
	 */
	private class MsSqlBadRowReaderWriter {
		private DataRecord dbOutRecord; // record from bcp error file
		private DataRecordMetadata dbOutMetadata; // format as bcp error file
		private Parser dbParser; // parse record from bcp error file
		private BufferedReader reader; // read from input stream (error file of bcp)
		private DataRecord errRecord = null;
		private OutputPort errPort = null;

		private DataRecordMetadata errMetadata; // format as output port

		// #@ dek 2, Sloupec 3: Neplatn hodnota znaku pro uren pevodu (CAST). @#
		private String strBadRowPattern = "\\D+(\\d+)\\D+(\\d+): (.+)";
		private Matcher badRowMatcher;

		private Log logger = LogFactory.getLog(PortDataConsumer.class);

		private int rowNumberFieldNo; // last field -2
		private int columnNumberFieldNo; // last field - 1
		private int errMsgFieldNo; // last field
		private final static int NUMBER_OF_ADDED_FIELDS = 3; // number of addded fields in errPortMetadata against dbIn(Out)Metadata

		/**
		 * Constructor for the MsSqlBadRowReaderWriter object
		 * 
		 * @param errPort Output port receiving consumed data.
		 * @throws ComponentNotReadyException
		 */
		private MsSqlBadRowReaderWriter(OutputPort errPort) throws ComponentNotReadyException {
			if (errPort == null) {
				throw new ComponentNotReadyException("No output port was found.");
			}

			this.errPort = errPort;

			errMetadata = errPort.getMetadata();
			if (errMetadata == null) {
				throw new ComponentNotReadyException("Output port hasn't assigned metadata.");
			}

			getNumberOfAddedFields();
			checkErrPortMetadata();

			errRecord = DataRecordFactory.newRecord(errMetadata);
			errRecord.init();

			this.dbOutMetadata = createDbOutMetadata();

			dbOutRecord = DataRecordFactory.newRecord(dbOutMetadata);
			dbOutRecord.init();

			dbParser = new DelimitedDataParser(dbOutMetadata, Defaults.DataParser.DEFAULT_CHARSET_DECODER);
			dbParser.init();

			Pattern badRowPattern = Pattern.compile(strBadRowPattern);
			badRowMatcher = badRowPattern.matcher("");
		}

		/**
		 * Gets index of added fields (rowNumber, columnNumber and errMsg).
		 */
		private void getNumberOfAddedFields() {
			int numFields = errMetadata.getNumFields();
			rowNumberFieldNo = numFields - 3;
			columnNumberFieldNo = numFields - 2;
			errMsgFieldNo = numFields - 1;
		}

		/**
		 * Create metadata so that they correspond to format of bcp error file
		 * 
		 * @return modified metadata
		 * @throws ComponentNotReadyException
		 */
		private DataRecordMetadata createDbOutMetadata() {
			DataRecordMetadata metadata = errMetadata.duplicate();
			metadata.setRecType(DataRecordMetadata.DELIMITED_RECORD);
			// delete last three fields
			for (int i = 0; i < NUMBER_OF_ADDED_FIELDS; i++) {
				metadata.delField(metadata.getNumFields() - 1);
			}

			for (DataFieldMetadata fieldMetadata : metadata) {
				fieldMetadata.setDelimiter(DEFAULT_COLUMN_DELIMITER);

				if (fieldMetadata.getType() == DataFieldMetadata.DATE_FIELD
						|| fieldMetadata.getType() == DataFieldMetadata.DATETIME_FIELD) {
					fieldMetadata.setFormatStr(DEFAULT_DATETIME_FORMAT);
				}
			}
			// re-set last delimiter
			metadata.getField(metadata.getNumFields() - 1).setDelimiter(DEFAULT_RECORD_DELIMITER);
			metadata.setRecordDelimiter("");

			return metadata;
		}

		/**
		 * check metadata at error port against metadata at input port
		 * if metadata isn't correct then throws ComponentNotReadyException
		 * 
		 * @param errMetadata
		 * @throws ComponentNotReadyException when metadata isn't correct
		 */
		private void checkErrPortMetadata() throws ComponentNotReadyException {
			// check if last - 2 field of errMetadata is integer - rowNumber
			if (errMetadata.getFieldType(rowNumberFieldNo) != DataFieldMetadata.INTEGER_FIELD) {
				throw new ComponentNotReadyException("Last but two field of " + StringUtils.quote(errMetadata.getName())
						+ " has different type from integer.");
			}

			// check if last - 1 field of errMetadata is integer - columnNumber
			if (errMetadata.getFieldType(columnNumberFieldNo) != DataFieldMetadata.INTEGER_FIELD) {
				throw new ComponentNotReadyException("Last but one field of " + StringUtils.quote(errMetadata.getName())
						+ " has different type from integer.");
			}

			// check if last field of errMetadata is string - errMsg
			if (errMetadata.getFieldType(errMsgFieldNo) != DataFieldMetadata.STRING_FIELD) {
				throw new ComponentNotReadyException("Last field of " + StringUtils.quote(errMetadata.getName())
						+ " has different type from string.");
			}

			if (dbMetadata == null) {
				return;
			}

			// check number of fields; if inNumFields == outNumFields + NUMBER_OF_ADDED_FIELDS
			if (errMetadata.getNumFields() != dbMetadata.getNumFields() + NUMBER_OF_ADDED_FIELDS) {
				throw new ComponentNotReadyException("Number of fields of " + StringUtils.quote(errMetadata.getName())
						+ " isn't equal number of fields of " + StringUtils.quote(dbMetadata.getName()) + " + "
						+ NUMBER_OF_ADDED_FIELDS + ".");
			}

			// check if other fields' type of errMetadata are equals as dbMetadata
			int count = 0;
			for (DataFieldMetadata dbFieldMetadata : dbMetadata) {
				if (!dbFieldMetadata.equals(errMetadata.getField(count++))) {
					throw new ComponentNotReadyException("Field " + StringUtils.quote(errMetadata.getField(count - 1).getName())
							+ " in " + StringUtils.quote(errMetadata.getName()) + " has different type from field "
							+ StringUtils.quote(dbFieldMetadata.getName()) + " in " 
							+ StringUtils.quote(dbMetadata.getName()) + ".");
				}
			}
		}

		/**
		 * Example of bad row in stream: 
		 * #@ dek 2, Sloupec 3: Neplatn hodnota znaku pro uren pevodu (CAST). @# 
		 * a s 32s 32.00 1970-01-01 12:34:45.000 fsd
		 * @throws JetelException
		 * 
		 * @see org.jetel.util.exec.DataConsumer
		 */
		private void run() throws JetelException {
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
							line = line + DEFAULT_RECORD_DELIMITER;
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
		 * 
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
			errRecord.getField(rowNumberFieldNo).setValue(rowNumber);
			errRecord.getField(columnNumberFieldNo).setValue(columnNumber);
			errRecord.getField(errMsgFieldNo).setValue(errMsg);

			for (int dbFieldNum = 0; dbFieldNum < dbRecord.getNumFields(); dbFieldNum++) {
				errRecord.getField(dbFieldNum).setValue(dbRecord.getField(dbFieldNum));
			}
			return errRecord;
		}

		/**
		 * It create and return InputStream from string
		 * 
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
				try {
					dbParser.close();
				} catch (IOException e) {
					logger.warn("DB parser wasn't closed.", e);
				}
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