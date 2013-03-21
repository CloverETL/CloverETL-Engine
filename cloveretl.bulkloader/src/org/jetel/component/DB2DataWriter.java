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
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.Defaults;
import org.jetel.data.formatter.DelimitedDataFormatter;
import org.jetel.data.formatter.FixLenDataFormatter;
import org.jetel.data.formatter.Formatter;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.ConfigurationStatus.Priority;
import org.jetel.exception.ConfigurationStatus.Severity;
import org.jetel.exception.JetelException;
import org.jetel.exception.TempFileCreationException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPort;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.CommandBuilder;
import org.jetel.util.ExceptionUtils;
import org.jetel.util.exec.DataConsumer;
import org.jetel.util.exec.LoggerDataConsumer;
import org.jetel.util.exec.PlatformUtils;
import org.jetel.util.exec.ProcBox;
import org.jetel.util.file.FileUtils;
import org.jetel.util.joinKey.JoinKeyUtils;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.property.RefResFlag;
import org.jetel.util.string.StringUtils;
import org.w3c.dom.Element;

/**
 *  <h3>DB2 data writer</h3>
 *
 * <table border="1">
 *  <th>Component:</th><td>&nbsp;</td>
 * <tr><td><h4><i>Name:</i></h4></td>
 * <td>DB2 data writer</td></tr>
 * <tr><td><h4><i>Category:</i></h4></td>
 * <td>&nbsp;</td></tr>
 * <tr><td><h4><i>Description:</i></h4></td>
 * <td> This component loads data to DB2 database using db2 load utility. There is created 
 * temporary file with db2 commands depending on input parameters. Data are read from given 
 * input file or from input port and loaded to database. On Linux/Unix system data transfer 
 * can be processed by named pipe.<br>Any generated scripts/commands can be optionally 
 * logged to help diagnose problems.<br>To use this component DB2 client must be installed 
 * and configured on the local host. Server and database have to be cataloged 
 * (see <a href="http://publib.boulder.ibm.com/infocenter/db2luw/v8/index.jsp?topic=/com.ibm.db2.udb.doc/opt/t0010791.htm">
 * Configuring a client to Query Patroller server connection using the command line processor</a>).<br>
 * <i>Note: In CloverETL date data field stores date as well as time or time stamp 
 * (date & time). In DB2 thees are three different types. The component recognizes them 
 * due to the format set on metadata, so be positive that formats on metadata are set 
 * correctly.</i>  </td></tr>
 * <tr><td><h4><i>Inputs:</i></h4></td>
 * <td>[0] - input records. It can be omited (mostly for debugging reasons). 
 * Then <b>file URL</b> has to be provided.</td></tr>
 * <tr><td><h4><i>Outputs:</i></h4></td>
 * <td>[0] - optionally one output port defined/connected - information about rejected 
 * records. Metadata on this port must have two numeric and one string field 
 * (eg. err_record - integer, err_column - integer, err_message - string). Rejected record 
 * number is recorded to first numeric field, and field number (for delimited metadata) or 
 * offset of offending value (for fix length metadata) is recorded to second numeric field. 
 * String field is fulfilled by error message.</td></tr>
 * <tr><td><h4><i>Comment:</i></h4></td>
 * <td>&nbsp;</td></tr>
 * </table>
 * <br>
 * <table border="1">
 * <th>XML attributes:</th><td>&nbsp;</td>
 *  <tr><td><b>type</b></td><td>"DB2_DATA_WRITER"</td></tr>
 *  <tr><td><b>id</b></td><td>component identification</td></tr>
 *  <tr><td><b>database</b></td><td> name of the database</td></tr>
 *  <tr><td><b>username</b></td><td>user name for DB2 database connection</td></tr>
 *  <tr><td><b>password</b></td><td>password for DB2 database connection</td></tr>
 *  <tr><td><b>table</b></td><td>DB2 table to store the records</td></tr>
 *  <tr><td><b>loadMode</b></td><td>one of: insert, replace, restart, terminate.<br>
 *  <i>insert</i> - adds the loaded data to the table without changing the existing table 
 *  data<br><i>replace</i> - deletes all existing data from the table, and inserts the 
 *  loaded data. The table definition and index definitions are not changed.<br><i>restart</i>
 *  - restarts a previously interrupted load operation. The load operation will 
 *  automatically continue from the last consistency point in the load, build, or delete 
 *  phase.<br><i>terminate</i> - terminates a previously interrupted load operation, and 
 *  rolls back the operation to the point in time at which it started, even if consistency 
 *  points were passed.</td></tr>
 *  <tr><td><b>fileURL</b><br><i>optional</i></td><td>Path to data file to be loaded. 
 *  <br>Normally this file is a temporary storage for data to be passed to db2 load utility 
 *  (if named pipe isn't used instead). If file URL is not specified, the file is created 
 *  in Clover or OS temporary directory and deleted after load finishes. 
 *  <br>
 *  If file URL is specified, temporary file is created within given path and name and not 
 *  deleted after being loaded. Next graph run overwrites it. 
 *  <br>
 *  There is one more meaning of this parameter. If input port is not specified, this file 
 *  is used only for reading by db2 load utility and must already contain data in format expected by 
 *  load. The file is neither deleted nor overwritten. 
 *  </td></tr>
 *  <tr><td><b>fileMetadata</b><br><i>optional</i></td><td>Specifies data structure in external 
 *  file. This metadata has to satisfy db2 load data structure, that means: fixed metadata are 
 *  not allowed, each column, except last has the same one char delimiter, last column has the 
 *  "\n" as delimiter</td></tr>
 *  <tr><td><b>columnDelimiter</b><br><i>optional</i></td><td> Defines character, which will be 
 *  used as column delimiter in data file. This attribute has higher priority then "coldel" 
 *  property in "parameters" attribute.</td></tr>
 *  <tr><td><b>fieldMap</b><br><i>optional</i></td><td>pairs of clover fields and db fields 
 *  (cloverField=dbField) separated by :;| {colon, semicolon, pipe}. It specifies mapping from 
 *  source (Clover's) fields to DB table fields. It should be used instead of <i>cloverFields</i> 
 *  and <i>dbFields</i> attributes, because it provides more clear mapping. If <i>fieldMap</i>
 *   attribute is found <i>cloverFields</i> and <i>dbFields<i> attributes are ignored.</td></tr>
 *  <tr><td><b>dbFields</b><br><i>optional</i></td><td>delimited list of target table's fields 
 *  to be populated. Input fields are mapped onto target fields (listed) in the order they are 
 *  present in Clover's record.</td></tr>
 *  <tr><td><b>cloverFields</b><br><i>optional</i></td><td>delimited list of input record's 
 *  fields.Only listed fields (in the order they appear in the list) will be considered for 
 *  mapping onto target table's fields. Combined with <i>dbFields</i> option you can specify 
 *  mapping from source (Clover's) fields to DB table fields. If no <i>dbFields</i> are 
 *  specified, then number of <i>cloverFields</i> must correspond to number of target DB 
 *  table fields.</td></tr>
 *  <tr><td><b>useNamedPipe</b><br><i>optional</i></td><td>On Linux/Unix OS a named pipe 
 *  can be used (instead of temporary file) to pass data to db2 load utility  
 *  port can be sent to pipe instead of temporary file</td></tr>
 *  <tr><td><b>sqlInterpreter</b><br><i>optional</i></td><td>Defines process and its parameters 
 *  to execute script with db2 commands (connect, load, disconnect). It has to have form:
 *  "interpreter name [parameters] ${} [parameters]", where in place of ${} will be put the 
 *  name of script file. Default values are: <i>db2 -f scriptFileName</i> - for Linux/Unix and 
 *  <i>db2cmd /c /i /w -f scriptFileName</i> - for Windows</td></tr>
 *  <tr><td><b>rejectedURL</b><br><i>optional</i></td><td> File (on db2 server) where rejected 
 *  records will be saved. <b>This file has to be in directory owned by database user</b>.</td></tr>
 *  <tr><td><b>batchURL</b><br><i>optional</i></td><td>Url of the file where connect, load and 
 *  disconnect commands for db2 load utility are stored. 
 *  <br>Normally the batch file is automatically generated, stored in current directory and deleted
 *  after load finishes. 
 *  If the file URL is specified, DataWriter tries to use it as is (generates it only if it doesn't 
 *  exist or its length is 0) and doesn't delete it after load finishes. 
 *  (It is reasonable to use this parameter in connection with <b>fileURL</b>, because batch file 
 *  contains name of temporary data file, which is randomly generated, if not provided explicitly).
 *  <br><b>Path can't contain white space characters.</b></td></tr>
 *  <tr><td><b>recordCount</b><br><i>optional</i></td><td>specifies how many records/rows should 
 *  be written to the output file. If <i>rowcount</> is set in <i>parameters</i> attribute, 
 *  value from <i>parameters</i> attribute will be used.</td></tr>
 *  <tr><td><b>recordSkip</b><br><i>optional</i></td><td>specifies how many records/rows should 
 *  be skipped before of writing the first record to the output file. <i>It can be used only in 
 *  case data is read from input port, not a flat file.</i></td></tr>
 *  <tr><td><b>maxErrors</b><br><i>optional</i></td><td> Stops the load operation after n 
 *  warnings. If n is zero, or this option is not specified, the load operation will continue 
 *  regardless of the number of warnings issued. If the load operation is stopped because the 
 *  threshold of warnings was encountered, another load operation can be started in RESTART mode. 
 *  The load operation will automatically continue from the last consistency point. Alternatively, 
 *  another load operation can be initiated in REPLACE mode, starting at the beginning of the 
 *  input file. If <i>warningcount,/i> is set in <i>parameters</i> attribute, value from 
 *  <i>parameters</i> attribute will be used.</b></td></tr>
 *  <tr><td><b>capturedWarningLines</b><br><i>optional</i></td><td>Maximal number of printed out 
 *  error messages or warnings.</td></tr>
 *  <tr><td><b>parameters</b><br><i>optional</i></td><td>All possible additional parameters 
 *  which can be passed on to load method (See  <a href="http://publib.boulder.ibm.com/infocenter/db2luw/v8/index.jsp?topic=/com.ibm.db2.udb.doc/core/r0008305.htm">
 * LOAD Command</a>  and <a href="http://publib.boulder.ibm.com/infocenter/db2luw/v8/topic/com.ibm.db2.udb.doc/core/r0011044.htm">
 * File type modifiers for load</a>). Parameters, in form <i>key=value</i> (or <i>key</i> - 
 * interpreted as <i>key=true</i>, if possible value are only "true" or "false") has to be 
 * separated by :;| {colon, semicolon, pipe}. If in parameter value occurs one of :;|, value 
 * has to be double quoted.<br><b>Load parameters</b><table>
 * <tr><td>lobsurl</td><td>The path to the data files containing LOB values to be loaded. 
 * The path must end with a slash (/). The names of the LOB data files are stored in the 
 * main data file, in the column that will be loaded into the LOB column.</td></tr>
 * <tr><td>anyorder</td><td>This modifier is used in conjunction with the <i>cpunum</i> 
 * parameter. Specifies that the preservation of source data order is not required, yielding 
 * significant additional performance benefit on SMP systems. If the value of <i>cpunum</i> 
 * is 1, this option is ignored. This option is not supported if <i>savecount</i>> 0, since 
 * crash recovery after a consistency point requires that data be loaded in sequence.</td></tr>
 * <tr><td>generatedignore</td><td>This modifier informs the load utility that data for all 
 * generated columns is present in the data file but should be ignored. This results in all 
 * generated column values being generated by the utility. This modifier cannot be used with 
 * either the <i>generatedmissing</i> or the <i>generatedoverride</i> modifier.</td></tr>
 * <tr><td>generatedmissing</td><td>If this modifier is specified, the utility assumes that 
 * the input data file contains no data for the generated column (not even NULLs). This 
 * results in all generated column values being generated by the utility. This modifier cannot 
 * be used with either the <i>generatedignore</i> or the <i>generatedoverride</i> modifier.</td></tr>
 * <tr><td>generatedoverride</td><td>This modifier instructs the load utility to accept 
 * user-supplied data for all generated columns in the table (contrary to the normal rules for 
 * these types of columns). When this modifier is used, any rows with no data or NULL data for 
 * a non-nullable generated column will be rejected (SQL3116W). This modifier cannot be used 
 * with either the <i>generatedmissing</i> or the <i>generatedignore</i> modifier.</td></tr>
 * <tr><td>identityignore</td><td>This modifier informs the load utility that data for the 
 * identity column is present in the data file but should be ignored. This results in all 
 * identity values being generated by the utility. The behaviour will be the same for both 
 * GENERATED ALWAYS and GENERATED BY DEFAULT identity columns. This means that for 
 * GENERATED ALWAYS columns, no rows will be rejected. This modifier cannot be used with either 
 * the <i>identitymissing</i> or the <i>identityoverride</i> modifier.</td></tr>
 * <tr><td>identitymissing</td><td>If this modifier is specified, the utility assumes that the 
 * input data file contains no data for the identity column (not even NULLs), and will 
 * therefore generate a value for each row. The behaviour will be the same for both 
 * GENERATED ALWAYS and GENERATED BY DEFAULT identity columns. This modifier cannot be used 
 * with either the <i>identityignore</i> or the <i>identityoverride</i> modifier.</td></tr>
 * <tr><td>identityoverride</td><td>This modifier should be used only when an identity column 
 * defined as GENERATED ALWAYS is present in the table to be loaded. It instructs the utility 
 * to accept explicit, non-NULL data for such a column (contrary to the normal rules for these 
 * types of identity columns). When this modifier is used, any rows with no data or NULL data 
 * for the identity column will be rejected (SQL3116W). This modifier cannot be used with 
 * either the <i>identitymissing</i> or the <i>identityignore</i> modifier.</td></tr>
 * <tr><td>indexfreespace</td><td>an integer between 0 and 99 inclusive. The value is 
 * interpreted as the percentage of each index page that is to be left as free space when 
 * load rebuilds the index. Load with <i>indexingmode=incremental</i> ignores this option. The 
 * first entry in a page is added without restriction; subsequent entries are added the percent 
 * free space threshold can be maintained. The default value is the one used at CREATE INDEX 
 * time.</td></tr>
 * <tr><td>norowwarnings</td><td>Suppresses all warnings about rejected rows.</td></tr>
 * <tr><td>pagefreespace</td><td>integer between 0 and 100 inclusive. The value is interpreted 
 * as the percentage of each data page that is to be left as free space. If the specified value 
 * is invalid because of the minimum row size, (for example, a row that is at least 3 000 bytes 
 * long, and an x value of 50), the row will be placed on a new page. If a value of 100 is 
 * specified, each row will reside on a new page.</td></tr>
 * <tr><td>subtableconvert</td><td>Valid only when loading into a single sub-table. Typical 
 * usage is to export data from a regular table, and then to invoke a load operation (using 
 * this modifier) to convert the data into a single sub-table.</td></tr>
 * <tr><td>totalfreespace</td><td>an integer greater than or equal to 0 . The value is 
 * interpreted as the percentage of the total pages in the table that is to be appended to the 
 * end of the table as free space. For example, if x is 20, and the table has 100 data pages 
 * after the data has been loaded, 20 additional empty pages will be appended. The total number 
 * of data pages for the table will be 120. The data pages total does not factor in the number 
 * of index pages in the table. This option does not affect the index object.</td></tr>
 * <tr><td>usedefaults</td><td>If a source column for a target table column has been specified, 
 * but it contains no data for one or more row instances, default values are loaded.</td></tr>
 * <tr><td>codepage</td><td>an ASCII character string. The value is interpreted as the code 
 * page of the data in the input data set. Converts character data (and numeric data specified 
 * in characters) from this code page to the database code page during the load operation. Use 
 * this parameter only when loading from extern file. Data loaded from the input port are 
 * properly coded by component (<i>codepage=1208</i>). See also <a href="http://publib.boulder.ibm.com/infocenter/db2luw/v8/index.jsp?topic=/com.ibm.db2.udb.doc/admin/r0004565.htm">
 * Supported territory codes and code pages</a>.</td></tr>
 * <tr><td>dateformat</td><td>format of the date in the source file. You needn't set this 
 * parameter if date format is specified by metadata or data is read from input port (used 
 * default formatting).</td></tr>
 * <tr><td>timeformat</td><td>the format of the time in the source file. You needn't set this 
 * parameter if date format is specified by metadata or data is read from input port (used 
 * default formatting).</td></tr>
 * <tr><td>timestampformat</td><td>the format of the time stamp in the source file. You needn't 
 * set this parameter if date format is specified by metadata or data is read from input port 
 * (used default formatting).</td></tr>
 * <tr><td>dumpfile</td><td>the fully qualified (according to the server database partition) 
 * name of an exception file to which rejected rows are written. A maximum of 32 KB of data is 
 * written per record. This parameter is equivalent to <i>rejectedURL</i> attribute.</td></tr>
 * <tr><td>dumpfileaccessall</td><td>Grants read access to 'OTHERS' when a dump file is created. 
 * This file type modifier is only valid when: <ol><li>it is used in conjunction with 
 * dumpfile file type modifier</li><li>the user has SELECT privilege on the load target 
 * table</li><li>it is issued on a DB2 server database partition that resides on a 
 * UNIX-based operating system.</li></ol></td></tr>
 * <tr><td>fastparse</td><td>Reduced syntax checking is done on user-supplied column values, 
 * and performance is enhanced. Tables loaded under this option are guaranteed to be 
 * architecturally correct, and the utility is guaranteed to perform sufficient data checking 
 * to prevent a segmentation violation or trap. Data that is in correct form will be loaded 
 * correctly.</td></tr>
 * <tr><td>implieddecimal</td><td>The location of an implied decimal point is determined by 
 * the column definition. It is no longer assumed to be at the end of the value. For example, 
 * the value 12345 is loaded into a DECIMAL(8,2) column as 123.45, not 12345.00. This modifier 
 * cannot be used with the <i>packeddecimal</i> modifier.</td></tr>
 * <tr><td>noeofchar</td><td>The optional end-of-file character x'1A' is not recognized as the 
 * end of file. Processing continues as if it were a normal character.</td></tr>
 * <tr><td>usegraphiccodepage</td><td>If <i>usegraphiccodepage</i> is given, the assumption 
 * is made that data being loaded into graphic or double-byte character large object 
 * (DBCLOB) data field(s) is in the graphic code page. The rest of the data is assumed to be 
 * in the character code page. The graphic codepage is associated with the character code 
 * page. LOAD determines the character code page through either the <i>codepage</i> modifier, 
 * if it is specified, or through the code page of the database if the <i>codepage</i> 
 * modifier is not specified.</td></tr>
 * <tr><td>binarynumerics</td><td>Numeric (but not DECIMAL) data must be in binary form, not 
 * the character representation. This avoids costly conversions.This option is supported only 
 * with fixed length records specified by the <i>reclen</i> option. The <i>noeofchar</i> option 
 * is assumed.The following rules apply: <ul><li>No conversion between data types is performed, 
 * with the exception of BIGINT, INTEGER, and SMALLINT.</li><li>Data lengths must match their 
 * target column definitions.</li><li>FLOATs must be in IEEE Floating Point format.</li><li>
 * Binary data in the load source file is assumed to be big-endian, regardless of the platform 
 * on which the load operation is running.</li></ul> <i>Note: NULLs cannot be present in the 
 * data for columns affected by this modifier. Blanks (normally interpreted as NULL) are 
 * interpreted as a binary value when this modifier is used.</i></td></tr>
 * <tr><td>nochecklengths</td><td>If <i>nochecklengths</i> is specified, an attempt is made to 
 * load each row, even if the source data has a column definition that exceeds the size of the 
 * target table column. Such rows can be successfully loaded if code page conversion causes the 
 * source data to shrink; for example, 4-byte EUC data in the source could shrink to 2-byte 
 * DBCS data in the target, and require half the space. This option is particularly useful if 
 * it is known that the source data will fit in all cases despite mismatched column definitions.</td></tr>
 * <tr><td>nullindchar</td><td>single character. Changes the character denoting a NULL value to 
 * given character. The default value of is Y. This modifier is case sensitive for EBCDIC 
 * data files, except when the character is an English letter. For example, if the NULL 
 * indicator character is specified to be the letter N, then n is also recognized as a NULL 
 * indicator.</td></tr>
 * <tr><td>packeddecimal</td><td>Loads packed-decimal data directly, since the 
 * <i>binarynumerics</i> modifier does not include the DECIMAL field type. This option is 
 * supported only with fixed length records specified by the <i>reclen</i> option. The 
 * <i>noeofchar</i> option is assumed. Supported values for the sign nibble are:<br>
 * + = 0xC 0xA 0xE 0xF<br> - = 0xD 0xB<br>NULLs cannot be present in the data for columns 
 * affected by this modifier. Blanks (normally interpreted as NULL) are interpreted as a 
 * binary value when this modifier is used. Regardless of the server platform, the byte 
 * order of binary data in the load source file is assumed to be big-endian; that is, when 
 * using this modifier on Windows operating systems, the byte order must not be reversed.
 * This modifier cannot be used with the <i>implieddecimal</i> modifier.</td></tr>
 * <tr><td>reclen</td><td>an integer with a maximum value of 32 767. <i>reclen</i> characters 
 * are read for each row, and a new-line character is not used to indicate the end of the row.</td></tr>
 * <tr><td>striptblanks</td><td>Truncates any trailing blank spaces when loading data into a 
 * variable-length field. This option cannot be specified together with <i>striptnulls.</i></td></tr>
 * <tr><td>striptnulls</td><td>Truncates any trailing NULLs (0??00 characters) when loading data 
 * into a variable-length field. This option cannot be specified together with <i>striptblanks</i>.</td></tr>
 * <tr><td>zoneddecimal</td><td>Loads zoned decimal data, since the binarynumerics modifier 
 * does not include the DECIMAL field type. This option is supported only with fixed length 
 * records specified by the <i>reclen</i> option. The <i>noeofchar</i> option is assumed. 
 * Half-byte sign values can be one of the following:<br>+ = 0xC 0xA 0xE 0xF<br>- = 0xD 0xB<br>
 * Supported values for digits are 0??0 to 0??9. Supported values for zones are 0??3 and 0xF. </td></tr>
 * <tr><td>chardel</td><td>single character string delimiter. The default value is a double 
 * quotation mark ("). The specified character is used in place of double quotation marks to 
 * enclose a character string.</td></tr>
 * <tr><td>coldel</td><td>single character column delimiter. The default value is a comma (,). 
 * The specified character is used in place of a comma to signal the end of a column. This 
 * parameter has lower prioryty then <i>columnDelimiter</i> attribute.</td></tr>
 * <tr><td>datesiso</td><td>Date format. Causes all date data values to be loaded in ISO format.</td></tr>
 * <tr><td>decplusblank</td><td>Plus sign character. Causes positive decimal values to be 
 * prefixed with a blank space instead of a plus sign (+). The default action is to prefix 
 * positive decimal values with a plus sign.</td></tr>
 * <tr><td>decpt</td><td>single character substitute for the period as a decimal point 
 * character. The default value is a period (.). The specified character is used in place of a 
 * period as a decimal point character.</td></tr>
 * <tr><td>delprioritychar</td><td>The current default priority for delimiters is: record 
 * delimiter, character delimiter, column delimiter. This modifier protects existing 
 * applications that depend on the older priority by reverting the delimiter priorities to: 
 * character delimiter, record delimiter, column delimiter.</td></tr>
 * <tr><td>keepblanks</td><td>Preserves the leading and trailing blanks in each field of type 
 * CHAR, VARCHAR, LONG VARCHAR, or CLOB. Without this option, all leading and tailing blanks 
 * that are not inside character delimiters are removed, and a NULL is inserted into the table 
 * for all blank fields.</td></tr>
 * <tr><td>nochardel</td><td>The load utility will assume all bytes found between the column 
 * delimiters to be part of the column's data. Character delimiters will be parsed as part of 
 * column data.</td></tr>
 * <tr><td>nodoubledel</td><td>Suppresses recognition of double character delimiters.</td></tr>
 * <tr><td>nullindicators</td><td>comma-separated list of positive integers specifying the 
 * column number of each null indicator field. The column number is the byte offset of the 
 * null indicator field from the beginning of a row of data. A column number of zero indicates 
 * that the corresponding data field always contains data. A value of Y in the NULL indicator 
 * column specifies that the column data is NULL. Any character other than Y in the NULL 
 * indicator column specifies that the column data is not NULL. The NULL indicator character 
 * can be changed using the <i>nullindchar</i> option. For example <i>nullinicators=0,0,23,32</i>
 *  means that positions 23 and 32 are used to indicate whether third and fourth columns 
 *  will be loaded NULL for a given row. If there is a Y in the column's null indicator 
 *  position for a given record, the column will be NULL.</td></tr>
 * <tr><td>savecount</td><td>Specifies that the load utility is to establish consistency points 
 * after every given number of rows. This value is converted to a page count, and rounded up to 
 * intervals of the extent size. The default value is zero, meaning that no consistency points 
 * will be established, unless necessary.</td></tr>
 * <tr><td>rowcount</td><td>Specifies the number of physical records in the file to be loaded. 
 * Allows a user to load only the first n rows in a file. It is the same as <i>recordCount</i> 
 * attribute.</td></tr>
 * <tr><td>warningcount</td><td>Stops the load operation after n warnings. It is the same as 
 * <i>maxErrors,/i> attribute</td></tr>
 * <tr><td>messagesurl</td><td>Specifies the destination for warning and error messages that occur 
 * during the load operation.</td></tr>
 * <tr><td>tmpurl</td><td>Specifies the name of the path to be used when creating temporary 
 * files during a load operation, and should be fully qualified according to the server 
 * database partition.</td></tr>
 * <tr><td>exceptiontable</td><td>Specifies the exception table into which rows in error will 
 * be copied. Any row that is in violation of a unique index or a primary key index is copied. 
 * Information that is written to the exception table is not written to the dump file. In a 
 * partitioned database environment, an exception table must be defined for those partitions 
 * on which the loading table is defined. The dump file, on the other hand, contains rows that 
 * cannot be loaded because they are invalid or have syntax errors.</td></tr>
 * <tr><td>statistics</td><td>Instructs load to collect statistics during the load according to 
 * the profile defined for this table. This profile must be created before load is executed. If 
 * the profile does not exist and load is instructed to collect statistics, a warning is 
 * returned and no statistics are collected.</tr>
 * <tr><td>copy</td><td>Specifies that a copy of the loaded data will be saved.</td></tr>
 * <tr><td>usetsm</td><td>Specifies that the copy will be stored using Tivoli Storage Manager (TSM).</td></tr>
 * <tr><td>numsesions</td><td>The number of I/O sessions to be used with TSM or the vendor product. 
 * The default value is 1.</td></tr>
 * <tr><td>recoverylib</td><td>The name of the shared library (DLL on Windows operating systems) 
 * containing the vendor backup and restore I/O functions to be used. It can contain the full 
 * path. If the full path is not given, it will default to the path where the user exit 
 * programs reside.</td></tr>
 * <tr><td>copyurl</td><td>Specifies the device or directory on which the copy image will be created.</i></td></tr>
 * <tr><td>nonrecoverable</td><td>Specifies that the load transaction is to be marked as 
 * non-recoverable and that it will not be possible to recover it by a subsequent roll forward 
 * action. The roll forward utility will skip the transaction and will mark the table into 
 * which data was being loaded as "invalid". The utility will also ignore any subsequent 
 * transactions against that table. After the roll forward operation is completed, such a table 
 * can only be dropped or restored from a backup (full or table space) taken after a commit 
 * point following the completion of the non-recoverable load operation.</td></tr>
 * <tr><td>withoutprompting</td><td>Specifies that the list of data files contains all the 
 * files that are to be loaded, and that the devices or directories listed are sufficient for 
 * the entire load operation. If a continuation input file is not found, or the copy targets 
 * are filled before the load operation finishes, the load operation will fail, and the table 
 * will remain in load pending state.</td></tr>
 * <tr><td>buffersize</td><td>Specifies the number of 4KB pages (regardless of the degree of 
 * parallelism) to use as buffered space for transferring data within the utility. If the value 
 * specified is less than the algorithmic minimum, the minimum required resource is used, and 
 * no warning is returned. This memory is allocated directly from the utility heap, whose size 
 * can be modified through the util_heap_sz database configuration parameter. If a value is not 
 * specified, an intelligent default is calculated by the utility at run time. The default is 
 * based on a percentage of the free space available in the utility heap at the instantiation 
 * time of the loader, as well as some characteristics of the table.</td></tr>
 * <tr><td>sortbuffersize</td><td>This option specifies a value that overrides the SORTHEAP 
 * database configuration parameter during a load operation. It is relevant only when loading 
 * tables with indexes and only when the <i>indexingmode</i> parameter is not specified as 
 * <i>deferred</i>. The value that is specified cannot exceed the value of SORTHEAP. This 
 * parameter is useful for throttling the sort memory that is used when loading tables with 
 * many indexes without changing the value of SORTHEAP, which would also affect general 
 * query processing.</td></tr>
 * <tr><td>cpunum</td><td>Specifies the number of processes or threads that the load utility 
 * will spawn for parsing, converting, and formatting records when building table objects. This 
 * parameter is designed to exploit intra-partition parallelism. It is particularly useful when 
 * loading presorted data, because record order in the source data is preserved. If the value 
 * of this parameter is zero, or has not been specified, the load utility uses an intelligent 
 * default value (usually based on the number of CPUs available) at run time.</i></td></tr>
 * <tr><td>disknum</td><td>Specifies the number of processes or threads that the load utility 
 * will spawn for writing data to the table space containers. If a value is not specified, the 
 * utility selects an intelligent default based on the number of table space containers and the 
 * characteristics of the table.</td></tr>
 * <tr><td>indexingmode</td><td>Specifies whether the load utility is to rebuild indexes or 
 * to extend them incrementally. Valid values are:<ul><li>autoselect - The load utility will 
 * automatically decide between rebuild or incremental mode. The decision is based on the 
 * amount of data being loaded and the depth of the index tree. Information relating to the 
 * depth of the index tree is stored in the index object. <i>autoselect</i> is the default 
 * indexing mode.</li><li>rebuild - All indexes will be rebuilt. The utility must have 
 * sufficient resources to sort all index key parts for both old and appended table data. 
 * </li><li>incremental - Indexes will be extended with new data. This approach consumes 
 * index free space. It only requires enough sort space to append index keys for the 
 * inserted records. This method is only supported in cases where the index object is valid 
 * and accessible at the start of a load operation (it is, for example, not valid 
 * immediately following a load operation in which the deferred mode was specified). If this 
 * mode is specified, but not supported due to the state of the index, a warning is returned, 
 * and the load operation continues in <i>rebuild</i> mode. Similarly, if a load restart 
 * operation is begun in the load build phase, <i>incremental</I> mode is not supported. 
 * <i>Incremental</i> indexing is not supported when all of the following conditions are 
 * true: <ul><li>The LOAD COPY option is specified (logretain or userexit is enabled).</li><li>
 * The table resides in a DMS table space.</li><li>The index object resides in a table space 
 * that is shared by other table objects belonging to the table being loaded. To bypass this 
 * restriction, it is recommended that indexes be placed in a separate table space.</li></ul><li>
 * deferred - The load utility will not attempt index creation if this mode is specified. 
 * Indexes will be marked as needing a refresh. The first access to such indexes that is 
 * unrelated to a load operation might force a rebuild, or indexes might be rebuilt when 
 * the database is restarted. This approach requires enough sort space for all key parts 
 * for the largest index. The total time subsequently taken for index construction is longer 
 * than that required in <i>rebuild</i> mode. Therefore, when performing multiple load 
 * operations with deferred indexing, it is advisable (from a performance viewpoint) to let 
 * the last load operation in the sequence perform an index <i>rebuild</i>, rather than 
 * allow indexes to be rebuilt at first non-load access. <i>Deferred</i> indexing is only 
 * supported for tables with non-unique indexes, so that duplicate keys inserted during the 
 * load phase are not persistent after the load operation.</td></tr>
 * <tr><td>allowreadaccess</td><td>Load will lock the target table in a share mode. The table 
 * state will be set to both LOAD IN PROGRESS and READ ACCESS. Readers can access the non-delta 
 * portion of the data while the table is being load.</td></tr>
 * <tr><td>indexcopytable</td><td>If the indexes are being rebuilt, a shadow copy of the index 
 * is built in table space tablespace-name and copied over to the original table space at the 
 * end of the load during an INDEX COPY PHASE. Only system temporary table spaces can be used 
 * with this option. If not specified then the shadow index will be created in the same table 
 * space as the index object. If the shadow copy is created in the same table space as the 
 * index object, the copy of the shadow index object over the old index object is instantaneous. 
 * If the shadow copy is in a different table space from the index object a physical copy is 
 * performed. This could involve considerable I/O and time. The copy happens while the table 
 * is off line at the end of a load during the INDEX COPY PHASE. Without this option the shadow 
 * index is built in the same table space as the original. Since both the original index and 
 * shadow index by default reside in the same table space simultaneously, there might be 
 * insufficient space to hold both indexes within one table space. Using this option ensures 
 * that you retain enough table space for the indexes. This option is ignored if the user does 
 * not specify <i>indexingmode=rebuild</i> or <i>indexingmode=autoselect</i>. This option 
 * will also be ignored if <i>indexingmode=autoselect</i> and load chooses to incrementally 
 * update the index.</td></tr>
 * <tr><td>checkpendingcascade</td><td>this option allows the user to specify whether or not 
 * the check pending state of the loaded table is immediately cascaded to all descendants 
 * (including descendant foreign key tables, descendant immediate materialized query tables 
 * and descendant immediate staging tables). Possible values:<ul><li>immediate - Indicates 
 * that the check pending state (read or no access mode) for foreign key constraints is 
 * immediately extended to all descendant foreign key tables. If the table has descendant 
 * immediate materialized query tables or descendant immediate staging tables, the check 
 * pending state is extended immediately to the materialized query tables and the staging 
 * tables. Note that for a LOAD INSERT operation, the check pending state is not extended 
 * to descendant foreign key tables even if the <i>checkpendingcascade=immediate</i> option 
 * is specified. When the loaded table is later checked for constraint violations, 
 * descendant foreign key tables that were placed in check pending read state will be put 
 * into check pending no access state.</li><li>deferred - Indicates that only the loaded 
 * table will be placed in the check pending state (read or no access mode). The states of 
 * the descendant foreign key tables, descendant immediate materialized query tables and 
 * descendant immediate staging tables will remain unchanged. Descendant foreign key tables 
 * might later be implicitly placed in the check pending no access state when their parent 
 * tables are checked for constraint violations. Descendant immediate materialized query 
 * tables and descendant immediate staging tables will be implicitly placed in the check 
 * pending no access state when one of its underlying tables is checked for integrity 
 * violations. A warning (SQLSTATE 01586) will be issued to indicate that dependent tables 
 * have been placed in the check pending state. See the Notes section of the SET INTEGRITY 
 * statement in the SQL Reference for when these descendant tables will be put into the 
 * check pending state.</li></ul></td></tr>
 * <tr><td>lockwithforce</td><td>The utility acquires various locks including table locks in 
 * the process of loading. Rather than wait, and possibly timeout, when acquiring a lock, this 
 * option allows load to force off other applications that hold conflicting locks on the target 
 * table. Applications holding conflicting locks on the system catalog tables will not be 
 * forced off by the load utility. Forced applications will roll back and release the locks the 
 * load utility needs. The load utility can then proceed.</td></tr>
 * <tr><td>partitionedConfig</td><td>Allows you to execute a load into a partitioned table. 
 * The <i>partitionedConfig</i> parameter allows you to specify partitioned database-specific 
 * configuration options. (<a href="http://publib.boulder.ibm.com/infocenter/db2luw/v8/topic/com.ibm.db2.udb.doc/admin/r0004613.htm">
 * Partitioned database load configuration options</a>)</td></tr>
 * </table></i></td></tr>
 *  </table>
 *  
 *  <h4>Example:</h4>
 *  Reading data from input port:
 *  <pre>&lt;Node database="mydb" userName="db2inst1" password="clover" table="mytab" 
 *  loadMode="insert" recordCount="20" id="DB2_DATA_WRITER1" type="DB2_DATA_WRITER"/&gt;
 *  </pre>
 *  Reading data from flat file:
 *  <pre>&lt;Node database="mydb" fileMetadata="Metadata0"  userName="db2inst1"  
 *  password="clover" table="mytab" loadMode="insert" 
 *  batchURL="${WORKSPACE}/output/sql.tmp" 
 *  fileURL="${WORKSPACE}/data/delimited/mytab_del.txt" parameters="dumpfileaccessall" 
 *  rejectedURL="/home/db2inst1/rejected.txt" id="DB2_DATA_WRITER0" type="DB2_DATA_WRITER"/&gt;
 *  
 * @author avackova (agata.vackova@javlinconsulting.cz) ; 
 * (c) JavlinConsulting s.r.o.
 *  www.javlinconsulting.cz
 *
 * @since Aug 3, 2007
 *
 */
public class DB2DataWriter extends Node {
	
	private enum LoadMode{
		insert,
		replace,
		restart,
		terminate
	}
	
	/**
	 * xml attributes for DB2DataWriterComponent
	 */
	private static final String XML_DATABASE_ATTRIBUTE = "database";
	public static final String XML_USERNAME_ATTRIBUTE = "userName";
	private static final String XML_PASSWORD_ATTRIBUTE = "password";	
    private static final String XML_TABLE_ATTRIBUTE = "table";
    private static final String XML_MODE_ATTRIBUTE = "loadMode";
    private static final String XML_FILEURL_ATTRIBUTE = "fileURL";
    private static final String XML_FILEMETADATA_ATTRIBUTE = "fileMetadata";
    private static final String XML_USEPIPE_ATTRIBUTE = "useNamedPipe";
    private static final String XML_COLUMNDELIMITER_ATTRIBUTE = "columnDelimiter";
	private static final String XML_INTERPRETER_ATTRIBUTE = "sqlInterpreter";
	private static final String XML_FIELDMAP_ATTRIBUTE = "fieldMap";
	private static final String XML_CLOVERFIELDS_ATTRIBUTE = "cloverFields";
	private static final String XML_DBFIELDS_ATTRIBUTE = "dbFields";
	private static final String XML_PARAMETERS_ATTRIBUTE = "parameters";
	private static final String XML_BATCHURL_ATTRIBUTE = "batchURL";
	private static final String XML_REJECTEDRECORDSURL_ATTRIBUTE = "rejectedURL";//on server
	private static final String XML_RECORD_COUNT_ATTRIBUTE = "recordCount";
	private static final String XML_RECORD_SKIP_ATTRIBUTE = "recordSkip";
	private static final String XML_MAXERRORS_ATRIBUTE = "maxErrors";
	private static final String XML_WARNING_LINES_ATTRIBUTE = "capturedWarningLines";
	private static final String XML_FAIL_ON_WARNINGS_ATTRIBUTE = "failOnWarnings";
	
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
	public static final String TMP_URL_PARAM = "tmpurl";
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
	private static final String DEFAULT_TABLE_LOAD_MODE = "insert";
	private static final int DEFAULT_ERROR_WARNINGS_NUMBER = 999;
	
	public final static String COMPONENT_TYPE = "DB2_DATA_WRITER";

	static Log logger = LogFactory.getLog(DB2DataWriter.class);

	private static final String FIXLEN_DATA = "asc";
	private static final String DELIMITED_DATA = "del";

	private final static int READ_FROM_PORT = 0;
	private final static int ERROR_PORT = 0;
	
	private final static String FILE_ENCODING="UTF-8";
	private final static String DB2_UTF_8_CODEPAGE="1208";
	
	private String database;
	private String user;
	private String psw;
	private String fileMetadataName;
	private String table;
	private LoadMode loadMode;
	private boolean usePipe = false;
	private boolean delimitedData;
	private char columnDelimiter = 0;
	private String interpreter;
	private String parameters;
	private String rejectedURL;
	private int recordSkip = -1;
	private int skipped = 0;
	private int warningNumber = DEFAULT_ERROR_WARNINGS_NUMBER;
	private boolean failOnWarnings = false;
	
	private Formatter formatter;
	private DataRecordMetadata fileMetadata;
	private Process proc;
	private DataRecordMetadata inMetadata;
	private DB2DataConsumer consumer;
	private LoggerDataConsumer errConsumer;
	private ProcBox box;
	private InputPort inPort;
	private DataRecord inRecord;
	private String command;
	private Properties properties = new Properties();
	private String[] cloverFields;
	private String[] dbFields;
	private String batchURL;
	private File batchFile;
	private String dataURL;
	private File dataFile;

	/**
	 * Constructor for DB2DataWriterComponent
	 * 
	 * @param id component identification
	 * @param database name of the database
	 * @param user  database user
	 * @param psw 	 password for database user.
	 * @param table name of table to load data
	 * @param mode load mode 
	 * @param dataURL URL to file with extern data
	 * @param fileMetadataId  	 specifies data structure in external file.
	 */
	public DB2DataWriter(String id, String database, String user, String psw, String table, 
			LoadMode mode,	String dataURL, String fileMetadataId) {
		super(id);
		this.database = database;
		this.user = user;
		this.psw = psw;
		this.dataURL = dataURL;
		this.fileMetadataName = fileMetadataId;
		this.table = table;
		this.loadMode = mode;
	}
	
	@Override
	public ConfigurationStatus checkConfig(ConfigurationStatus status) {
        super.checkConfig(status);
        
        if(!checkInputPorts(status, 0, 1) || !checkOutputPorts(status, 0, 1)) {
        	return status;
        }
        //--Check mandatory attributes
		if (StringUtils.isEmpty(database)) {
			status.add(new ConfigurationProblem(StringUtils.quote(XML_DATABASE_ATTRIBUTE) + " attribute have to be set.", Severity.ERROR,
					this, Priority.HIGH, XML_DATABASE_ATTRIBUTE));
		}
		if (StringUtils.isEmpty(table)) {
			status.add(new ConfigurationProblem(StringUtils.quote(XML_TABLE_ATTRIBUTE) + " attribute have to be set.", Severity.ERROR,
					this, Priority.HIGH, XML_TABLE_ATTRIBUTE));
		}
		if (StringUtils.isEmpty(user)) {
			status.add(new ConfigurationProblem(StringUtils.quote(XML_USERNAME_ATTRIBUTE) + " attribute have to be set.", Severity.ERROR,
					this, Priority.HIGH, XML_USERNAME_ATTRIBUTE));
		}
		if (StringUtils.isEmpty(psw)) {
			status.add(new ConfigurationProblem(StringUtils.quote(XML_PASSWORD_ATTRIBUTE) + " attribute have to be set.", Severity.ERROR,
					this, Priority.HIGH, XML_PASSWORD_ATTRIBUTE));
		}
		if (loadMode == null) {
			status.add(new ConfigurationProblem(StringUtils.quote(XML_MODE_ATTRIBUTE) + " attribute have to be set.", Severity.ERROR,
					this, Priority.HIGH, XML_MODE_ATTRIBUTE));
		}
		// --- Check column delimiter 
        if (columnDelimiter == 0 && parameters != null){
			String[] param = StringUtils.split(parameters);
			int index;
			for (String string : param) {
				index = string.indexOf(EQUAL_CHAR);
				if (index > -1) {
					properties.setProperty(string.substring(0, index).toLowerCase(), 
							StringUtils.unquote(string.substring(index + 1)));
				}else{
					properties.setProperty(string.toLowerCase(), String.valueOf(true));
				}
			}
			if (properties.contains(COL_DEL_PARAM)) {
				columnDelimiter = properties.getProperty(COL_DEL_PARAM).charAt(0);
			}			
		}
		if (Character.isWhitespace(columnDelimiter)) {
			status.add(new ConfigurationProblem(StringUtils.quote(String.valueOf(columnDelimiter)) + " is not allowed as column delimiter",
					Severity.ERROR, this, Priority.NORMAL, XML_COLUMNDELIMITER_ATTRIBUTE));
		}
		
		// Check data file
		if (dataURL == null && getInPorts().isEmpty()) {
			status.add(new ConfigurationProblem("There is neither input port nor data file URL specified.", Severity.ERROR, this,
					Priority.NORMAL, XML_FILEURL_ATTRIBUTE));			
		} else if (dataURL != null){
			try {
				initDataFile();
			} catch (ComponentNotReadyException e) {
				status.add(new ConfigurationProblem(ExceptionUtils.getMessage(e), Severity.ERROR, this, Priority.NORMAL, XML_FILEURL_ATTRIBUTE));
			}
		}
		
		//---Data is read from port
		if (!getInPorts().isEmpty()) {
			if (usePipe && PlatformUtils.isWindowsPlatform()) {
				status.add(new ConfigurationProblem("Pipe transfer not supported on Windows", Severity.WARNING, this,
						Priority.NORMAL, XML_USEPIPE_ATTRIBUTE));
			}
			inMetadata = getInputPort(READ_FROM_PORT).getMetadata();
		}
		
		// Check FileMetadata
		if (fileMetadataName != null) {
			try {
				fileMetadata = getGraph().getDataRecordMetadata(fileMetadataName, true);
			} catch (Exception e) {
				status.add(new ConfigurationProblem(ExceptionUtils.getMessage(e), Severity.ERROR, this, Priority.NORMAL, XML_FILEMETADATA_ATTRIBUTE));
			}
			if (fileMetadata == null) {
				status.add(new ConfigurationProblem("File metadata ID is not valid", Severity.ERROR, this, Priority.NORMAL,
						XML_FILEMETADATA_ATTRIBUTE));
			} else {
				if (fileMetadata.getRecType() == DataRecordMetadata.MIXED_RECORD) {
					status.add(new ConfigurationProblem("Only fixlen or delimited metadata allowed", Severity.ERROR, this, Priority.HIGH,
							XML_FILEMETADATA_ATTRIBUTE));
				}
			}
		}
		if (interpreter != null && !interpreter.contains("${}")) {
			status.add(new ConfigurationProblem("Incorect form of " + XML_INTERPRETER_ATTRIBUTE + " attribute:" + interpreter +
					"\nUse form:\"interpreter [parameters] ${} [parameters]\"", Severity.ERROR, this,
					Priority.HIGH, XML_INTERPRETER_ATTRIBUTE));
		}
		try {
			if (batchURL != null) {
				batchFile = new File(FileUtils.getFile(getGraph().getRuntimeContext().getContextURL(), batchURL));
			}
			if (batchFile == null) {
				batchFile = getGraph().getAuthorityProxy().newTempFile("tmp", ".bat", -1);
			}
			if (!batchFile.canWrite()) {
				status.add(new ConfigurationProblem("Can not create batch file", Severity.ERROR, this, Priority.NORMAL));
			}
		} catch (IOException e) {
			status.add(new ConfigurationProblem(ExceptionUtils.getMessage(e), Severity.ERROR, this, Priority.NORMAL, XML_BATCHURL_ATTRIBUTE));
		} catch (TempFileCreationException e) {
			status.add(new ConfigurationProblem(ExceptionUtils.getMessage(e), Severity.ERROR, this, Priority.NORMAL, XML_BATCHURL_ATTRIBUTE));
		}
        return status;
	}

	@Override
	public void init() throws ComponentNotReadyException {
        if(isInitialized()) return;
		super.init();
		
		//read parameters from string to properties
		if (parameters != null){
			String[] param = StringUtils.split(parameters);
			int index;
			for (String string : param) {
				index = string.indexOf(EQUAL_CHAR);
				if (index > -1) {
					properties.setProperty(string.substring(0, index).toLowerCase(), 
							StringUtils.unquote(string.substring(index + 1)));
				}else{
					properties.setProperty(string.toLowerCase(), String.valueOf(true));
				}
			}
		}
		//set field columnDelimiter and proper property to the same value 
		//(columnDelimiter attribute has the higher priority then delimiter set in parameters
		if (columnDelimiter != 0) {
			properties.setProperty(COL_DEL_PARAM, String.valueOf(columnDelimiter));
		}else if (properties.contains(COL_DEL_PARAM)) {
			columnDelimiter = properties.getProperty(COL_DEL_PARAM).charAt(0);
		}
		if (Character.isWhitespace(columnDelimiter)) 
			throw new ComponentNotReadyException(this, XML_COLUMNDELIMITER_ATTRIBUTE, 
					StringUtils.quote(String.valueOf(columnDelimiter)) + 
					" is not allowed as column delimiter");
		
		//prepare metadata for formatting input data
		if (fileMetadataName != null) {
			fileMetadata = getGraph().getDataRecordMetadata(fileMetadataName, true);
		}

		if (!getInPorts().isEmpty()) {
			if (dataURL != null) {
				initDataFile();
			}
			try {
				if (dataFile == null) {
					dataFile = getGraph().getAuthorityProxy().newTempFile("data", ".tmp", -1);
				}
				/*
				 * why?!
				 */
				//dataFile.delete();
			} catch (Exception e) {
				throw new ComponentNotReadyException(this, "Can't create temporary data file", e);
			}			
			if (usePipe && PlatformUtils.isWindowsPlatform()) {
				logger.warn("Node " + this.getId() + " warning: Pipe transfer not " +
						"supported on Windows - switching it off");
				usePipe = false;
			}
			inMetadata = getInputPort(READ_FROM_PORT).getMetadata();
			delimitedData = fileMetadata != null ? 
					fileMetadata.getRecType() != DataRecordMetadata.FIXEDLEN_RECORD : 
					inMetadata.getRecType() != DataRecordMetadata.FIXEDLEN_RECORD;
			if (fileMetadata == null) {
				//create metadata for formatting from input metadata
				switch (inMetadata.getRecType()) {
				case DataRecordMetadata.FIXEDLEN_RECORD:
					fileMetadata = setDB2DateFormat(inMetadata);
					break;
				case DataRecordMetadata.DELIMITED_RECORD:
				case DataRecordMetadata.MIXED_RECORD:
					fileMetadata = setDB2DateFormat(convertToDB2Delimited(inMetadata));
					break;
				default:
					throw new ComponentNotReadyException(
							"Unknown record type: " + inMetadata.getRecType());
				}
			}else{//create metadata for formatting from selected metadata
				switch (fileMetadata.getRecType()) {
				case DataRecordMetadata.DELIMITED_RECORD:
					fileMetadata = setDB2DateFormat(convertToDB2Delimited(fileMetadata));
					break;
				case DataRecordMetadata.FIXEDLEN_RECORD:
					fileMetadata = setDB2DateFormat(fileMetadata);
					break;
				default:
					throw new ComponentNotReadyException(this, XML_FILEMETADATA_ATTRIBUTE, 
							"Only fixlen or delimited metadata allowed");
				}
			}
			//create and init formatter
			if (delimitedData) {
				formatter = new DelimitedDataFormatter(FILE_ENCODING);
			}else{
				formatter = new FixLenDataFormatter(FILE_ENCODING);
				properties.setProperty(STRIP_BLANKS_PARAM, TRUE);
			}
			properties.setProperty(CODE_PAGE_PARAM, DB2_UTF_8_CODEPAGE);
			formatter.init(fileMetadata);
		}else{//there is not input port connected, data is read from existing file
			if (dataURL != null) {
				initDataFile();

				if (fileMetadata == null) throw new ComponentNotReadyException(this,
						XML_FILEMETADATA_ATTRIBUTE, "File metadata has to be defined");
				switch (fileMetadata.getRecType()) {
				case DataRecordMetadata.DELIMITED_RECORD:
					delimitedData = true;
					fileMetadata = setDB2DateFormat(convertToDB2Delimited(fileMetadata));
					break;
				case DataRecordMetadata.FIXEDLEN_RECORD:
					delimitedData = false;
					fileMetadata = setDB2DateFormat(fileMetadata);
					break;
				default:
					throw new ComponentNotReadyException(this, XML_FILEMETADATA_ATTRIBUTE, 
							"Only fixlen or delimited metadata allowed");
				}
			}else{
				throw new ComponentNotReadyException(this, 
						"There is neither input port nor data file URL specified.");
			}
				
		}
		//if data are not delimited method L must be specified and all fields listed
		if (cloverFields == null && !delimitedData) {
			cloverFields = new String[fileMetadata.getNumFields()];
			int i = 0;
			for (DataFieldMetadata field : fileMetadata) {
				//TODO Labels:
				//cloverFields[i++] = field.getLabelOrName();
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
				command = (System.getProperty("os.name").startsWith("Windows") ? "db2cmd /c /i /w " : "") +
						"db2 -f " + prepareBatch();
			}
		} catch (IOException e) {
			throw new ComponentNotReadyException(this, e);
		}
	}
	
	private void initDataFile() throws ComponentNotReadyException {
		// We want this method to fail atomically, so create a temporary data file.
		File newDataFile = null;

		try {
			newDataFile = new File(FileUtils.getFile(getGraph().getRuntimeContext().getContextURL(), dataURL));
		} catch (MalformedURLException exception) {
			throw new ComponentNotReadyException(this, "The fileURL attribute is invalid!", exception);
		}

		if (!newDataFile.exists()) {
			throw new ComponentNotReadyException(this, "File " + newDataFile.getAbsolutePath() + " not found!");
		}

		// The data file is valid, save it.
		dataFile = newDataFile;
	}

	@Override
	public synchronized void reset() throws ComponentNotReadyException {
		super.reset();
	}
	
	@Override
	public synchronized void free() {
		super.free();
		if (formatter != null) {
			try {
				formatter.close();
			} catch (IOException e) {
				logger.error(e);
			}
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
	private DataRecordMetadata setDB2DateFormat(DataRecordMetadata metadata){
		//get formats from parameters
		String dateFormat = properties.getProperty(DATE_FORMAT_PARAM);
		String timeFormat = properties.getProperty(TIME_FORMAT_PARAM);
		String timeDateFormat = properties.getProperty(TIME_STAMP_FORMAT_PARAM);
		//prepare new metadata
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
				formatString = field.getFormat();
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
						logger.info("Node " + this.getId() + " info: Time stamp " +
								"format set to " + StringUtils.quote(formatString));
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
						logger.info("Node " + this.getId() + " info: Date format " +
								"set to " + StringUtils.quote(formatString));
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
						logger.info("Node " + this.getId() + " info: Time format " +
								"set to " + StringUtils.quote(formatString));
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
				logger.info("Node " + this.getId() + " info: Time stamp format set " +
						"to " + StringUtils.quote(DEFAULT_DATETIME_FORMAT));
				timeDateFormat = DEFAULT_DATETIME_FORMAT;
				properties.setProperty(TIME_STAMP_FORMAT_PARAM, dateFormat);
			}
			if (dateFormat == null) {
				logger.info("Node " + this.getId() + " info: Date format set to " + 
						StringUtils.quote(DEFAULT_DATE_FORMAT));
				dateFormat = DEFAULT_DATE_FORMAT;
				properties.setProperty(DATE_FORMAT_PARAM, dateFormat);
			}
			if (timeFormat == null){
				logger.info("Node " + this.getId() + " info: Time format set to " + 
						StringUtils.quote(DEFAULT_TIME_FORMAT));
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
//		set proper parameter for fixed length metadata
		if (out.getRecType() == DataRecordMetadata.FIXEDLEN_RECORD && !out.isSpecifiedRecordDelimiter()) {
			properties.put(REC_LEN_PARAM, String.valueOf(out.getRecordSize()));
		}
		return out;
	}
		
	/**
	 * Creates delimited metadata from fixed or delimited and sets the proper field delimiter
	 * 
	 * @param metadata input metadata
	 * @return new metadata
	 */
	private DataRecordMetadata convertToDB2Delimited(DataRecordMetadata metadata){
		DataRecordMetadata fMetadata = new DataRecordMetadata(metadata.getName() + "_delimited", DataRecordMetadata.DELIMITED_RECORD);
		// TODO Labels:
		//fMetadata.setLabel(metadata.getLabel());
		boolean delimiterFound = columnDelimiter != 0;
		int delimiterFieldIndex = -1;
		DataFieldMetadata field, newField;
		//all fields, except last have to have identical,  one char delimiter
		for (int i=0; i < metadata.getNumFields() - 1; i++){
			field = metadata.getField(i);
			newField = field.duplicate();
			//if found first "good" delimiter set it for all fields
			if (!delimiterFound && field.isDelimited()){
				if (field.getDelimiters()[0].length() == 1 && !Character.isWhitespace(field.getDelimiters()[0].charAt(0))) {
					delimiterFound = true;
					columnDelimiter = field.getDelimiters()[0].charAt(0);
					delimiterFieldIndex = i;
				}
			}
			//set defined delimiter
			if (delimiterFound){
				newField.setDelimiter(String.valueOf(columnDelimiter));
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
			logger.info("Node " + this.getId() + " info: " + 
					StringUtils.quote(String.valueOf(columnDelimiter)) + " set as " +
					"column delimiter.");
			properties.setProperty(COL_DEL_PARAM, String.valueOf(columnDelimiter));
		}
		for (int i=0; i< delimiterFieldIndex; i++){
			fMetadata.getField(i).setDelimiter(String.valueOf(columnDelimiter));
		}
		//check last field
		newField = metadata.getField(metadata.getNumFields() - 1).duplicate();
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
		
		try {
			command.append(dataFile.getCanonicalPath());
		} catch (IOException e) {
		}
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
	 * If script file doesn't exist creates it
	 * 
	 * @return path to the script file
	 * @throws IOException
	 * @throws ComponentNotReadyException
	 */
	private String prepareBatch() throws IOException, ComponentNotReadyException{
		if (batchURL != null) {
			batchFile = new File(FileUtils.getFile(getGraph().getRuntimeContext().getContextURL(), batchURL));
			if (batchFile.length() > 0) {
				logger.info("Node " + this.getId() + " info: Batch file exist. " +
						"New batch file will not be created.");
				return batchFile.getCanonicalPath();
			}
		}
		if (batchFile == null) {
			try {
				batchFile = getGraph().getAuthorityProxy().newTempFile("tmp", ".bat", -1);
			} catch (TempFileCreationException e) {
				throw new IOException(e);
			}
		}		
		// TODO Labels:
		//Writer batchWriter = new OutputStreamWriter(new FileOutputStream(batchFile), Charset.forName(FILE_ENCODING));
		FileWriter batchWriter = new FileWriter(batchFile);

		batchWriter.write(prepareConnectCommand());
		batchWriter.write(prepareLoadCommand());
		batchWriter.write(prepareDisconnectCommand());

		batchWriter.close();
		return batchURL != null ? batchFile.getCanonicalPath() : batchFile.getName();
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
		box = new ProcBox(proc, null, consumer, errConsumer);
		
		new Thread(){
			@Override
			public void run() {
				FileOutputStream fos = null;
				try {
					
					fos = new FileOutputStream(dataFile);
				
					formatter.setDataTarget(fos);
					int i = 0;
					while (runIt && ((inRecord = inPort.readRecord(inRecord)) != null)) {
						if (skipped >= recordSkip) {
							formatter.write(inRecord);
						}else{
							skipped++;
						}
					}
					formatter.finish();
				} catch (Exception e) {
					throw new RuntimeException(e);
				} finally {
					try {
						if (fos != null) {
							fos.close();
						}
					} catch (IOException e2) {
						throw new RuntimeException(e2);
						// TODO: probably better thread-specific error handling should be used
					}
				}
			}
		}.start();
		return box.join();
	}
	
	@Override
	public void preExecute() throws ComponentNotReadyException {
		super.preExecute();
		if (firstRun()) {//a phase-dependent part of initialization
    		//all necessary elements have been initialized in init()
		} else {
			if (formatter != null) {
				formatter.reset();
			}
		}
	}

	@Override
	public Result execute() throws Exception {
		inPort = getInputPort(READ_FROM_PORT);
		inRecord =null;
		if (inMetadata != null) {
			inRecord = DataRecordFactory.newRecord(fileMetadata);
			inRecord.init();
		}
		
		consumer = new DB2DataConsumer(LoggerDataConsumer.LVL_DEBUG, warningNumber, getOutputPort(ERROR_PORT));
		errConsumer = new LoggerDataConsumer(LoggerDataConsumer.LVL_ERROR, warningNumber);

		int exitValue = 0;
		//create named pipe
		if (!getInPorts().isEmpty() && usePipe) {
			try {
				proc = Runtime.getRuntime().exec("mkfifo " + dataFile.getCanonicalPath());
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
					OutputStream os = null;
					try {
						os = new FileOutputStream(dataFile);
						//save data in temporary file
						formatter.setDataTarget(os);
						while (runIt && ((inRecord = inPort.readRecord(inRecord)) != null)) {
							if (skipped >= recordSkip) {
								formatter.write(inRecord);
							}else{
								skipped++;
							}
						}
						formatter.finish();
					} finally {
						if (os != null) {
							os.close();
						}
					}
				}
				//read data from file
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
		//exitValue=2:     DB2 command or SQL statement warning 
		if (exitValue == 2 || consumer.getLoaded() != consumer.getRead() || 
				!getInPorts().isEmpty() && (consumer.getRead() != getInputPort(READ_FROM_PORT).getInputRecordCounter())) {
			if (consumer.getLoaded() != consumer.getRead() || 
					consumer.getRead() != getInputPort(READ_FROM_PORT).getInputRecordCounter()) {
				logger.warn("Not all records were loaded to database:");
				logger.info("Number of rows read = " + consumer.getRead());
				logger.info("Number of rows loaded = " + consumer.getLoaded());
				logger.info("Number of rows rejected = " + consumer.getRejected());
			}
			if (isFailOnWarnings()) {
				throw new JetelException("Process raised a warning (exit value 2)");
			}
		}
		
		if (getOutputPort(ERROR_PORT) != null) {
			getOutputPort(ERROR_PORT).eof();
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
		if (!getInPorts().isEmpty() && dataURL==null && !dataFile.delete()) {
			logger.warn("Tmp data file was not deleted.");
		}
		if (batchURL == null && !batchFile.delete()){
			logger.warn("Tmp batch file was not deleted.");
		}
	}
	
    /**
     * Reads node from xml
     * 
     * @param graph
     * @param xmlElement
     * @return
     * @throws XMLConfigurationException
     * @throws AttributeNotFoundException 
     */
    public static Node fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException, AttributeNotFoundException {
        ComponentXMLAttributes xattribs = new ComponentXMLAttributes(xmlElement, graph);

        DB2DataWriter writer = new DB2DataWriter(xattribs.getString(XML_ID_ATTRIBUTE),
                xattribs.getString(XML_DATABASE_ATTRIBUTE),
                xattribs.getString(XML_USERNAME_ATTRIBUTE),
                xattribs.getString(XML_PASSWORD_ATTRIBUTE),
                xattribs.getString(XML_TABLE_ATTRIBUTE),
                LoadMode.valueOf(xattribs.getString(XML_MODE_ATTRIBUTE, DEFAULT_TABLE_LOAD_MODE).toLowerCase()),
                xattribs.getString(XML_FILEURL_ATTRIBUTE, null),
                xattribs.getString(XML_FILEMETADATA_ATTRIBUTE, null));
		if (xattribs.exists(XML_FIELDMAP_ATTRIBUTE)){
			String[] pairs = StringUtils.split(xattribs.getString(XML_FIELDMAP_ATTRIBUTE));
			String[] cloverFields = new String[pairs.length];
			String[] dbFields = new String[pairs.length];
			String[] pair;
			for (int i=0;i<pairs.length;i++){
				pair = JoinKeyUtils.getMappingItemsFromMappingString(pairs[i]);
				cloverFields[i] = pair[0];
				dbFields[i] = StringUtils.quote(pair[1]);
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
        	writer.setBatchURL(xattribs.getStringEx(XML_BATCHURL_ATTRIBUTE,RefResFlag.SPEC_CHARACTERS_OFF));
        }
        if (xattribs.exists(XML_WARNING_LINES_ATTRIBUTE)) {
        	writer.setWarningNumber(xattribs.getInteger(XML_WARNING_LINES_ATTRIBUTE));
        }
        if (xattribs.exists(XML_FAIL_ON_WARNINGS_ATTRIBUTE)) {
        	writer.setFailOnWarnings(xattribs.getBoolean(XML_FAIL_ON_WARNINGS_ATTRIBUTE));
        }
        return writer;
    }
	
    @Override
    public void toXML(Element xmlElement) {
		super.toXML(xmlElement);
		if (batchURL != null) {
			xmlElement.setAttribute(XML_BATCHURL_ATTRIBUTE, batchURL);
		}		
		if (cloverFields != null && dbFields != null) {
			StringBuilder map = new StringBuilder(cloverFields[0] + "=" + dbFields[0] + ";");
			for (int i=1 ; i<cloverFields.length ; i++){
				map.append(cloverFields[i]);
				map.append('=');
				map.append(dbFields[i]);
				map.append(';');
			}
			xmlElement.setAttribute(XML_FIELDMAP_ATTRIBUTE, map.toString());
		}else if (cloverFields != null){
			xmlElement.setAttribute(XML_CLOVERFIELDS_ATTRIBUTE, StringUtils
					.stringArraytoString(cloverFields, ';'));
		}else if (dbFields != null) {
			xmlElement.setAttribute(XML_DBFIELDS_ATTRIBUTE, StringUtils
					.stringArraytoString(dbFields, ';'));
		}	
		if (columnDelimiter != 0) {
			xmlElement.setAttribute(XML_COLUMNDELIMITER_ATTRIBUTE, String
					.valueOf(columnDelimiter));
		}		
		xmlElement.setAttribute(XML_DATABASE_ATTRIBUTE, database);
		if (fileMetadataName != null) {
			xmlElement.setAttribute(XML_FILEMETADATA_ATTRIBUTE,
					fileMetadataName);
		}		
		if (dataURL != null) {
			xmlElement.setAttribute(XML_FILEURL_ATTRIBUTE, dataURL);
		}		
		if (interpreter != null) {
			xmlElement.setAttribute(XML_INTERPRETER_ATTRIBUTE, interpreter);
		}		
		xmlElement.setAttribute(XML_MODE_ATTRIBUTE, String.valueOf(loadMode));
		xmlElement.setAttribute(XML_PASSWORD_ATTRIBUTE, psw);
		if (properties.containsKey(ROW_COUNT_PARAM)) {
			xmlElement.setAttribute(XML_RECORD_COUNT_ATTRIBUTE, properties
					.getProperty(ROW_COUNT_PARAM));
		}		
		if (recordSkip > 0) {
			xmlElement.setAttribute(XML_RECORD_SKIP_ATTRIBUTE, String
					.valueOf(recordSkip));
		}		
		if (rejectedURL != null) {
			xmlElement.setAttribute(XML_REJECTEDRECORDSURL_ATTRIBUTE,
					rejectedURL);
		}		
		xmlElement.setAttribute(XML_TABLE_ATTRIBUTE, table);
		xmlElement.setAttribute(XML_USEPIPE_ATTRIBUTE, String.valueOf(usePipe));
		xmlElement.setAttribute(XML_USERNAME_ATTRIBUTE, user);
		if (warningNumber > 0) {
			xmlElement.setAttribute(XML_WARNING_LINES_ATTRIBUTE, String
					.valueOf(warningNumber));
		}		
		if (parameters != null){
			xmlElement.setAttribute(XML_PARAMETERS_ATTRIBUTE, parameters);
		}else if (!properties.isEmpty()) {
			StringBuilder props = new StringBuilder();

			for (Entry<Object, Object> entry : properties.entrySet()) {
				String key = (String) entry.getKey();
				String value = (String) entry.getValue();

				props.append(key);
				props.append('=');
				props.append(StringUtils.isQuoted(value) ? value : StringUtils.quote(value));
				props.append(';');
			}

			xmlElement.setAttribute(XML_PARAMETERS_ATTRIBUTE, props.toString());
		}
	}
	
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

	public int getWarningNumber() {
		return warningNumber;
	}

	public void setWarningNumber(int warningNumber) {
		this.warningNumber = warningNumber;
	}
	
	public boolean isFailOnWarnings() {
		return failOnWarnings;
	}

	public void setFailOnWarnings(boolean failOnWarnings) {
		this.failOnWarnings = failOnWarnings;
	}

	/**
	 * Return list of all adding parameters (parameters attribute).
	 * Deprecated parameters mustn't be used.
	 * It is intended for use in GUI in parameter editor.
	 * @return list of parameters that is viewed in parameters editor
	 */
	public static String[] getAddingParameters() {
		return new String[] {
			LOBS_URL_PARAM,
			ANY_ORDER_PARAM,
			GENERATED_IGNORE_PARAM,
			GENERATED_MISSING_PARAM,
			GENERATED_OVERRIDE_PARAM,
			IDENTITY_IGNORE_PARAM,
			IDENTITY_MISSING_PARAM,
			IDENTITY_OVERRIDE_PARAM,
			INDEX_FREE_SPACE_PARAM,
			NO_HEADER_PARAM,
			NO_ROW_WARNINGS_PARAM,
			PAGE_FREE_SPACE_PARAM,
			SUBTABLE_CONVERT_PARAM,
			TOTAL_FREE_SPACE_PARAM,
			USE_DEFAULTS_PARAM,
			CODE_PAGE_PARAM,
			DATE_FORMAT_PARAM,
			DUMP_FILE_PARAM,
			DUMP_FILE_ACCESS_ALL_PARAM,
			FAST_PARSE_PARAM,
			TIME_FORMAT_PARAM,
			IMPLIED_DECIMAL_PARAM,
			TIME_STAMP_FORMAT_PARAM,
			NO_EOF_CHAR_PARAM,
			USER_GRAPHIC_CODE_PAGE_PARAM,
			BINARY_NUMERICS_PARAM,
			NO_CHECK_LENGTHS_PARAM,
			NULL_IND_CHAR_PARAM,
			PACKED_DECIMAL_PARAM,
			REC_LEN_PARAM,
			STRIP_BLANKS_PARAM,
			STRIP_NULLS_PARAM,
			ZONED_DECIMAL_PARAM,
			CHAR_DEL_PARAM,
			COL_DEL_PARAM,
			DATES_ISO_PARAM,
			DEC_PLUS_BLANK_PARAM,
			DECIMAL_POINT_PARAM,
			DEL_PRIORYTY_CHAR_PARAM,
			DL_DEL_PARAM,
			KEEP_BLANKS_PARAM,
			NO_CHAR_DEL_PARAM,
			NO_DOUBLE_DEL_PARAM,
			NULL_INDICATORS_PARAM,
			SAVE_COUNT_PARAM,
			ROW_COUNT_PARAM,
			WARNING_COUNT_PARAM,
			MESSAGES_URL_PARAM,
			TMP_URL_PARAM,
			DL_LINK_TYPE_PARAM,
			DL_URL_DEFAULT_PREFIX_PARAM,
			DL_URL_REPLACE_PREFIX_PARAM,
			DL_URL_SUFFIX_PARAM,
			EXCEPTION_TABLE_PARAM,
			STATISTIC_PARAM,
			COPY_PARAM,
			USE_TSM_PARAM,
			NUM_SESSIONS_PARAM,
			RECOVERY_LIBRARY_PARAM,
			COPY_URL_PARAM,
			NONRECOVERABLE_PARAM,
			WITHOUT_PROMPTING_PARAM,
			BUFFER_SIZE_PARAM,
			SORT_BUFFER_SIZE_PARAM,
			CPU_NUM_PARAM,
			DISK_NUM_PARAM,
			INDEXING_MODE_PARAM,
			ALLOW_READ_ACCESS_PARAM,
			INDEX_COPY_TABLE_PARAM,
			CHECK_PENDING_CASCADE_PARAM,
			LOCK_WITH_FORCE_PARAM,
			PARTITIONED_CONFIG_PARAM,
		};
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
class DB2DataConsumer implements DataConsumer {
	
	long read;
	long skipped;
	long loaded;
	long rejected;
	long deleted;
	long committed;
	private String errorMessage;
	private boolean partRead = false;
	private OutputPort errPort;
	private DataRecord errRecord;
	private Pattern rowPattern = Pattern.compile("\"F\\p{Digit}*-\\p{Digit}*\"");
	private String row;
	private String tmp; 
	private int quotationIndex;
	private Matcher matcher;
	
	private final static int ERR_ROW_FIELD = 0;
	private final static int ERR_COLUMN_FIELD = 1;
	private final static int ERR_MESSAGE_FIELD = 2;
	
	/**
	 * Debug log level.
	 */
	public static final int LVL_DEBUG = 0;
	/**
	 * Warning log level.
	 */
	public static final int LVL_WARN = 1;
	/**
	 * Error log level.
	 */
	public static final int LVL_ERROR = 2;
	
	private int maxLines;
	private int linesRead;
	private BufferedReader reader;

	static Log logger = LogFactory.getLog(DB2DataConsumer.class);

	/**
	 * Constructor from superclass
	 * 
	 * @param level
	 * @param maxLines
	 */
	public DB2DataConsumer(int level, int maxLines, OutputPort port) {
		this.maxLines = maxLines;
		linesRead = 0;
		errPort = port;
		if (errPort != null) {
			errRecord = DataRecordFactory.newRecord(errPort.getMetadata());
			errRecord.init();
		}
	}
	
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
			read = Long.parseLong(line.substring(line.indexOf('=') + 1).trim());
		}
		if (line.contains("rows skipped")) {
			skipped = Long.parseLong(line.substring(line.indexOf('=') + 1).trim());
		}
		if (line.contains("rows loaded")) {
			loaded = Long.parseLong(line.substring(line.indexOf('=') + 1).trim());
		}
		if (line.contains("rows rejected")) {
			rejected = Long.parseLong(line.substring(line.indexOf('=') + 1).trim());
		}
		if (line.contains("rows deleted")) {
			deleted = Long.parseLong(line.substring(line.indexOf('=') + 1).trim());
		}
		if (line.contains("rows committed")) {
			committed = Long.parseLong(line.substring(line.indexOf('=') + 1).trim());
		}
		if (line.matches("^SQL\\d+.*")){
			// remember first line of error message
			errorMessage = line;
			partRead = true;
		}else if (partRead) {
			//if line is not blank it is continuation of error message
			partRead = !StringUtils.isBlank(line);
			if (partRead) {
				errorMessage = errorMessage.concat(line);
			}else{//whole error message read
				if (errPort != null && errorMessage.contains("row") && errorMessage.contains("column")) {
					//find out if error message is about rejected record
					matcher = rowPattern.matcher(errorMessage);
					if (matcher.find()) {
						//parse info about rejected record for three parts
						row = matcher.group();
						errRecord.getField(ERR_ROW_FIELD).setValue(Integer.parseInt(
								row.substring(row.indexOf('-') + 1, row.length() - 1)));
						//Example error message;SQL3191N  The field in row "F0-3", column "2" which begins with "test1" does not match....
						tmp = errorMessage.substring(errorMessage.indexOf("column"));//tmp = column "2" which begins...
						quotationIndex = tmp.indexOf('"');
						errRecord.getField(ERR_COLUMN_FIELD).setValue(Integer.parseInt(
								tmp.substring(quotationIndex + 1, tmp.indexOf('"', quotationIndex + 1))));
						errRecord.getField(ERR_MESSAGE_FIELD).setValue(errorMessage);
						try {
							errPort.writeRecord(errRecord);
						} catch (Exception e) {
							throw new JetelException(e);
						}
					}				 
				}
				if (maxLines == 0 || linesRead++ < maxLines) {
					logger.debug(errorMessage);
				}
			}
		}else if (!StringUtils.isEmpty(line)){
			if (maxLines == 0 || linesRead++ < maxLines) {
				logger.debug(line);
			}
		}
		return true;
	}

	public long getCommitted() {
		return committed;
	}

	public long getDeleted() {
		return deleted;
	}

	public long getLoaded() {
		return loaded;
	}

	public long getRead() {
		return read;
	}

	@Override
	public void setInput(InputStream stream) {
		reader = new BufferedReader(new InputStreamReader(stream));
	}

	@Override
	public void close() {
	}

	public long getRejected() {
		return rejected;
	}

	public long getSkipped() {
		return skipped;
	}
	
}