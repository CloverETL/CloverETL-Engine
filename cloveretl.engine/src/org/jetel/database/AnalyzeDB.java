/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2002-04  David Pavlis <david_pavlis@hotmail.com>
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
package org.jetel.database;

import java.io.*;
import java.sql.*;
import java.util.Properties;

/**
 *  Class easing creation of Clover metadata describing data originating in Database.<br>
 *  This "utility" connects to database and based on specified SQL query, generates
 *  XML file containing metadata describing structure of record in DB table.<br>
 *  This metadata can be later used by <i>DBInputTable</i> or <i>DBOutputTable</i>.<br>  
 *
 * Command line parameters:<br>
 * <pre>
 * -dbDriver   JDBC driver to use
 * -dbURL      Database name (URL)
 * -config     *Config/Property file containing parameters
 * -user       *User name
 * -password   *User's password
 * -d          *Delimiter to use (standard is [,])
 * -o          *Output file to use (standard is stdout)
 * -f          *Read SQL query from filename"
 * -q          *SQL query on command line
 * -info       *Displays list of driver's properties
 *
 * Parameters marked [*] are optional. Either -f or -q parameter must be present.
 * If -config option is specified, mandatory parameters are loaded from property file.
 * </pre>
 * Example of calling AnalyzeDB to produce metadata for EMPLOYEE table from EMPLOYEE database. The result is
 * stored in <i>employee.fmt</i> file.<br>
 * <pre>java org.jetel.database.AnalyzeDB -config interbase.cfg -o employee.fmt -q "select * from EMPLOYEE"</pre>
 * <br>
 * Example of content of config/property file (interbase.cfg):<br>
 * <pre> dbDriver=interbase.interclient.Driver
 * dbURL=jdbc:interbase://localhost/opt/borland/interbase/examples/database/employee.gdb
 * user=SYSDBA
 * password=masterkey
 * driverLibrary=/opt/borland/interbase/jdbc/lib/jdbcdrv.jar
 * </pre>
 * <u>Note:</u>
 * The <tt>driverLibrary</tt> option is not used by JDBC driver (thus not required). It is option which allows CloverETL automatically
 * load-in addition library containing JDBC driver if class specified in option dbDriver can't be found in standard CLASSPATH.<br>
 * @author     dpavlis
 * @since    September 25, 2002
 */
public class AnalyzeDB {

	private final static int BUFFER_SIZE = 2048;
	private final static String VERSION = "1.1";
	private final static String LAST_UPDATED = "2004/05/17";  
	private final static String DEFAULT_DELIMITER = ",";
	private final static String DEFAULT_XML_ENCODING="UTF-8";

	private static Driver driver;
	private static String delimiter;
	private static String filename;
	private static String queryFilename;
	private static String query;
	private static boolean showDriverInfo;


	/**
	 *  Main method
	 *
	 * @param  argv  Command line arguments
	 * @since        September 25, 2002
	 */
	public static void main(String argv[]) {
		Properties config=new Properties();
		int optionSwitch = 0;

		if (argv.length == 0) {
			printInfo();
			System.exit(-1);
		}

		for (int i = 0; i < argv.length; i++) {
			if (argv[i].equalsIgnoreCase("-dbDriver")) {
				config.setProperty("dbDriver",argv[++i]);
				optionSwitch |= 0x01;
			} else if (argv[i].equalsIgnoreCase("-dbURL")) {
				config.setProperty("dbURL",argv[++i]);
				optionSwitch |= 0x02;
			} else if (argv[i].equalsIgnoreCase("-d")) {
				delimiter = argv[++i];
			} else if (argv[i].equalsIgnoreCase("-o")) {
				filename = argv[++i];
			} else if (argv[i].equalsIgnoreCase("-f")) {
				queryFilename = argv[++i];
				optionSwitch |= 0x04;
			} else if (argv[i].equalsIgnoreCase("-q")) {
				query = argv[++i];
				optionSwitch |= 0x04;
			} else if (argv[i].equalsIgnoreCase("-info")) {
				showDriverInfo=true;
			} else if (argv[i].equalsIgnoreCase("-user")) {
				config.setProperty("user",argv[++i]);
			} else if (argv[i].equalsIgnoreCase("-password")) {
				config.setProperty("password",argv[++i]);
			} else if (argv[i].equalsIgnoreCase("-config")) {
				try{
					InputStream stream=new BufferedInputStream(new FileInputStream(argv[++i]));
					config.load(stream);
					stream.close();
					if (config.getProperty("dbDriver")!=null) optionSwitch |= 0x01; 
					if (config.getProperty("dbURL")!=null) optionSwitch |= 0x02;
				}catch(Exception ex){
					System.err.println("[Error] "+ex.getMessage());
					System.exit(-1);
				}
				
			} else {
				System.err.println("[Error] Unknown option: " + argv[i] + "\n");
				printInfo();
				System.exit(-1);
			}
		}
		if (((optionSwitch ^ 0x07) != 0) && !(showDriverInfo && ((optionSwitch ^ 0x03)==0))) {
			System.err.println("[Error] Parameter is missing !\n");
			printInfo();
			System.exit(-1);
		}
		if (queryFilename != null) {
			int length;
			int offset = 0;
			char[] buffer = new char[BUFFER_SIZE];
			FileReader reader = null;
			StringBuffer stringBuf = new StringBuffer();
			try {
				reader = new FileReader(queryFilename);
				while ((length = reader.read(buffer)) > 0) {
					stringBuf.append(buffer, offset, length);
					offset += length;
				}
				reader.close();
			}
			catch (FileNotFoundException ex) {
				System.err.println("[Error] " + ex.getMessage());
				System.exit(-1);
			}
			catch (IOException ex) {
				System.err.println("[Error] " + ex.getMessage());
				System.exit(-1);
			}
			query = stringBuf.toString();
		}
		if (delimiter == null) {
			delimiter = DEFAULT_DELIMITER;
		}
		try {
			doAnalyze(config);
		}
		catch (Exception ex) {
			System.err.println("\n[Error] " + ex.getMessage());
			System.exit(-1);
		}
	}


	/**
	 *  Description of the Method
	 *
	 * @exception  IOException   Description of Exception
	 * @exception  SQLException  Description of Exception
	 * @since                    September 25, 2002
	 */
	private static void doAnalyze(Properties config) throws IOException, SQLException {
		PrintStream print;
		DBConnection connection;
		boolean utf8Encoding=false;
		// initialization of PrintStream
		if (filename != null) {
			try{
				print = new PrintStream(new FileOutputStream(filename),true,DEFAULT_XML_ENCODING);
				utf8Encoding=true;
			}catch(UnsupportedEncodingException ex){
				System.out.println("Error:"+ex.getMessage());
				print = new PrintStream(new FileOutputStream(filename));
			}
		} else {
			print = System.out;
		}
		// load in Database Driver & try to connect to database
		connection=new DBConnection(config.getProperty("dbDriver"), config.getProperty("dbURL"),"","");
		connection.setProperty(config);
		connection.connect();

		// do we want just to display driver properties ?
		if (showDriverInfo){
			printDriverProperty(config);
			System.exit(0);
		}
		// Execute Query
		ResultSet resultSet = connection.getStatement().executeQuery(query);
		ResultSetMetaData metadata = resultSet.getMetaData();

		// here we print XML description of data 
		print.print("<?xml version=\"1.0\"");
		if (utf8Encoding) print.println(" encoding=\""+DEFAULT_XML_ENCODING+"\" ?>");
		else print.println("?>");
		print.println("<!-- Automatically generated from database " + connection.getConnection().getMetaData().getURL() + " -->");
		print.println("<Record name=\"" + metadata.getTableName(1) + "\" type=\"delimited\">");

		for (int i = 1; i <= metadata.getColumnCount(); i++) {
			print.println(dbMetadata2jetel(metadata, i));
		}

		print.println("</Record>");

		if (print != System.out) {
			print.close();
		}
		resultSet.close();
		connection.close();
	}


	/**
	 *  Description of the Method
	 *
	 * @param  metadata          Description of Parameter
	 * @param  fieldNo           Description of Parameter
	 * @return                   Description of the Returned Value
	 * @exception  SQLException  Description of Exception
	 * @since                    September 25, 2002
	 */
	private static String dbMetadata2jetel(ResultSetMetaData metadata, int fieldNo) throws SQLException {
		StringBuffer strBuf = new StringBuffer();
		char fieldType = SQLUtil.sqlType2jetel(metadata.getColumnType(fieldNo));

		// head
		strBuf.append("\t<Field name=\"");
		strBuf.append(metadata.getColumnName(fieldNo));

		// DATA TYPE
		strBuf.append("\" type=\"");
		try{
			strBuf.append(SQLUtil.jetelType2Str(fieldType));
		}catch(Exception ex){
			throw new RuntimeException(ex.getMessage() + " field name " + metadata.getColumnName(fieldNo));
		}
		
		strBuf.append("\"");
		// NULLABLE
		if (metadata.isNullable(fieldNo) == ResultSetMetaData.columnNullable) {
			strBuf.append(" nullable=\"yes\"");
		}
		// DELIMITER
		strBuf.append(" delimiter=\"");
		if (fieldNo == metadata.getColumnCount()) {
			strBuf.append("\\n");
			// last field by default delimited by NEWLINE character
		} else {
			strBuf.append(delimiter);
		}
		strBuf.append("\"");
		/*
		*  this is not safe - at least Oracle JDBC driver reports NUMBER to be currency
		* // FORMAT (in case of currency)
		* if (metadata.isCurrency(fieldNo)) {
		*	strBuf.append(" format=\"�#.#\"");
		* }
		*/

		// end
		strBuf.append(" />");

		return strBuf.toString();
	}


	/**
	 *  Print list of command line parameters
	 *
	 * @since    September 25, 2002
	 */
	private static void printInfo() {
		System.out.println("*** Jetel AnalyzeDB (" + VERSION + ") created on "+LAST_UPDATED+" (c) 2002-04 D.Pavlis, released under GNU Lesser General Public license ***\n");
		System.out.println("Usage:");
		System.out.println("-dbDriver   JDBC driver to use");
		System.out.println("-dbURL      Database name (URL)");
		System.out.println("-config     *Config/Property file containing parameters");
		System.out.println("-user       *User name");
		System.out.println("-password   *User's password");
		System.out.println("-d          *Delimiter to use (standard is [,])");
		System.out.println("-o          *Output file to use (standard is stdout)");
		System.out.println("-f          *Read SQL query from filename");
		System.out.println("-q          *SQL query on command line");
		System.out.println("-info       *Displays list of driver's properties");
		System.out.println("\nParameters marked [*] are optional. Either -f or -q parameter must be present.");
		System.out.println("If -config option is specified, mandatory parameters are loaded from property file.");
		System.out.println("When output is directed to file (-o option used), UTF-8 encoding is used - this should");
		System.out.println("be the preffered way as some format characters can't be represented as pure ASCII.\n");
	}

	private static void printDriverProperty(Properties config) throws SQLException{
		DriverPropertyInfo[] driverProperty=driver.getPropertyInfo(config.getProperty("dbURL"), config);
		System.out.println("*** DRIVER PROPERTY INFORMATION ***");
		for(int i=0;i<driverProperty.length;i++){
			System.out.println(driverProperty[i].name+" = "+driverProperty[i].value+" : "+driverProperty[i].description);
		}
		System.out.println("*** END OF LIST ***");
	}
}

