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
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.*;
import java.util.Properties;
import org.jetel.util.ComponentXMLAttributes;
import org.jetel.util.PropertyRefResolver;
import org.w3c.dom.NamedNodeMap;

/**
 *  CloverETL class for connecting to databases.<br>
 *  It practically wraps around JDBC's Connection class and adds some useful
 *  methods.
 *  <br>
 *  <table border="1">
 *  <th>XML attributes:</th>
 *  <tr><td><b>id</b></td><td>connection identification</td>
 *  <tr><td><b>dbConfig</b><br><i>optional</i></td><td>filename of the config file from which to take connection parameters<br>
 *  If used, then all other attributes are ignored.</td></tr>
 *  <tr><td><b>dbDriver</b></td><td>name of the JDBC driver</td></tr>
 *  <tr><td><b>dbURL</b></td><td>URL of the database (aka connection string)</td></tr>
 *  <tr><td><b>user</b><br><i>optional</i></td><td>username to use when connecting to DB</td></tr>
 *  <tr><td><b>password</b><br><i>optional</i></td><td>password to use when connecting to DB</td></tr>
 *  <tr><td><b>driverLibrary</b><br><i>optional</i></td><td>name of Java library file (.jar,.zip,...) where
 *  to search for class containing JDBC driver specified in <tt>dbDriver<tt> parameter.</td></tr>
 * <tr><td><b>transactionIsolation</b><br><i>optional</i></td><td>Allows specifying certain transaction isolation level. 
 * Following are the valid options:<br><ul>
 * <li>READ_UNCOMMITTED</li>
 * <li>READ_COMMITTED</li>
 * <li>REPEATABLE_READ</li>
 * <li>SERIALIZABLE</li>
 * </ul><i>Note: If not specified, then the driver's default is used.</i></td></tr> 
 * </table>
 *  <h4>Example:</h4>
 *  <pre>&lt;DBConnection id="InterbaseDB" dbConfig="interbase.cfg"/&gt;</pre>
 *
 * @author      dpavlis
 * @since       21. bøezen 2004
 * @revision    $Revision$
 * @created     January 15, 2003
 */
public class DBConnection {

	String dbDriverName;
	String dbURL;
	Driver dbDriver;
	Connection dbConnection;
	Properties config;

	private final static String JDBC_DRIVER_LIBRARY_NAME = "driverLibrary";
	public final static String TRANSACTION_ISOLATION_PROPERTY_NAME="transactionIsolation";

	/**
	 *  Constructor for the DBConnection object
	 *
	 * @param  dbDriver  Description of the Parameter
	 * @param  dbURL     Description of the Parameter
	 * @param  user      Description of the Parameter
	 * @param  password  Description of the Parameter
	 */
	public DBConnection(String dbDriver, String dbURL, String user, String password) {
		this.config = new Properties();
		config.setProperty("user", user);
		config.setProperty("password", password);
		this.dbDriverName = dbDriver;
		this.dbURL = dbURL;
	}


	/**
	 *  Constructor for the DBConnection object
	 *
	 * @param  configFilename  properties filename containing definition of driver, dbURL, username, password
	 */
	public DBConnection(String configFilename) {
		this.config = new Properties();

		try {
			InputStream stream = new BufferedInputStream(new FileInputStream(configFilename));
			this.config.load(stream);
			stream.close();
			this.dbDriverName = config.getProperty("dbDriver");
			this.dbURL = config.getProperty("dbURL");

		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}


	
	/**
	 * Method which connects to database and if successful, sets various
	 * connection parameters. If as a property "transactionIsolation" is defined, then
	 * following options are allowed:<br>
	 * <ul>
	 * <li>READ_UNCOMMITTED</li>
	 * <li>READ_COMMITTED</li>
	 * <li>REPEATABLE_READ</li>
	 * <li>SERIALIZABLE</li>
	 * </ul>
	 * 
	 * @see java.sql.Connection#setTransactionIsolation(int)
	 */
	public void connect() {
		try {
			dbDriver = (Driver) Class.forName(dbDriverName).newInstance();
		} catch (ClassNotFoundException ex) {
			// let's try to load in any addition .jar library (if specified)
			String jdbcDriverLibrary = config
					.getProperty(JDBC_DRIVER_LIBRARY_NAME);
			if (jdbcDriverLibrary != null) {
				String urlString = "file:" + jdbcDriverLibrary;
				URL[] myURLs;
				try {
					myURLs = new URL[] { new URL(urlString) };
					URLClassLoader classLoader = new URLClassLoader(myURLs);
					dbDriver = (Driver) Class.forName(dbDriverName, true,
							classLoader).newInstance();
				} catch (MalformedURLException ex1) {
					throw new RuntimeException("Malformed URL: "
							+ ex1.getMessage());
				} catch (ClassNotFoundException ex1) {
					throw new RuntimeException("Can not find class: " + ex1);
				} catch (Exception ex1) {
					throw new RuntimeException("General exception: "
							+ ex1.getMessage());
				}
			} else {
				throw new RuntimeException("Can't load DB driver :"
						+ ex.getMessage());
			}
		} catch (Exception ex) {
			throw new RuntimeException("Can't load DB driver :"
					+ ex.getMessage());
		}
		try {
			dbConnection = dbDriver.connect(dbURL, this.config);
		} catch (SQLException ex) {
			throw new RuntimeException("Can't connect to DB :"
					+ ex.getMessage());
		}
		if (dbConnection == null) {
			throw new RuntimeException(
					"Not suitable driver for specified DB URL : " + dbDriver
							+ " ; " + dbURL);
		}
		// try to set Transaction isolation level, it it was specified
		if (config.containsKey(TRANSACTION_ISOLATION_PROPERTY_NAME)) {
			int trLevel;
			String isolationLevel = config
					.getProperty(TRANSACTION_ISOLATION_PROPERTY_NAME);
			if (isolationLevel.equalsIgnoreCase("READ_UNCOMMITTED")) {
				trLevel = Connection.TRANSACTION_READ_UNCOMMITTED;
			} else if (isolationLevel.equalsIgnoreCase("READ_COMMITTED")) {
				trLevel = Connection.TRANSACTION_READ_COMMITTED;
			} else if (isolationLevel.equalsIgnoreCase("REPEATABLE_READ")) {
				trLevel = Connection.TRANSACTION_REPEATABLE_READ;
			} else if (isolationLevel.equalsIgnoreCase("SERIALIZABLE")) {
				trLevel = Connection.TRANSACTION_SERIALIZABLE;
			} else {
				trLevel = Connection.TRANSACTION_NONE;
			}
			try {
				dbConnection.setTransactionIsolation(trLevel);
			} catch (SQLException ex) {
				// we do nothing, if anything goes wrong, we just
				// leave whatever was the default
			}
		}
	}


	/**
	 *  Description of the Method
	 *
	 * @exception  SQLException  Description of the Exception
	 */
	public void close() throws SQLException {
		dbConnection.close();
	}


	/**
	 *  Gets the connection attribute of the DBConnection object
	 *
	 * @return    The connection value
	 */
	public Connection getConnection() {
		return dbConnection;
	}


	/**
	 *  Gets the statement attribute of the DBConnection object
	 *
	 * @return                   The statement value
	 * @exception  SQLException  Description of the Exception
	 */
	public Statement getStatement() throws SQLException {
		return dbConnection.createStatement();
	}


	/**
	 *  Description of the Method
	 *
	 * @param  sql               Description of the Parameter
	 * @return                   Description of the Return Value
	 * @exception  SQLException  Description of the Exception
	 */
	public PreparedStatement prepareStatement(String sql) throws SQLException {
		return dbConnection.prepareStatement(sql);
	}


	/**
	 *  Sets the property attribute of the DBConnection object
	 *
	 * @param  name   The new property value
	 * @param  value  The new property value
	 */
	public void setProperty(String name, String value) {
		config.setProperty(name, value);
	}


	/**
	 *  Sets the property attribute of the DBConnection object
	 *
	 * @param  properties  The new property value
	 */
	public void setProperty(Properties properties) {
		config.putAll(properties);
	}


	/**
	 *  Gets the property attribute of the DBConnection object
	 *
	 * @param  name  Description of the Parameter
	 * @return       The property value
	 */
	public String getProperty(String name) {
		return config.getProperty(name);
	}


	/**
	 *  Description of the Method
	 *
	 * @param  nodeXML  Description of the Parameter
	 * @return          Description of the Return Value
	 */
	public static DBConnection fromXML(org.w3c.dom.Node nodeXML) {
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(nodeXML);
		NamedNodeMap attributes = nodeXML.getAttributes();
		DBConnection con;
		// for resolving reference in additional parameters
		// this should be changed in the future when ComponentXMLAttributes
		// is enhanced to support iterating through attributes
		PropertyRefResolver refResolver=new PropertyRefResolver();

		try {
			// do we have dbConfig parameter specified ??
			if (xattribs.exists("dbConfig")) {
				return new DBConnection(xattribs.getString("dbConfig"));
			} else {

				String dbDriver = xattribs.getString("dbDriver");
				String dbURL = xattribs.getString("dbURL");
				String user = null;
				String password = null;

				con = new DBConnection(dbDriver, dbURL, user, password);

				// assign rest of attributes/parameters to connection properties so
				// it can be retrieved by DB JDBC driver
				for (int i = 0; i < attributes.getLength(); i++) {
					con.setProperty(attributes.item(i).getNodeName(), refResolver.resolveRef(attributes.item(i).getNodeValue()));
				}

				return con;
			}

		} catch (Exception ex) {
			System.err.println(ex.getMessage());
			return null;
		}

	}


	/**
	 *  Description of the Method
	 *
	 * @return    Description of the Return Value
	 */
	public String toString() {
		return dbURL;
	}
}

