/*
 *  jETeL/Clover - Java based ETL application framework.
 *  Copyright (C) 2002  David Pavlis
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 2 of the License, or
 *  (at your option) any later version.
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
package org.jetel.database;
import java.io.*;
import java.sql.*;
import java.util.Properties;
import org.jetel.util.ComponentXMLAttributes;

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
 *  </table>  
 *
 *  <h4>Example:</h4>  
 *  <pre>&lt;DBConnection id="InterbaseDB" dbConfig="interbase.cfg"/&gt;</pre>
 *
 *@author     dpavlis
 *@created    January 15, 2003
 */
public class DBConnection {

	String dbDriverName;
	String dbURL;
	String user;
	String password;
	Driver dbDriver;
	Connection dbConnection;


	/**
	 *  Constructor for the DBConnection object
	 *
	 *@param  dbDriver  Description of the Parameter
	 *@param  dbURL     Description of the Parameter
	 *@param  user      Description of the Parameter
	 *@param  password  Description of the Parameter
	 */
	public DBConnection(String dbDriver, String dbURL, String user, String password) {
		this.dbDriverName = dbDriver;
		this.dbURL = dbURL;
		this.user = user;
		this.password = password;
	}


	/**
	 *  Constructor for the DBConnection object
	 *
	 *@param  configFilename  properties filename containing definition of driver, dbURL, username, password
	 */
	public DBConnection(String configFilename) {
		Properties config = new Properties();

		try {
			InputStream stream = new BufferedInputStream(new FileInputStream(configFilename));
			config.load(stream);
			stream.close();
			this.dbDriverName = config.getProperty("dbDriver");
			this.dbURL = config.getProperty("dbURL");
			this.user = config.getProperty("user");
			this.password = config.getProperty("password");

		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}


	/**
	 *  Description of the Method
	 */
	public void connect() {
		Properties property = new Properties();
		if (user != null) {
			property.setProperty("user", user);
		}
		if (password != null) {
			property.setProperty("password", password);
		}

		try {
			dbDriver = (Driver) Class.forName(dbDriverName).newInstance();
			dbConnection = dbDriver.connect(dbURL, property);
		} catch (SQLException ex) {
			throw new RuntimeException("Can't connect to DB :" + ex.getMessage());
		} catch (Exception ex) {
			throw new RuntimeException("Can't load DB driver :" + ex.getMessage());
		}
		if (dbConnection == null) {
			throw new RuntimeException("Not suitable driver for specified DB URL : " + dbDriver + " ; " + dbURL);
		}
	}


	/**
	 *  Description of the Method
	 *
	 *@exception  SQLException  Description of the Exception
	 */
	public void close() throws SQLException {
		dbConnection.close();
	}


	/**
	 *  Gets the connection attribute of the DBConnection object
	 *
	 *@return    The connection value
	 */
	public Connection getConnection() {
		return dbConnection;
	}


	/**
	 *  Gets the statement attribute of the DBConnection object
	 *
	 *@return                   The statement value
	 *@exception  SQLException  Description of the Exception
	 */
	public Statement getStatement() throws SQLException {
		return dbConnection.createStatement();
	}


	/**
	 *  Description of the Method
	 *
	 *@param  sql               Description of the Parameter
	 *@return                   Description of the Return Value
	 *@exception  SQLException  Description of the Exception
	 */
	public PreparedStatement prepareStatement(String sql) throws SQLException {
		return dbConnection.prepareStatement(sql);
	}


	/**
	 *  Description of the Method
	 *
	 *@param  nodeXML  Description of the Parameter
	 *@return          Description of the Return Value
	 */
	public static DBConnection fromXML(org.w3c.dom.Node nodeXML) {
		ComponentXMLAttributes xattribs=new ComponentXMLAttributes(nodeXML);

		try{
			// do we have dbConfig parameter specified ??
			if (xattribs.exists("dbConfig")){
				return new DBConnection(xattribs.getString("dbConfig"));
			}else{
			
			String dbDriver = xattribs.getString("dbDriver");
			String dbURL = xattribs.getString("dbURL");
			String user=null;
			String password=null;
				
			if (xattribs.exists("user")){
				user = xattribs.getString("user");
			}
			if (xattribs.exists("password")){
				user = xattribs.getString("password");
			}
			
			return new DBConnection(dbDriver, dbURL, user, password);
			}
				
		}catch(Exception ex){
			System.err.println(ex.getMessage());
			return null;
		}
		
	}
	
	public String toString(){
		return dbURL;
	}
}

