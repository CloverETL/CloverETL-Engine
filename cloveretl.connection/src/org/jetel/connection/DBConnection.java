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
package org.jetel.connection;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.Defaults;
import org.jetel.database.IConnection;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.JetelException;
import org.jetel.graph.GraphElement;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.ComponentXMLAttributes;
import org.jetel.util.PropertyRefResolver;
import org.jetel.util.StringUtils;
import org.jetel.util.crypto.Enigma;
import org.w3c.dom.Element;
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
 *  <tr><td><b>driverLibrary</b><br><i>optional</i></td><td>name(s) (full path) of Java library file(s) (.jar,.zip,...) where
 *  to search for class containing JDBC driver specified in <tt>dbDriver</tt> parameter.<br>
 *  In case of more libraries, use system path separator to delimit them (e.g. ";").</td></tr>
 * <tr><td><b>transactionIsolation</b><br><i>optional</i></td><td>Allows specifying certain transaction isolation level. 
 * Following are the valid options:<br><ul>
 * <li>READ_UNCOMMITTED</li>
 * <li>READ_COMMITTED</li>
 * <li>REPEATABLE_READ</li>
 * <li>SERIALIZABLE</li>
 * </ul>
 * <tr><td><b>threadSafeConnection</b><br><i>optional</i></td><td>if set, each thread gets its own connection. <i>Can be used
 * to prevent problems when multiple components conversate with DB through the same connection object which is
 * not thread safe.</i></td></tr>
 * <i>Note: Default value of this property is true.</i></td></tr> 
 * </table>
 *  <h4>Example:</h4>
 *  <pre>&lt;DBConnection id="InterbaseDB" dbConfig="interbase.cfg"/&gt;</pre>
 * <i>Note: any XML attribute name can also be used in the dbConfig file. If the option is set there, then
 * the value is applied to the connection object created.</i>
 * <h4>Example of dbConfig file:</h4>
 * <pre>**********
 * dbDriver=oracle.jdbc.driver.OracleDriver
 * dbURL=jdbc:oracle:thin:@@//localhost:1521/mytestdb
 * user=noname
 * password=free
 * defaultRowPrefetch=10
 * driverLibrary=c:/Orahome91/jdbc/lib/ojdbc14.jar
 * threadSafeConnection=true
 * ********</pre>
 * 
 * The XML DTD describing the internal structure is as follows:
 * 
 *  * &lt;!ATTLIST Connection
 *              id ID #REQUIRED
 *              type NMTOKEN (JDBC) #REQUIRED
 *              dbDriver CDATA #IMPLIED
 *              dbURL CDATA #IMPLIED
 *              dbConfig CDATA #IMPLIED
 *              driverLibrary CDATA #IMPLIED
 *              user CDATA #IMPLIED
 *              password CDATA #IMPLIED
 *              threadSafeConnection NMTOKEN (true | false) #IMPLIED
 *              passwordEncrypted NMTOKEN (true | false) #IMPLIED
 *              transactionIsolation (READ_UNCOMMITTED | READ_COMMITTED |
 *                                 REPEATABLE_READ | SERIALIZABLE ) #IMPLIED&gt;
 *                                 
 * @author      dpavlis
 * @since       21. b?ezen 2004
 * @revision    $Revision$
 * @created     January 15, 2003
 */
public class DBConnection extends GraphElement implements IConnection {

    private static Log logger = LogFactory.getLog(DBConnection.class);

    String configFileName;
	Driver dbDriver;
	Connection dbConnection;
	Properties config;
	boolean threadSafeConnections;
	boolean isPasswordEncrypted;
	private Map openedConnections;

	public final static String JDBC_DRIVER_LIBRARY_NAME = "driverLibrary";
	public final static String TRANSACTION_ISOLATION_PROPERTY_NAME="transactionIsolation";

	public  static final String XML_DBURL_ATTRIBUTE = "dbURL";
	public  static final String XML_DBDRIVER_ATTRIBUTE = "dbDriver";
	public  static final String XML_DBCONFIG_ATTRIBUTE = "dbConfig";
	public static final String XML_PASSWORD_ATTRIBUTE = "password";
	public static final String XML_USER_ATTRIBUTE = "user";
	public static final String XML_THREAD_SAFE_CONNECTIONS="threadSafeConnection";
	public static final String XML_IS_PASSWORD_ENCRYPTED = "passwordEncrypted"; 
	
	// not yet used by component
	public static final String XML_NAME_ATTRIBUTE = "name";
	/**
	 *  Constructor for the DBConnection object
	 *
	 * @param  dbDriver  Description of the Parameter
	 * @param  dbURL     Description of the Parameter
	 * @param  user      Description of the Parameter
	 * @param  password  Description of the Parameter
	 */
	public DBConnection(String id, String dbDriver, String dbURL, String user, String password) {
        super(id);
	    this.openedConnections=new HashMap();
	    this.config = new Properties();
		try{
		    config.setProperty(XML_USER_ATTRIBUTE, user);
		    config.setProperty(XML_PASSWORD_ATTRIBUTE, password);
		    config.setProperty(XML_DBDRIVER_ATTRIBUTE, dbDriver);
		    config.setProperty(XML_DBURL_ATTRIBUTE, dbURL);
		    }catch(NullPointerException ex){
		        // do nothing in constructor - will probably fail later
		    }
		this.threadSafeConnections=true;
		this.isPasswordEncrypted=false;
	}


	/**
	 *  Constructor for the DBConnection object (not used in engine yet)
	 *
	 * @param  configFilename  properties filename containing definition of driver, dbURL, username, password
	 */
	public DBConnection(String id, String configFilename) {
        super(id);
	    this.openedConnections=new HashMap();
	    this.config = new Properties();
	    this.configFileName = configFilename;
	}

	public DBConnection(String id, Properties configProperties) {
        super(id);
	    this.openedConnections=new HashMap();
		this.config = configProperties;
		this.threadSafeConnections=parseBoolean(configProperties.getProperty(XML_THREAD_SAFE_CONNECTIONS,"true"));
		this.isPasswordEncrypted=parseBoolean(config.getProperty(XML_IS_PASSWORD_ENCRYPTED,"false"));
	}
	
    /* (non-Javadoc)
     * @see org.jetel.graph.GraphElement#init()
     */
    synchronized public void init() throws ComponentNotReadyException {
        if(isInitialized()) return;
        super.init();
        
        if(!StringUtils.isEmpty(configFileName)) {
            try {
                InputStream stream = null;
                if (!new File(configFileName).exists()) {
                    // config file not found on file system - try classpath
                    stream = getClass().getClassLoader().getResourceAsStream(configFileName);
                    if(stream == null) {
                        throw new FileNotFoundException("Config file for db connection " + getId() + " not found (" + configFileName + ")");
                    }
                    stream = new BufferedInputStream(stream);
                } else {
                    stream = new BufferedInputStream(new FileInputStream(configFileName));
                }
                
                this.config.load(stream);
                stream.close();
                this.threadSafeConnections=parseBoolean(config.getProperty(XML_THREAD_SAFE_CONNECTIONS,"true"));
                this.isPasswordEncrypted=parseBoolean(config.getProperty(XML_IS_PASSWORD_ENCRYPTED,"false"));

            } catch (Exception ex) {
                throw new ComponentNotReadyException(ex);
            }
        }
        connect();
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
	private void connect() {
        if(!isInitialized()) {
            throw new RuntimeException("DBConnection (" + getId() +") is not initialized.");
        }
	    String dbDriverName;
	    if (dbDriver==null){
	        dbDriverName=config.getProperty(XML_DBDRIVER_ATTRIBUTE);
	        try {
	            dbDriver = (Driver) Class.forName(dbDriverName).newInstance();
	        } catch (ClassNotFoundException ex) {
	            // let's try to load in any additional .jar library (if specified) (one or more)
                // separator of individual libraries depends on platform - UNIX - ":" Win - ";"
	            String jdbcDriverLibrary = config
	            .getProperty(JDBC_DRIVER_LIBRARY_NAME);
	            if (jdbcDriverLibrary != null) {
	                String[] libraryPaths=jdbcDriverLibrary.split(Defaults.DEFAULT_PATH_SEPARATOR_REGEX);
	                URL[] myURLs= new URL[libraryPaths.length];
	                	// try to create URL directly, if failed probably the protocol is missing, so use File.toURL
	                    for(int i=0;i<libraryPaths.length;i++){
	                    	try {
	                    		// valid url
	                    		myURLs[i]=new URL(libraryPaths[i]);
	                    	} catch (MalformedURLException ex1) {
	                    		try {
	                    			// probably missing protocol prefix, try to load it as a file
	                    			myURLs[i] = new File(libraryPaths[i]).toURI().toURL();
	                    		} catch (MalformedURLException ex2) {
	                    			throw new RuntimeException("Malformed URL: " + ex1.getMessage());
	                    		}
	                    	}
	                }
	                
	                try {
	                    URLClassLoader classLoader = new URLClassLoader(myURLs,Thread.currentThread().getContextClassLoader());
	                    dbDriver = (Driver) Class.forName(dbDriverName,true,classLoader).newInstance();
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
	    }
		try {
		    // handle encrypted password
		    if (isPasswordEncrypted){
		        decryptPassword(this.config);
		        isPasswordEncrypted=false;
		    }
			dbConnection = dbDriver.connect(config.getProperty(XML_DBURL_ATTRIBUTE), this.config);
		} catch (SQLException ex) {
			throw new RuntimeException("Can't connect to DB :"
					+ ex.getMessage());
		}
		if (dbConnection == null) {
			throw new RuntimeException(
					"Not suitable driver for specified DB URL : " + dbDriver
							+ " ; " + config.getProperty(XML_DBURL_ATTRIBUTE));
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
	synchronized public void free() {
        super.free();

        if(threadSafeConnections) {
    	    for(Iterator i = openedConnections.values().iterator(); i.hasNext();) {
    	        try {
                    Connection c = ((Connection)i.next());
                    if(!c.getAutoCommit()) {
                        c.commit();
                    }
                    c.close();
                } catch (SQLException e) {
                    logger.warn(getId() + " - close operation failed.");
                }
    	    }
        } else {
            try {
                if (!dbConnection.isClosed()) {
                    if (!dbConnection.getAutoCommit()) {
                        dbConnection.commit();
                    }
                    dbConnection.close();
                }
            } catch (SQLException e) {
                logger.warn(getId() + " - close operation failed.");
            }            
        }
	}


	/**
	 *  Gets the connection attribute of the DBConnection object. If threadSafe option
	 * is set, then each call will result in new connection being created.
	 *
	 * @return    The database connection (JDBC)
	 */
	public synchronized Connection getConnection() {
	    Connection con=null;
	    if (threadSafeConnections){
	        con=(Connection)openedConnections.get(Thread.currentThread());
	        if(con==null){
	            connect();
	            con=dbConnection;
	            openedConnections.put(Thread.currentThread(),con);
	        }
		}else{
		    try{
	            if (dbConnection == null || dbConnection.isClosed()){
	                connect();
	            }
	        }catch(SQLException ex){
	            throw new RuntimeException(
						"Can't establish or reuse existing connection : " + dbDriver
								+ " ; " + config.getProperty(XML_DBURL_ATTRIBUTE));
	        }
		    con=dbConnection;
		}
	    return con;
	}


	/**
	 *  Creates new statement with default parameters and returns it
	 *
	 * @return                   The new statement 
	 * @exception  SQLException  Description of the Exception
	 */
	public Statement getStatement() throws SQLException {
		return getConnection().createStatement();
	}

	/**
	 * Creates new statement with specified parameters
	 * 
	 * @param type			one of the following ResultSet constants: ResultSet.TYPE_FORWARD_ONLY, 
	 * 						ResultSet.TYPE_SCROLL_INSENSITIVE, or ResultSet.TYPE_SCROLL_SENSITIVE
	 * @param concurrency	one of the following ResultSet constants: ResultSet.CONCUR_READ_ONLY or 
	 * 						ResultSet.CONCUR_UPDATABLE
	 * @param holdability	one of the following ResultSet constants: ResultSet.HOLD_CURSORS_OVER_COMMIT 
	 * 						or ResultSet.CLOSE_CURSORS_AT_COMMIT
	 * @return				The new statement
	 * @throws SQLException
	 */
	public Statement getStatement(int type,int concurrency,int holdability) throws SQLException {
	    return getConnection().createStatement(type,concurrency,holdability);
	}

	/**
	 *  Creates new prepared statement with default parameters
	 *
	 * @param  sql               SQL/DML query
	 * @return                   Description of the Return Value
	 * @exception  SQLException  Description of the Exception
	 */
	public PreparedStatement prepareStatement(String sql) throws SQLException {
		return getConnection().prepareStatement(sql);
	}

	/**
	 * Creates new prepared statement with specified parameters
	 * 
	 * @param sql			SQL/DML query
	 * @param resultSetType
	 * @param resultSetConcurrency
	 * @param resultSetHoldability
	 * @return
	 * @throws SQLException
	 */
	public PreparedStatement prepareStatement(String sql, int resultSetType,
            int resultSetConcurrency,
            int resultSetHoldability) throws SQLException {
		return getConnection().prepareStatement(sql,resultSetType,resultSetConcurrency,resultSetHoldability);
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
		this.decryptPassword(this.config);
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
	public static DBConnection fromXML(TransformationGraph graph, Element nodeXML) {
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(nodeXML, graph);
		NamedNodeMap attributes = nodeXML.getAttributes();
		DBConnection con;
		// for resolving reference in additional parameters
		// this should be changed in the future when ComponentXMLAttributes
		// is enhanced to support iterating through attributes
		PropertyRefResolver refResolver=new PropertyRefResolver(graph);

		try {
            String id = xattribs.getString(XML_ID_ATTRIBUTE);
			// do we have dbConfig parameter specified ??
			if (xattribs.exists(XML_DBCONFIG_ATTRIBUTE)) {
				return new DBConnection(id, xattribs.getString(XML_DBCONFIG_ATTRIBUTE));
			} else {

				String dbDriver = xattribs.getString(XML_DBDRIVER_ATTRIBUTE);
				String dbURL = xattribs.getString(XML_DBURL_ATTRIBUTE);
				String user = "";
				String password = "";

				con = new DBConnection(id, dbDriver, dbURL, user, password);

				//check thread safe option
				if (xattribs.exists(XML_THREAD_SAFE_CONNECTIONS)){
				    con.setThreadSafeConnections(xattribs.getBoolean(XML_THREAD_SAFE_CONNECTIONS));
				}
				
				//check passwordEncrypted option
				if (xattribs.exists(XML_IS_PASSWORD_ENCRYPTED)){
				    con.setPasswordEncrypted(xattribs.getBoolean(XML_IS_PASSWORD_ENCRYPTED));
				}
				
				// assign rest of attributes/parameters to connection properties so
				// it can be retrieved by DB JDBC driver
				for (int i = 0; i < attributes.getLength(); i++) {
					con.setProperty(attributes.item(i).getNodeName(), refResolver.resolveRef(attributes.item(i).getNodeValue()));
				}
				con.decryptPassword(con.config);

				return con;
			}

		} catch (Exception ex) {
			System.err.println(ex.getMessage());
			return null;
		}

	}

	public void saveConfiguration(OutputStream outStream) throws IOException {
		Properties propsToStore = new Properties();
		propsToStore.putAll(config);
		propsToStore.put(XML_THREAD_SAFE_CONNECTIONS,Boolean.toString(this.threadSafeConnections));
		propsToStore.put(XML_IS_PASSWORD_ENCRYPTED,Boolean.toString(this.isPasswordEncrypted));
		propsToStore.store(outStream,null);
	}
	

	/**
	 *  Description of the Method
	 *
	 * @return    Description of the Return Value
	 */
	public String toString() {
	    StringBuffer strBuf=new StringBuffer(255);
	    strBuf.append("DBConnection driver[").append(config.getProperty(XML_DBDRIVER_ATTRIBUTE));
	    strBuf.append("]:url[").append(config.getProperty(XML_DBURL_ATTRIBUTE));
	    strBuf.append("]:user[").append(config.getProperty(XML_USER_ATTRIBUTE)).append("]");
		return strBuf.toString();
	}
	
    /**
     * @return Returns the threadSafeConnections.
     */
    public boolean isThreadSafeConnections() {
        return threadSafeConnections;
    }
    /**
     * @param threadSafeConnections The threadSafeConnections to set.
     */
    public void setThreadSafeConnections(boolean threadSafeConnections) {
        this.threadSafeConnections = threadSafeConnections;
    }
    
    private boolean parseBoolean(String s) {
        return s != null && s.equalsIgnoreCase("true");
    }
    
 
    /** Decrypt the password entry in the configuration properties if the
     * isPasswordEncrypted property is set to "y" or "yes". If any error occurs
     * and decryption fails, the original password entry will be used.
     * 
     * @param configProperties
     *            configuration properties
     */
    private void decryptPassword(Properties configProperties) {
        if (isPasswordEncrypted){
            Enigma enigma = Enigma.getInstance();
            String decryptedPassword = null;
            try {
                decryptedPassword = enigma.decrypt(configProperties.getProperty(XML_PASSWORD_ATTRIBUTE));
            } catch (JetelException e) {
                logger.error("Can't decrypt password on DBConnection (id=" + this.getId() + "). Please set the password as engine parameter -pass.");
            }
            // If password decryption returns failure, try with the password
            // as it is.
            if (decryptedPassword != null) {
                configProperties.setProperty(XML_PASSWORD_ATTRIBUTE, decryptedPassword);
            }
        }
    } 
    /**
     * @return Returns the isPasswordEncrypted.
     */
    public boolean isPasswordEncrypted() {
        return isPasswordEncrypted;
    }
    /**
     * @param isPasswordEncrypted The isPasswordEncrypted to set.
     */
    public void setPasswordEncrypted(boolean isPasswordEncrypted) {
        this.isPasswordEncrypted = isPasswordEncrypted;
    }

    /* (non-Javadoc)
     * @see org.jetel.graph.GraphElement#checkConfig()
     */
    @Override
    public ConfigurationStatus checkConfig(ConfigurationStatus status) {
        super.checkConfig(status);
        //TODO
        return status;
    }

    /* (non-Javadoc)
     * @see org.jetel.database.IConnection#createMetadata(java.util.Properties)
     */
    public DataRecordMetadata createMetadata(Properties parameters) throws SQLException {
        Statement statement;
        ResultSet resultSet;

        String sqlQuery = parameters.getProperty("sqlQuery");
        if(StringUtils.isEmpty(sqlQuery)) {
            throw new IllegalArgumentException("JDBC stub for clover metadata can't find sqlQuery parameter.");
        }
        
        statement = getStatement();
        resultSet = statement.executeQuery(sqlQuery);
        return SQLUtil.dbMetadata2jetel(resultSet.getMetaData());
    }

}

