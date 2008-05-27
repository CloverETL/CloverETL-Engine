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
package org.jetel.connection.jdbc;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.connection.jdbc.driver.JdbcDriver;
import org.jetel.connection.jdbc.driver.JdbcDriverDescription;
import org.jetel.connection.jdbc.driver.JdbcDriverFactory;
import org.jetel.connection.jdbc.specific.DBConnectionInstance;
import org.jetel.connection.jdbc.specific.DefaultJdbcSpecific;
import org.jetel.connection.jdbc.specific.JdbcSpecific;
import org.jetel.connection.jdbc.specific.JdbcSpecificDescription;
import org.jetel.connection.jdbc.specific.JdbcSpecificFactory;
import org.jetel.connection.jdbc.specific.JdbcSpecific.OperationType;
import org.jetel.data.Defaults;
import org.jetel.database.IConnection;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.JetelException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.GraphElement;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.crypto.Enigma;
import org.jetel.util.file.FileUtils;
import org.jetel.util.primitive.TypedProperties;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.string.StringUtils;
import org.w3c.dom.Element;


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
 *  <tr><td><b>jndi</b></td><td>JNDI name of JDBC data source. Use it to access data source specified by application server. If used, attributes dbDriver, dbURL, user, password and driverLibrary are ignored.</td></tr>
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

    public final static String TRANSACTION_ISOLATION_PROPERTY_NAME="transactionIsolation";
    public final static String SQL_QUERY_PROPERTY = "sqlQuery";

    public final static String XML_JDBC_SPECIFIC_ATTRIBUTE = "jdbcSpecific";
    public final static String XML_DRIVER_LIBRARY_ATTRIBUTE = "driverLibrary";
    public static final String XML_JNDI_NAME_ATTRIBUTE = "jndiName";
    public static final String XML_DBURL_ATTRIBUTE = "dbURL";
    public static final String XML_DBDRIVER_ATTRIBUTE = "dbDriver";
    public static final String XML_DBCONFIG_ATTRIBUTE = "dbConfig";
    public static final String XML_DATABASE_ATTRIBUTE = "database"; // database type - used to lookup in build-in JDBC drivers
    public static final String XML_PASSWORD_ATTRIBUTE = "password";
    public static final String XML_USER_ATTRIBUTE = "user";
    public static final String XML_THREAD_SAFE_CONNECTIONS="threadSafeConnection";
    public static final String XML_IS_PASSWORD_ENCRYPTED = "passwordEncrypted";
    
    public static final String XML_JDBC_PROPERTIES_PREFIX = "jdbc.";
    
    public static final String EMBEDDED_UNLOCK_CLASS = "com.ddtek.jdbc.extensions.ExtEmbeddedConnection";

    // not yet used by component
    public static final String XML_NAME_ATTRIBUTE = "name";

    //Driver dbDriver;//
    //Connection dbConnection;//
    
    private String configFileName;
    
    private String dbUrl;
    private boolean threadSafeConnections;
    private boolean isPasswordEncrypted;
    private String jndiName;
    private String database;
    private String dbDriver;
    private String user;
    private String password;
    private String driverLibrary;
    private String jdbcSpecificId;
    
    // properties specific to the JDBC connection (not used by Clover)
    private TypedProperties jdbcProperties;
    
    private Map<CacheKey, DBConnectionInstance> connectionsCache = new HashMap<CacheKey, DBConnectionInstance>();
    private DBConnectionInstance connectionInstance; //this variable is used in case threadSafe = false
    //private JdbcBaseConfig configBase;//

    //private ClassLoader classLoader;//

    private JdbcDriver jdbcDriver;
    private JdbcSpecific jdbcSpecific;
    private URL[] driverLibraryURLs;
    
    /**
     *  Constructor for the DBConnection object (not used in engine yet)
     *
     * @param  configFilename  properties filename containing definition of driver, dbURL, username, password
     */
    public DBConnection(String id, String configFilename) {
        super(id);
        this.configFileName = configFilename;
    }

    public DBConnection(String id, Properties properties) {
        super(id);
        
        fromProperties(properties);
        
    }

    /**
     * Iterates over the properties and finds which are the standard DBConnection properties
     * and which are the JDBC connection properties.
     * 
     * @param configProperties
     */
	private void fromProperties(Properties properties) {
		TypedProperties typedProperties = new TypedProperties(properties, getGraph());

		setUser(typedProperties.getStringProperty(XML_USER_ATTRIBUTE, null));
		setPassword(typedProperties.getStringProperty(XML_PASSWORD_ATTRIBUTE, null));
		setDbUrl(typedProperties.getStringProperty(XML_DBURL_ATTRIBUTE, null));
		setDbDriver(typedProperties.getStringProperty(XML_DBDRIVER_ATTRIBUTE, null));
		setDatabase(typedProperties.getStringProperty(XML_DATABASE_ATTRIBUTE, null));
		setDriverLibrary(typedProperties.getStringProperty(XML_DRIVER_LIBRARY_ATTRIBUTE, null));
		setJdbcSpecificId(typedProperties.getStringProperty(XML_JDBC_SPECIFIC_ATTRIBUTE, null));
		setJndiName(typedProperties.getStringProperty(XML_JNDI_NAME_ATTRIBUTE, null));
		setThreadSafeConnections(typedProperties.getBooleanProperty(XML_THREAD_SAFE_CONNECTIONS, true));
		setPasswordEncrypted(typedProperties.getBooleanProperty(XML_IS_PASSWORD_ENCRYPTED, false));

		jdbcProperties = typedProperties.getPropertiesStartWith(XML_JDBC_PROPERTIES_PREFIX);
	}
    
	/**
	 * Prepares properties needed to establish connection.
	 * Resulted properties collection contains all extra properties 
	 * (properties with prefix 'jdbc.') and user name and password.
	 * @return
	 */
	public Properties createConnectionProperties() {
		Properties ret = new Properties();
		
		ret.putAll(getExtraProperties());
		
		ret.setProperty(XML_USER_ATTRIBUTE, getUser());
		ret.setProperty(XML_PASSWORD_ATTRIBUTE, getPassword());
		
		return ret;
	}
	
    /* (non-Javadoc)
     * @see org.jetel.graph.GraphElement#init()
     */
    synchronized public void init() throws ComponentNotReadyException {
        if(isInitialized()) return;
        super.init();
        
        if(!StringUtils.isEmpty(configFileName)) {
            try {
            	URL projectURL = getGraph() != null ? getGraph().getProjectURL() : null;
                InputStream stream = FileUtils.getFileURL(projectURL, configFileName).openStream();

                Properties tempProperties = new Properties();
                tempProperties.load(stream);
                fromProperties(tempProperties);
                stream.close();
            } catch (Exception ex) {
                throw new ComponentNotReadyException(ex);
            }
        }

        prepareDriverLibraryURLs();
        prepareJdbcSpecific();
        prepareJdbcDriver();
        
        //check validity of the given url
        try {
            if (!getJdbcDriver().getDriver().acceptsURL(getDbUrl())) {
                throw new ComponentNotReadyException("Unacceptable connection url: '" + getDbUrl() + "'");
            }
        } catch (SQLException e) {
            throw new ComponentNotReadyException(e);
        } 

        //decrypt password
        decryptPassword();
    }

    private void prepareJdbcDriver() throws ComponentNotReadyException {
        if(!StringUtils.isEmpty(getJndiName())) {
        	return;
        }
        
        if(!StringUtils.isEmpty(getDatabase())) {
            //database connection is parameterized by DB identifier to the list of build-in JDBC drivers
            String database = getDatabase();
            JdbcDriverDescription jdbcDriverDescription = JdbcDriverFactory.getJdbcDriverDescriptor(database);
            
            if(jdbcDriverDescription == null) {
                throw new ComponentNotReadyException("Can not create JDBC driver '" + database + "'. This type of JDBC driver is not supported.");
            }
            
            jdbcDriver = jdbcDriverDescription.createJdbcDriver();
        } else {
        	//database connection is full specified by dbDriver and driverLibrary attributes
            jdbcDriver = new JdbcDriver(null, null, getDbDriver(), getDriverLibraryURLs(), getJdbcSpecific());
        }
    }

    public synchronized DBConnectionInstance getConnection(String elementId) throws JetelException {
    	return getConnection(elementId, OperationType.UNKNOWN);
    }

    /**
     *  Gets the connection attribute of the DBConnection object. If threadSafe option
     * is set, for each graph element there is created new connection object
     *
     * @return    The database connection (JDBC)
     * @throws JetelException 
     */
    public synchronized DBConnectionInstance getConnection(String elementId, OperationType operationType) throws JetelException {
        DBConnectionInstance connection = null;
        
        if (isThreadSafeConnections()) {
        	CacheKey key = new CacheKey(elementId, operationType);
            connection = (DBConnectionInstance) connectionsCache.get(key);
            if (connection == null) {
                connection = new DBConnectionInstance(this, connect(operationType), operationType);
                connectionsCache.put(key, connection);
            }
        } else {
            if (connectionInstance == null) {
                connectionInstance = new DBConnectionInstance(this, connect(operationType), operationType);
            }
            connection = connectionInstance;
        }
        
        return connection;
    }

    private Connection connect(OperationType operationType) throws JetelException {
    	if (!StringUtils.isEmpty(getJndiName())) {
        	try {
            	Context initContext = new InitialContext();
           		DataSource ds = (DataSource)initContext.lookup(getJndiName());
               	return ds.getConnection();
        	} catch (Exception e) {
        		throw new JetelException("Cannot establish DB connection to JNDI:" + getJndiName() + " " + e.getMessage(), e);
        	}
    	} else {
        	try {
				return getJdbcSpecific().createSQLConnection(operationType, this);
			} catch (JetelException e) {
				throw new JetelException("Cannot establish DB connection (" + getId() + ").", e);
			}
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
//    private void connect() {
//        logger.debug("DBConnection (" + getId() +"), component ["+Thread.currentThread().getName() +"] attempts to connect to the database");
//
//        if(!isInitialized()) {
//            throw new RuntimeException("DBConnection (" + getId() +") is not initialized.");
//        }
//        if (dbDriver == null){
//            try {
//                prepareDbDriver();
//            } catch (ComponentNotReadyException e) {
//                throw new RuntimeException(e); 
//            }
//        }
//        try {
//            // handle encrypted password
//            if (isPasswordEncrypted){
//                decryptPassword(this.config);
//                isPasswordEncrypted=false;
//            }
//            
//            Properties connectionProps = new Properties();
//            String user = config.getProperty(XML_USER_ATTRIBUTE);
//            if (user != null) {
//            	connectionProps.setProperty(XML_USER_ATTRIBUTE, user);
//            }
//            String password = config.getProperty(XML_PASSWORD_ATTRIBUTE);
//            if (password != null) {
//            	connectionProps.setProperty(XML_PASSWORD_ATTRIBUTE, password);
//            }
//            connectionProps.putAll(jdbcConfig);
//            dbConnection = dbDriver.connect(config.getProperty(XML_DBURL_ATTRIBUTE), connectionProps);
//
//            // unlock initiatesystems driver
//            try {
//                Class embeddedConClass;
//                if (classLoader == null) {
//                    embeddedConClass = Class.forName(EMBEDDED_UNLOCK_CLASS);
//                } else {
//                    embeddedConClass = Class.forName(EMBEDDED_UNLOCK_CLASS, true, classLoader);
//                }
//                if (embeddedConClass != null) {
//                    if(embeddedConClass.isInstance(dbConnection)) {
//                            java.lang.reflect.Method unlockMethod = 
//                                embeddedConClass.getMethod("unlock", new Class[] { String.class});
//                            unlockMethod.invoke(dbConnection, new Object[] { "INITIATESYSTEMSINCJDBCPW" });
//                    }
//                }
//            } catch (Exception ex) {
//            }
//        } catch (SQLException ex) {
//            throw new RuntimeException("Can't connect to DB :"
//                    + ex.getMessage());
//        }
//        if (dbConnection == null) {
//            throw new RuntimeException(
//                    "Not suitable driver for specified DB URL : " + dbDriver
//                            + " ; " + config.getProperty(XML_DBURL_ATTRIBUTE));
//        }
//        // try to set Transaction isolation level, it it was specified
//        if (config.containsKey(TRANSACTION_ISOLATION_PROPERTY_NAME)) {
//            int trLevel;
//            String isolationLevel = config
//                    .getProperty(TRANSACTION_ISOLATION_PROPERTY_NAME);
//            if (isolationLevel.equalsIgnoreCase("READ_UNCOMMITTED")) {
//                trLevel = Connection.TRANSACTION_READ_UNCOMMITTED;
//            } else if (isolationLevel.equalsIgnoreCase("READ_COMMITTED")) {
//                trLevel = Connection.TRANSACTION_READ_COMMITTED;
//            } else if (isolationLevel.equalsIgnoreCase("REPEATABLE_READ")) {
//                trLevel = Connection.TRANSACTION_REPEATABLE_READ;
//            } else if (isolationLevel.equalsIgnoreCase("SERIALIZABLE")) {
//                trLevel = Connection.TRANSACTION_SERIALIZABLE;
//            } else {
//                trLevel = Connection.TRANSACTION_NONE;
//            }
//            try {
//                dbConnection.setTransactionIsolation(trLevel);
//            } catch (SQLException ex) {
//                // we do nothing, if anything goes wrong, we just
//                // leave whatever was the default
//            }
//        }
//        // DEBUG logger.debug("DBConenction (" + getId() +") finishes connect function to the database at " + simpleDateFormat.format(new Date()));
//    }

    /**
     *  Description of the Method
     *
     * @exception  SQLException  Description of the Exception
     */
    synchronized public void free() {
        if (!isInitialized()) return;
        super.free();

        if (threadSafeConnections) {
            for (DBConnectionInstance connectionInstance : connectionsCache.values()) {
            	Connection connection = connectionInstance.getSqlConnection();
            	closeConnection(connection);
            }
        } else {
        	if (connectionInstance != null) {
            	Connection connection = connectionInstance.getSqlConnection();
            	closeConnection(connection);
        	}
        }
    }

    private void closeConnection(Connection connection) {
        try {
        	if (!connection.isClosed()) {
                if (!connection.getAutoCommit()) {
                    connection.commit();
                }
                connection.close();
        	}
        } catch (SQLException e) {
            logger.warn("DBConnection '" + getId() + "' close operation failed.");
        }
    }
    
    /**
     *  Creates new statement with default parameters and returns it
     *
     * @return                   The new statement 
     * @exception  SQLException  Description of the Exception
     */
//    public Statement getStatement() throws SQLException {
//        return getConnection().createStatement();
//    }

    /**
     * Creates new statement with specified parameters
     * 
     * @param type          one of the following ResultSet constants: ResultSet.TYPE_FORWARD_ONLY, 
     *                      ResultSet.TYPE_SCROLL_INSENSITIVE, or ResultSet.TYPE_SCROLL_SENSITIVE
     * @param concurrency   one of the following ResultSet constants: ResultSet.CONCUR_READ_ONLY or 
     *                      ResultSet.CONCUR_UPDATABLE
     * @param holdability   one of the following ResultSet constants: ResultSet.HOLD_CURSORS_OVER_COMMIT 
     *                      or ResultSet.CLOSE_CURSORS_AT_COMMIT
     * @return              The new statement
     * @throws SQLException
     */
//    public Statement getStatement(int type,int concurrency,int holdability) throws SQLException {
//        return getConnection().createStatement(type,concurrency,holdability);
//    }

    /**
     *  Creates new prepared statement with default parameters
     *
     * @param  sql               SQL/DML query
     * @return                   Description of the Return Value
     * @exception  SQLException  Description of the Exception
     */
//    public PreparedStatement prepareStatement(String sql) throws SQLException {
//        return getConnection().prepareStatement(sql);
//    }
//
//    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)  throws SQLException{
//        return getConnection().prepareStatement(sql, autoGeneratedKeys);
//    }
//    
//    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException{
//        return getConnection().prepareStatement(sql, columnIndexes);
//    }
    
    /**
     * Creates new prepared statement with specified parameters
     * 
     * @param sql           SQL/DML query
     * @param resultSetType
     * @param resultSetConcurrency
     * @param resultSetHoldability
     * @return
     * @throws SQLException
     */
//    public PreparedStatement prepareStatement(String sql, int resultSetType,
//            int resultSetConcurrency,
//            int resultSetHoldability) throws SQLException {
//        return getConnection().prepareStatement(sql,resultSetType,resultSetConcurrency,resultSetHoldability);
//    }
//
    /**
     *  Sets the property attribute of the DBConnection object
     *
     * @param  name   The new property value
     * @param  value  The new property value
     */
//    public void setProperty(String name, String value) {
//    	Properties p = new Properties();
//    	p.setProperty(name, value);
//    	loadProperties(p);
//    }


    /**
     *  Sets the property attribute of the DBConnection object
     *
     * @param  properties  The new property value
     */
//    public void setProperty(Properties properties) {
//    	loadProperties(properties);
//        this.decryptPassword(this.config);
//    }


    /**
     *  Gets the property attribute of the DBConnection object
     *
     * @param  name  Description of the Parameter
     * @return       The property value
     */
//    public String getProperty(String name) {
//        return config.getProperty(name);
//    }


    /**
     *  Description of the Method
     *
     * @param  nodeXML  Description of the Parameter
     * @return          Description of the Return Value
     * @throws XMLConfigurationException 
     */
    public static DBConnection fromXML(TransformationGraph graph, Element nodeXML) throws XMLConfigurationException {
        ComponentXMLAttributes xattribs = new ComponentXMLAttributes(nodeXML, graph);

        try {
            String id = xattribs.getString(XML_ID_ATTRIBUTE);
            // do we have dbConfig parameter specified ??
            if (xattribs.exists(XML_DBCONFIG_ATTRIBUTE)) {
                return new DBConnection(id, xattribs.getString(XML_DBCONFIG_ATTRIBUTE));
            } else {
                Properties connectionProps  = xattribs.attributes2Properties(new String[] {XML_ID_ATTRIBUTE});
                
                return new DBConnection(id, connectionProps);
            }
		} catch (Exception e) {
            throw new XMLConfigurationException("DBConnection: " 
            		+ xattribs.getString(XML_ID_ATTRIBUTE, "unknown ID") + ":" + e.getMessage(), e);
		}
    }
    
    public void saveConfiguration(OutputStream outStream) throws IOException {
        Properties propsToStore = new Properties();

        TypedProperties extraProperties = getExtraProperties();
        Set<Object> jdbcProps = extraProperties.keySet();
        for (Object key : jdbcProps) {
        	String propName = (String) key; 
			propsToStore.setProperty(XML_JDBC_PROPERTIES_PREFIX + propName, extraProperties.getProperty(propName));
		}

        propsToStore.setProperty(XML_USER_ATTRIBUTE, getUser());
        propsToStore.setProperty(XML_PASSWORD_ATTRIBUTE, getPassword());
        propsToStore.setProperty(XML_DBURL_ATTRIBUTE, getDbUrl());
        propsToStore.setProperty(XML_DBDRIVER_ATTRIBUTE, getDbDriver());
        propsToStore.setProperty(XML_DATABASE_ATTRIBUTE, getDatabase());
        propsToStore.setProperty(XML_DRIVER_LIBRARY_ATTRIBUTE, getDriverLibrary());
        propsToStore.setProperty(XML_JDBC_SPECIFIC_ATTRIBUTE, getJdbcSpecificId());
        propsToStore.setProperty(XML_JNDI_NAME_ATTRIBUTE, getJndiName());
        propsToStore.setProperty(XML_THREAD_SAFE_CONNECTIONS, Boolean.toString(isThreadSafeConnections()));
        propsToStore.setProperty(XML_IS_PASSWORD_ENCRYPTED, Boolean.toString(isPasswordEncrypted()));

        propsToStore.store(outStream, null);
    }
    

    /**
     *  Description of the Method
     *
     * @return    Description of the Return Value
     */
    public String toString() {
    	if (!isInitialized()) {
    		return "DBConnection id='" + getId() + "' - not initialized";
    	}
    	
        StringBuffer strBuf = new StringBuffer(255);
        strBuf.append("DBConnection driver[").append(getJdbcDriver());
        strBuf.append("]:jndi[").append(getJndiName());
        strBuf.append("]:url[").append(getDbUrl());
        strBuf.append("]:user[").append(getUser()).append("]");
        
        return strBuf.toString();
    }
    
    /** Decrypt the password entry in the configuration properties if the
     * isPasswordEncrypted property is set to "y" or "yes". If any error occurs
     * and decryption fails, the original password entry will be used.
     * 
     * @param configProperties
     *            configuration properties
     */
    private void decryptPassword() {
        if (isPasswordEncrypted()) {
            Enigma enigma = getGraph().getEnigma();
            String decryptedPassword = null;
            try {
                decryptedPassword = enigma.decrypt(getPassword());
            } catch (JetelException e) {
                logger.error("Can't decrypt password on DBConnection (id=" + this.getId() + "). Please set the password as engine parameter -pass.");
            }
            // If password decryption returns failure, try with the password
            // as it is.
            if (decryptedPassword != null) {
            	setPassword(decryptedPassword);
            }
        }
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
    	if (!isInitialized()) {
    		throw new IllegalStateException("DBConnection has to be initialized to be able to create metadata.");
    	}
    	
        Statement statement;
        ResultSet resultSet;

        String sqlQuery = parameters.getProperty(SQL_QUERY_PROPERTY);
        if(StringUtils.isEmpty(sqlQuery)) {
            throw new IllegalArgumentException("JDBC stub for clover metadata can't find sqlQuery parameter.");
        }
        
        Connection connection;
		try {
			connection = connect(OperationType.UNKNOWN);
		} catch (JetelException e) {
			throw new SQLException(e.getMessage());
		}
        statement = connection.createStatement();
        resultSet = statement.executeQuery(sqlQuery);
        
        return SQLUtil.dbMetadata2jetel(resultSet.getMetaData(), getJdbcSpecific());
    }


//	public Properties getJdbcConfig() {
//		return jdbcConfig;
//	}

//	public JdbcBaseConfig getConfigBase(){
//		if (configBase != null) return configBase;
//		String tmp = config.getProperty(XML_DATABASE_ATTRIBUTE);
//        if(!StringUtils.isEmpty(tmp)) {
//        	configBase = JdbcConfigFactory.createConfig(tmp);
//        	return configBase;
//        }
//    	tmp = config.getProperty(XML_DBDRIVER_ATTRIBUTE);
//        if(!StringUtils.isEmpty(tmp)) {
//        	configBase = JdbcConfigFactory.createConfig(tmp);
//        	return configBase;
//        }
//        try {
//			configBase = JdbcConfigFactory.createConfig(getConnection(getId()).getMetaData().getDriverName());
//		} catch (SQLException e) {
//			logger.warn("Problem creating connection configuration", e);
//			configBase = JdbcBaseConfig.getInstance();
//			logger.info("Using connection configuration for " + configBase.getTargetDBName());
//		}
//		return configBase;
//	}

//	public Properties getConnectionProperties() {
//  Properties connectionProps = new Properties();
//  
//  connectionProps.setProperty(XML_USER_ATTRIBUTE, getUser());
//  connectionProps.setProperty(XML_PASSWORD_ATTRIBUTE, getPassword());
//  
//  connectionProps.putAll(jdbcConfig);
//
//  return null;
//}

    public boolean isThreadSafeConnections() {
        return threadSafeConnections;
    }
    
    protected void setThreadSafeConnections(boolean threadSafeConnections) {
        this.threadSafeConnections = threadSafeConnections;
    }
 
    public boolean isPasswordEncrypted() {
        return isPasswordEncrypted;
    }
    
    protected void setPasswordEncrypted(boolean isPasswordEncrypted) {
        this.isPasswordEncrypted = isPasswordEncrypted;
    }

	public String getJndiName() {
		return jndiName;
	}

	protected void setJndiName(String jndiName) {
		this.jndiName = jndiName;
	}

	public JdbcDriver getJdbcDriver() {
		return jdbcDriver;
	}

	protected void setJdbcDriver(JdbcDriver jdbcDriver) {
		this.jdbcDriver = jdbcDriver;
	}

	public String getDbUrl() {
		return dbUrl;
	}
	
	protected void setDbUrl(String dbUrl) {
		this.dbUrl = dbUrl;
	}

	public String getUser() {
		return user;
	}
	
	protected void setUser(String user) {
		this.user = user;
	}

	public String getPassword() {
		return password;
	}

	protected void setPassword(String password) {
		this.password = password;
	}

	public String getDatabase() {
		return database;
	}

	protected void setDatabase(String database) {
		this.database = database;
	}

	public String getDbDriver() {
		return dbDriver;
	}

	protected void setDbDriver(String dbDriver) {
		this.dbDriver = dbDriver;
	}

	public String getDriverLibrary() {
		return driverLibrary;
	}

	private void prepareDriverLibraryURLs() throws ComponentNotReadyException {
		if(!StringUtils.isEmpty(driverLibrary)) {
	        String[] libraryPaths = driverLibrary.split(Defaults.DEFAULT_PATH_SEPARATOR_REGEX);
	    	driverLibraryURLs = new URL[libraryPaths.length];
	
	    	for(int i = 0; i < libraryPaths.length; i++) {
	            try {
	                driverLibraryURLs[i] = FileUtils.getFileURL(getGraph() != null ? getGraph().getProjectURL() : null, libraryPaths[i]);
	            } catch (MalformedURLException ex1) {
	                throw new ComponentNotReadyException("Can not create JDBC connection '" + getId() + "'. Malformed URL: " + ex1.getMessage(), ex1);
	            }
	        }
		}
	}
	
	private URL[] getDriverLibraryURLs() {
		return driverLibraryURLs;
	}
	
	protected void setDriverLibrary(String driverLibrary) {
		this.driverLibrary = driverLibrary;
	}

	public String getJdbcSpecificId() {
		return jdbcSpecificId;
	}

	protected void setJdbcSpecificId(String jdbcSpecificId) {
		this.jdbcSpecificId = jdbcSpecificId;
	}

	private void prepareJdbcSpecific() throws ComponentNotReadyException {
		if(!StringUtils.isEmpty(getJdbcSpecificId())) {
			JdbcSpecificDescription jdbcSpecificDescription = JdbcSpecificFactory.getJdbcSpecificDescription(getJdbcSpecificId());
			if(jdbcSpecificDescription != null) {
				jdbcSpecific = jdbcSpecificDescription.getJdbcSpecific();
			} else {
				throw new ComponentNotReadyException("JDBC specific '" + getJdbcSpecificId() + "' does not exist.");
			}
		}
	}
	
	public JdbcSpecific getJdbcSpecific() {
		if(jdbcSpecific != null) {
			return jdbcSpecific;
		} else {
			JdbcSpecific ret = getJdbcDriver().getJdbcSpecific();
			if(ret != null) {
				return ret;
			} else {
				return DefaultJdbcSpecific.INSTANCE;
			}
		}
	}
	
	public TypedProperties getExtraProperties() {
		return jdbcProperties;
	}
	
	/**
	 * This class is used as a key value to the connectionsCache map.
	 * 
	 * @author Martin Zatopek (martin.zatopek@javlinconsulting.cz)
	 *         (c) Javlin Consulting (www.javlinconsulting.cz)
	 *
	 * @created May 22, 2008
	 */
	private static class CacheKey {
		private String elementId;
		private OperationType operationType;
		
		private int hashCode;
		
		public CacheKey(String elementId, OperationType operationType) {
			this.elementId = elementId;
			this.operationType = operationType;
		}

		public String getElementId() {
			return elementId;
		}

		public OperationType getOperationType() {
			return operationType;
		}
		
		@Override
		public boolean equals(Object obj) {
			if(super.equals(obj)) {
				return true;
			}
			
			if(!(obj instanceof CacheKey)) {
				return false;
			}
			
			CacheKey key = (CacheKey) obj;
			
			return elementId.equals(key.elementId) && operationType == key.operationType;
		}
		
		@Override
		public int hashCode() {
			if(hashCode == 0) {
				hashCode = (23 + elementId.hashCode()) * 37 + operationType.hashCode();
			}
			return hashCode;
		}
	}

}

