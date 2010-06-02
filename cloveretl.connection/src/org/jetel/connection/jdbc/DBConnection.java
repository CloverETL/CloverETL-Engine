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
import java.util.Enumeration;
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
import org.jetel.connection.jdbc.specific.JdbcSpecific;
import org.jetel.connection.jdbc.specific.JdbcSpecificDescription;
import org.jetel.connection.jdbc.specific.JdbcSpecificFactory;
import org.jetel.connection.jdbc.specific.JdbcSpecific.OperationType;
import org.jetel.connection.jdbc.specific.impl.DefaultJdbcSpecific;
import org.jetel.data.Defaults;
import org.jetel.database.IConnection;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.JetelException;
import org.jetel.exception.NotInitializedException;
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
 *  It practically wraps around JDBC's Connection class and adds some useful methods.
 *  <br>
 *  <table border="1">
 *  <th>XML attributes:</th>
 *  <tr><td><b>id</b></td><td>connection identification</td>
 *  <tr><td><b>dbConfig</b><br><i>optional</i></td><td>filename of the config file from which to take connection parameters<br>
 *  If used, then all other attributes are ignored.</td></tr>
 *  <tr><td><b>jndi</b></td><td>JNDI name of JDBC data source. 
 *  Use it to access data source specified by application server. 
 *  If used, attributes dbDriver, dbURL, user, password and driverLibrary are ignored.</td></tr>
 *  <tr><td><b>database</b></td><td>database identifier, which is used to lookup build-in drivers</td></tr>
 *  <tr><td><b>dbDriver</b></td><td>name of the JDBC driver</td></tr>
 *  <tr><td><b>dbURL</b></td><td>URL of the database (aka connection string)</td></tr>
 *  <tr><td><b>user</b><br><i>optional</i></td><td>username to use when connecting to DB</td></tr>
 *  <tr><td><b>password</b><br><i>optional</i></td><td>password to use when connecting to DB</td></tr>
 *  <tr><td><b>driverLibrary</b><br><i>optional</i></td><td>name(s) (full path) of Java library file(s) (.jar,.zip,...) where
 *  to search for class containing JDBC driver specified in <tt>dbDriver</tt> parameter.<br>
 *  In case of more libraries, use system path separator to delimit them (e.g. ";").</td></tr>
 * <tr><td><b>threadSafeConnection</b><br><i>optional</i></td><td>if set, each thread gets its own connection. <i>Can be used
 * to prevent problems when multiple components conversate with DB through the same connection object which is
 * not thread safe.</i></td></tr>
 * <i>Note: Default value of this property is true.</i></td></tr> 
 * </table>
 *  <h4>Example:</h4>
 *  <pre>&lt;Connection type="JDBC" dbConfig="connection.cfg" id="Connection2" /&gt;</pre>
 * <i>Note: any XML attribute name can also be used in the dbConfig file. If the option is set there, then
 * the value is applied to the connection object created.</i>
 * <h4>Example of dbConfig file:</h4>
 * <pre>**********
 * driverLibrary=file\:/C\:/jdbcDrivers/ojdbc14.jar
 * user=test
 * dbDriver=oracle.jdbc.OracleDriver
 * name=OracleDB
 * password=test
 * threadSafeConnection=true
 * dbURL=jdbc\:oracle\:thin\:@koule\:1521\:xe
 * ********</pre>
 * 
 * The XML DTD describing the internal structure is as follows:
 * 
 *  * &lt;!ATTLIST Connection
 *              id ID #REQUIRED
 *              type NMTOKEN (JDBC) #REQUIRED
 *              database CDATA #IMPLIED
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
 * @author      dpavlis, mzatopek
 * @since       21. b?ezen 2004
 * @revision    $Revision$
 * @created     January 15, 2003
 */
public class DBConnection extends GraphElement implements IConnection {

    private static final Log logger = LogFactory.getLog(DBConnection.class);

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
    public static final String XML_HOLDABILITY  = "holdability";
    public static final String XML_TRANSACTION_ISOLATION = "transactionIsolation";
    
    public static final String XML_JDBC_PROPERTIES_PREFIX = "jdbc.";
    
    /**
     * Enum for the transaction isolation property values.
     * 
     * @author Jaroslav Urban (jaroslav.urban@javlin.eu)
     *         (c) Javlin a.s. (www.javlin.eu)
     *
     * @since Oct 2, 2009
     */
    public static enum TransactionIsolation {
    	TRANSACTION_NONE(Connection.TRANSACTION_NONE),
    	READ_UNCOMMITTED(Connection.TRANSACTION_READ_UNCOMMITTED),
    	READ_COMMITTED(Connection.TRANSACTION_READ_COMMITTED),
    	REPEATABLE_READ(Connection.TRANSACTION_REPEATABLE_READ),
    	SERIALIZABLE(Connection.TRANSACTION_SERIALIZABLE);
    	
    	private int code;
    	
    	// map of transaction isolation codes to their enum values
    	private static HashMap<Integer, TransactionIsolation> codeMap = 
    		new HashMap<Integer, TransactionIsolation>();
    	
    	static {
    		TransactionIsolation[] values = TransactionIsolation.values();
    		for (TransactionIsolation transationIsolation : values) {
				codeMap.put(transationIsolation.getCode(), transationIsolation);
			}
    	}
    	
    	/**
    	 * @param code
    	 * @return enum value for the transaction isolation code.
    	 */
    	public static TransactionIsolation fromCode(int code) {
    		return codeMap.get(code);
    	}
    	
    	/**
    	 * Allocates a new <tt>TransactionIsolation</tt> object.
    	 *
    	 * @param code
    	 */
    	private TransactionIsolation(int code) {
    		this.code = code;
    	}
    	
    	/**
    	 * @return the number code for the transaction isolation.
    	 */
    	public int getCode() {
    		return this.code;
    	}
    }
    
    /**
     * Enum for the holdability property values.
     * 
     * @author Jaroslav Urban (jaroslav.urban@javlin.eu)
     *         (c) Javlin a.s. (www.javlin.eu)
     *
     * @since Oct 2, 2009
     */
    public static enum Holdability {
    	HOLD_CURSORS(ResultSet.HOLD_CURSORS_OVER_COMMIT),
    	CLOSE_CURSORS(ResultSet.CLOSE_CURSORS_AT_COMMIT);
    	
    	private int code;
    	
    	// map of holdability codes to their enum values
    	private static HashMap<Integer, Holdability> codeMap = 
    		new HashMap<Integer, Holdability>();
    	
    	static {
    		Holdability[] values = Holdability.values();
    		for (Holdability holdability : values) {
				codeMap.put(holdability.getCode(), holdability);
			}
    	}
    	
    	/**
    	 * @param code
    	 * @return enum value for the holdability code.
    	 */
    	public static Holdability fromCode(int code) {
    		return codeMap.get(code);
    	}


    	/**
    	 * Allocates a new <tt>Holdability</tt> object.
    	 *
    	 * @param code
    	 */
    	private Holdability(int code) {
    		this.code = code;
    	}
    	
    	/**
    	 * @return the number code for the holdability.
    	 */
    	public int getCode() {
    		return this.code;
    	}
    }
    
    // not yet used by component
    public static final String XML_NAME_ATTRIBUTE = "name";

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
    private Integer holdability;
    private Integer transactionIsolation;
    
    // properties specific to the JDBC connection (not used by Clover)
    private TypedProperties jdbcProperties;
    
    private Map<CacheKey, DBConnectionInstance> connectionsCache = new HashMap<CacheKey, DBConnectionInstance>();
    private DBConnectionInstance connectionInstance; //this variable is used in case threadSafe = false

    private JdbcDriver jdbcDriver;
    private JdbcSpecific jdbcSpecific;
    private URL[] driverLibraryURLs;
    
    /**
     *  Constructor for the DBConnection object.
     *
     * @param  configFilename  properties filename containing definition of driver, dbURL, username, password
     */
    public DBConnection(String id, String configFilename) {
        super(id);
        this.configFileName = configFilename;
    }

    /**
     * Constructor.
     * @param id
     * @param properties
     */
    public DBConnection(String id, Properties properties) {
        super(id);
        
        fromProperties(properties);
    }

    /**
     * Iterates over the given properties and finds which are the standard DBConnection properties
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
		try {
			setHoldability(typedProperties.getIntProperty(XML_HOLDABILITY));
		} catch (NumberFormatException e) {
			String propertyValue = typedProperties.getStringProperty(XML_HOLDABILITY);
			try {
				Holdability holdability = Holdability.valueOf(propertyValue);
				setHoldability(holdability.getCode());
			} catch (IllegalArgumentException ex) {
				logger.warn("Unknown holdability");
			}
		}
		try {
			setTransactionIsolation(typedProperties.getIntProperty(XML_TRANSACTION_ISOLATION));
		} catch (Exception e) {
			String propertyValue = typedProperties.getStringProperty(XML_TRANSACTION_ISOLATION);
			try {
				TransactionIsolation transactionIsolation = TransactionIsolation.valueOf(propertyValue);
				setTransactionIsolation(transactionIsolation.getCode());
			} catch (IllegalArgumentException ex) {
				logger.warn("Unknown transaction isolation");
			}
		}

		// strip the "jdbc." prefix from the custom properties
		jdbcProperties = new TypedProperties(null, getGraph());;
		TypedProperties customProps = typedProperties.getPropertiesStartWith(XML_JDBC_PROPERTIES_PREFIX);
		Set<Object> keys = customProps.keySet();
		for (Object key : keys) {
			String value = customProps.getProperty((String) key);

			String newKey = (String) key;
			newKey = newKey.substring(XML_JDBC_PROPERTIES_PREFIX.length());
		
			jdbcProperties.setProperty(newKey, value);
		}
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
            	URL projectURL = getGraph() != null ? getGraph().getRuntimeContext().getContextURL() : null;
                InputStream stream = FileUtils.getInputStream(projectURL, configFileName);

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
        // but only for jdbc connection
        try {
            if (StringUtils.isEmpty(getJndiName())) {
            	if (!getJdbcDriver().getDriver().acceptsURL(getDbUrl())) {
            		throw new ComponentNotReadyException("Unacceptable connection url: '" + getDbUrl() + "'");
            	}
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
            jdbcDriver = new JdbcDriver(null, getDbDriver(), getDbDriver(), getDriverLibraryURLs(), getJdbcSpecific(), null);
        }
    }

    /**
     * @param elementId
     * @return
     * @throws JetelException
     */
    public synchronized DBConnectionInstance getConnection(String elementId) throws JetelException {
    	return getConnection(elementId, OperationType.UNKNOWN);
    }

    /**
     * Returns connection instance for the given elementId and operation type. If this db connection
     * is threads safe, all connection are cached, and elementId and operation type 
     * servers as key to hash map.
     * This is main method for using DBConnection class. Each graph element can claim its respective db connection.  
     * @param elementId
     * @param operationType
     * @return
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
				return getJdbcSpecific().createSQLConnection(this, operationType);
			} catch (JetelException e) {
				throw new JetelException("Cannot establish DB connection (" + getId() + ").", e);
			}
    	}
    }

    @Override
    public synchronized void reset() throws ComponentNotReadyException {
    	super.reset();
    	//TODO: Issue 2939; close connections only when desired (make changes to JdbcSpecific)
//    	closeConnections();
    }

    /**
     * Commits and closes all allocated connections.
     * 
     * @see org.jetel.graph.GraphElement#free()
     */
    @Override
    public synchronized void free() {
        if (!isInitialized()) {
            throw new NotInitializedException(this);
        }

		super.free();
		closeConnections();

		if (jdbcDriver != null) {
			jdbcDriver.free();
		}
	}

    private void closeConnections() {
        if (threadSafeConnections) {
            for (DBConnectionInstance connectionInstance : connectionsCache.values()) {
            	Connection connection = connectionInstance.getSqlConnection();
            	closeConnection(connection);
            }
            connectionsCache.clear();
        }

        if (connectionInstance != null) {
			Connection connection = connectionInstance.getSqlConnection();
			closeConnection(connection);
			connectionInstance = null;
		}
    }

    private void closeConnection(Connection connection) {
        try {
        	if (!connection.isClosed()) {
                if (!connection.getAutoCommit()) {
                    try {
						connection.commit();
					} catch (SQLException e) {
			            logger.warn("DBConnection '" + getId() + "' commit operation failed.");
					}
                }
                connection.close();
        	}
        } catch (SQLException e) {
            logger.warn("DBConnection '" + getId() + "' close operation failed.");
        }
    }
    
    /**
     * Closes connection stored in cache under key specified by elementId and OperationType.UNKNOWN.
     * Closed connection is also removed from cache.
     */
    public synchronized void closeConnection(String elementId) {
    	closeConnection(elementId, OperationType.UNKNOWN);
    }
    
    /**
     * Closes connection stored in cache under key specified by elementId and operationType.
     * Connection is closed only if DBConnection is thread-safe.
     * Closed connection is also removed from cache.
     */
    public synchronized void closeConnection(String elementId, OperationType operationType) {
    	if (isThreadSafeConnections()) {
        	CacheKey key = new CacheKey(elementId, operationType);
        	DBConnectionInstance connection = connectionsCache.remove(key); 
        	closeConnection(connection.getSqlConnection());
        }
    }
    
    /**
     * Creates DBConnection based on xml node.
     * 
     * @param graph
     * @param nodeXML
     * @return
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

    /**
     * Saves to the given output stream all DBConnection properties.
     * @param outStream
     * @throws IOException
     */
    public void saveConfiguration(OutputStream outStream) throws IOException {
    	saveConfiguration(outStream, null);
    }
    
    /**
     * Saves to the given output stream all DBConnection properties.
     * @param outStream
     * @throws IOException
     */
    public void saveConfiguration(OutputStream outStream, Properties moreProperties) throws IOException {
        Properties propsToStore = new Properties();

        TypedProperties extraProperties = getExtraProperties();
        Set<Object> jdbcProps = extraProperties.keySet();
        for (Object key : jdbcProps) {
        	String propName = (String) key; 
			propsToStore.setProperty(XML_JDBC_PROPERTIES_PREFIX + propName, extraProperties.getProperty(propName));
		}

        if (moreProperties != null) {
        	for(Enumeration enu = moreProperties.propertyNames(); enu.hasMoreElements(); ) {
        		String key = (String) enu.nextElement();
        		propsToStore.setProperty(key, moreProperties.getProperty(key));
        	}
        }
        
        
        if(getUser() != null) {
        	propsToStore.setProperty(XML_USER_ATTRIBUTE, getUser());
        }
        if(getPassword() != null) {
        	propsToStore.setProperty(XML_PASSWORD_ATTRIBUTE, getPassword());
        }
        if(getDbUrl() != null) {
        	propsToStore.setProperty(XML_DBURL_ATTRIBUTE, getDbUrl());
        }
        if(getDbDriver() != null) {
        	propsToStore.setProperty(XML_DBDRIVER_ATTRIBUTE, getDbDriver());
        }
        if(getDatabase() != null) {
        	propsToStore.setProperty(XML_DATABASE_ATTRIBUTE, getDatabase());
        }
        if(getDriverLibrary() != null) {
        	propsToStore.setProperty(XML_DRIVER_LIBRARY_ATTRIBUTE, getDriverLibrary());
        }
        if(getJdbcSpecificId() != null) {
        	propsToStore.setProperty(XML_JDBC_SPECIFIC_ATTRIBUTE, getJdbcSpecificId());
        }
        if(getJndiName() != null) {
        	propsToStore.setProperty(XML_JNDI_NAME_ATTRIBUTE, getJndiName());
        }
        if (getHoldability() != null) {
        	propsToStore.setProperty(XML_HOLDABILITY, Integer.toString(getHoldability()));
        }
        if (getTransactionIsolation() != null) {
        	propsToStore.setProperty(XML_TRANSACTION_ISOLATION, Integer.toString(getTransactionIsolation()));
        }
        propsToStore.setProperty(XML_THREAD_SAFE_CONNECTIONS, Boolean.toString(isThreadSafeConnections()));
        propsToStore.setProperty(XML_IS_PASSWORD_ENCRYPTED, Boolean.toString(isPasswordEncrypted()));

        propsToStore.store(outStream, null);
    }
    

    /* (non-Javadoc)
     * @see org.jetel.graph.GraphElement#toString()
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
    
    /** 
     * Decrypts the password entry in the configuration properties if the
     * isPasswordEncrypted property is set to "y" or "yes". If any error occurs
     * and decryption fails, the original password entry will be used.
     * 
     * @param configProperties configuration properties
     * @throws ComponentNotReadyException 
     */
    private void decryptPassword() throws ComponentNotReadyException {
        if (isPasswordEncrypted()) {
        	if (getGraph() == null)
        		return; 
            Enigma enigma = getGraph().getEnigma();
            if (enigma == null) {
            	logger.error("Can't decrypt password on DBConnection (id=" + this.getId() + "). Please set the password as engine parameter -pass.");
                //throw new ComponentNotReadyException(this, "Can't decrypt password on DBConnection (id=" + this.getId() + "). Please set the password as engine parameter -pass.");
                return;
            }
            String decryptedPassword = null;
            try {
                decryptedPassword = enigma.decrypt(getPassword());
            } catch (JetelException e) {
                logger.error("Can't decrypt password on DBConnection (id=" + this.getId() + "). Incorrect password.");
                //throw new ComponentNotReadyException(this, "Can't decrypt password on DBConnection (id=" + this.getId() + "). Please set the password as engine parameter -pass.", e);
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
	                driverLibraryURLs[i] = FileUtils.getFileURL(getGraph() != null ? getGraph().getRuntimeContext().getContextURL() : null, libraryPaths[i]);
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
			JdbcDriver jdbcDriver = getJdbcDriver();
			if(jdbcDriver != null) {
				JdbcSpecific ret = getJdbcDriver().getJdbcSpecific();
				if (ret != null) {
					return ret;
				}
			}
			return DefaultJdbcSpecific.getInstance();
		}
	}
	
	public TypedProperties getExtraProperties() {
		return jdbcProperties;
	}
	
	/**
	 * This class is used as a key value to the connectionsCache map.
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

	/**
	 * @return the holdability
	 */
	public Integer getHoldability() {
		return holdability;
	}

	/**
	 * @param holdability the holdability to set
	 */
	public void setHoldability(Integer holdability) {
		this.holdability = holdability;
	}

	/**
	 * @return the transactionIsolation
	 */
	public Integer getTransactionIsolation() {
		return transactionIsolation;
	}

	/**
	 * @param transactionIsolation the transactionIsolation to set
	 */
	public void setTransactionIsolation(Integer transactionIsolation) {
		this.transactionIsolation = transactionIsolation;
	}

}

