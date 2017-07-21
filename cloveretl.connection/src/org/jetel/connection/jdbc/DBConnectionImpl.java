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
import java.io.InputStream;
import java.net.URL;
import java.sql.Connection;
import java.sql.Driver;
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
import org.jetel.connection.jdbc.driver.JdbcDriverDescription;
import org.jetel.connection.jdbc.driver.JdbcDriverFactory;
import org.jetel.connection.jdbc.driver.JdbcDriverImpl;
import org.jetel.connection.jdbc.specific.JdbcSpecificDescription;
import org.jetel.connection.jdbc.specific.JdbcSpecificFactory;
import org.jetel.connection.jdbc.specific.impl.DefaultJdbcSpecific;
import org.jetel.data.Defaults;
import org.jetel.database.sql.DBConnection;
import org.jetel.database.sql.JdbcDriver;
import org.jetel.database.sql.JdbcSpecific;
import org.jetel.database.sql.JdbcSpecific.OperationType;
import org.jetel.database.sql.SqlConnection;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.JetelException;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.compile.ClassLoaderUtils;
import org.jetel.util.crypto.Enigma;
import org.jetel.util.file.FileUtils;
import org.jetel.util.primitive.TypedProperties;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.property.RefResFlag;
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
 * @created     January 15, 2003
 */
public class DBConnectionImpl extends AbstractDBConnection {

    private static final Log logger = LogFactory.getLog(DBConnection.class);
    
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
    
    private Map<CacheKey, SqlConnection> connectionsCache = new HashMap<CacheKey, SqlConnection>();
    private SqlConnection sharedConnection; //this variable is used in case threadSafe = false

    private JdbcDriver jdbcDriver;
    private JdbcSpecific jdbcSpecific;
    private URL[] driverLibraryURLs;
    
    /**
     *  Constructor for the DBConnection object.
     *
     * @param  configFilename  properties filename containing definition of driver, dbURL, username, password
     */
    public DBConnectionImpl(String id, String configFilename) {
        super(id);
        this.configFileName = configFilename;
    }

    /**
     * Constructor.
     * @param id
     * @param properties
     */
    public DBConnectionImpl(String id, Properties properties) {
        super(id);
        
        fromProperties(properties);
    }

    /**
     * Iterates over the given properties and finds which are the standard DBConnection properties
     * and which are the JDBC connection properties.
     * 
     * @param configProperties
     */
	protected void fromProperties(Properties properties) {
		TypedProperties typedProperties = new TypedProperties(properties, getGraph());

		setUser(typedProperties.getStringProperty(XML_USER_ATTRIBUTE, null));
		setPassword(typedProperties.getStringProperty(XML_PASSWORD_ATTRIBUTE, null));
		setDbUrl(typedProperties.getStringProperty(XML_DBURL_ATTRIBUTE, null, RefResFlag.SPEC_CHARACTERS_OFF));
		setDbDriver(typedProperties.getStringProperty(XML_DBDRIVER_ATTRIBUTE, null));
		setDatabase(typedProperties.getStringProperty(XML_DATABASE_ATTRIBUTE, null));
		setDriverLibrary(typedProperties.getStringProperty(XML_DRIVER_LIBRARY_ATTRIBUTE, null, RefResFlag.SPEC_CHARACTERS_OFF));
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
	@Override
	public Properties createConnectionProperties() {
		Properties ret = new Properties();
		
		ret.putAll(getExtraProperties());
		
		if (getUser() != null) {
			ret.setProperty(XML_USER_ATTRIBUTE, getUser());
		}
		if (getPassword() != null) {
			ret.setProperty(XML_PASSWORD_ATTRIBUTE, getPassword());
		}
		
		return ret;
	}
	
    /* (non-Javadoc)
     * @see org.jetel.graph.GraphElement#init()
     */
    @Override
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
            jdbcDriver = new JdbcDriverImpl(null, getDbDriver(), getDbDriver(), getDriverLibraryURLs(), getJdbcSpecific(), null);
        }
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
    @Override
	public synchronized SqlConnection getConnection(String elementId, OperationType operationType) throws JetelException {
    	SqlConnection connection = null;
        
        if (isThreadSafeConnections()) {
        	CacheKey key = new CacheKey(elementId, operationType);
            connection = connectionsCache.get(key);
            if (connection != null) {
            	if (!isValid(connection)) {
            		closeConnection(connection);
            		connection = null;
            	}
            }
            if (connection == null) {
                connection = connect(operationType);
                connectionsCache.put(key, connection);
            }
        } else {
        	if (sharedConnection != null) {
        		if (!isValid(sharedConnection)) {
        			closeConnection(sharedConnection);
        			sharedConnection = null;
        		}
        	}
            if (sharedConnection == null) {
                sharedConnection = connect(operationType);
            }
            connection = sharedConnection;
        }
        
        return connection;
    }
    
    protected boolean isValid(SqlConnection connection) {
    	
    	try {
    		try {
    			return connection.isValid(Defaults.DBConnection.VALIDATION_TIMEOUT);
    		} catch (Throwable t) {
    			if (t instanceof ThreadDeath) {
    				throw (ThreadDeath)t;
    			}
    			logger.info("Connection does not support validation, checking whether closed.");
    		}
    		return !connection.isClosed();
    	} catch (Exception e) {
    		logger.warn("Error while validating DB connection.", e);
    	}
    	return false;
    }
    
    /**
     * Guess jdbc specific for this {@link DBConnection} based on given {@link Connection}.
     * This is used for {@link Connection} given from JNDI interface to guess proper {@link JdbcSpecific}.
     */
    private void updateJdbcSpecific(Connection connection) {
       	if (jdbcSpecific == null) {
       		JdbcSpecificDescription jdbcSpecificDescription = JdbcSpecificFactory.getJdbcSpecificDescription(connection);
       		if (jdbcSpecificDescription != null) {
       			jdbcSpecific = jdbcSpecificDescription.getJdbcSpecific();
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
            return;
        }

		super.free();
		closeConnections();

		if (jdbcDriver != null) {
			jdbcDriver.free();
		}
	}

    private void closeConnections() {
        if (threadSafeConnections) {
            for (SqlConnection connection: connectionsCache.values()) {
            	closeConnection(connection);
            }
            connectionsCache.clear();
        }

        if (sharedConnection != null) {
			closeConnection(sharedConnection);
			sharedConnection = null;
		}
    }

    private void closeConnection(SqlConnection connection) {
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
     * Closes connection stored in cache under key specified by elementId and operationType.
     * Connection is closed only if DBConnection is thread-safe.
     * Closed connection is also removed from cache.
     */
    @Override
	public synchronized void closeConnection(String elementId, OperationType operationType) {
    	boolean batchMode = getGraph() == null ? true : getGraph().getRuntimeContext().isBatchMode();
    	if (isThreadSafeConnections() && batchMode) {
        	CacheKey key = new CacheKey(elementId, operationType);
        	SqlConnection connection = connectionsCache.remove(key);
        	if (connection != null)
        		closeConnection(connection);
        }
    }
    
    /**
     * Creates DBConnection based on xml node.
     * 
     * @param graph
     * @param nodeXML
     * @return
     * @throws XMLConfigurationException
     * @throws AttributeNotFoundException 
     */
    public static DBConnection fromXML(TransformationGraph graph, Element nodeXML) throws XMLConfigurationException, AttributeNotFoundException {
        ComponentXMLAttributes xattribs = new ComponentXMLAttributes(nodeXML, graph);

        try {
            String id = xattribs.getString(XML_ID_ATTRIBUTE);
            // do we have dbConfig parameter specified ??
            if (xattribs.exists(XML_DBCONFIG_ATTRIBUTE)) {
                return new DBConnectionImpl(id, xattribs.getString(XML_DBCONFIG_ATTRIBUTE));
            } else {
                Properties connectionProps  = xattribs.attributes2Properties(new String[] {XML_ID_ATTRIBUTE});
                
                return new DBConnectionImpl(id, connectionProps);
            }
		} catch (Exception e) {
            throw new XMLConfigurationException("DBConnection: " 
            		+ xattribs.getString(XML_ID_ATTRIBUTE, "unknown ID") + ":" + e.getMessage(), e);
		}
    }

    
    

    /* (non-Javadoc)
     * @see org.jetel.graph.GraphElement#toString()
     */
    @Override
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

	@Override
	public DataRecordMetadata createMetadata(Properties parameters) throws SQLException {
    	if (!isInitialized()) {
    		throw new IllegalStateException("DBConnection has to be initialized to be able to create metadata.");
    	}
    	
        Statement statement = null;
        ResultSet resultSet = null;

        String sqlQuery = parameters.getProperty(SQL_QUERY_PROPERTY);
        if(StringUtils.isEmpty(sqlQuery)) {
            throw new IllegalArgumentException("JDBC stub for clover metadata can't find sqlQuery parameter.");
        }

        int index = sqlQuery.toUpperCase().indexOf("WHERE");

		if (index >= 0) {
			sqlQuery = sqlQuery.substring(0, index).concat("WHERE 0=1");
		} else {
			sqlQuery = sqlQuery.concat(" WHERE 0=1");
		}

        Connection connection;
		try {
			connection = connect(OperationType.UNKNOWN);
		} catch (JetelException e) {
			throw new SQLException(e);
		} catch (JetelRuntimeException e) {
			throw new SQLException(e);
		}
        
        try {
            statement = connection.createStatement();
            resultSet = statement.executeQuery(sqlQuery);
            DataRecordMetadata drMetaData = SQLUtil.dbMetadata2jetel(resultSet.getMetaData(), getJdbcSpecific());
            return drMetaData;
        } finally {
            // make sure we close all connection resources
        	SQLUtil.closeConnection(resultSet, statement, connection);
        }
    }

    @Override
	public boolean isThreadSafeConnections() {
        return threadSafeConnections;
    }
    
    protected void setThreadSafeConnections(boolean threadSafeConnections) {
        this.threadSafeConnections = threadSafeConnections;
    }
 
    @Override
	public boolean isPasswordEncrypted() {
        return isPasswordEncrypted;
    }
    
    protected void setPasswordEncrypted(boolean isPasswordEncrypted) {
        this.isPasswordEncrypted = isPasswordEncrypted;
    }

	@Override
	public String getJndiName() {
		return jndiName;
	}

	protected void setJndiName(String jndiName) {
		this.jndiName = jndiName;
	}

	@Override
	public JdbcDriver getJdbcDriver() {
		return jdbcDriver;
	}

	protected void setJdbcDriver(JdbcDriver jdbcDriver) {
		this.jdbcDriver = jdbcDriver;
	}

	@Override
	public String getDbUrl() {
		return dbUrl;
	}
	
	protected void setDbUrl(String dbUrl) {
		this.dbUrl = dbUrl;
	}

	@Override
	public String getUser() {
		return user;
	}
	
	protected void setUser(String user) {
		this.user = user;
	}

	@Override
	public String getPassword() {
		return password;
	}

	protected void setPassword(String password) {
		this.password = password;
	}

	@Override
	public String getDatabase() {
		return database;
	}

	protected void setDatabase(String database) {
		this.database = database;
	}

	@Override
	public String getDbDriver() {
		return dbDriver;
	}

	protected void setDbDriver(String dbDriver) {
		this.dbDriver = dbDriver;
	}

	@Override
	public String getDriverLibrary() {
		return driverLibrary;
	}

	private void prepareDriverLibraryURLs() throws ComponentNotReadyException {
		if(!StringUtils.isEmpty(driverLibrary)) {
			URL contextURL = getGraph() != null ? getGraph().getRuntimeContext().getContextURL() : null;
			try {
				driverLibraryURLs = ClassLoaderUtils.getClassloaderUrls(contextURL, driverLibrary);
			} catch (Exception e) {
				throw new ComponentNotReadyException("Can not create JDBC connection '" + getId() + "'.", e);
	        }
		}
	}
	
	private URL[] getDriverLibraryURLs() {
		return driverLibraryURLs;
	}
	
	protected void setDriverLibrary(String driverLibrary) {
		this.driverLibrary = driverLibrary;
	}

	@Override
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
	
	@Override
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
	
    /**
     * @return type of associated result set
     * @throws ComponentNotReadyException
     */
    @Override
	public int getResultSetType() throws ComponentNotReadyException {
		Class<?> typesClass;
		ClassLoader classLoader;

		JdbcDriver driver = getJdbcDriver();
		if (driver != null) {
			classLoader = driver.getClassLoader();
		} else {
			classLoader = DBConnection.class.getClassLoader();
		}
		
		try {
			typesClass = classLoader.loadClass(getJdbcSpecific().getTypesClassName());
		} catch (ClassNotFoundException e) {
			throw new ComponentNotReadyException("Invalid Types class name in jdbc specific: " + getJdbcSpecific().getTypesClassName(), e);
		}
		try {
			return typesClass.getField(getJdbcSpecific().getResultSetParameterTypeField()).getInt(null);
		} catch (Exception e) {
			throw new ComponentNotReadyException("Invalid ResultSet type field name in jdbc specific: " + getJdbcSpecific().getResultSetParameterTypeField(), e);
		}
    }
    
	
	@Override
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
	@Override
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
	@Override
	public Integer getTransactionIsolation() {
		return transactionIsolation;
	}

	/**
	 * @param transactionIsolation the transactionIsolation to set
	 */
	@Override
	public void setTransactionIsolation(Integer transactionIsolation) {
		this.transactionIsolation = transactionIsolation;
	}

	/**
	 * Sets up a java.sql.Connection according given DBConnection object.
	 * @param dbConnection
	 * @return
	 * @throws JetelException
	 */
	protected Connection createConnection() {
		JdbcDriver jdbcDriver = getJdbcDriver();
		// -pnajvar
		// this is a bad hack, workaround for issue 2668
		if (jdbcDriver == null) { 
			throw new JetelRuntimeException("JDBC driver couldn't be obtained");
		}
		Driver driver = jdbcDriver.getDriver();
		Connection connection;
		Properties connectionProperties = new Properties(jdbcDriver.getProperties());
		connectionProperties.putAll(createConnectionProperties());
		
        try {
            connection = driver.connect(getDbUrl(), connectionProperties);
        } catch (SQLException ex) {
            throw new JetelRuntimeException("Can't connect to DB: " + ex.getMessage(), ex);
        }
        if (connection == null) {
            throw new JetelRuntimeException("Not suitable driver for specified DB URL (" + driver + " / " + getDbUrl());
        }
        
        return connection;
	}

	@Override
	protected SqlConnection connect(OperationType operationType) throws JetelException {
    	if (!StringUtils.isEmpty(getJndiName())) {
        	try {
            	Context initContext = new InitialContext();
           		DataSource ds = (DataSource)initContext.lookup(getJndiName());
               	Connection jndiConnection = ds.getConnection();
               	//update jdbc specific of this DBConnection according given JNDI connection
               	updateJdbcSpecific(jndiConnection);
               	//wrap the given JNDI connection to a DefaultConnection instance 
               	return getJdbcSpecific().createSQLConnection(this, jndiConnection, operationType);
        	} catch (Exception e) {
        		throw new JetelException("Cannot establish DB connection to JNDI:" + getJndiName() + " " + e.getMessage(), e);
        	}
    	} else {
        	try {
				return getJdbcSpecific().createSQLConnection(this, createConnection(), operationType);
			} catch (JetelException e) {
				throw new JetelException("Cannot establish DB connection (" + getId() + ").", e);
			}
    	}
    }
}

