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
package org.jetel.hadoop.connection;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.database.ConnectionFactory;
import org.jetel.database.IConnection;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.JetelException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.ContextProvider;
import org.jetel.graph.GraphElement;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.plugin.PluginDescriptor;
import org.jetel.util.classloader.GreedyURLClassLoader;
import org.jetel.util.crypto.Enigma;
import org.jetel.util.file.FileUtils;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.property.PropertiesUtils;
import org.jetel.util.property.PropertyRefResolver;
import org.jetel.util.string.StringUtils;
import org.w3c.dom.Element;

/**
 * @author dpavlis (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @author rmirek, tkramolis
 * 
 * @see org.apache.hadoop.fs.FileSystem
 */
public class HadoopConnection extends GraphElement implements IConnection {

	public static final String HADOOP_CONNECTION_PROVIDER_CLASS = "org.jetel.component.hadooploader.HadoopConnectionInstance";
	public static final String HADOOP_MAPREDUCE_PROVIDER_CLASS = "org.jetel.component.hadooploader.HadoopMapReduceJobSender";
	public static final String HADOOP_CONNECTION_PROVIDER_JAR = "./lib/cloveretl.component.hadooploader.jar";

	public static final String LIB_PATH_SEPARATOR = ";";

	// XML key constants
	public static final String HADOOP_CONFIG_KEY = "config";
	public static final String HADOOP_CORE_LIBRARY_KEY = "hadoopJar";
	public static final String HADOOP_CUSTOM_PARAMETERS_KEY = "hadoopParams";
	public static final String HADOOP_FS_HOST_KEY = "host";
	public static final String HADOOP_FS_PORT_KEY = "port";
	public static final String HADOOP_MAPRED_HOST_KEY = "hostMapred";
	public static final String HADOOP_MAPRED_PORT_KEY = "portMapred";
	public static final String HADOOP_USER_NAME_KEY = "username";
	public static final String HADOOP_PASSWORD_KEY = "password";
	public static final String HADOOP_PASSWORD_ENCRYPTED_KEY = "passwordEncrypted";

	public static final String[] HADOOP_USED_PROPERTIES_KEYS = new String[] { HADOOP_FS_HOST_KEY, HADOOP_FS_PORT_KEY,
			HADOOP_MAPRED_HOST_KEY, HADOOP_MAPRED_PORT_KEY, HADOOP_USER_NAME_KEY, HADOOP_PASSWORD_KEY };

	private static Log logger = LogFactory.getLog(HadoopConnection.class);

	// default connection settings constants
	public static final Map<String, Object> PROPERTIES_DEFAULT = Collections
			.unmodifiableMap(new HashMap<String, Object>() {
				{
					put(HADOOP_FS_PORT_KEY, 8020);
					put(HADOOP_MAPRED_PORT_KEY, 8021);
				}
			});
	public static final String CONNECTION_TYPE_ID = "HADOOP";
	public static final String HADOOP_URI_STR_FORMAT = "hdfs://%s:%s/";
	
	// values of user inputs for the connection
	private boolean encryptPassword;
	private String hadoopCoreJar;
	private Properties prop;

	// if the connection is loaded or linked this holds configuration location
	private String configurationLocation;

	// services of Hadoop file system and map/reduce API
	private IHadoopConnection fsConnection;
	private IHadoopMapReduceJobSender mapReduceJobSender;
	
	private boolean fsConnected;

	// relative paths (in XML_HADOOP_CORE_LIBRARY_ATTRIBUTE property) are within this context; used from Designer (validate connection)
	private URL contextURL;
	
	private static Map<Set<URL>, ClassLoader> classLoaderCache = new HashMap<Set<URL>, ClassLoader>();

	/**
	 * Description of the Method
	 * 
	 * @param nodeXML
	 *            Description of the Parameter
	 * @return Description of the Return Value
	 * @throws Exception
	 */
	public static HadoopConnection fromXML(TransformationGraph graph, Element nodeXML) throws XMLConfigurationException {
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(nodeXML, graph);
		try {
			if (xattribs.exists(HADOOP_CONFIG_KEY)) {
				return new HadoopConnection(xattribs.getString(XML_ID_ATTRIBUTE), xattribs.getString(HADOOP_CONFIG_KEY));
			} else {
				HadoopConnection con = new HadoopConnection(xattribs.getString(XML_ID_ATTRIBUTE), null);
				try {
					con.setConnectionParameters(xattribs.attributes2Properties(new String[0]));
				} catch (ComponentNotReadyException ex) {
					throw new XMLConfigurationException("Hadoop connection " + con.getId() + " could not be loaded "
							+ "from XML. Required attribute probably missing.", ex);
				}
				return con;
			}
		} catch (AttributeNotFoundException ex) {
			throw new XMLConfigurationException("HadoopConnection: "
					+ xattribs.getString(XML_ID_ATTRIBUTE, "unknown ID") + ":" + ex.getMessage(), ex);
		}
	}

	public HadoopConnection(String id) {
		this(id, null);
	}

	protected HadoopConnection(String id, String configurationFileLocation) {
		super(id);
		this.configurationLocation = configurationFileLocation;
	}

	// Must fail atomically!
	private void setConnectionParameters(Properties propertiesToSet) throws ComponentNotReadyException {
		if (!propertiesToSet.containsKey(HADOOP_FS_HOST_KEY)) {
			throw new ComponentNotReadyException(
					"Cannot initialize Hadoop connection, Hadoop file system host is missing.");
		}
		// store in local variable first to ensure atomic fail
		Properties localCopy = new Properties();
		for (String key : HADOOP_USED_PROPERTIES_KEYS) {
			String value = (String) propertiesToSet.get(key);
			if (value != null) {
				value = value.trim();
			}
			if (!StringUtils.isEmpty(value)) {
				localCopy.setProperty(key, value);
			} else if (PROPERTIES_DEFAULT.containsKey(key)) {
				localCopy.setProperty(key, PROPERTIES_DEFAULT.get(key).toString());
			}
		}
		// TODO this way, our GUI properties get into hadoop Configuration, which is useless

		// parse addition properties
		String additionalParams = propertiesToSet.getProperty(HADOOP_CUSTOM_PARAMETERS_KEY, null);
		if (additionalParams != null && !additionalParams.isEmpty()) {
			Properties additionalProp = PropertiesUtils.parseProperties(additionalParams);
			if (additionalProp != null) {
				localCopy.putAll(additionalProp);
			}
		}
		// set up this instance
		encryptPassword = Boolean.parseBoolean(propertiesToSet.getProperty(HADOOP_PASSWORD_ENCRYPTED_KEY,
				String.valueOf(false)));
		hadoopCoreJar = propertiesToSet.getProperty(HADOOP_CORE_LIBRARY_KEY);
		setName(propertiesToSet.getProperty(XML_NAME_ATTRIBUTE, getId()));
		this.prop = localCopy;
	}

	@Override
	public synchronized void init() throws ComponentNotReadyException {
		if (isInitialized()) {
			logger.trace("HadoopConnection has been initialized twice or more times. Not neccessary.");
			return;
		}
		super.init();

		if (configurationLocation != null) {
			initConnFromProperties(readFileToProperties(configurationLocation), getGraph().getGraphProperties());
		}
		if (encryptPassword && getPassword() != null) {
			try {
				setPassword(decryptPassword(getPassword()));
			} catch (JetelException ex) {
				throw new ComponentNotReadyException(this, "Cannot decrypt encrypted user password of "
						+ "Hadoop connection (id=" + getId() + ").", ex);
			}
		}

		fsConnection = initLoadLibrariesAndCreateFsProvider();
	}

	public void initConnFromProperties(Properties initProperties, Properties graphProperties)
			throws ComponentNotReadyException {
		if (initProperties == null) {
			throw new NullPointerException("initProperties");
		}
		Properties localPropCopy = new Properties();
		localPropCopy.putAll(initProperties);
		new PropertyRefResolver(graphProperties).resolveAll(localPropCopy);
		setConnectionParameters(localPropCopy);
	}

	private Properties readFileToProperties(String configFileLocation) {
		Properties config = new Properties();
		URL contextURL = getGraph().getRuntimeContext().getContextURL();
		InputStream stream = null;
		try {
			stream = Channels.newInputStream(FileUtils.getReadableChannel(contextURL, configFileLocation));
			config.load(stream);
		} catch (IOException ex) {
			throw new RuntimeException("Configuration file for Hadoop connection not found (" + configFileLocation
					+ ") or could not be read.", ex);
		} finally {
			if (stream != null) {
				try {
					stream.close();
				} catch (IOException ex) {
					logger.warn("Could not close configuration file for Hadoop connection (" + configFileLocation
							+ ").", ex);
				}
			}
		}
		return config;
	}

	protected String decryptPassword(String encryptedPasword) throws JetelException {
		if (encryptedPasword == null) {
			throw new NullPointerException("encryptedPasword");
		}
		Enigma enigma = getGraph().getEnigma();
		if (enigma == null) {
			throw new JetelException("Can't decrypt password of HadoopConnection (id=" + getId()
					+ "). Please set the decryption password as engine parameter -pass.");
		}

		String decryptedPassword;
		try {
			decryptedPassword = enigma.decrypt(encryptedPasword);
		} catch (JetelException ex) {
			throw new JetelException("Can't decrypt password of HadoopConnection (id=" + getId()
					+ "). Probably incorrect decryption password (engine parameter -pass) .", ex);
		}
		if (decryptedPassword == null || decryptedPassword.isEmpty()) {
			throw new JetelException("Can't decrypt password of HadoopConnection (id=" + getId() + ").");
		}
		return decryptedPassword;
	}

	private IHadoopConnection initLoadLibrariesAndCreateFsProvider() throws ComponentNotReadyException {
		List<URL> providerClassPath = new ArrayList<URL>();

		if (contextURL == null) {
			TransformationGraph graph = ContextProvider.getGraph();
			if (graph != null) {
				contextURL = graph.getRuntimeContext().getContextURL();
			}
		}
		
		if (hadoopCoreJar != null && !hadoopCoreJar.isEmpty()) {
			String urls[] = parseHadoopJarsList(hadoopCoreJar);
			for (String url : urls) {
				try {
					URL hadoopJar = FileUtils.getFileURL(contextURL, url);
					providerClassPath.add(hadoopJar);
				} catch (MalformedURLException ex) {
					throw new ComponentNotReadyException("Cannot load library from '" + url + "'", ex);
				}
			}
		}

		try {
			PluginDescriptor hadoopPluginDescriptor = ConnectionFactory.getConnectionDescription(CONNECTION_TYPE_ID).getPluginDescriptor();
			providerClassPath.add(hadoopPluginDescriptor.getURL(HADOOP_CONNECTION_PROVIDER_JAR));
		} catch (MalformedURLException e) {
			throw new ComponentNotReadyException("Invalid URL of Hadoop connection implementation module", e);
		}
		
		// Reuse classloader for equal classpaths; solves PermGen space OutOfMemoryError
		HashSet<URL> classPathSet = new HashSet<URL>(providerClassPath);
		ClassLoader classLoader = classLoaderCache.get(classPathSet);
		
		if (classLoader == null) {
			classLoader = new GreedyURLClassLoader(providerClassPath.toArray(new URL[0]), getClass().getClassLoader());
			classLoaderCache.put(classPathSet, classLoader);
			
			logger.debug("Hadoop connection " + getId() + " uses new classloader with classpath: " + providerClassPath);
		} else {
			logger.debug("Hadoop connection " + getId() + " uses cached classloader with classpath: " + providerClassPath);
		}
		

		try {
			Class<?> hadoopImplementationClass = classLoader.loadClass(HADOOP_CONNECTION_PROVIDER_CLASS);
			return (IHadoopConnection) hadoopImplementationClass.newInstance();
		} catch (RuntimeException ex) {
			throw ex; // runtime exceptions are not to be changed to ComponentNotReadyException
		} catch (Exception ex) {
			throw new ComponentNotReadyException(
					"Internal Error. (Could not find CloverETL Hadoop Implementation module.)", ex);
		}
	}

	public static String[] parseHadoopJarsList(String jarsList) {
		if (jarsList == null) {
			return null;
		}
		return jarsList.split(LIB_PATH_SEPARATOR);
	}

	// TODO rename to getFsProvider
	public IHadoopConnection getConnection() throws IOException {
		if (fsConnection == null) {
			throw new IllegalStateException("File system provider is not ready. Method init() must be called "
					+ "on this instance first. Instance: " + this);
		}
		
		if (fsConnected) { // TODO this flag is suspicious; someone can close the fsConnection itself
			return fsConnection;
		}
		
		URI hURI;
		try {
			hURI = new URI(String.format(HADOOP_URI_STR_FORMAT, getFsHost(), getFsPort()));
		} catch (URISyntaxException ex) {
			throw new RuntimeException("Invalid Hadoop file system host/port definition.", ex);
		}

		// TODO translate properties keys using mapping from properties bundle
		if (!fsConnection.connect(hURI, this.prop, getUser())) {
			throw new IOException("Could not connect to hadoop file system at " + hURI);
		}
		fsConnected = true;
		return fsConnection;
	}

	@Override
	public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		return super.checkConfig(status);
	}

	@Override
	public synchronized void postExecute() throws ComponentNotReadyException {
		try {
			close();
		} catch (IOException e) {
			throw new ComponentNotReadyException("Failed to close connection", e);
		}
	}

	@Override
	public synchronized void free() {
		if (isInitialized()) {
			super.free();
			try {
				close();
			} catch (IOException ex) {
				logger.warn("There was a problem closing HDFS connection. The cluster might be disconected "
						+ "or it could be already closed.", ex);
			}
			fsConnection = null;
		}
	}

	protected void close() throws IOException {
		fsConnected = false;
		if (fsConnection != null) {
			fsConnection.close();
		}
	}

	@Override
	public DataRecordMetadata createMetadata(Properties parameters) {
		throw new UnsupportedOperationException("Hadoop connection doesn't support operation 'createMetadata()'");
	}

	public String getUser() {
		return prop.getProperty(HADOOP_USER_NAME_KEY, null);
	}

	public String getPassword() {
		return getPassword(prop);
	}

	protected static String getPassword(Properties prop) {
		return prop.getProperty(HADOOP_PASSWORD_KEY, null);
	}

	private void setPassword(String newPassword) {
		prop.setProperty(HADOOP_PASSWORD_KEY, newPassword);
	}

	public String getFsHost() {
		return prop.getProperty(HADOOP_FS_HOST_KEY);
	}

	public int getFsPort() {
		return Integer.parseInt(prop.getProperty(HADOOP_FS_PORT_KEY));
	}

	public String getMapredHost() {
		return prop.getProperty(HADOOP_MAPRED_HOST_KEY, null);
	}

	public int getMapredPort() {
		return Integer.parseInt(prop.getProperty(HADOOP_MAPRED_PORT_KEY));
	}

	public boolean isEncryptPassword() {
		return encryptPassword;
	}

	public URL getContextURL() {
		return contextURL;
	}

	public void setContextURL(URL contextURL) {
		this.contextURL = contextURL;
	}
}
