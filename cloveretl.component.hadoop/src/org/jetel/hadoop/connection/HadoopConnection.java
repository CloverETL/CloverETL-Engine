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
import java.net.URL;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.Defaults;
import org.jetel.database.ConnectionFactory;
import org.jetel.database.IConnection;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.ConfigurationStatus.Priority;
import org.jetel.exception.ConfigurationStatus.Severity;
import org.jetel.exception.JetelException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.ContextProvider;
import org.jetel.graph.GraphElement;
import org.jetel.graph.Node;
import org.jetel.graph.TransformationGraph;
import org.jetel.hadoop.component.HadoopReader;
import org.jetel.hadoop.component.HadoopWriter;
import org.jetel.hadoop.service.filesystem.HadoopConnectingFileSystemService;
import org.jetel.hadoop.service.filesystem.HadoopFileSystemConnectionData;
import org.jetel.hadoop.service.filesystem.HadoopFileSystemService;
import org.jetel.hadoop.service.mapreduce.HadoopConnectingMapReduceService;
import org.jetel.hadoop.service.mapreduce.HadoopMapReduceConnectionData;
import org.jetel.hadoop.service.mapreduce.HadoopMapReduceInfoService;
import org.jetel.hadoop.service.mapreduce.HadoopMapReduceService;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.plugin.PluginDescriptor;
import org.jetel.util.compile.ClassLoaderUtils;
import org.jetel.util.crypto.Enigma;
import org.jetel.util.file.FileUtils;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.property.PropertiesUtils;
import org.jetel.util.property.PropertyRefResolver;
import org.jetel.util.string.StringUtils;
import org.w3c.dom.Element;

/**
 * <p> Represents configurable connection to Hadoop cluster. Allows for both file system and map/reduce operations on
 * the cluster. However, a use case is supported when instance of this class is used only to connect to Hadoop file
 * system. </p>
 * 
 * <p> This class initializes and connects services that perform file system and map/reduce operations. They are
 * connected according to user specified connection configuration. The services provided do not offer connection related
 * operations as this class alone should take care of connection management.</p>
 * 
 * TODO to avoid code duplication and unnecessary methods (like {@link #createMetadata(Properties)}) perhaps common
 * superclass for Connections should be introduced and this class should be made its descendant. For example such class
 * can take care of exporting and externalization of the connections.
 * 
 * @author David Pavlis &lt;<a href="mailto:info@cloveretl.com">info@cloveretl.com</a>&gt;
 * @author Rastislav Mirek &lt;<a href="mailto:rmirek@mail.muni.cz">rmirek@mail.muni.cz</a>&gt</br> &#169; Javlin, a.s
 *         (<a href="http://www.javlin.eu">www.javlin.eu</a>) &lt;<a
 *         href="mailto:info@cloveretl.com">info@cloveretl.com</a>&gt
 * @created 15.10.2012
 * @see HadoopProvidersFactory
 * @see HadoopJobSender
 * @see HadoopWriter
 * @see HadoopReader
 * @see HadoopConnectingFileSystemService
 * @see HadoopConnectingMapReduceService
 */
public class HadoopConnection extends GraphElement implements IConnection {

	public static final String CONNECTION_TYPE_ID = "HADOOP";
	public static final String HADOOP_PROVIDER_JAR = "./lib/cloveretl.hadoop.provider.jar";
	public static final String HADOOP_VERSION = "hadoop-0.20.2"; // temporary constant
	/**
	 * Used as separator character for URLs to Hadoop API libraries. TODO: this should be removed and
	 * Defaults.DEFAULT_PATH_SEPARATOR_REGEX should be somehow used instead.
	 */
	public static final String LIB_PATH_SEPARATOR = ";";

	// XML key constants
	// TODO change these constants together with their counterparts in designer to allow better user understanding of
	// their meaning when user is directly editing configuration XML. See comments for recommended values of constants
	public static final String XML_CONFIG_KEY = "config"; // configurationLocation
	public static final String XML_CORE_LIBRARY_KEY = "hadoopJar"; // hadoopApiJars
	public static final String XML_CUSTOM_PARAMETERS_KEY = "hadoopParams"; // additionalConnectionParams
	public static final String XML_FS_HOST_KEY = "host"; // fileSystemHost
	public static final String XML_FS_PORT_KEY = "port"; // fileSystemPort
	public static final String XML_MAPRED_HOST_KEY = "hostMapred"; // mapReduceHost
	public static final String XML_MAPRED_PORT_KEY = "portMapred"; // mapReducePort
	public static final String XML_USER_NAME_KEY = "username";
	public static final String XML_PASSWORD_KEY = "password";
	public static final String XML_PASSWORD_ENCRYPTED_KEY = "passwordEncrypted";

	public static final String INVALID_URL_MESSAGE_WITH_ID = "Failed to create Hadoop connection with ID '%s': couldn't parse Hadoop libraries into URLs.";
	public static final String INVALID_URL_MESSAGE = "Cannot parse Hadoop libraries into URLs.";
	public static final String CANNOT_DECRYPT_PASSWORD_MESSAGE_FORMAT = "Can't decrypt password of HadoopConnection (id=%s).";

	// default connection settings constants
	public static final int DEFAULT_FS_PORT = 8020;
	public static final int DEFAULT_JOBTRACKER_PORT = 8021;
	private static final Log LOG = LogFactory.getLog(HadoopConnection.class);
	
	// attributes of this properties for XML keys from user input
	private Properties prop;

	// services of Hadoop file system and map/reduce API
	private HadoopConnectingMapReduceService mapReduceService;
	private HadoopConnectingFileSystemService fileSystemService;
	private HadoopMapReduceInfoService mapReduceInfoService;
	
	// connection info needed by the Hadoop services
	private HadoopFileSystemConnectionData fsConnectionData;
	
	// relative paths (in XML_HADOOP_CORE_LIBRARY_ATTRIBUTE property) are within this context; used from Designer (validate connection)
	// TODO this attribute is a hack that can potentially cause trouble and should be removed. Relative paths to
	// libraries should either be allowed to be relative only to graph folder or relative paths should be resolved in
	// designer and only absolute path should be provided to HadoopConnection. The last option is to add method for
	// getting this context to TransformationGraph.
	private URL contextURL;
	
	/**
	 * Creates new instance of {@code HadoopConnection}. Initialization is delayed to init method. This just transforms
	 * graph to properties that are stored in attribute.
	 * @param graph Graph this new connection is part of.
	 * @param nodeXML XML node corresponding to new connection.
	 * @return Not initialized instance of {@code HadoopConnection}.
	 * @throws XMLConfigurationException If ID attribute of the connection is not found.
	 * @throws AttributeNotFoundException 
	 */
	public static HadoopConnection fromXML(TransformationGraph graph, Element nodeXML) throws XMLConfigurationException, AttributeNotFoundException {
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(nodeXML, graph);
		return new HadoopConnection(xattribs.getString(XML_ID_ATTRIBUTE), xattribs.attributes2Properties(new String[] { XML_ID_ATTRIBUTE }), graph);
	}

	/**
	 * Creates instance of {@code HadoopMapReduceInfoService} for given version of Hadoop API, URLs to required Hadoop
	 * libraries and context URL for resolving relative paths to those libraries. This methos is intended to be used in
	 * situations there no instance of {@code HadoopConnection} is available for example in Designer.
	 * 
	 * @param hadoopVersion Non-null nor empty string specifying version of Hadoop API for which corresponding service
	 *        provider is returned. If not supported API version is specified <code>null</code> is returned.
	 * @param contextURL Context URL for resolving relative paths to Hadoop API libraries. May not be <code>null</code>.
	 * @param hadoopCoreJars URLs separated by {@code HadoopConnection#LIB_PATH_SEPARATOR} to required Hadoop libraries
	 *        and context URL for resolving relative paths to those libraries. If <code>null</code> or empty no Hadoop
	 *        API libraries are loaded.
	 * @return Service providing operations that might be vary with different Hadoop API version but do not need
	 *         connection to Hadoop cluster to be performed.
	 * @see HadoopConnection#getMapReduceInfoService()
	 */
	public static HadoopMapReduceInfoService getMapReduceInfoService(String hadoopVersion, URL contextURL, String hadoopCoreJars) {
		if (hadoopVersion == null) {
			throw new NullPointerException("hadoopVersion");
		}
		if (hadoopVersion.isEmpty()) {
			throw new IllegalArgumentException("hadoopoVersion is empty");
		}
		if (contextURL == null) {
			throw new NullPointerException("contextURL");
		}
		if (!HadoopProvidersFactory.isVersionSupported(hadoopVersion)) {
			return null;
		}
		try {
			List<URL> libraries = getProviderClassPathURLList(contextURL, hadoopCoreJars);
			return HadoopProvidersFactory.createMapReduceInfoService(hadoopVersion, libraries);
		} catch (MalformedURLException ex) {
			LOG.error(INVALID_URL_MESSAGE, ex);
			return null;
		} catch (HadoopException ex) {
			LOG.error("Cannot instantiate provider for map/reduce information service for Hadoop version " + hadoopVersion, ex);
			return null;
		}
	}

	/**
	 * Internal constructor used in {@link #fromXML(TransformationGraph, Element)}.
	 * @param id Id of the connection.
	 * @param prop Connection configuration as given in {@link #fromXML(TransformationGraph, Element)}.
	 * @param Graph that this {@code HadoopConnection} is part of. This may be <code>null</code> but it is desirable to
	 *        set this to the right value whenever possible at all. The value is passed to supper constructor.
	 */
	private HadoopConnection(String id, Properties prop, TransformationGraph graph) {
		super(id, graph, StringUtils.isEmpty(prop.getProperty(XML_NAME_ATTRIBUTE)) ? id : prop.getProperty(XML_NAME_ATTRIBUTE));
		this.prop = prop;
	}

	/**
	 * Constructs not initialized Hadoop connection.
	 * @param id Id of the connection.
	 * @param initProperties Configuration of the connection as specified by user. The property keys are XML constants
	 *        of this class.
	 * @param graphProperties Properties used to resolve CTL expressions in connection configuration.
	 * @see #init()
	 */
	public HadoopConnection(String id, Properties initProperties, Properties graphProperties) {
		this(id, resolveInitProperties(initProperties, graphProperties), (TransformationGraph) null);
	}

	/**
	 * Resolves CTL expressions in given connection configuration based on specified graph properties and returns
	 * resolved configuration.
	 * 
	 * @param initProperties Configuration of Hadoop connection as specified by user. Keys are XML constants of this
	 *        class.
	 * @param graphProperties Graph properties used to resolve CTL expressions in connection configuration.
	 * @return A copy of connection properties that is resolved based on graph properties.
	 */
	private static Properties resolveInitProperties(Properties initProperties, Properties graphProperties) {
		if (initProperties == null) {
			throw new NullPointerException("initProperties");
		}
		Properties localPropCopy = new Properties();
		localPropCopy.putAll(initProperties);
		new PropertyRefResolver(graphProperties).resolveAll(localPropCopy);
		return localPropCopy;
	}

	/**
	 * Initializes the connection. Calling this method is prerequisite to some other operations. If
	 * {@link #XML_CONFIG_KEY} value was specified this method reads connection configuration from specified file
	 * first (more precisely, loaded connection properties are added to current properties, possibly overwriting
	 * already existing ones). Then services needed for communication with Hadoop cluster are instantiated 
	 * based on configuration of this connection, but they are not yet connected here. Connection is
	 * established lazily by calling {@link #getFileSystemService()} or {@link #getMapReduceService()} method. 
	 * 
	 * @see Node#isInitialized()
	 * @see #getFileSystemService()
	 * @see #getMapReduceService()
	 * @see #getMapReduceInfoService()
	 */
	@Override
	public synchronized void init() throws ComponentNotReadyException {
		if (isInitialized()) {
			LOG.trace("HadoopConnection has been initialized twice or more times. Not neccessary.");
			return;
		}
		
		LOG.debug("Initializing Hadoop connection '" + getName() + "' (ID: " + getId() + ")");

		loadConfigFileIfNeeded(prop);

		List<URL> libraries;
		try {
			libraries = getProviderClassPathURLList(contextURL, prop.getProperty(XML_CORE_LIBRARY_KEY));
		} catch (MalformedURLException ex) {
			throw new ComponentNotReadyException(String.format(INVALID_URL_MESSAGE_WITH_ID, getId()), ex);
		}
		try {
			fileSystemService = HadoopProvidersFactory.createFileSystemService(HADOOP_VERSION, libraries);
			fsConnectionData = new HadoopFileSystemConnectionData(prop.getProperty(XML_FS_HOST_KEY),
					getPropInt(XML_FS_PORT_KEY, DEFAULT_FS_PORT), getUserName());
			if (isMapReduceSupported()) {
				mapReduceService = HadoopProvidersFactory.createMapReduceService(HADOOP_VERSION, libraries);
				mapReduceInfoService = HadoopProvidersFactory.createMapReduceInfoService(HADOOP_VERSION, libraries);
			}
		} catch (HadoopException ex) {
			free(); // roll back
			throw new ComponentNotReadyException("Could not create instance of Hadoop provider. Reason: " + ex.getMessage(), ex);
		} catch (RuntimeException ex) {
			free(); // roll back
			throw ex;
		}
		super.init();
	}

	protected void loadConfigFileIfNeeded(Properties prop) throws ComponentNotReadyException {
		if (prop.getProperty(XML_CONFIG_KEY) != null) {
			prop.putAll(resolveInitProperties(readFileToProperties(prop.getProperty(XML_CONFIG_KEY)), getGraph().getGraphProperties()));
		}
	}

	/**
	 * Reads specified file and parses Properties from content of the file.
	 * @param configFileLocation Location of the file to be read and parsed.
	 * @return Properties parsed from file content.
	 * @throws ComponentNotReadyException If {@link IOException} is thrown while reading the file.
	 */
	private Properties readFileToProperties(String configFileLocation) throws ComponentNotReadyException {
		Properties config = new Properties();
		InputStream stream = null;
		try {
			stream = Channels.newInputStream(FileUtils.getReadableChannel(getContextURL(contextURL), configFileLocation));
			config.load(stream);
		} catch (IOException ex) {
			throw new ComponentNotReadyException("Configuration file for Hadoop connection not found ("
					+ configFileLocation + ") or could not be read.", ex);
		} finally {
			if (stream != null) {
				try {
					stream.close();
				} catch (IOException ex) {
					LOG.warn("Could not close configuration file for Hadoop connection (" + configFileLocation + ").", ex);
				}
			}
		}
		return config;
	}

	/**
	 * <p>Parses string containing locations of Hadoop API libraries into list of URLs that of that libraries. </p>
	 * 
	 * @param contextURL URL to resolve relative URLs of Hadoop API jars against.
	 * @param hadoopCoreJars String containing locations of Hadoop API libraries.
	 * @return Absolute URLs of Hadoop API libraries.
	 * @throws MalformedURLException If some of locations specified are not correct URLs.
	 * @see #LIB_PATH_SEPARATOR
	 */
	private static List<URL> getProviderClassPathURLList(URL contextURL, String hadoopCoreJars) throws MalformedURLException {
		List<URL> providerClassPath = new ArrayList<URL>();
		
		contextURL = getContextURL(contextURL);
		
		if (!StringUtils.isEmpty(hadoopCoreJars)) {
			providerClassPath = new ArrayList<URL>(Arrays.asList(ClassLoaderUtils.getClassloaderUrls(contextURL, hadoopCoreJars)));
		}
		
		PluginDescriptor hadoopProviderDescriptor = ConnectionFactory.getConnectionDescription(CONNECTION_TYPE_ID).getPluginDescriptor();
		providerClassPath.add(hadoopProviderDescriptor.getURL(HADOOP_PROVIDER_JAR));
		
		if (LOG.isDebugEnabled()) {
			LOG.debug(getClassPathReport(contextURL, providerClassPath));
		}
		
		System.out.println(getClassPathReport(contextURL, providerClassPath)); // FIXME delete this debug printout for junit test run on jenkins
		
		return providerClassPath;
	}

	private static String getClassPathReport(URL contextURL, List<URL> providerClassPath) {
		StringBuilder sb = new StringBuilder();
		sb.append("Hadoop connection libraries status");
		sb.append("\n  Context URL: ").append(contextURL);
		sb.append("\n  Working directory: ").append(System.getProperty("user.dir"));
		sb.append("\n  Classpath (").append(providerClassPath.size()).append(" entries):");
		for (URL url : providerClassPath) {
			sb.append("\n    ").append(url);
		}
		return sb.toString();
	}

	/**
	 * @return parameter if not null, otherwise tries to get runtime context URL of context provider graph.
	 */
	private static URL getContextURL(URL contextURL) {
		if (contextURL == null) {
			return ContextProvider.getContextURL();
		}
		return contextURL;
	}

	/**
	 * <p>Splits given string containing .jar URLs into separate URL strings.</p>
	 * 
	 * TODO This method splits locations base on {@link #LIB_PATH_SEPARATOR}. Replace by
	 * {@link Defaults#DEFAULT_PATH_SEPARATOR_REGEX}.
	 * @param jars String containing list of URLs separated by divider.
	 * @return Array of separate URL strings.
	 */
	public static String[] parseHadoopJarsList(String jars) {
		if (jars == null) {
			return null;
		}
		return jars.split(LIB_PATH_SEPARATOR);
	}

	@Override
	public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		Properties config = new Properties();
		config.putAll(prop);
		
		try {
			loadConfigFileIfNeeded(config);
		} catch (ComponentNotReadyException e) { // thrown iff loading of a config file fails
			status.add(e.getMessage(), e, Severity.ERROR, this, Priority.NORMAL, XML_CONFIG_KEY);
			return super.checkConfig(status);
		}
		
		if (!config.containsKey(XML_FS_HOST_KEY) || StringUtils.isEmpty(config.getProperty(XML_FS_HOST_KEY))) {
			status.add("Cannot initialize Hadoop connection, Hadoop file system host is missing.", Severity.ERROR, this, Priority.NORMAL, XML_FS_HOST_KEY);
		}
		if (!config.containsKey(XML_CORE_LIBRARY_KEY) || StringUtils.isEmpty(config.getProperty(XML_CORE_LIBRARY_KEY))) {
			status.add("Cannot initialize Hadoop connection, Hadoop API .jar libraries are missing.", Severity.WARNING, this, Priority.NORMAL, XML_CORE_LIBRARY_KEY);
		}
		if (!StringUtils.isEmpty(config.getProperty(XML_FS_PORT_KEY)) && Integer.parseInt(config.getProperty(XML_FS_PORT_KEY)) < 0) {
			status.add("Port cannot be negative number.", Severity.ERROR, this, Priority.NORMAL, XML_FS_PORT_KEY);
		}
		if (!StringUtils.isEmpty(config.getProperty(XML_MAPRED_PORT_KEY)) && Integer.parseInt(config.getProperty(XML_MAPRED_PORT_KEY)) < 0) {
			status.add("Port cannot be negative number.", Severity.ERROR, this, Priority.NORMAL, XML_MAPRED_PORT_KEY);
		}
		if (!StringUtils.isEmpty(config.getProperty(XML_MAPRED_PORT_KEY)) && StringUtils.isEmpty(config.getProperty(XML_MAPRED_HOST_KEY))) {
			status.add("Jobtracker port is specified but jobtracter host address is not.", Severity.ERROR, this, Priority.NORMAL, XML_MAPRED_PORT_KEY);
		}
		return super.checkConfig(status);
	}

	/**
	 * Gets service that allows performing file system operations on Hadoop cluster file system. 
	 * This service is connected to Hadoop cluster as specified in configuration of {@code HadoopConnection}. 
	 * For example in case the file system is HDFS it is connected to namenode of Hadoop cluster as configured.
	 * This method may only be called after successful initialization of the connection.
	 * @return Adequate file system service connected to Hadoop cluster file system as specified by user in
	 *         configuration of {@code HadoopConnection}. Never returns <code>null</code>.
	 * @throws IOException 
	 */
	public HadoopFileSystemService getFileSystemService() throws IOException {
		checkInitialized();
		connectFileSystemService();
		return fileSystemService;
	}

	private void connectFileSystemService() throws IOException {
		if (!fileSystemService.isConnected()) {
			fileSystemService.connect(fsConnectionData, getAdditionalProperties());
		}
	}

	/**
	 * As {@link #getFileSystemServiceUnconnected()}, but without assurance that the service will be connected
	 * to a namenode. Few operations of the service may not require the connection to be active,
	 * e.g. creation of a sequence file parser. {@link #init()} has to be called before this method.
	 * @return Adequate file system service as specified by user in configuration of {@code HadoopConnection}.
	 *  ONever returns <code>null</code>.
	 */
	public HadoopFileSystemService getFileSystemServiceUnconnected() {
		checkInitialized();
		return fileSystemService;
	}
	
	/**
	 * Gets service for sending Hadoop map/reduce jobs to Hadoop cluster jobtracter. This service is connected to
	 * jobtracker specified in configuration of {@code HadoopConnection} and thus the jobs are sent to that jobtracker.
	 * This method may only be called after successful initialization of the connection.
	 * @return Adequate map/reduce service connected to jobtracker as specified by user in configuration of
	 *         {@code HadoopConnection}. Never returns <code>null</code>.
	 * @throws  
	 * @throws IOException 
	 * @throws IllegalStateException If {@link #init()} has not been called successfully or if this
	 *         {@code HadoopConnection} does not support map/reduce.
	 * @see {@link #isMapReduceSupported()}
	 */
	public HadoopMapReduceService getMapReduceService() throws IOException {
		checkInitialized();
		if (!isMapReduceSupported()) {
			throw new IllegalStateException(
					"Cannot call connectToMapReduce(). Map/reduce is not supported by this connection because user did not provide jobtracker information.");
		}
		connectMapReduceService();
		return mapReduceService;
	}

	private void connectMapReduceService() throws IOException {
		if (!mapReduceService.isConnected()) {
			mapReduceService.connect(new HadoopMapReduceConnectionData(prop.getProperty(XML_FS_HOST_KEY),
					getPropInt(XML_FS_PORT_KEY, DEFAULT_FS_PORT), fileSystemService.getFSMasterURLTemplate(), prop
							.getProperty(XML_MAPRED_HOST_KEY), getPropInt(XML_MAPRED_PORT_KEY,
							DEFAULT_JOBTRACKER_PORT), getUserName()),
					getAdditionalProperties());
		}
	}

	/**
	 * Gets service that provides Hadoop API version dependent operations. This method may only be called after
	 * successful initialization of the connection. If the connection has not been initialized call
	 * {@link HadoopConnection#getMapReduceInfoService(String, URL, String)} instead.
	 * @return Adequate {@code HadoopMapReduceInfoService} instance for this connection. Never returns
	 *         <code>null</code>.
	 * @see HadoopConnection#getMapReduceInfoService(String, URL, String)
	 * @see #getMapReduceService()
	 */
	public HadoopMapReduceInfoService getMapReduceInfoService() {
		checkInitialized();
		return mapReduceInfoService;
	}
	
	/**
	 * Returns parameters which the file system service used when it established its connection.
	 * @return file system service connection parameters
	 */
	public HadoopFileSystemConnectionData getFileSystemConnectionData() {
		checkInitialized();
		return fsConnectionData;
	}

	/**
	 * Indicates whether or not does this {@code HadoopConnection} also allow connection to map/reduce jobtracker. That
	 * connection does not have to be specified by user and thus not supported. It is not required that this connection
	 * is initialized first.
	 * @return <code>true</code> if and only if the user has supplied map/reduce jobtracker host address
	 *         for this connection.
	 */
	public boolean isMapReduceSupported() {
		if (isInitialized()) {
			return mapReduceService != null;
		} else {
			Properties config = new Properties();
			config.putAll(prop);
			try {
				loadConfigFileIfNeeded(config);
			} catch (ComponentNotReadyException e) {
				return false;
			}
			return !StringUtils.isEmpty(config.getProperty(XML_MAPRED_HOST_KEY));
		}
	}

	/**
	 * Validates configured connection to file system of Hadoop cluster (in case of HDFS that is connection to namenode)
	 * and if map/reduce is supported by this connection it also validates connection to jobtracker. The connection is
	 * valid if and only if this method does not throw exception and returns <code>null</code>. This method may only be
	 * called after successful initialization of the connection.
	 * @return <code>null</code> if validation was successful, a message describing validation problem otherwise.
	 * @throws IOException If it is not possible to connect to Hadoop file system or jobtracker. In such case connection
	 *         is invalid.
	 * @see #init()
	 */
	public String validateConnection() throws IOException {
		checkInitialized();
		
		connectFileSystemService();
		String fsValidationResult = fileSystemService.validateConnection();
		
		String mrValidationResult = null;
		if (isMapReduceSupported()) {
			connectMapReduceService();
			mrValidationResult = mapReduceService.validateConnection();
		}
		
		if (fsValidationResult == null && mrValidationResult == null) {
			return null;
		}
		String result = fsValidationResult == null ? "" :
				"Hadoop file system validation failed! Reason:\n  " + fsValidationResult + "\n";
		result += mrValidationResult == null ? "" :
				"Hadoop map/reduce validation failed! Reason:\n  " + mrValidationResult;
		return result;
	}

	/**
	 * Checks if {@link #init()} has been called on this instance. If not throws exception.
	 * @throws IllegalStateException If this connection has not been initialized.
	 */
	private void checkInitialized() {
		if (!isInitialized() || fileSystemService == null) {
			throw new IllegalStateException("Hadoop connection is not ready. Method init() must be called successfully"
					+ " on this instance first. Instance: " + this);
		}
	}

	@Override
	public synchronized void free() {
		if (isInitialized() || fileSystemService != null || mapReduceService != null || mapReduceInfoService != null) {
			super.free();
			try {
				try {
					if (fileSystemService != null && fileSystemService.isConnected()) {
						fileSystemService.close();
					}
				} finally {
					if (mapReduceService != null && mapReduceService.isConnected()) {
						mapReduceService.close();
					}
				}
			} catch (IOException ex) {
				LOG.error("There was a problem closing connection to Hadoop cluster. The cluster might be already "
						+ "disconected.", ex);
			} finally {
				fileSystemService = null;
				mapReduceService = null;
				mapReduceInfoService = null;
			}
		}
	}

	/**
	 * <p>Sets context URL that is used to resolve relative paths to Hadoop API libraries.</p>
	 * 
	 * <p>TODO This contextURL attribute can potentially cause trouble and should be removed. Relative paths to
	 * libraries should either be allowed to be relative only to graph folder (see
	 * {@link TransformationGraph#getRuntimeContext()}) or relative paths should be resolved in designer and only
	 * absolute path should be provided to HadoopConnection. The last option is to add method for getting this context
	 * to TransformationGraph.</p>
	 * 
	 * @param contextURL Context to be used. If <code>null</code> relative paths are not resolved.
	 */
	public void setContextURL(URL contextURL) {
		this.contextURL = contextURL;
	}

	/**
	 * Parses and returns additional settings specified by user for this {@code HadoopConnection}.
	 * @return Properties
	 */
	public Properties getAdditionalProperties() {
		Properties result = new Properties();
		String additionalParams = prop.getProperty(XML_CUSTOM_PARAMETERS_KEY, null);
		if (!StringUtils.isEmpty(additionalParams)) {
			Properties additionalProp = PropertiesUtils.parseProperties(additionalParams);
			if (additionalProp != null) {
				result.putAll(additionalProp);
			}
		}
		return result;
	}

	/**
	 * Fetches integer setting from configuration of this {@code HadoopConnection}.
	 * @param key XML key for the property.
	 * @param defaultValue Value to be returned if
	 * @return Value of the setting (property) identified by key if it is set, default value otherwise.
	 */
	protected int getPropInt(String key, int defaultValue) {
		return StringUtils.isEmpty(prop.getProperty(key)) ? defaultValue : Integer.parseInt(prop.getProperty(key));
	}

	public String getUserName() {
		return prop.getProperty(XML_USER_NAME_KEY);
	}

	/**
	 * Decrypts password based on set enigma.
	 * @param encryptedPasword Encrypted password to be decrypted.
	 * @return Decrypted password.
	 * @throws JetelException If enigma is not set or it cannot be used to decrypt given password.
	 * @see Enigma
	 */
	protected String decryptPassword(String encryptedPasword) throws JetelException {
		if (encryptedPasword == null) {
			throw new NullPointerException("encryptedPasword");
		}
		Enigma enigma = getGraph().getEnigma();
		if (enigma == null) {
			throw new JetelException(String.format(CANNOT_DECRYPT_PASSWORD_MESSAGE_FORMAT, getId())
					+ " Please set the decryption password as engine parameter -pass.");
		}

		String decryptedPassword;
		try {
			decryptedPassword = enigma.decrypt(encryptedPasword);
		} catch (JetelException ex) {
			throw new JetelException(String.format(CANNOT_DECRYPT_PASSWORD_MESSAGE_FORMAT, getId())
					+ " Probably incorrect decryption password (engine parameter -pass).", ex);
		}
		if (decryptedPassword == null || decryptedPassword.isEmpty()) {
			throw new JetelException(String.format(CANNOT_DECRYPT_PASSWORD_MESSAGE_FORMAT, getId()));
		}
		return decryptedPassword;
	}

	/**
	 * Do not call this method, this operation is not supported for {@code HadoopConnection}.
	 * @throws UnsupportedOperationException When this method is called.
	 */
	@Override
	public DataRecordMetadata createMetadata(Properties parameters) {
		throw new UnsupportedOperationException("Hadoop connection doesn't support operation 'createMetadata()'");
	}
}