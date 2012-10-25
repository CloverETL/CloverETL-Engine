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
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.Defaults;
import org.jetel.database.ConnectionFactory;
import org.jetel.database.IConnection;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.ConfigurationStatus.Priority;
import org.jetel.exception.ConfigurationStatus.Severity;
import org.jetel.exception.JetelException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.GraphElement;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.plugin.PluginClassLoader;
import org.jetel.util.classloader.GreedyURLClassLoader;
import org.jetel.util.crypto.Enigma;
import org.jetel.util.file.FileUtils;
import org.jetel.util.primitive.TypedProperties;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.property.PropertiesUtils;
import org.jetel.util.property.PropertyRefResolver;
import org.jetel.util.property.RefResFlag;
import org.jetel.util.string.StringUtils;
import org.w3c.dom.Element;

/**
 * @author dpavlis (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @see org.apache.hadoop.fs.FileSystem
 */        

public class HadoopConnection extends GraphElement implements IConnection {
	
		public final static String HADOOP_CONNECTION_IMPLEMENTATION_JAR_NAME =
			"cloveretl.component.hadooploader.jar";
		private final static String HADOOP_CONNECTION_IMPLEMENTATION_CLASS =
			"org.jetel.component.hadooploader.HadoopConnectionInstance";
		private final static String HADOOP_CONNECTION_IMPLEMENTATION_JAR =
			"./lib/" + HADOOP_CONNECTION_IMPLEMENTATION_JAR_NAME;	
		
		private static final String HADOOP_LOAD_ERROR = 
				"Cannot load Hadoop java implementation. Make sure you specify correct hadoop-core.jar " +
				"or that your system classpath contains them.";
		
		private static final String HADOOP_LOAD_CHECKING_CLASS = "org.apache.hadoop.fs.FileAlreadyExistsException";
			
		private static final String ERROR_LOADING_IMPL_MOD =
				"Internal Error. (Could not find CloverETL Hadoop Implementation module.)";
			
	
	    private static final Log logger = LogFactory.getLog(HadoopConnection.class);
		
		public static final String XML_CONFIG_ATTRIBUTE = "config";
		public static final String XML_HADOOP_CORE_LIBRARY_ATTRIBUTE = "hadoopJar";
		public static final String XML_HADOOP_PARAMETERS = "hadoopParams"; 
		public static final String XML_HADOOP_HDFS_HOST = "host";
		public static final String XML_HADOOP_HDFS_PORT = "port";
		public static final String XML_HADOOP_USER = "user";
	    public static final String XML_HADOOP_MAPRED_HOST = "hostMapred";
	    public static final String XML_HADOOP_MAPRED_PORT = "portMapred";
		
		public static final String XML_USERNAME_ATTRIBUTE = "username";
		public static final String XML_PASSWORD_ATTRIBUTE = "password";
		public static final String XML_PASSWORD_ENCRYPTED = "passwordEncrypted";

		public static final String HADOOP_DEFAULT_HDFS_PORT = "8020";
		public static final String HADOOP_DEFAULT_JOBTRACKER_PORT = "8021";
		public static final String HADOOP_URI_STR_FORMAT = "hdfs://%s:%s/";
		public static final String CONNECTION_TYPE_ID = "HADOOP";
		
		private String user;
		private String pwd;
		private String host;
		private String hostMapred;
		private String port;
		private String portMapred;
		private String password;
		private boolean passwordEncrypt;
		
		private Properties properties;
		private ClassLoader classLoader;
		private URL[] loaderJars;
		private String hadoopCoreJar;
		private String hadoopParameters;
		private URL hadoopModuleImplementationPath;
		private IHadoopConnection connection;
		
		
		public HadoopConnection(String id, String host, String port,
				String user, String pwd, boolean passwordEncrypt, String hadoopCoreJar,Properties properties) {
			super(id);
			setName(id);
			this.host=host;
			this.port=port;
			this.user = user;
			this.pwd = pwd;
			this.passwordEncrypt = passwordEncrypt;
			this.hadoopCoreJar = hadoopCoreJar;
			this.properties=properties;
		}
		
		
		public HadoopConnection(String id){
			super(id);
			setName(id);
		}
		
		private static Properties readConfig(URL contextURL, String cfgFile, TransformationGraph graph) {
			Properties config = new Properties();
			try {
	            InputStream stream = Channels.newInputStream(FileUtils.getReadableChannel(contextURL, cfgFile));
				config.load(stream);
				stream.close();
			} catch (Exception ex) {
				throw new RuntimeException("Config file for Hadoop connection not found (" + cfgFile +")", ex);
			}
			(new PropertyRefResolver(graph.getGraphProperties())).resolveAll(config);
			return config;
		}


		@Override
		synchronized public void init() throws ComponentNotReadyException {
			if (isInitialized()) return;
			super.init();

			initExternal(); // must be called first
			
			// init properties - additional Hadoop config parameters
			try {
				if (!StringUtils.isEmpty(this.hadoopParameters)) {
					Properties prop=PropertiesUtils.parseProperties(this.hadoopParameters);
				if (this.properties == null)
					this.properties = prop;
				else
					this.properties.putAll(prop);
				}
			} catch (Exception ex) {
				logger.debug(ex);
			} 
		

			initPassword();
			initClassLoading();
					
		}
			

		public void close(){
			if (connection!=null){
				try{
					connection.close();
				}catch(Exception ex){
					// do nothing
				}
				connection=null;
			}
		}
		
		private void initExternal() throws ComponentNotReadyException {
			if (this.properties!=null && this.properties.isEmpty() ) {
				loadFromTypedProperties(new TypedProperties(this.properties));
			}else{
				if (StringUtils.isEmpty(this.host))
				throw new ComponentNotReadyException(this,"Can not initialize from external config file - file is empty");
			}
		}
		
		private void initPassword() throws ComponentNotReadyException {
			if (!passwordEncrypt) {
				return;
			}

			Enigma enigma = getGraph().getEnigma();
			if (enigma == null) {
				throw new ComponentNotReadyException(this,"Can't decrypt password on HadoopConnection (id=" + this.getId() + "). Please set the password as engine parameter -pass.");
			}

			String decryptedPassword = null;
			try {
				decryptedPassword = enigma.decrypt(password);
			} catch (JetelException e) {
				throw new ComponentNotReadyException(this,"Can't decrypt password on HadoopConnection (id=" + this.getId() + "). Incorrect password.", e);
			}
			// If password decryption fails, try to use the unencrypted password
			if (decryptedPassword != null) {
				password = decryptedPassword;
				passwordEncrypt = false;
			}
		}
		
		
		
		/* (non-Javadoc)
		 * @see org.jetel.graph.GraphElement#free()
		 */
		@Override
		synchronized public void free() {
	        if(!isInitialized()) return;
	        super.free();

	        try {
	        	if (connection != null)
	        		connection.close();
	        	connection = null;
			} catch (IOException e1) {
				// ignore it, the connection is probably already closed
			}
		}



		@Override
		public synchronized void preExecute() throws ComponentNotReadyException {
			super.preExecute();
			if (firstRun()) {
				init();
			} else {
				if (getGraph().getRuntimeContext().isBatchMode()) {
					init();
				}
			}
		}
		
		@Override
		public synchronized void postExecute() throws ComponentNotReadyException {
			super.postExecute();
			try {
				if (getGraph().getRuntimeContext().isBatchMode()) {
					if (connection !=null) connection.close();
					connection = null;
				} else { //for now no difference between batch & non-batch
					if (connection !=null) connection.close();
					connection = null;
				}
			} catch (IOException e) {
				throw new ComponentNotReadyException(e);
			}
		}
	
		
		/**
		 * Get Hadoop filesystem to which this connection is attached.
		 * 
		 * @return Hadoop distributed filesystem object
		 */
		public IHadoopConnection getConnection() throws IOException, ComponentNotReadyException {
			if(connection ==null){
				try {
					connection = instantiateConnection();
					URI hURI=new URI(String.format(HADOOP_URI_STR_FORMAT, this.host, this.port));
					if (!StringUtils.isEmpty(this.user)){
						connection.connect(hURI, this.properties, this.user);
					}else{
						connection.connect(hURI, this.properties);
					}
				} catch (HadoopConnectionException e) {
					throw new ComponentNotReadyException(this,e);
				} catch (URISyntaxException e) {
					throw new IOException("Invalid HDFS host/port definition.",e);
				} 
			}
			return connection;

		}
		
		
		@Override
		public DataRecordMetadata createMetadata(Properties parameters) {
			throw new UnsupportedOperationException("Hadoop connection doesn't support operation 'createMetadata()'");
		}

		/* (non-Javadoc)
		 * @see org.jetel.graph.GraphElement#checkConfig(org.jetel.exception.ConfigurationStatus)
		 */
		@Override
		public ConfigurationStatus checkConfig(ConfigurationStatus status) {
	        super.checkConfig(status);
	        try{
	        //  do not try to connecti.	
	        //	init();
	        //	connection.close();
	        }catch(Exception ex){
	        	status.add(new ConfigurationProblem("Error: "+ex.getMessage(), Severity.ERROR, this, Priority.NORMAL));
	        }
			return status;
		}

			
		/**
		 *  Description of the Method
		 *
		 * @param  nodeXML  Description of the Parameter
		 * @return          Description of the Return Value
		 * @throws XMLConfigurationException 
		 */
		public static HadoopConnection fromXML(TransformationGraph graph, Element nodeXML) throws XMLConfigurationException {
			ComponentXMLAttributes xattribs = new ComponentXMLAttributes(nodeXML, graph);
			HadoopConnection con;
			try {
				if (xattribs.exists(XML_CONFIG_ATTRIBUTE)) {
					// TODO move readConfig() to init() method - fromXML shouldn't read anything from external files
					Properties config = readConfig(graph.getRuntimeContext().getContextURL(), 
							xattribs.getString(XML_CONFIG_ATTRIBUTE), graph);
					
					con = new HadoopConnection(xattribs.getString(XML_ID_ATTRIBUTE),
							config.getProperty(XML_HADOOP_HDFS_HOST), 
							config.getProperty(XML_HADOOP_HDFS_PORT, HADOOP_DEFAULT_HDFS_PORT),
							config.getProperty(XML_USERNAME_ATTRIBUTE, null),
							config.getProperty(XML_PASSWORD_ATTRIBUTE, null),
							Boolean.valueOf(config.getProperty(XML_PASSWORD_ENCRYPTED, "false")), 
							config.getProperty(XML_HADOOP_CORE_LIBRARY_ATTRIBUTE, null),config);
					
					if (config.containsKey(XML_NAME_ATTRIBUTE))
						con.setName(config.getProperty(XML_NAME_ATTRIBUTE));
					
					if (config.containsKey(XML_HADOOP_PARAMETERS))
						con.setHadoopParams(config.getProperty(XML_HADOOP_PARAMETERS));
					
					if (config.containsKey(XML_HADOOP_MAPRED_HOST))
						con.setHostMapred(config.getProperty(XML_HADOOP_MAPRED_HOST));
					
					if (config.containsKey(XML_HADOOP_MAPRED_PORT))
						con.setPortMapred(config.getProperty(XML_HADOOP_MAPRED_PORT));
					
				} else {
					con = new HadoopConnection(xattribs.getString(XML_ID_ATTRIBUTE),
							xattribs.getString(XML_HADOOP_HDFS_HOST),
							xattribs.getString(XML_HADOOP_HDFS_PORT,HADOOP_DEFAULT_HDFS_PORT),
							xattribs.getString(XML_USERNAME_ATTRIBUTE, null),
							xattribs.getString(XML_PASSWORD_ATTRIBUTE, null),
							xattribs.getBoolean(XML_PASSWORD_ENCRYPTED, false),
							xattribs.getString(XML_HADOOP_CORE_LIBRARY_ATTRIBUTE, null),
							xattribs.attributes2Properties(new String[]{XML_HADOOP_HDFS_HOST, XML_HADOOP_HDFS_PORT}, RefResFlag.REGULAR));
					
					if (xattribs.exists(XML_NAME_ATTRIBUTE))
						con.setName(xattribs.getString(XML_NAME_ATTRIBUTE));
					
					if (xattribs.exists(XML_HADOOP_PARAMETERS))
						con.setHadoopParams(xattribs.getString(XML_HADOOP_PARAMETERS));
					
					if (xattribs.exists(XML_HADOOP_MAPRED_HOST))
						con.setHostMapred(xattribs.getString(XML_HADOOP_MAPRED_HOST));
					
					if (xattribs.exists(XML_HADOOP_MAPRED_PORT))
						con.setPortMapred(xattribs.getString(XML_HADOOP_MAPRED_PORT));
				}
				
				
			} catch (Exception e) {
	            throw new XMLConfigurationException("HadoopConnection: " 
	            		+ xattribs.getString(XML_ID_ATTRIBUTE, "unknown ID") + ":" + e.getMessage(), e);
			}

			
			
			return con;
		}
		
		public String getUser() {
			return user;
		}

		public void setUser(String user) {
			this.user = user;
		}


		public String getPwd() {
			return pwd;
		}


		public void setPwd(String pwd) {
			this.pwd = pwd;
		}


		public String getHost() {
			return host;
		}


		public void setHost(String host) {
			this.host = host;
		}


		public String getPort() {
			return port;
		}


		public void setPort(String port) {
			this.port = port;
		}


		public String getHostMapred() {
			return hostMapred;
		}


		public void setHostMapred(String hostMapred) {
			this.hostMapred = hostMapred;
		}


		public String getPortMapred() {
			return portMapred;
		}


		public void setPortMapred(String portMapred) {
			this.portMapred = portMapred;
		}


		public String getPassword() {
			return password;
		}


		public void setPassword(String password) {
			this.password = password;
		}


		public boolean isPasswordEncrypt() {
			return passwordEncrypt;
		}


		public void setPasswordEncrypt(boolean passwordEncrypt) {
			this.passwordEncrypt = passwordEncrypt;
		}
		
		public void setHadoopParams(String params){
			this.hadoopParameters=params;
		}
		
		public String getHadoopParams(){
			return this.hadoopParameters;
		}
		
		public URL getHadoopModuleImplementationPath() {
			return hadoopModuleImplementationPath;
		}


		public void setHadoopModuleImplementationPath(URL hadoopModuleImplementationPath) {
			this.hadoopModuleImplementationPath = hadoopModuleImplementationPath;
		}

		
		public void loadFromProperties(Properties properties) throws ComponentNotReadyException {
			loadFromTypedProperties(new TypedProperties(properties));
		}
		
		private void loadFromTypedProperties(TypedProperties properties) throws ComponentNotReadyException {
			this.host=properties.getStringProperty(XML_HADOOP_HDFS_HOST);
			this.port=properties.getStringProperty(XML_HADOOP_HDFS_PORT, HADOOP_DEFAULT_HDFS_PORT);
			this.user=properties.getStringProperty(XML_HADOOP_USER,null);
			
			if (!properties.containsKey(XML_HADOOP_CORE_LIBRARY_ATTRIBUTE))
				throw new ComponentNotReadyException("Hadoop core library jar not defined.");
				
			this.hadoopCoreJar=properties.getStringProperty(XML_HADOOP_CORE_LIBRARY_ATTRIBUTE);
			this.password=properties.getStringProperty(XML_PASSWORD_ATTRIBUTE);
			this.passwordEncrypt=properties.getBooleanProperty(XML_PASSWORD_ENCRYPTED, false);
			this.hadoopParameters=properties.getStringProperty(XML_HADOOP_PARAMETERS,null);
			
		}
		
		
		private void initClassLoader() {
			if (classLoader != null) {
				return;
			}
			
			if (loaderJars==null || loaderJars.length == 0) {
				// for running in server where all jars are available on class path
				classLoader = getClass().getClassLoader();
			}
			else {
				classLoader = new GreedyURLClassLoader(loaderJars, getClass().getClassLoader());
			}
		}
		
		private void initClassLoading() throws ComponentNotReadyException {
			List<URL> additionalJars = new ArrayList<URL>();

			if (hadoopCoreJar != null && !hadoopCoreJar.isEmpty()) {
				String urls[] = hadoopCoreJar.split("\\n|"+Defaults.DEFAULT_PATH_SEPARATOR_REGEX);
				for (String url:urls){
					URL hadoopJar;
					try {
						hadoopJar = new URL(url);
					} catch (MalformedURLException e) {
						try {
							hadoopJar = new URL("file:" + url);
						} catch (MalformedURLException ex) {
							throw new ComponentNotReadyException(
									"Cannot load library from '"
											+ url + "'", ex);
						}
					}
					additionalJars.add(hadoopJar);
				}
			}

			ClassLoader thisClassLoader = this.getClass().getClassLoader();

			if (thisClassLoader instanceof PluginClassLoader) {
				PluginClassLoader thisPluginClassLoader = (PluginClassLoader) thisClassLoader;
				try {
					additionalJars.add(thisPluginClassLoader.getPluginDescriptor()
							.getURL(HADOOP_CONNECTION_IMPLEMENTATION_JAR));
				} catch (MalformedURLException e1) {
					throw new ComponentNotReadyException(e1);
				}
			} else if (hadoopModuleImplementationPath != null) {
				additionalJars.add(hadoopModuleImplementationPath);
			} else {
				try {
					additionalJars.add(ConnectionFactory.getConnectionDescription(CONNECTION_TYPE_ID).getPluginDescriptor().
						getURL(HADOOP_CONNECTION_IMPLEMENTATION_JAR));
				} catch (MalformedURLException e) {
					throw new ComponentNotReadyException(e);
				}
			}
			
			if (additionalJars.size() == 0) {
				throw new ComponentNotReadyException(ERROR_LOADING_IMPL_MOD);
			}

			loaderJars = (URL[]) additionalJars.toArray(new URL[0]);
		}
		
		private IHadoopConnection instantiateConnection() throws HadoopConnectionException {
			//logger.debug("connectWithLoader()");
			IHadoopConnection conn;
			initClassLoader();
			
			/*DEBUG:
			for(URL url: ((GreedyURLClassLoader)classLoader).getURLs()){
			logger.debug(url);
			}*/
			
			// load and check clover part
			Class<?> hadoopImplementationClass;
			try {
				hadoopImplementationClass = classLoader.loadClass(HADOOP_CONNECTION_IMPLEMENTATION_CLASS);
				conn = (IHadoopConnection)hadoopImplementationClass.newInstance();
				
			} catch (ClassNotFoundException e) {
				throw new HadoopConnectionException(ERROR_LOADING_IMPL_MOD, e);
			} catch (InstantiationException e) {
				throw new HadoopConnectionException(ERROR_LOADING_IMPL_MOD, e);
			} catch (IllegalAccessException e) {
				throw new HadoopConnectionException(ERROR_LOADING_IMPL_MOD, e);
			}
			return conn;
		}
		
}
