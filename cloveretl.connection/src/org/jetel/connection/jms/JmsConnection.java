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
package org.jetel.connection.jms;

import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Properties;
import java.util.StringTokenizer;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.naming.NoInitialContextException;

import org.apache.commons.io.IOUtils;
import org.jetel.data.Defaults;
import org.jetel.database.IConnection;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.JetelException;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.GraphElement;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.compile.ClassLoaderUtils;
import org.jetel.util.crypto.Enigma;
import org.jetel.util.file.FileUtils;
import org.jetel.util.primitive.TypedProperties;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.property.RefResFlag;
import org.w3c.dom.Element;

/**
 *  CloverETL class for connecting to JMS (Java Messaging Service) destinations.<br>
 *  <br>
 *  <table border="1">
 *  <th>XML attributes:</th>
 *  <tr><td><b>id</b></td><td>connection identification</td>
 *  <tr><td><b>name</b></td><td>connection name</td>
 *  <tr><td><b>config</b></td><td>filename of the config file from which to take connection parameters<br>
 *  If used, then all following attributes are ignored.</td></tr>
*  <tr><td><b>iniCtxFactory</b></td><td>JNDI initial context factory</td>
*  <tr><td><b>providerUrl</b></td><td>JNDI provider URL</td>
*  <tr><td><b>connectionFactory</b></td><td>factory creating JMS connections</td>
*  <tr><td><b>username</b></td><td>username for connection factory</td>
*  <tr><td><b>password</b></td><td>password for connection factory</td>
*  <tr><td><b>passwordEncrypted</b></td><td>if this flag is true, 
*  attribute password is encrypted and 
*  another password will be required to decrypt password attribute and establish this connection.</td>
*  <tr><td><b>destId</b></td><td>JMS destination</td>
*  <tr><td><b>libraries</b></td><td>List of URLs to libraries (jar or zip files) neccessery for JMS connector. URLs are separated by ";".</td>
 * </table>
 *  <h4>Example:</h4>
 * <pre>&lt;Connection id="dest" type="JMS"<br>
 *   iniCtxFactory="org.apache.activemq.jndi.ActiveMQInitialContextFactory"<br>
 *   providerUrl="vm://localhost"<br>
 *   connectionFactory="ConnectionFactory"<br>
 *   destId="dynamicQueues/Clover"/&gt;
 * </pre>
 * <h4>Example of config file:</h4>
 * <pre>
 * iniCtxFactory=org.apache.activemq.jndi.ActiveMQInitialContextFactory
 * providerUrl=vm://localhost
 * connectionFactory=ConnectionFactory
 * destId=dynamicQueues/Clover
 * </pre>
 *                                 
* @author Jan Hadrava (jan.hadrava@javlinconsulting.cz), Javlin Consulting (www.javlinconsulting.cz)
* @since 09/15/06  
*/
public class JmsConnection extends GraphElement implements IConnection {
	public static final String LIBRARY_PATH_SEPARATOR = Defaults.DEFAULT_PATH_SEPARATOR_REGEX;

	public static final String XML_CONFIG_ATTRIBUTE = "config";
	public static final String XML_NAME_ATTRIBUTE = "name";
	public static final String XML_LIBRARIES_ATTRIBUTE = "libraries";
	public static final String XML_INICTX_FACTORY_ATTRIBUTE = "iniCtxFactory";
	public static final String XML_PROVIDER_URL_ATTRIBUTE = "providerUrl";
	public static final String XML_CON_FACTORY_ATTRIBUTE = "connectionFactory";
	public static final String XML_USERNAME_ATTRIBUTE = "username";
	public static final String XML_PASSWORD_ATTRIBUTE = "password";
	public static final String XML_PASSWORD_ENCRYPTED = "passwordEncrypted";
	public static final String XML_DESTINATION_ATTRIBUTE = "destId";

	private String iniCtxFtory;
	private String libraries;
	private String providerUrl;
	private String conFtory;
	private String user;
	private String pwd;
	private String destId;
	private boolean passwordEncrypted;
	
	private Connection connection = null;
	private Session session = null;
	private Destination destination = null;
	private URL[] librariesUrls = null;
	private URL contextURL;
	private ClassLoader loader;
	
	private ConnectionFactory factory = null;
	private Context initCtx = null;

	public JmsConnection(String id, String iniCtxFtory, String providerUrl, String conFtory,
			String user, String pwd, String destId, boolean passwordEncrypted, String libraries) {
		super(id);
		this.iniCtxFtory = iniCtxFtory;
		this.providerUrl = providerUrl;
		this.conFtory = conFtory;
		this.user = user;
		this.pwd = pwd;
		this.destId = destId;
		this.passwordEncrypted = passwordEncrypted;
		this.libraries = libraries;
		
		if (iniCtxFtory == null) { // use jndi.properties
			try {
				initCtx = new InitialContext();
				// Let's lookup factory and destination before worker context is damaged by classloaders.
				Object o = initCtx.lookup(conFtory);
				if (o instanceof ConnectionFactory) {
					factory = (ConnectionFactory) o;
				}
				Object d = initCtx.lookup(destId);
				if (d instanceof Destination) {
					destination = (Destination)d;
				}
				
			} catch (NamingException e) {
				initCtx = null;
				factory = null;
				destination = null;
			}
		} 		
	}
	
	private static TypedProperties readConfig(URL contextURL, String cfgFile, TransformationGraph graph) {
		TypedProperties config = new TypedProperties(null, graph);
		InputStream stream = null;
		try {
            stream = FileUtils.getInputStream(contextURL, cfgFile);
			config.load(stream);
		} catch (Exception ex) {
			throw new RuntimeException("Config file for JMS connection not found (" + cfgFile +")", ex);
		} finally {
			IOUtils.closeQuietly(stream);
		}
		return config;
	}

	@Override
	synchronized public void init() throws ComponentNotReadyException {
		if (isInitialized()) return;
		super.init();

		// prepare context URL
		if (contextURL == null) {
			contextURL = getContextURL();
		}

		try {

			if (libraries != null)
				this.librariesUrls = getLibrariesURL(contextURL, libraries);

			ClassLoader prevCl = null;
			try {
				if (libraries != null) {
					// Save the class loader so that you can restore it later
					prevCl = Thread.currentThread().getContextClassLoader();
					// Create the class loader by using the given URL
					loader = getAuthorityProxy().createClassLoader(librariesUrls, this.getClass().getClassLoader(), true);
					// InitialContext uses thread Class-Loader
					Thread.currentThread().setContextClassLoader(loader);
				}

				try {
					if (iniCtxFtory != null) {
						Hashtable<String, String> properties = new Hashtable<String, String>();
						properties.put(Context.INITIAL_CONTEXT_FACTORY, iniCtxFtory);
						properties.put(Context.PROVIDER_URL, providerUrl);
						initCtx = new InitialContext(properties);
					} else { // use jndi.properties
						if (initCtx == null) {
							initCtx = new InitialContext();
						}
					}
				} catch (NoInitialContextException e) {
					if (e.getRootCause() instanceof ClassNotFoundException)
						throw new ComponentNotReadyException("No class definition found (add to classpath)", e);
					else
						throw new ComponentNotReadyException("Cannot create initial context", e);
				} catch (Exception e) {
					throw new ComponentNotReadyException("Cannot create initial context", e);
				}
				try {
					if (factory == null) { 
						Object o = initCtx.lookup(conFtory);
						if (o instanceof ConnectionFactory) {
							factory = (ConnectionFactory) o;
						} else {
							if (o == null)
								throw new ComponentNotReadyException("Cannot find connection factory " + ConnectionFactory.class + " with jndiName:" + conFtory + " in the ctx:"+initCtx);
							else
								throw new ComponentNotReadyException("Cannot find connection factory (interface may be loaded by different classloader) " + ConnectionFactory.class + " loaded by:" + ConnectionFactory.class.getClassLoader() + " with jndiName:" + conFtory + " found:" + o + " " + (o != null ? (("" + o.getClass() + " loaded by:" + o.getClass().getClassLoader())) : ""));
						}
					}
				} catch (ComponentNotReadyException e) {
					throw e;
				} catch (NamingException e) {
					throw e;
				} catch (Exception e) {
					throw new ComponentNotReadyException("Cannot create connection factory", e);
				}
				if (factory == null)
					throw new ComponentNotReadyException("Cannot create connection factory");

				if (passwordEncrypted) {
					Enigma enigma = getGraph().getEnigma();
					if (enigma == null) {
						throw new ComponentNotReadyException("Can't decrypt password on JmsConnection (id=" + this.getId() + "). Please set the password as engine parameter -pass.");
					}
					// Enigma enigma = Enigma.getInstance();
					String decryptedPassword = null;
					try {
						decryptedPassword = enigma.decrypt(pwd);
					} catch (JetelException e) {
						throw new ComponentNotReadyException("Can't decrypt password on JmsConnection (id=" + this.getId() + "). Incorrect password.", e);
					}
					// If password decryption fails, try to use the unencrypted password
					if (decryptedPassword != null) {
						pwd = decryptedPassword;
						passwordEncrypted = false;
					}
				}
			} finally {
				if (loader != null)
					Thread.currentThread().setContextClassLoader(prevCl);
			}
		} catch (NoClassDefFoundError e) {
			throw new ComponentNotReadyException("No class definition found (add to classpath)", e);
		} catch (NamingException e) {
			if (e.getRootCause() instanceof NoClassDefFoundError)
				throw new ComponentNotReadyException("No class definition found (add to classpath)", e);
			else if (e.getRootCause() instanceof ClassNotFoundException)
				throw new ComponentNotReadyException("No class definition found (add to classpath)", e);
			else
				throw new ComponentNotReadyException("Cannot create initial context", e);
		} catch (IllegalStateException e) {
			throw new ComponentNotReadyException(e);
		}
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.graph.GraphElement#free()
	 */
	@Override
	synchronized public void free() {
        if(!isInitialized()) return;
        super.free();
        final ClassLoader ctxLoader = Thread.currentThread().getContextClassLoader();
        try {
        	if (loader != null) {
        		Thread.currentThread().setContextClassLoader(loader);
        	}
        	if (connection != null)
        		connection.close();
        	connection = null;
		} catch (JMSException e) {
			logger.warn("Error while closing JMS connection.", e);
		} finally {
			Thread.currentThread().setContextClassLoader(ctxLoader);
		}
	}

	public void initSession() throws ComponentNotReadyException {
		final ClassLoader ctxLoder = Thread.currentThread().getContextClassLoader();
		try {
			if (loader != null) {
				Thread.currentThread().setContextClassLoader(loader);
			}
			session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		if (session == null)
			throw new ComponentNotReadyException("Cannot create JMS session");
		} catch (JMSException e) {
			throw new ComponentNotReadyException(e);
		} finally {
			Thread.currentThread().setContextClassLoader(ctxLoder);
		}
	}
	

	@Override
	public synchronized void preExecute() throws ComponentNotReadyException {
		super.preExecute();
		if (firstRun()) {
			initConnection();
		} else {
			if (getGraph().getRuntimeContext().isBatchMode()) {
				initConnection();
			}
		}
		initSession();
	}
	
	@Override
	public synchronized void postExecute() throws ComponentNotReadyException {
		super.postExecute();
		final ClassLoader ctxLoader = Thread.currentThread().getContextClassLoader();
		try {
			if (loader != null) {
				Thread.currentThread().setContextClassLoader(loader);
			}
			if (getGraph().getRuntimeContext().isBatchMode()) {
				connection.close();
				connection = null;
				session = null;
			} else {
				session.close();
				session = null;
			}
		} catch (JMSException e) {
			throw new ComponentNotReadyException(e);
		} finally {
			Thread.currentThread().setContextClassLoader(ctxLoader);
		}
	}

	public void initConnection() throws ComponentNotReadyException {
		ClassLoader prevCl = null;
		
		try {
			if (loader != null) {
				// Save the class loader so that you can restore it later
				prevCl = Thread.currentThread().getContextClassLoader();
				Thread.currentThread().setContextClassLoader(loader);
			}
			try {
				connection = factory.createConnection(user, pwd);
			} catch (Exception e) {
				throw new ComponentNotReadyException("Cannot establish JMS connection", e);
			}
			if (connection == null)
				throw new ComponentNotReadyException("Cannot establish JMS connection");
			try {
				if (destination == null) {
					Object o = initCtx.lookup(destId);
					if (!(o instanceof Destination))
						throw new ComponentNotReadyException(this, "Specified destination " + destId + " doesn't contain instance of " + Destination.class + ", but:" + o);
					destination = (Destination) o;
				}
			} catch (NamingException e) {
				throw new ComponentNotReadyException("Cannot find destination \"" + destId + "\" in initial context");
			}
			if (destination == null)
				throw new ComponentNotReadyException("Cannot find destination in \"" + destId + "\" initial context");
			connection.start();
		} catch (JMSException ex) {
			throw new ComponentNotReadyException(ex);
		} finally {
			if (prevCl != null) {
				//thread classloader restoration
				Thread.currentThread().setContextClassLoader(prevCl);
			}
		}
	}
	
	@Override
	public DataRecordMetadata createMetadata(Properties parameters) {
		throw new UnsupportedOperationException("JMS connection doesn't support operation 'createMetadata()'");
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.GraphElement#checkConfig(org.jetel.exception.ConfigurationStatus)
	 */
	@Override
	public ConfigurationStatus checkConfig(ConfigurationStatus status) {
        super.checkConfig(status);
		return status;
	}

	public Session getSession() {
		return session;
	}

	public MessageProducer createProducer() throws JMSException {
		final ClassLoader ctxLoader = Thread.currentThread().getContextClassLoader();
		try {
			if (loader != null) {
				Thread.currentThread().setContextClassLoader(loader);
			}
			if (session == null)
				throw new IllegalStateException("JMS session is not initialized");
			return session.createProducer(destination);
		} finally {
			Thread.currentThread().setContextClassLoader(ctxLoader);
		}
	}
	
	public MessageConsumer createConsumer(String selector) throws JMSException {
		final ClassLoader ctxLoader = Thread.currentThread().getContextClassLoader();
		try {
			if (loader != null) {
				Thread.currentThread().setContextClassLoader(loader);
			}
			if (session == null)
				throw new IllegalStateException("JMS session is not initialized");
			return session.createConsumer(destination, selector);
		} finally {
			Thread.currentThread().setContextClassLoader(ctxLoader);
		}
	}
		
	/**
	 *  Description of the Method
	 *
	 * @param  nodeXML  Description of the Parameter
	 * @return          Description of the Return Value
	 * @throws XMLConfigurationException 
	 * @throws AttributeNotFoundException 
	 */
	public static JmsConnection fromXML(TransformationGraph graph, Element nodeXML) throws XMLConfigurationException, AttributeNotFoundException {
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(nodeXML, graph);
		JmsConnection con;
		if (xattribs.exists(XML_CONFIG_ATTRIBUTE)) {
			// TODO move readConfig() to init() method - fromXML shouldn't read anything from external files
			TypedProperties config = readConfig(graph.getRuntimeContext().getContextURL(), 
					xattribs.getString(XML_CONFIG_ATTRIBUTE), graph);
			con = new JmsConnection(xattribs.getString(XML_ID_ATTRIBUTE),
					config.getStringProperty(XML_INICTX_FACTORY_ATTRIBUTE, null),
					config.getStringProperty(XML_PROVIDER_URL_ATTRIBUTE, null),
					config.getStringProperty(XML_CON_FACTORY_ATTRIBUTE, null),
					config.getStringProperty(XML_USERNAME_ATTRIBUTE, null),
					config.getStringProperty(XML_PASSWORD_ATTRIBUTE, null, RefResFlag.PASSWORD),
					config.getStringProperty(XML_DESTINATION_ATTRIBUTE, null),
					config.getBooleanProperty(XML_PASSWORD_ENCRYPTED, false), 
					config.getStringProperty(XML_LIBRARIES_ATTRIBUTE, null)
					);
		} else {
			con = new JmsConnection(xattribs.getString(XML_ID_ATTRIBUTE),
					xattribs.getString(XML_INICTX_FACTORY_ATTRIBUTE, null),
					xattribs.getString(XML_PROVIDER_URL_ATTRIBUTE, null),
					xattribs.getString(XML_CON_FACTORY_ATTRIBUTE, null),
					xattribs.getString(XML_USERNAME_ATTRIBUTE, null),
					xattribs.getStringEx(XML_PASSWORD_ATTRIBUTE, null, RefResFlag.PASSWORD),
					xattribs.getString(XML_DESTINATION_ATTRIBUTE, null),
					xattribs.getBoolean(XML_PASSWORD_ENCRYPTED, false),
					xattribs.getString(XML_LIBRARIES_ATTRIBUTE, null)
					);
		}

		return con;
	}

	/**
	 * @param urls
	 * @return
	 */
	public static String getLibrariesString(URL[] urls){
		StringBuilder sb = new StringBuilder();
		for (URL u : urls){
			if (sb.length()>0)
				sb.append(LIBRARY_PATH_SEPARATOR);
			sb.append(u);
		}
		return sb.toString();
	}

	/**
	 * @param urls
	 * @return
	 */
	public static String getLibrariesString(String[] urls){
		StringBuilder sb = new StringBuilder();
		for (String u : urls){
			if (sb.length()>0)
				sb.append(LIBRARY_PATH_SEPARATOR);
			sb.append(u);
		}
		return sb.toString();
	}
	
	public static URL[] getLibrariesURL(String libraryPath) {
		return getLibrariesURL(null, libraryPath);
	}
	
	/**
	 * 
	 * @param libraryPath
	 * @return
	 */
	public static URL[] getLibrariesURL(URL contextURL, String libraryPath) {
		try {
			return ClassLoaderUtils.getClassloaderUrls(contextURL, libraryPath);
		} catch (Exception e) {
			throw new JetelRuntimeException("Can not create JMS connection.", e);
		}
	}

	/**
	 * 
	 * @param libraryPath
	 * @return
	 */
	public static String[] getLibraries(String libraryPath) {
		if (libraryPath == null || libraryPath.length() == 0) {
			return new String[0];
		}
		StringTokenizer libTok = new StringTokenizer(libraryPath, LIBRARY_PATH_SEPARATOR);
		List<String> newLibraries = new ArrayList<String>(libTok.countTokens());
		String libraryURLString = null;
		while (libTok.hasMoreElements()) {
				libraryURLString = libTok.nextToken(); 
				newLibraries.add(libraryURLString);
		}// while
		return (String[]) newLibraries.toArray(new String[newLibraries.size()]);
	}

	public void setContextURL(URL contextURL) {
		this.contextURL = contextURL;
	}
	
}
