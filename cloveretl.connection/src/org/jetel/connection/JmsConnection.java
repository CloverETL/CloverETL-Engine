/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2006 Javlin Consulting <info@javlinconsulting>
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

import java.io.InputStream;
import java.net.URL;
import java.util.Hashtable;
import java.util.Properties;

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

import org.jetel.database.IConnection;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.graph.GraphElement;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.ComponentXMLAttributes;
import org.jetel.util.FileUtils;
import org.w3c.dom.Element;

/**
 *  CloverETL class for connecting to JMS (Java Messaging Service) destinations.<br>
 *  <br>
 *  <table border="1">
 *  <th>XML attributes:</th>
 *  <tr><td><b>id</b></td><td>connection identification</td>
 *  <tr><td><b>config</b></td><td>filename of the config file from which to take connection parameters<br>
 *  If used, then all other attributes are ignored.</td></tr>
*  <tr><td><b>iniCtxFactory</b></td><td>JNDI initial context factory</td>
*  <tr><td><b>providerUrl</b></td><td>JNDI provider URL</td>
*  <tr><td><b>connectionFactory</b></td><td>factory creating JMS connections</td>
*  <tr><td><b>username</b></td><td>username for connection factory</td>
*  <tr><td><b>password</b></td><td>password for connection factory</td>
*  <tr><td><b>destId</b></td><td>JMS destination</td>
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

	public  static final String XML_CONFIG_ATTRIBUTE = "config";
	private static final String XML_INICTX_FACTORY_ATTRIBUTE = "iniCtxFactory";
	private static final String XML_PROVIDER_URL_ATTRIBUTE = "providerUrl";
	private static final String XML_CON_FACTORY_ATTRIBUTE = "connectionFactory";
	private static final String XML_USERNAME_ATTRIBUTE = "username";
	private static final String XML_PASSWORD_ATTRIBUTE = "password";
	private static final String XML_DESTINATION_ATTRIBUTE = "destId";

	private String iniCtxFtory;
	private String providerUrl;
	private String conFtory;
	private String user;
	private String pwd;
	private String destId;
	
	private Connection connection = null;
	private Session session = null;
	private Destination destination = null;

	JmsConnection(String id, String iniCtxFtory, String providerUrl, String conFtory,
			String user, String pwd, String destId) {
		super(id);
		this.iniCtxFtory = iniCtxFtory;
		this.providerUrl = providerUrl;
		this.conFtory = conFtory;
		this.user = user;
		this.pwd = pwd;
		this.destId = destId;
	}
	
	private static Properties readConfig(URL contextURL, String cfgFile) {
		Properties config = new Properties();
		try {
            InputStream stream = null;
            URL url = FileUtils.getFileURL(contextURL, cfgFile);
            stream = url.openStream();
            
//            if (!new File(cfgFile).exists()) {
//                // config file not found on file system - try classpath
//                stream = JmsConnection.class.getClassLoader().getResourceAsStream(cfgFile);
//                if(stream == null) {
//                    throw new FileNotFoundException("Config file for JMS connection not found (" + cfgFile +")");
//                }
//                stream = new BufferedInputStream(stream);
//            } else {
//                stream = new BufferedInputStream(new FileInputStream(cfgFile));
//            }
            
			config.load(stream);
			stream.close();
		} catch (Exception ex) {
			throw new RuntimeException("Config file for JMS connection not found (" + cfgFile +")", ex);
		}
		return config;
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.GraphElement#init()
	 */
	synchronized public void init() throws ComponentNotReadyException {
        if(isInitialized()) return;
		super.init();
		
		try {
			Context ctx = null;
			if (iniCtxFtory != null) {
				Hashtable<String, String> properties = new Hashtable<String, String>();
				properties.put(Context.INITIAL_CONTEXT_FACTORY, iniCtxFtory);
				properties.put(Context.PROVIDER_URL, providerUrl);
				ctx = new InitialContext(properties);			
			} else {	// use jndi.properties
				ctx = new InitialContext();
			}
		    ConnectionFactory ftory = (ConnectionFactory)ctx.lookup(conFtory);		    
			connection = ftory.createConnection(user, pwd);
			session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);			
			destination = (Destination)ctx.lookup(destId);
			connection.start();
		} catch (NamingException e) {
			throw new ComponentNotReadyException(e);
		} catch (JMSException e) {
			throw new ComponentNotReadyException(e);
		}
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.GraphElement#free()
	 */
	synchronized public void free() {
        super.free();

        try {
			connection.close();
		} catch (JMSException e1) {
			// ignore it, the connection is probably already closed
		}
		// re-initialization
//		try {
//			init();
//		} catch (ComponentNotReadyException e) {
//			throw new RuntimeException("Unexpected exception", e);
//		}
	}

	public DataRecordMetadata createMetadata(Properties parameters)
			throws Exception {
		throw new UnsupportedOperationException("JMS connection doesn't support operation 'createMetadata()'");
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.GraphElement#checkConfig(org.jetel.exception.ConfigurationStatus)
	 */
	public ConfigurationStatus checkConfig(ConfigurationStatus status) {
        super.checkConfig(status);
        //TODO
		return status;
	}

	public Session getSession() {
		return session;
	}

	public MessageProducer createProducer() throws JMSException {
		return session.createProducer(destination);		
	}
	
	public MessageConsumer createConsumer(String selector) throws JMSException {
		return session.createConsumer(destination, selector);		
	}
		
	/**
	 *  Description of the Method
	 *
	 * @param  nodeXML  Description of the Parameter
	 * @return          Description of the Return Value
	 */
	public static JmsConnection fromXML(TransformationGraph graph, Element nodeXML) {
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(nodeXML, graph);
		JmsConnection con;
		try {
			if (xattribs.exists(XML_CONFIG_ATTRIBUTE)) {
				Properties config = readConfig(graph.getRuntimeParameters().getProjectURL(), xattribs.getString(XML_CONFIG_ATTRIBUTE));
				con = new JmsConnection(xattribs.getString(XML_ID_ATTRIBUTE),
						config.getProperty(XML_INICTX_FACTORY_ATTRIBUTE, null),
						config.getProperty(XML_PROVIDER_URL_ATTRIBUTE, null),
						config.getProperty(XML_CON_FACTORY_ATTRIBUTE, null),
						config.getProperty(XML_USERNAME_ATTRIBUTE, null),
						config.getProperty(XML_PASSWORD_ATTRIBUTE, null),
						config.getProperty(XML_DESTINATION_ATTRIBUTE, null));
			} else {
				con = new JmsConnection(xattribs.getString(XML_ID_ATTRIBUTE),
						xattribs.getString(XML_INICTX_FACTORY_ATTRIBUTE, null),
						xattribs.getString(XML_PROVIDER_URL_ATTRIBUTE, null),
						xattribs.getString(XML_CON_FACTORY_ATTRIBUTE, null),
						xattribs.getString(XML_USERNAME_ATTRIBUTE, null),
						xattribs.getString(XML_PASSWORD_ATTRIBUTE, null),
						xattribs.getString(XML_DESTINATION_ATTRIBUTE, null));
			}
		} catch (Exception ex) {
			System.err.println(ex.getMessage());
			return null;
		}

		return con;
	}

}
