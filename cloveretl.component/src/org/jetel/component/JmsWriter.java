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
package org.jetel.component;

import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Properties;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.naming.Context;
import javax.naming.InitialContext;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.JetelException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.TransformationGraph;
import org.jetel.util.ComponentXMLAttributes;
import org.jetel.util.DynamicJavaCode;
import org.w3c.dom.Element;


/**
*  <h3>JmsWriter Component</h3>
*
* <table border="1">
*  <th>Component:</th>
* <tr><td><h4><i>Name:</i></h4></td>
* <td>JmsWriter</td></tr>
* <tr><td><h4><i>Category:</i></h4></td>
* <td></td></tr>
* <tr><td><h4><i>Description:</i></h4></td>
* <td>Transforms incoming data records to JMS messages using user-specified transformation class
* (so-called processor). The processor is supposed to implement interface <code>DataRecord2JmsMsg</code>.
* The processor may be specified either by class name or by inline Java code.
* </td></tr>
* <tr><td><h4><i>Inputs:</i></h4></td>
* <td>One input port for incoming data records.</td></tr>
* <tr><td><h4><i>Outputs:</i></h4></td>
* <td></td></tr>
* <tr><td><h4><i>Comment:</i></h4></td>
* <td></td></tr>
* </table>
*  <br>
*  <table border="1">
*  <th>XML attributes:</th>
*  <tr><td><b>type</b></td><td>"JMS_WRITER"</td></tr>
*  <tr><td><b>id</b></td><td>component identification</td>
*  <tr><td><b>iniCtxFactory</b></td><td>JNDI initial context factory</td>
*  <tr><td><b>providerUrl</b></td><td>JNDI provider URL</td>
*  <tr><td><b>connectionFactory</b></td><td>factory creating JMS connections</td>
*  <tr><td><b>username</b></td><td>username for connection factory</td>
*  <tr><td><b>password</b></td><td>password for connection factory</td>
*  <tr><td><b>destId</b></td><td>JMS destination</td>
*  <tr><td><b>processorCode</b></td><td>Inline Java code defining processor class</td>
*  <tr><td><b>processorClass</b></td><td>Name of processor class</td>
*  </tr>
*  </table>
*
* @author Jan Hadrava (jan.hadrava@javlinconsulting.cz), Javlin Consulting (www.javlinconsulting.cz)
* @since 09/15/06  
* @see         org.jetel.data.parser.FixLenDataParser
*/
public class JmsWriter extends Node {
	public final static String COMPONENT_TYPE = "JMS_READER";

	static Log logger = LogFactory.getLog(MysqlDataReader.class);

	private static final String XML_INICTX_FACTORY_ATTRIBUTE = "iniCtxFactory";
	private static final String XML_PROVIDER_URL_ATTRIBUTE = "providerUrl";
	private static final String XML_CON_FACTORY_ATTRIBUTE = "connectionFactory";
	private static final String XML_USERNAME_ATTRIBUTE = "username";
	private static final String XML_PASSWORD_ATTRIBUTE = "password";
	private static final String XML_DESTINATION_ATTRIBUTE = "destId";
	private static final String XML_PSORCODE_ATTRIBUTE = "processorCode";
	private static final String XML_PSORCLASS_ATTRIBUTE = "processorClass";

	// component attributes
	private String iniCtxFtory;
	private String providerUrl;
	private String conFtory;
	private String user;
	private String pwd;
	private String destId;
	private String psorClass;
	private String psorCode;
	private Properties psorProperties;

	private InputPort inPort;
	
	private Connection connection;
	private MessageProducer producer;	
	private DataRecord2JmsMsg psor;

	/** Sole ctor.
	 * @param id Component ID
	 * @param iniCtxFtory Initial context factory 
	 * @param providerUrl Provider URL
	 * @param conFtory JMS Connection factory name
	 * @param user Username for connection factory
	 * @param pwd Password for connection factory
	 * @param destId JMS destination ID
	 * @param psorClass Processor class
	 * @param psorCode Inline processor definition
	 * @param psorProperties Properties to be passed to data processor.
	 */
	public JmsWriter(String id, String iniCtxFtory, String providerUrl, String conFtory,
			String user, String pwd, String destId, String psorClass, String psorCode,
			Properties psorProperties) {
		super(id);
		this.iniCtxFtory = iniCtxFtory;
		this.providerUrl = providerUrl;
		this.conFtory = conFtory;
		this.user = user;
		this.pwd = pwd;
		this.destId = destId;
		this.psorClass = psorClass;
		this.psorCode = psorCode;
		this.psorProperties = psorProperties;
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.GraphElement#init()
	 */
	public void init() throws ComponentNotReadyException {
		if (getInPorts().size() != 1) {
			throw new ComponentNotReadyException("Exactly one input port is required");
		}
		if (psorClass == null && psorCode == null) {
			throw new ComponentNotReadyException("Message processor not specified");
		}

		inPort = getInputPort(0);
		
		Context ctx = null;
		try {
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
			Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);			
			Destination destination = (Destination)ctx.lookup(destId);
			producer = session.createProducer(destination);
			psor = psorCode != null ? createProcessorDynamic(psorCode) : createProcessor(psorClass);
			psor.init(inPort.getMetadata(), session, psorProperties);
			connection.start();
		} catch (Exception e) {
			throw new ComponentNotReadyException("Unable to initialize JMS consumer: " + e.getMessage());
		}
	}

	/** Creates processor instance of a class specified by its name.
	 * @param psorClass
	 * @return
	 * @throws ComponentNotReadyException
	 */
	private static DataRecord2JmsMsg createProcessor(String psorClass) throws ComponentNotReadyException {
		DataRecord2JmsMsg psor;
        try {
            psor =  (DataRecord2JmsMsg)Class.forName(psorClass).newInstance();
        }catch (InstantiationException ex){
            throw new ComponentNotReadyException("Can't instantiate msg processor class: "+ex.getMessage());
        }catch (IllegalAccessException ex){
            throw new ComponentNotReadyException("Can't instantiate msg processor class: "+ex.getMessage());
        }catch (ClassNotFoundException ex) {
            throw new ComponentNotReadyException("Can't find specified msg processor class: " + psorClass);
        }
		return psor;
	}

	/**
	 * Creates processor instance of a class specified by its source code.
	 * @param psorCode
	 * @return
	 * @throws ComponentNotReadyException
	 */
	private static DataRecord2JmsMsg createProcessorDynamic(String psorCode) throws ComponentNotReadyException {
		DynamicJavaCode dynCode = new DynamicJavaCode(psorCode);
        dynCode.setCaptureCompilerOutput(true);
        logger.info(" (compiling dynamic source) ");
        // use DynamicJavaCode to instantiate transformation class
        Object transObject = null;
        try {
            transObject = dynCode.instantiate();
        } catch (RuntimeException ex) {
            logger.debug(dynCode.getCompilerOutput());
            logger.debug(dynCode.getSourceCode());
            throw new ComponentNotReadyException("Msg processor code is not compilable.\n" + "Reason: " + ex.getMessage());
        }
        if (transObject instanceof DataRecord2JmsMsg) {
            return (DataRecord2JmsMsg)transObject;
        } else {
            throw new ComponentNotReadyException("Provided msg processor class doesn't implement required interface.");
        }
    }

	/* (non-Javadoc)
	 * @see org.jetel.graph.Node#run()
	 */
	public void run() {
		DataRecord currentRecord = new DataRecord(inPort.getMetadata());
		currentRecord.init();
		DataRecord nextRecord = new DataRecord(inPort.getMetadata());
		nextRecord.init();
		try {
			nextRecord = inPort.readRecord(nextRecord);
			while (runIt && nextRecord != null) {
				// move next to current; read new next
				DataRecord rec = currentRecord;
				currentRecord = nextRecord;
				nextRecord = inPort.readRecord(rec);
				// last message may differ from the other ones
				Message msg = nextRecord != null ? psor.createMsg(currentRecord) : psor.createLastMsg(currentRecord);
				if (msg == null) {
					throw new JetelException(psor.getErrorMsg());
				}
				producer.send(msg);
			}
			// send terminating message
			if (runIt) {
				Message termMsg = psor.createLastMsg(null);
				if (termMsg != null) {
					producer.send(termMsg);
				}
			}
			psor.finished();
			// set node status
			if (runIt) {
				resultMsg = "succeeded";
				resultCode = Node.RESULT_OK;
			} else {
				resultMsg = "stopped";
				resultCode = Node.RESULT_ABORTED;			
			}
		} catch (Exception e) {
			if (runIt) {
				logger.error("Component " + getId() + " terminated by exception", e);
	            resultMsg = e.getMessage();
	            resultCode = Node.RESULT_ERROR;
			} else {
				resultMsg = "stopped";
				resultCode = Node.RESULT_ABORTED;							
			}
		}
        closeConnection();
        broadcastEOF();
	}

	/**
	 * Tries to close JMS connection. It keeps silence regardless of operation success/failure.
	 */
	private void closeConnection() {
		try {
			connection.close();
		} catch (JMSException e) {
			logger.error("Cannot close JMS connection", e);
		}
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.graph.Node#getType()
	 */
	public String getType() {
		return COMPONENT_TYPE;
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.Node#toXML(org.w3c.dom.Element)
	 */
	public void toXML(Element xmlElement) {
		super.toXML(xmlElement);
	
		xmlElement.setAttribute(XML_ID_ATTRIBUTE, getId());
		if (iniCtxFtory != null) {
			xmlElement.setAttribute(XML_INICTX_FACTORY_ATTRIBUTE, iniCtxFtory);
		}
		if (providerUrl != null) {
			xmlElement.setAttribute(XML_PROVIDER_URL_ATTRIBUTE, providerUrl);
		}
		if (conFtory != null) {
			xmlElement.setAttribute(XML_CON_FACTORY_ATTRIBUTE, conFtory);
		}
		if (user != null) {
			xmlElement.setAttribute(XML_USERNAME_ATTRIBUTE, user);
		}
		if (pwd != null) {
			xmlElement.setAttribute(XML_PASSWORD_ATTRIBUTE, pwd);
		}
		if (destId != null) {
			xmlElement.setAttribute(XML_DESTINATION_ATTRIBUTE, destId);
		}
		if (psorCode != null) {
			xmlElement.setAttribute(XML_PSORCODE_ATTRIBUTE, psorCode);
		}
		if (psorClass != null) {
			xmlElement.setAttribute(XML_PSORCLASS_ATTRIBUTE, psorClass);
		}
		// set processor attributes
		for (Enumeration<Object> names = psorProperties.keys(); names.hasMoreElements();) {
			Object name = names.nextElement();
			Object value = psorProperties.get(name);
			xmlElement.setAttribute((String)name, (String)value);
		}
	}
	
	/** Creates an instance according to XML specification.
	 * @param graph
	 * @param xmlElement
	 * @return
	 * @throws XMLConfigurationException
	 */
	public static Node fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException {
		JmsWriter jmsReader = null;
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(xmlElement, graph);
		
		try {
			jmsReader = new JmsWriter(xattribs.getString(XML_ID_ATTRIBUTE),
					xattribs.getString(XML_INICTX_FACTORY_ATTRIBUTE, null),
					xattribs.getString(XML_PROVIDER_URL_ATTRIBUTE, null),
					xattribs.getString(XML_CON_FACTORY_ATTRIBUTE, null),
					xattribs.getString(XML_USERNAME_ATTRIBUTE, null),
					xattribs.getString(XML_PASSWORD_ATTRIBUTE, null),
					xattribs.getString(XML_DESTINATION_ATTRIBUTE, null),
					xattribs.getString(XML_PSORCLASS_ATTRIBUTE, "org.jetel.component.DataRecord2JmsMsgProperties"),
					xattribs.getString(XML_PSORCODE_ATTRIBUTE, null),
					xattribs.attributes2Properties(new String[]{	// all unknown attributes will be passed to the processor
							XML_ID_ATTRIBUTE, XML_INICTX_FACTORY_ATTRIBUTE,
							XML_PROVIDER_URL_ATTRIBUTE, XML_CON_FACTORY_ATTRIBUTE,
							XML_USERNAME_ATTRIBUTE, XML_PASSWORD_ATTRIBUTE,
							XML_DESTINATION_ATTRIBUTE, XML_PSORCLASS_ATTRIBUTE,
							XML_PSORCODE_ATTRIBUTE
					}));
		} catch (Exception ex) {
	           throw new XMLConfigurationException(COMPONENT_TYPE + ":" + xattribs.getString(XML_ID_ATTRIBUTE," unknown ID ") + ":" + ex.getMessage(),ex);
		}
		return jmsReader; 
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.GraphElement#checkConfig(org.jetel.exception.ConfigurationStatus)
	 */
	public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		return status;
	}

}
