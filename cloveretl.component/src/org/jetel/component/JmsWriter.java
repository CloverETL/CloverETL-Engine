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
import org.jetel.component.jms.DataRecord2JmsMsg;
import org.jetel.connection.JmsConnection;
import org.jetel.data.DataRecord;
import org.jetel.database.IConnection;
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
*  <tr><td><b>connection</b></td><td>JMS connection ID</td>
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

	private static final String XML_CONNECTION_ATTRIBUTE = "connection";
	private static final String XML_PSORCODE_ATTRIBUTE = "processorCode";
	private static final String XML_PSORCLASS_ATTRIBUTE = "processorClass";

	// component attributes
	private String conId;
	private String psorClass;
	private String psorCode;
	private Properties psorProperties;

	private InputPort inPort;
	
	private JmsConnection connection;
	private MessageProducer producer;	
	private DataRecord2JmsMsg psor;

	/** Sole ctor.
	 * @param id Component ID
	 * @param conId JMS connection ID
	 * @param psorClass Processor class
	 * @param psorCode Inline processor definition
	 * @param psorProperties Properties to be passed to data processor.
	 */
	public JmsWriter(String id, String conId, String psorClass, String psorCode,
			Properties psorProperties) {
		super(id);
		this.conId = conId;
		this.psorClass = psorClass;
		this.psorCode = psorCode;
		this.psorProperties = psorProperties;
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.GraphElement#init()
	 */
	public void init() throws ComponentNotReadyException {
		super.init();
		if (getInPorts().size() != 1) {
			throw new ComponentNotReadyException("Exactly one input port is required");
		}
		if (psorClass == null && psorCode == null) {
			throw new ComponentNotReadyException("Message processor not specified");
		}
		IConnection c = getGraph().getConnection(conId);
		if (c == null || !(c instanceof JmsConnection)) {
			throw new ComponentNotReadyException("Specified connection '" + conId + "' doesn't seem to be a JMS connection");
		}

		connection = (JmsConnection)c;
		inPort = getInputPort(0);
		psor = psorCode != null ? createProcessorDynamic(psorCode) : createProcessor(psorClass);

		try {
			connection.init();
			producer = connection.createProducer();
		} catch (Exception e) {
			throw new ComponentNotReadyException("Unable to initialize JMS consumer: " + e.getMessage());
		}
		psor.init(inPort.getMetadata(), connection.getSession(), psorProperties);
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
		connection.connect();
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
	}

	/**
	 * Tries to close JMS connection. It keeps silence regardless of operation success/failure.
	 */
	private void closeConnection() {
		try {
			producer.close();
		} catch (JMSException e) {
			// ignore it, the connection is probably already closed
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
		if (conId != null) {
			xmlElement.setAttribute(XML_CONNECTION_ATTRIBUTE, conId);
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
					xattribs.getString(XML_CONNECTION_ATTRIBUTE, null),
					xattribs.getString(XML_PSORCLASS_ATTRIBUTE, "org.jetel.component.jmswriter.DataRecord2JmsMsgProperties"),
					xattribs.getString(XML_PSORCODE_ATTRIBUTE, null),
					xattribs.attributes2Properties(new String[]{	// all unknown attributes will be passed to the processor
							XML_ID_ATTRIBUTE, XML_CONNECTION_ATTRIBUTE,
							XML_PSORCLASS_ATTRIBUTE, XML_PSORCODE_ATTRIBUTE
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
