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
package org.jetel.component;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Properties;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.component.jms.JmsMsg2DataRecord;
import org.jetel.connection.jms.JmsConnection;
import org.jetel.data.DataRecord;
import org.jetel.database.IConnection;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.Node;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.AutoFilling;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.property.RefResFlag;
import org.w3c.dom.Element;


/**
 *  <h3>JmsReader Component</h3>
 *
 * <table border="1">
 *  <th>Component:</th>
 * <tr><td><h4><i>Name:</i></h4></td>
 * <td>JmsReader</td></tr>
 * <tr><td><h4><i>Category:</i></h4></td>
 * <td></td></tr>
 * <tr><td><h4><i>Description:</i></h4></td>
 * <td>Receives JMS messages and transforms them to data records using user-specified transformation class
 * (so-called processor). The processor is supposed to implement interface <code>JmsMsg2DataRecord</code>.
 * The processor may be specified either by class name or by Java code.
 * </td></tr>
 * <tr><td><h4><i>Inputs:</i></h4></td>
 * <td></td></tr>
 * <tr><td><h4><i>Outputs:</i></h4></td>
 * <td>One output port to resulting data records.</td></tr>
 * <tr><td><h4><i>Comment:</i></h4></td>
 * <td></td></tr>
 * </table>
 *  <br>
 *  <table border="1">
 *  <th>XML attributes:</th>
 *  <tr><td><b>type</b></td><td>"JMS_READER"</td></tr>
 *  <tr><td><b>id</b></td><td>component identification</td>
 *  <tr><td><b>connection</b></td><td>JMS connection ID</td>
 *  <tr><td><b>selector</b></td><td>JMS selector specifying messages to be processed</td>
 *  <tr><td><b>processorCode</b></td><td>Inline Java code defining processor class</td>
 *  <tr><td><b>processorClass</b></td><td>Name of processor class</td>
 *  <tr><td><b>processorURL</b></td><td>path to the file with processor code</td></tr>
 *  <tr><td><b>charset</b><br><i>optional</i></td><td>encoding of extern source</td></tr>
 *  <tr><td><b>maxMsgCount</b></td><td>Maximal number of messages to be processed.
 *  0 means there's no constraint on count of messages.</td>
 *  <tr><td><b>timeout</b></td><td>Maximal time to await next message. 0 means forever</td>
 *  </tr>
 *  </table>
 *  
 *  When both attributes <b>maxMsgCount</b> and <b>timeout</b> are set to 0, node keeps awaiting for new messages. 
 *  Also Phase, which this node is embedded in, never stops.
 *
 * @author Jan Hadrava (jan.hadrava@javlinconsulting.cz), Javlin Consulting (www.javlinconsulting.cz)
 * @since 09/15/06  
 * @see         org.jetel.data.parser.FixLenDataParser
 */
public class JmsReader extends Node {
	public final static String COMPONENT_TYPE = "JMS_READER";

	static Log logger = LogFactory.getLog(JmsReader.class);

	public static final String XML_CONNECTION_ATTRIBUTE = "connection";
	private static final String XML_SELECTOR_ATTRIBUTE = "selector";
	private static final String XML_PSORCODE_ATTRIBUTE = "processorCode";
	private static final String XML_PSORCLASS_ATTRIBUTE = "processorClass";
	private static final String XML_PSORURL_ATTRIBUTE = "processorURL";
	private static final String XML_CHARSET_ATTRIBUTE = "charset";
	private static final String XML_MAXMSGCNT_ATTRIBUTE = "maxMsgCount";
	private static final String XML_TIMEOUT_ATTRIBUTE = "timeout";

	// component attributes
	private String conId;
	private String selector;
	private String psorClass;
	private String psorCode;
	private String psorURL = null;
	private String charset = null;
	private int maxMsgCount;
	private int timeout;
	private Properties psorProperties;

	private JmsConnection connection;
	private MessageConsumer consumer;	
	private JmsMsg2DataRecord psor;

	private int msgCounter = 0;
	private boolean exhausted = false;
	private Message lastMsg = null;
	
    private AutoFilling autoFilling = new AutoFilling();
	
	/** Sole ctor.
	 * @param id Component ID
	 * @param conId JMS connection ID
	 * @param selector JMS message selector
	 * @param psorClass Processor class
	 * @param psorCode Inline processor definition
	 * @param maxMsgCount Max msg count
	 * @param timeout Timeout
	 * @param psorProperties Properties to be passed to msg processor.
	 */
	public JmsReader(String id, String conId, String selector, String psorClass, 
			String psorCode, String psorURL, int maxMsgCount, int timeout, Properties psorProperties) {
		super(id);
		this.conId = conId;
		this.selector = selector;
		this.psorClass = psorClass;
		this.psorCode = psorCode;
		this.psorURL = psorURL;
		this.maxMsgCount = maxMsgCount;
		this.timeout = timeout;
		this.psorProperties = psorProperties;
	}

	public JmsReader(String id, String conId, String selector, JmsMsg2DataRecord psor,
			int maxMsgCount, int timeout, Properties psorProperties) {
		super(id);
		this.conId = conId;
		this.selector = selector;
		this.psor = psor;
		this.maxMsgCount = maxMsgCount;
		this.timeout = timeout;
		this.psorProperties = psorProperties;
	}

	@Override
	public void init() throws ComponentNotReadyException {
        if(isInitialized()) return;
		super.init();
		
		if (psorClass == null && psorCode == null && psorURL == null) {
			psorClass = "org.jetel.component.jms.JmsMsg2DataRecordProperties";
		}
		IConnection c = getGraph().getConnection(conId);
		if (c == null || !(c instanceof JmsConnection)) {
			throw new ComponentNotReadyException("Specified connection '" + conId + "' doesn't seem to be a JMS connection");
		}
		connection = (JmsConnection)c;
		
		if (psor == null) {
	    	psor = getTransformFactory().createTransform();
		}		
		psor.setNode(this);
		psor.init(getOutputPort(0).getMetadata(), psorProperties);
		
		List<DataRecordMetadata> lDataRecordMetadata;
    	if ((lDataRecordMetadata = getOutMetadata()).size() > 0) {
    		autoFilling.addAutoFillingFields(lDataRecordMetadata.get(0));
    	}
		autoFilling.setFilename(conId + ": " + (psorClass != null ? psorClass : psorCode));
	}

	private TransformFactory<JmsMsg2DataRecord> getTransformFactory() {
    	TransformFactory<JmsMsg2DataRecord> transformFactory = TransformFactory.createTransformFactory(JmsMsg2DataRecord.class);
    	transformFactory.setTransform(psorCode);
    	transformFactory.setTransformClass(psorClass);
    	transformFactory.setTransformUrl(psorURL);
    	transformFactory.setCharset(charset);
    	transformFactory.setComponent(this);
    	return transformFactory;
	}
	
	@Override
	public void preExecute() throws ComponentNotReadyException {
		super.preExecute();
		psor.preExecute();

		if (firstRun()) {//a phase-dependent part of initialization
    		//all necessary elements have been initialized in init()
		} else {
			psor.reset();
	    	msgCounter = 0;
	    	exhausted = false;
	    	lastMsg = null;
			autoFilling.reset();
		}
		try {
			consumer = connection.createConsumer(selector);
		} catch (Exception e) {
			throw new ComponentNotReadyException("Unable to initialize JMS consumer", e);
		}
	}

	@Override
	public void postExecute() throws ComponentNotReadyException {
		super.postExecute();
        psor.postExecute();
        psor.finished();
		closeConnection();
	}

	@Override
	synchronized public void reset() throws ComponentNotReadyException {
    	super.reset();
    }

	@Override
	public synchronized void free() {
		super.free();
		closeConnection();
	}

	/**
	 * Receives next JMS message. 
	 * @return null when processor doesn't require more messages or reader constraints forbid retrieving
	 * more data, retrieved message otherwise.
	 * @throws JMSException
	 */
	private Message getMsg() throws JMSException {
		if (psor.endOfInput()) {
			exhausted = true;
		}
		if (maxMsgCount > 0 && msgCounter == maxMsgCount) {
			exhausted = true;
		}
		if (exhausted) {
			return null;
		}

		lastMsg = consumer.receive(timeout);

		if (lastMsg == null) {	// timeout
			exhausted = true;
		} else {
			msgCounter++;
		}
		return lastMsg;
	}
		
	@Override
	public Result execute() throws Exception {
		// TODO krivanekm: Does the interruptor really work? 
		Interruptor interruptor = new Interruptor();
		registerChildThread(interruptor); //register interrupter as a child thread of this component
		interruptor.start();	// run thread taking care about interrupting blocking msg receive calls
		
		try {
			for (Message msg = getMsg(); msg != null; msg = getMsg()) {
				DataRecord rec = psor.extractRecord(msg);
				if (rec == null) {
					logger.debug("Unable to extract data from JMS message; message skipped");
					continue;
				}
		        autoFilling.setAutoFillingFields(rec);
				writeRecordBroadcast(rec);
			}
		} catch (javax.jms.JMSException e) {
			if (e.getCause() instanceof InterruptedException) {
				throw (InterruptedException)e.getCause();
			} else {
				throw e;
			}
		} catch (Exception e) {
			throw e;
		} finally {
	        broadcastEOF();
		}
		Result r = runIt ? Result.FINISHED_OK : Result.ABORTED;
		interruptor.finished(); // CLO-2475: terminate the interruptor separately, do not use JmsReader.runIt to stop it
		return r;
	}

	/**
	 * Tries to close JMS connection. It keeps silence regardless of operation success/failure.
	 */
	synchronized private void closeConnection() {
		try {
			if (consumer != null) consumer.close();
		} catch (JMSException e) {
			// ignore it, the connection is probably already closed
		}
	}
	
	@Override
	public String getType() {
		return COMPONENT_TYPE;
	}

	/** Creates an instance according to XML specification.
	 * @param graph
	 * @param xmlElement
	 * @return
	 * @throws XMLConfigurationException
	 * @throws AttributeNotFoundException 
	 */
	public static Node fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException, AttributeNotFoundException {
		JmsReader jmsReader = null;
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(xmlElement, graph);
		
		jmsReader = new JmsReader(xattribs.getString(XML_ID_ATTRIBUTE),
				xattribs.getString(XML_CONNECTION_ATTRIBUTE, null),
				xattribs.getString(XML_SELECTOR_ATTRIBUTE, null),
				xattribs.getString(XML_PSORCLASS_ATTRIBUTE, null),
				xattribs.getString(XML_PSORCODE_ATTRIBUTE, null),
				xattribs.getStringEx(XML_PSORURL_ATTRIBUTE, null, RefResFlag.URL),
				xattribs.getInteger(XML_MAXMSGCNT_ATTRIBUTE, 0),
				xattribs.getInteger(XML_TIMEOUT_ATTRIBUTE, 0),
				xattribs.attributes2Properties(new String[]{	// all unknown attributes will be passed to the processor 
						XML_ID_ATTRIBUTE, XML_CONNECTION_ATTRIBUTE, XML_SELECTOR_ATTRIBUTE,
						XML_PSORCLASS_ATTRIBUTE, XML_PSORCODE_ATTRIBUTE,
						XML_MAXMSGCNT_ATTRIBUTE, XML_TIMEOUT_ATTRIBUTE
				}));
		jmsReader.setCharset(xattribs.getString(XML_CHARSET_ATTRIBUTE, null));
		return jmsReader; 
	}

	@Override
	public ConfigurationStatus checkConfig(ConfigurationStatus status) {
        super.checkConfig(status);
        
        if(!checkInputPorts(status, 0, 0)
        		|| !checkOutputPorts(status, 1, Integer.MAX_VALUE)) {
        	return status;
        }
        
        if (charset != null && !Charset.isSupported(charset)) {
        	status.add(new ConfigurationProblem(
            		"Charset "+charset+" not supported!", 
            		ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL));
        }

        //check transformation
		if (psor == null) {
			TransformFactory<JmsMsg2DataRecord> factory = getTransformFactory();
			if (factory.isTransformSpecified()) {
				getTransformFactory().checkConfig(status);
			}
		}
        
        return status;
	}

	/**
	 * Used to interrupt JMS reader waiting for new JMS message. 
	 */
	private class Interruptor extends Thread {
		private static final int sleepInterval = 400;
		
		private volatile boolean runIt = false;
		
		public Interruptor() {
			super(Thread.currentThread().getName() + ".Interruptor");			
		}
		
		/**
		 * @see java.lan.Thread#run()
		 */
		@Override
		public void run() {
			while (JmsReader.this.runIt && Interruptor.this.runIt) {
				try {
					sleep(sleepInterval);
				} catch (InterruptedException e) {
					break;
				}
			}
			//closeConnection();
		}
		
		public void finished() {
			Interruptor.this.runIt = false;
		}
		
	}

	public String getCharset() {
		return charset;
	}

	public void setCharset(String charset) {
		this.charset = charset;
	}

}
