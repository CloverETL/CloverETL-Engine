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
package org.jetel.component.jms;

import java.util.Properties;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;

import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.TransactionMethod;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;

/**
 * Interface to be implemented by record processors for component JmsWriter.
 * @author Jan Hadrava (jan.hadrava@javlinconsulting.cz), Javlin Consulting (www.javlinconsulting.cz)
 * @since 11/28/06  
 * @see org.jetel.component.JmsWriter
 *
 */
public interface DataRecord2JmsMsg {
	/**
	 * Initialize the processor.
	 * @param metadata Metadata for the records which will be processed by other methods.
	 * @param session JMS session (may be used for creation of JMS messages).
	 * @param props Contains remaining init parameters.
	 * @throws ComponentNotReadyException
	 */
	public void init(DataRecordMetadata metadata, Session session, Properties props) throws ComponentNotReadyException;

    /**
     * This is also initialization method, which is invoked before each separate graph run.
     * Contrary the init() procedure here should be allocated only resources for this graph run.
     * All here allocated resources should be released in #postExecute() method.
     * 
     * @throws ComponentNotReadyException some of the required resource is not available or other
     * precondition is not accomplish
     */
    public void preExecute() throws ComponentNotReadyException; 

	/**
	 * Releases resources. 
     * 
     * @param transactionMethod type of transaction finalize method; was the graph/phase run successful?
     * @throws ComponentNotReadyException
     */
    public void postExecute(TransactionMethod transactionMethod) throws ComponentNotReadyException;
	
	/**
	 * Transforms data record to JMS message. Is called for all data records. 
	 * @param record Data record to be transformed to JMS message
	 * @return JMS message
	 * @throws JMSException
	 */
	public Message createMsg(DataRecord record) throws JMSException;
	
	/**
	 * This method isn't called explicitly since 2.8. Use {@link #createLastMsg()} instead.
	 * @param 
	 * @return  
	 * @throws JMSException
	 * @deprecated since 2.8 - parameter has no meaning any longer - use {@link #createLastMsg()} instead
	 */
	@Deprecated
	public Message createLastMsg(DataRecord record) throws JMSException;
	
	/**
	 * This method is called after last record and is supposed to return message terminating JMS output.
	 * If it returns null, no terminating message is sent.
	 * @since 2.8
	 * @return JMS message or null.
	 * @throws JMSException
	 */
	public Message createLastMsg() throws JMSException;

	/**
	 * Use postExecute method.
	 */
	@Deprecated
	public void finished();
	
	/**
	 * Nomen omen.
	 * @return
	 */
	public String getErrorMsg();

    /**
     * Method which passes graph instance into processor implementation. 
     * It's called just before {@link #init(DataRecordMetadata, Properties)} 
     * @param graph
     */
    public void setGraph(TransformationGraph graph);
	
    public TransformationGraph getGraph();
	
	/**
	 * Use preExecute method. 
	 */
    @Deprecated
	public void reset() throws ComponentNotReadyException;
}
