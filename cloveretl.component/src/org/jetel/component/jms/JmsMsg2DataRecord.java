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

import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.TransactionMethod;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;

/**
 * Interface to be implemented by message processors for component JmsReader.
 * @author Jan Hadrava (jan.hadrava@javlinconsulting.cz), Javlin Consulting (www.javlinconsulting.cz)
 * @since 11/28/06  
 * @see org.jetel.component.JmsReader
 *
 */
public interface JmsMsg2DataRecord {
	/**
	 * Initialize the processor.
	 * @param metadata Metadata for the records which will be processed by other methods.
	 * @param props Contains remaining init parameters.
	 * @throws ComponentNotReadyException
	 */
	public void init(DataRecordMetadata metadata, Properties props) throws ComponentNotReadyException;
	
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
	 * May be used to end processing of input JMS messages
	 * @return
	 */
	public boolean endOfInput();
	
	/**
	 * Transform JMS message to data record.
	 * @param msg The message to be transformed
	 * @return Data record; null indicates that the message is not accepted by the processor. 
	 * @throws JMSException
	 */
	public DataRecord extractRecord(Message msg) throws JMSException;
	
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
	public void reset();
}
