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
package org.jetel.component.jmswriter;

import java.util.Properties;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;

import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
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
	 * Transforms data record to JMS message. Is called for all data records except the last one. 
	 * @param record Data record to be transformed to JMS message
	 * @return JMS message
	 * @throws JMSException
	 */
	public Message createMsg(DataRecord record) throws JMSException;
	
	/**
	 * Transforms data record to JMS message. It is called for the last record. After last record it is called
	 * one more time with null parameter and is supposed to return message terminating JMS output. 
	 * @param record Data record to be transformed to JMS message or null.
	 * @return JMS message, null for missing terminating message. 
	 * @throws JMSException
	 */
	public Message createLastMsg(DataRecord record) throws JMSException;
	
	/**
	 * Releases resources.  
	 */
	public void finished();
	
	/**
	 * Nomen omen.
	 * @return
	 */
	public String getErrorMsg();
}
