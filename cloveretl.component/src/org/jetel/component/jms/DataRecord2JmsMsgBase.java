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
package org.jetel.component.jms;

import java.util.Properties;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;

import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.metadata.DataRecordMetadata;

/**
 * Simple partial implementation of DataRecord2JmsMsg interface. Supposed to be extended by full implementations.
 * @author Jan Hadrava (jan.hadrava@javlinconsulting.cz), Javlin Consulting (www.javlinconsulting.cz)
 * @since 11/28/06  
 */
public abstract class DataRecord2JmsMsgBase implements DataRecord2JmsMsg {
	protected String errMsg;
	protected DataRecordMetadata metadata; 
	protected Session session;

	/* (non-Javadoc)
	 * @see org.jetel.component.DataRecord2JmsMsg#init(org.jetel.metadata.DataRecordMetadata, javax.jms.Session, java.util.Properties)
	 */
	public void init(DataRecordMetadata metadata, Session session, Properties props) throws ComponentNotReadyException {
		errMsg = null;
		this.metadata = metadata;
		this.session = session;
	}

	/* (non-Javadoc)
	 * @see org.jetel.component.DataRecord2JmsMsg#finished()
	 */
	public void finished() {
	}

	/* (non-Javadoc)
	 * @see org.jetel.component.DataRecord2JmsMsg#createLastMsg(org.jetel.data.DataRecord)
	 */
	public Message createLastMsg(DataRecord record) throws JMSException {
		return record == null ? null : createMsg(record);
	}

	/* (non-Javadoc)
	 * @see org.jetel.component.DataRecord2JmsMsg#getErrorMsg()
	 */
	public String getErrorMsg() {
		return errMsg;
	}

	/**
	 * Sets error message. Suppposed to be used in subclasses.
	 * @param errMsg
	 */
	protected void setErrorMsg(String errMsg) {
		this.errMsg = errMsg;
	}

	public void reset() throws ComponentNotReadyException {
		setErrorMsg(null);
	}
		
}
