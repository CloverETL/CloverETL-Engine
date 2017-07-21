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

import org.jetel.component.AbstractDataTransform;
import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.CloverPublicAPI;

/**
 * Simple partial implementation of DataRecord2JmsMsg interface. Supposed to be extended by full implementations.
 * 
 * @author Jan Hadrava (jan.hadrava@javlinconsulting.cz), Javlin Consulting (www.javlinconsulting.cz)
 * @since 11/28/06
 */
@CloverPublicAPI
public abstract class DataRecord2JmsMsgBase extends AbstractDataTransform implements DataRecord2JmsMsg {

	protected String errMsg;
	protected DataRecordMetadata metadata;
	protected Session session;

	@Override
	public void init(DataRecordMetadata metadata, Session session, Properties props) throws ComponentNotReadyException {
		errMsg = null;
		this.metadata = metadata;
		this.session = session;
	}

	@Override
	public void preExecute(Session session) throws ComponentNotReadyException {
		super.preExecute();
		this.session = session;
	}

	@Override
	@Deprecated
	public Message createLastMsg(DataRecord record) throws JMSException {
		return createLastMsg();
	}

	@Override
	public Message createLastMsg() throws JMSException {
		return null;
	}

	@Override
	public String getErrorMsg() {
		return errMsg;
	}

	protected void setErrorMsg(String errMsg) {
		this.errMsg = errMsg;
	}

	/**
	 * @deprecated Use {@link #preExecute()} method.
	 */
	@Deprecated
	@Override
	public void reset() {
		setErrorMsg(null);
	}

}
