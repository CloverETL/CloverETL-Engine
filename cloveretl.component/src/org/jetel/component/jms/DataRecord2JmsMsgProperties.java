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
import javax.jms.TextMessage;

import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.metadata.DataRecordMetadata;

/**
 * Class transforming data records to JMS messages (TextMessage). Message body may be filled with textual
 * representation of one field of the record. You can specify name of such field by component attribute "bodyField". 
 * 
 * All the other fields are saved using string properties in
 * the msg header. Property names are same as field names. Values contain textual representation of field values.
 * Last message has same format as all the others. Terminating message is not used.
 * @author Jan Hadrava, Javlin Consulting (www.javlinconsulting.cz)
 * @since 11/28/06  
 * @see javax.jms.TextMessage
 */
public class DataRecord2JmsMsgProperties extends DataRecord2JmsMsgBase {
	private static final String DEFAULT_BODYFIELD_VALUE = "bodyField";
	private final String PROPNAME_BODYFIELD = "bodyField";
	// index of field to be represented by message body.
	protected int bodyField;

	public void init(DataRecordMetadata metadata, Session session, Properties props)  throws ComponentNotReadyException {
		super.init(metadata, session, props);
		String bodyFieldName = props.getProperty(PROPNAME_BODYFIELD);
		if (bodyFieldName == null) {
			// no bodyField specified - try default
			int bodyFieldDefault = metadata.getFieldPosition(DEFAULT_BODYFIELD_VALUE);
			if (bodyFieldDefault >= 0)
				bodyField = bodyFieldDefault;
			else
				bodyField = -1;
		} else {
			bodyField = metadata.getFieldPosition(bodyFieldName);
			if (bodyField < 0) {
				throw new ComponentNotReadyException("Invalid body field name");
			}
		}
	}

	public Message createMsg(DataRecord record) throws JMSException {
		TextMessage msg = session.createTextMessage();
		int fieldCnt = record.getNumFields();
		if (bodyField > -1) {
			msg.setText(record.getField(bodyField).toString());
		}
		for (int fieldIdx = 0; fieldIdx < fieldCnt; fieldIdx++) {
			if (fieldIdx == bodyField) {
				continue;
			}
			DataField field = record.getField(fieldIdx);
			msg.setStringProperty(field.getMetadata().getLabelOrName(), field.toString());
		}
		msg.setJMSPriority(Message.DEFAULT_PRIORITY);
		return msg;
	}

}
