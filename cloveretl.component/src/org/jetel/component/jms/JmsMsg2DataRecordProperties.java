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

import java.util.Enumeration;
import java.util.Properties;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;

import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.metadata.DataRecordMetadata;

/**
 * Class transforming JMS messages (TextMessage) to data records. Message body may be filled with textual
 * representation of one field of the record. All the other fields are saved using string properties in
 * the msg header. Property names are same as respective field names. Values contain textual representation of field values.
 * Last message has same format as all the others. Terminating message is not supported.
 * @author Jan Hadrava, Javlin Consulting (www.javlinconsulting.cz)
 * @since 11/28/06  
 * @see javax.jms.TextMessage
 */
public class JmsMsg2DataRecordProperties extends JmsMsg2DataRecordBase {
	private final String PROPNAME_BODYFIELD = "bodyField";
	
	// index of field to be represented by message body.
	protected int bodyField;

	protected DataRecord record;
	
	/* (non-Javadoc)
	 * @see org.jetel.component.JmsMsg2DataRecordBase#init(org.jetel.metadata.DataRecordMetadata, java.util.Properties)
	 */
	public void init(DataRecordMetadata metadata, Properties props) throws ComponentNotReadyException {
		super.init(metadata, props);
		String bodyFieldName = props.getProperty(PROPNAME_BODYFIELD);
		if (bodyFieldName == null) {
			bodyField = -1;
		} else {
			bodyField = metadata.getFieldPosition(bodyFieldName);
			if (bodyField < 0) {
				throw new ComponentNotReadyException("Invalid field name");
			}
		}

		record = new DataRecord(metadata);
		record.init();
	}

	/* (non-Javadoc)
	 * @see org.jetel.component.JmsMsg2DataRecord#extractRecord(javax.jms.Message)
	 */
	public DataRecord extractRecord(Message msg) throws JMSException {
		record.reset();
		for (Enumeration names = msg.getPropertyNames(); names.hasMoreElements();) {
			String name = (String)names.nextElement();
			try {
				String value = msg.getStringProperty(name);
				if (value == null) {	// value is not accessible
					continue;
				}
				record.getField(name).fromString(value);
			} catch (IndexOutOfBoundsException e) {
				// field with given name doesn't exist
				continue;
			}
		}
		if (bodyField != -1) {
			if (!(msg instanceof TextMessage)) {
				throw new JMSException("Incoming message is supposed to be TextMessage but it isn't");
			}
			record.getField(bodyField).fromString(((TextMessage)msg).getText());
		}
		return record;
	}

	/*
	 * (non-Javadoc)
	 * @see org.jetel.component.jms.JmsMsg2DataRecord#reset()
	 */
	public void reset() {
		super.reset();
		// no operations needed
	}

}
