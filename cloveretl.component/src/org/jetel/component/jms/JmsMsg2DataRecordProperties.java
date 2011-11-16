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

import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.Enumeration;
import java.util.Properties;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;

import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.bytes.CloverBuffer;

/**
 * Class transforming JMS messages (TextMessage or BytesMessage) to data records. One field of the record may be filled by body of the message. 
 * You can specify name of this field by component attribute "bodyField". 
 * If message is of type BytesMessage, use attribute "msgCharset" to specify encoding of characters in the message.  
 * 
 * All the other fields are saved using string properties in
 * the msg header. Property names are same as respective field names. Values contain textual representation of field values.
 * Last message has same format as all the others. Terminating message is not supported.
 * @author Jan Hadrava, Javlin Consulting (www.javlinconsulting.cz)
 * @since 11/28/06  
 * @see javax.jms.TextMessage
 */
public class JmsMsg2DataRecordProperties extends JmsMsg2DataRecordBase {
	private static final String DEFAULT_BODYFIELD_VALUE = "bodyField";
	private final String PROPNAME_BODYFIELD = "bodyField";
	private final String PROPNAME_CHARSET = "msgCharset";
	
	// index of field to be represented by message body.
	protected int bodyField;

	protected DataRecord record;

	/**
	 * Used for conversion to character data.
	 */
	protected CharsetDecoder decoder = null;
	protected Charset usedByteMsgCharset = null;
	
	public void init(DataRecordMetadata metadata, Properties props) throws ComponentNotReadyException {
		super.init(metadata, props);
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
				throw new ComponentNotReadyException("Invalid bodyField name");
			}
		}
		
		String byteMsgCharset = props.getProperty(PROPNAME_CHARSET);
		if (byteMsgCharset == null)  
			usedByteMsgCharset = Charset.forName(Defaults.DataParser.DEFAULT_CHARSET_DECODER); 
		else
			usedByteMsgCharset = Charset.forName(byteMsgCharset);
		decoder = usedByteMsgCharset.newDecoder();

		record = new DataRecord(metadata);
		record.init();
	}

	public DataRecord extractRecord(Message msg) throws JMSException {
		record.reset();
		for (Enumeration<?> names = msg.getPropertyNames(); names.hasMoreElements();) {
			String name = (String)names.nextElement();
			try {
				String value = msg.getStringProperty(name);
				if (value == null) {	// value is not accessible
					continue;
				}
				record.getFieldByLabel(name).fromString(value);
			} catch (IndexOutOfBoundsException e) {
				// field with given name doesn't exist
				continue;
			}
		}
		if (bodyField != -1) {
			if (msg instanceof TextMessage) {
				record.getField(bodyField).fromString(((TextMessage)msg).getText());
			} else if (msg instanceof BytesMessage) {
				//BytesMessage bmsg = session.createBytesMessage();
				//bmsg.writeBytes( record.getField(bodyField).toString().getBytes() );
				
				BytesMessage bmsg = (BytesMessage)msg;
				byte[] buffer = new byte[(int)bmsg.getBodyLength()];
				int readBytes = bmsg.readBytes(buffer);
				if (readBytes != (int)bmsg.getBodyLength())
					throw new JMSException("Difference in read byte array. Expected lenght:"+bmsg.getBodyLength()+" read:"+readBytes);
				CloverBuffer dataBuffer = CloverBuffer.wrap(buffer);
				try {
					record.getField(bodyField).fromByteBuffer(dataBuffer, decoder);
				} catch (CharacterCodingException e) { // convert it to bad-format exception
					throw new BadDataFormatException( "Invalid encoding of characters in message body. Used charset:"+usedByteMsgCharset);
				}
			} else
				throw new JMSException("Incoming message is supposed to be TextMessage or BytesMessage, but it is "+msg.getClass().getCanonicalName());

		}
		return record;
	}

}
