/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2002-04  David Pavlis <david_pavlis@hotmail.com>
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
package org.jetel.graph.dictionary;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.Properties;

import org.jetel.data.Defaults;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.util.string.StringUtils;

/**
 * @author Martin Zatopek (martin.zatopek@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created Jul 18, 2008
 */
public class ReadableChannelDictionaryType extends DictionaryType {

	public static final String TYPE_ID = "readable.channel";

	private static final String VALUE_ATTRIBUTE = "value";

	private static final String CHARSET_ATTRIBUTE = "charset";
	
	public ReadableChannelDictionaryType() {
		super(TYPE_ID, ReadableByteChannel.class);
	}

	@Override
	public Object init(Object value, Dictionary dictionary) throws ComponentNotReadyException {
		if ( value == null ) {
			return null;
		} else if (value instanceof ReadableByteChannel) {
			return value;
		} else if (value instanceof InputStream) {
			return Channels.newChannel((InputStream) value);
		} else if (value instanceof StringAsReadableChannel) {
			try {
				return ((StringAsReadableChannel) value).getReadableChannel();
			} catch (UnsupportedEncodingException e) {
				throw new ComponentNotReadyException(dictionary, e);
			}
		} else {
			throw new ComponentNotReadyException(dictionary, "Unknown source type for a Readable channel dictionary type (" + value + ").");
		}
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.graph.dictionary.DictionaryType#isParsePropertiesSupported()
	 */
	@Override
	public boolean isParsePropertiesSupported() {
		return true;
	}
	
	@Override
	public Object parseProperties(Properties properties) throws AttributeNotFoundException {
		String data = properties.getProperty(VALUE_ATTRIBUTE);
		String charset = properties.getProperty(CHARSET_ATTRIBUTE);
		if (data == null) {
			throw new AttributeNotFoundException(VALUE_ATTRIBUTE, "Attribute " + VALUE_ATTRIBUTE + " is not available in a Readable channel dictionary element.");
		}
		return new StringAsReadableChannel(data, charset);
	}

	public boolean isValidValue(Object value) {
		return value == null 
			|| value instanceof ReadableByteChannel
			|| value instanceof InputStream
			|| value instanceof StringAsReadableChannel;
	}

	public static class StringAsReadableChannel {
		public String data;
		
		public String charset;
		
		public StringAsReadableChannel(String data, String charset) {
			this.data = data;
			this.charset = charset;
		}
		
		public ReadableByteChannel getReadableChannel() throws UnsupportedEncodingException {
			byte[] byteArray;
			if (StringUtils.isEmpty(charset)) {
				byteArray = data.getBytes(Defaults.DataFormatter.DEFAULT_CHARSET_ENCODER);
			} else {
				byteArray = data.getBytes(charset);
			}
			
			return Channels.newChannel(new ByteArrayInputStream(byteArray));
		}
	}
	
}
