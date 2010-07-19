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
package org.jetel.graph.dictionary;

import java.io.UnsupportedEncodingException;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;
import java.util.Properties;

import org.jetel.ctl.data.TLType;
import org.jetel.ctl.data.TLTypePrimitive;
import org.jetel.data.Defaults;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.util.string.StringUtils;

/**
 * String dictionary type represents integer element in the dictionary.
 * 
 * @author csochor (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created Jun 21, 2010
 */
public class ByteDictionaryType extends DictionaryType {

	public static final String TYPE_ID = "byte";

	private static final String VALUE_ATTRIBUTE = "value";

	private static final String CHARSET_ATTRIBUTE = "charset";
	
	public ByteDictionaryType() {
		super(TYPE_ID, ReadableByteChannel.class);
	}

	@Override
	public Object init(Object value, Dictionary dictionary) throws ComponentNotReadyException {
		return value;
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
		
		try {
			if (StringUtils.isEmpty(charset)) {
					return  data.getBytes(Defaults.DataFormatter.DEFAULT_CHARSET_ENCODER);
			} else {
				return  data.getBytes(charset);
			}
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException("Error parse byte with charset"+charset, e);
		}
	}

	@Override
	public boolean isFormatPropertiesSupported() {
		return true;
	}

	@Override
	public Properties formatProperties(Object value) {
		if (value != null) {
			final String outCharset = Defaults.DataFormatter.DEFAULT_CHARSET_ENCODER;
			Properties result = new Properties();
			result.setProperty(CHARSET_ATTRIBUTE, outCharset);
			result.setProperty(VALUE_ATTRIBUTE, new String((byte[])value, Charset.forName(outCharset)));
			return result;
		} else {
			return null;
		}
	}
	
	public boolean isValidValue(Object value) {
		return value == null 
		|| value instanceof byte[];
	}

	@Override
	public TLType getTLType() {
		return TLTypePrimitive.BYTEARRAY;
	}

}
