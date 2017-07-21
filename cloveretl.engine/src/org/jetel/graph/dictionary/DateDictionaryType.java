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

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import org.jetel.ctl.data.TLType;
import org.jetel.ctl.data.TLTypePrimitive;
import org.jetel.data.Defaults;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.metadata.DataFieldType;

/**
 * String dictionary type represents date element in the dictionary.
 * 
 * @author csochor (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created Jun 21, 2010
 */
public class DateDictionaryType extends DictionaryType {

	public static final String TYPE_ID = "date";

	private static final String VALUE_ATTRIBUTE = "value";

	private static final DateFormat format = new SimpleDateFormat(Defaults.DEFAULT_DATETIME_FORMAT);

	/**
	 * Constructor.
	 */
	public DateDictionaryType() {
		super(TYPE_ID, Date.class);
	}

	@Override
	public Object init(Object value, Dictionary dictionary) throws ComponentNotReadyException {
		return value;
	}

	@Override
	public boolean isParsePropertiesSupported() {
		return true;
	}

	@Override
	public Object parseProperties(Properties properties) throws AttributeNotFoundException {
		String valueString = properties.getProperty(VALUE_ATTRIBUTE);
		if (valueString == null) {
			return null;
		} else {
			try {
				return format.parse((String) valueString);
			} catch (ParseException e) {
				throw new RuntimeException("Error parse date '" + valueString + "'", e);
			}
		}
	}

	@Override
	public boolean isFormatPropertiesSupported() {
		return true;
	}

	@Override
	public Properties formatProperties(Object value) {
		if (value != null) {
			Properties result = new Properties();
			result.setProperty(VALUE_ATTRIBUTE, format.format((Date)value));
			return result;
		} else {
			return null;
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.jetel.graph.dictionary.IDictionaryType#isValidValue(java.lang.Object)
	 */
	@Override
	public boolean isValidValue(Object value) {
		return value == null
				|| value instanceof Date;
	}

	@Override
	public TLType getTLType() {
		return TLTypePrimitive.DATETIME;
	}
	
	@Override
	public DataFieldType getFieldType() {
		return DataFieldType.DATE;
	}

}
