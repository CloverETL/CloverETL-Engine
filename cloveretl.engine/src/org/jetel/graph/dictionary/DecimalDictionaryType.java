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

import java.math.BigDecimal;
import java.util.Properties;

import org.jetel.ctl.data.TLType;
import org.jetel.ctl.data.TLTypePrimitive;
import org.jetel.data.primitive.Decimal;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.metadata.DataFieldType;

/**
 * String dictionary type represents decimal element in the dictionary.
 * 
 * @author csochor (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created Jun 21, 2010
 */
public class DecimalDictionaryType extends DictionaryType {

	public static final String TYPE_ID = "decimal";

	private static final String VALUE_ATTRIBUTE = "value";

	/**
	 * Constructor.
	 */
	public DecimalDictionaryType() {
		super(TYPE_ID, BigDecimal.class);
	}

	@Override
	public Object init(Object value, Dictionary dictionary) throws ComponentNotReadyException {
		if (value == null) {
			return null;
		} else if (value instanceof Decimal) {
			return ((Decimal) value).getBigDecimalOutput();
		} else if (value instanceof BigDecimal) {
			return value;
		} else {
			throw new ComponentNotReadyException(dictionary, "Unknown source type for a Decimal dictionary type (" + value + ").");
		}
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
			return new BigDecimal((String) valueString);
		}
	}

	@Override
	public boolean isFormatPropertiesSupported() {
		return true;
	}

	@Override
	public Properties formatProperties(Object value) {
		if (value != null) {
			if (value instanceof Decimal) {
				value = ((Decimal) value).getBigDecimalOutput();
			}
			Properties result = new Properties();
			result.setProperty(VALUE_ATTRIBUTE, ((BigDecimal) value).toString());
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
		return value == null || value instanceof BigDecimal || value instanceof Decimal;
	}

	@Override
	public TLType getTLType() {
		return TLTypePrimitive.DECIMAL;
	}
	
	@Override
	public DataFieldType getFieldType() {
		return DataFieldType.DECIMAL;
	}

}
