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

import java.util.Properties;

import org.jetel.ctl.data.TLType;
import org.jetel.ctl.data.TLTypePrimitive;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.metadata.DataFieldType;

/**
 * String dictionary type represents boolean element in the dictionary.
 * 
 * @author csochor (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created Jun 21, 2010
 */
public class BooleanDictionaryType extends DictionaryType {

	public static final String TYPE_ID = "boolean";

	private static final String VALUE_ATTRIBUTE = "value";

	/**
	 * Constructor.
	 */
	public BooleanDictionaryType() {
		super(TYPE_ID, Boolean.class);
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
			return Boolean.valueOf((String) valueString);
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
			result.setProperty(VALUE_ATTRIBUTE, ((Boolean) value).toString());
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
				|| value instanceof Boolean;
	}

	@Override
	public TLType getTLType() {
		return TLTypePrimitive.BOOLEAN;
	}

	@Override
	public DataFieldType getFieldType() {
		return DataFieldType.BOOLEAN;
	}
	
}
