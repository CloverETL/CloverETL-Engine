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

import java.util.Properties;

import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.ComponentNotReadyException;

/**
 * String dictionary type represents string-like element in the dictionary.
 * 
 * @author Martin Zatopek (martin.zatopek@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created 10.3.2008
 */
public class StringDictionaryType extends DictionaryType {

	public static final String TYPE_ID = "string";

	private static final String VALUE_ATTRIBUTE = "value";
	
	/**
	 * Constructor.
	 */
	public StringDictionaryType() {
		super(TYPE_ID, String.class);
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.graph.dictionary.DictionaryType#init(java.lang.Object, org.jetel.graph.dictionary.Dictionary)
	 */
	@Override
	public Object init(Object value, Dictionary dictionary) throws ComponentNotReadyException {
		if( value == null ){
			return null;
		} else {
			return value.toString();
		}
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.graph.dictionary.DictionaryType#isParsePropertiesSupported()
	 */
	@Override
	public boolean isParsePropertiesSupported() {
		return true;
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.graph.dictionary.DictionaryType#getValue(java.util.Properties)
	 */
	@Override
	public Object parseProperties(Properties properties) throws AttributeNotFoundException {
		return properties.getProperty(VALUE_ATTRIBUTE);
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.dictionary.DictionaryType#isFormatPropertiesSupported()
	 */
	@Override
	public boolean isFormatPropertiesSupported() {
		return true;
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.graph.dictionary.DictionaryType#formatProperties(java.lang.Object)
	 */
	@Override
	public Properties formatProperties(Object value) {
		if (value != null) {
			Properties result = new Properties();
			result.setProperty(VALUE_ATTRIBUTE, (String) value);
			return result;
		} else {
			return null;
		}
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.graph.dictionary.IDictionaryType#isValidValue(java.lang.Object)
	 */
	public boolean isValidValue(Object value) {
		return value == null 
				|| value instanceof String
				|| value instanceof StringBuilder
				|| value instanceof StringBuffer;
	}

}
