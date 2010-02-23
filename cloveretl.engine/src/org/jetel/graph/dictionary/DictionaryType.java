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
 * @author Martin Zatopek (martin.zatopek@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created Jul 17, 2008
 */
public abstract class DictionaryType implements IDictionaryType {

	private String typeId;
	
	private Class<?> valueClass;
	
	public DictionaryType(String typeId, Class<?> valueClass) {
		this.typeId = typeId;
		this.valueClass = valueClass;
	}
	
	public String getTypeId() {
		return typeId;
	}
	
	public Class<?> getValueClass() {
		return valueClass;
	}
	
	public Object init(Object value, Dictionary dictionary) throws ComponentNotReadyException {
		if (value != null && !valueClass.isInstance(value)) {
			throw new ComponentNotReadyException(dictionary, "Unknown source type for '" + getTypeId() + "' dictionary element (" + value + ").");
		}
		return value;
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.dictionary.IDictionaryType#isParsePropertiesSupported()
	 */
	public boolean isParsePropertiesSupported() {
		return false;
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.dictionary.IDictionaryType#parseProperties(java.util.Properties)
	 */
	public Object parseProperties(Properties properties) throws AttributeNotFoundException {
		throw new UnsupportedOperationException();
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.dictionary.IDictionaryType#isFormatPropertiesSupported()
	 */
	public boolean isFormatPropertiesSupported() {
		return false;
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.graph.dictionary.IDictionaryType#formatteProperties(java.lang.Object)
	 */
	public Properties formatProperties(Object value) {
		throw new UnsupportedOperationException();
	}
		
	@Override
	public String toString() {
		return "[" + getClass().getName() + "] " + typeId + ", " + valueClass.getName();
	}
	
}
