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
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.metadata.DataFieldContainerType;
import org.jetel.metadata.DataFieldType;

/**
 * This interface represents a type of a dictionary entry. 
 * Implementation serves to provide ability to build up dictionary value from the given properties
 * or validates a given value as a proper content of a dictionary entry.
 * 
 * It is intended to be implemented in various external plugins for their internal dictionary value types.
 *  
 * @author Martin Zatopek (martin.zatopek@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created 10.3.2008
 */
public interface IDictionaryType {

	/**
	 * @return identifier of this dictionary type
	 */
	public String getTypeId();
	
	/**
	 * @return Class object of a dictionary value
	 */
	public Class<?> getValueClass();
	
	/**
	 * Creates final dictionary value based on the given pre-value.
	 */
	public Object init(Object value, Dictionary dictionary) throws ComponentNotReadyException;

	/**
	 * Is the Properties object supported as an intermediate format?
	 * Can parseProperties() method be invoked?
	 * @return
	 */
	public boolean isParsePropertiesSupported();
	
	/**
	 * Returns a dictionary value based on the given properties.
	 * This method can throw {@link UnsupportedOperationException}, check isParsePropertiesSupported() method.
	 */
	public Object parseProperties(Properties properties) throws AttributeNotFoundException;
	
	/**
	 * Is the Properties object supported as an intermediate format?
	 * Can formatProperties() method be invoked?
	 * @return
	 */
	public boolean isFormatPropertiesSupported();
	
	/**
	 * Create properties object which represents given value.
	 * This method can throw {@link UnsupportedOperationException}, check isFormatPropertiesSupported() method.
	 * @param value
	 * @return
	 */
	public Properties formatProperties(Object value);
	
	/**
	 * Tests, whether the given value is acceptable for this dictionary entry type.
	 */
	public boolean isValidValue(Object value);

	/**
	 * Returns CTL type equivalent.
	 * @return CTL type or null if this type is not supported in CTL 
	 */
	public TLType getTLType();
	
	/**
	 * @return corresponding metadata field type, i.e. DataFieldType.STRING, ... or <code>null</code> if does not exist
	 */
	public DataFieldType getFieldType();
	
	/**
	 * @return corresponding metadata field type, i.e. DataFieldType.STRING, ... or <code>null</code> if does not exist
	 * 
	 * NOTE: contentType is used only for List and Map dictionary types,
	 * where field type is actually encoded in contentType
	 */
	public DataFieldType getFieldType(String contentType);

	/**
	 * @return container type SINGLE, LIST or MAP of this dictionary type; null is returned
	 * for non clover data types (for example channels)
	 */
	public DataFieldContainerType getFieldContainerType();
	
}
