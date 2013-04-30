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
	
	@Override
	public String getTypeId() {
		return typeId;
	}
	
	@Override
	public Class<?> getValueClass() {
		return valueClass;
	}
	
	@Override
	public Object init(Object value, Dictionary dictionary) throws ComponentNotReadyException {
		if (value != null && !valueClass.isInstance(value)) {
			throw new ComponentNotReadyException(dictionary, "Unknown source type for '" + getTypeId() + "' dictionary element (" + value + ").");
		}
		return value;
	}

	@Override
	public boolean isParsePropertiesSupported() {
		return false;
	}

	@Override
	public Object parseProperties(Properties properties) throws AttributeNotFoundException {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean isFormatPropertiesSupported() {
		return false;
	}
	
	@Override
	public Properties formatProperties(Object value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public TLType getTLType() {
		return null;
	}

	@Override
	public DataFieldType getFieldType() {
		return null;
	}
	
	@Override
	public DataFieldType getFieldType(String contentType) {
		//content type is ignored for most of data types
		//only lists and maps are affected by content type
		return getFieldType();
	}
	
	@Override
	public DataFieldContainerType getFieldContainerType() {
		//SINGLE is returned for regular data types
		//lists and maps has custom implementations
		return getFieldType() != null ? DataFieldContainerType.SINGLE : null;
	}
	
	@Override
	public String toString() {
		return "[" + getClass().getName() + "] " + typeId + ", " + valueClass.getName();
	}
	
}
