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

import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelException;

/**
 * @author Martin Zatopek (martin.zatopek@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @param <T>
 * @created Jul 15, 2008
 */
public class DictionaryEntry {

	private IDictionaryType type;
	
	private Object value;

	private Object defaultValue;
	
	private boolean isDefault;
	
	private boolean isRequired;
	
	private boolean isInput;
	
	private boolean isOutput;
	
	private String contentType;
	
	public DictionaryEntry(IDictionaryType type) {
		this(type, false, false, false);
	}

	public DictionaryEntry(IDictionaryType type, boolean isRequired, boolean isInput, boolean isOutput) {
		this.type = type;
		this.isDefault = false;
		this.isRequired = isRequired;
		this.isInput = isInput;
		this.isOutput = isOutput;
	}

	public void init(Dictionary dictionary) throws ComponentNotReadyException {
		ClassLoader formerClassLoader = Thread.currentThread().getContextClassLoader();
		try {
			Thread.currentThread().setContextClassLoader(type.getClass().getClassLoader());
			value = type.init(value, dictionary);
		} catch (Exception e) {
			throw new ComponentNotReadyException(dictionary, "Can't initialize dictionary type " + type + ".", e);
		} finally {
			Thread.currentThread().setContextClassLoader(formerClassLoader);
		}
		
		defaultValue = value;
	}

	public void reset() {
		value = defaultValue;
	}
	
	public IDictionaryType getType() {
		return type;
	}
	
	public Object getValue() {
		return value;
	}
	
	void setValue(Object value) throws JetelException {
		if (type.isValidValue(value)) {
			this.value = value;
		} else {
			throw new JetelException("The dictionary entry type '" + type.getTypeId() + "' is incompatible with the value '" + value + "'.");
		}
	}

	public boolean isDefault() {
		return isDefault;
	}

	void setDefault(boolean isDefault) {
		this.isDefault = isDefault;
	}

	public boolean isRequired() {
		return isRequired;
	}

	void setRequired(boolean isRequired) {
		this.isRequired = isRequired;
	}

	public boolean isInput() {
		return isInput;
	}

	void setInput(boolean isInput) {
		this.isInput = isInput;
	}

	public boolean isOutput() {
		return isOutput;
	}

	void setOutput(boolean isOutput) {
		this.isOutput = isOutput;
	}
	
	void setContentType(String contentType) {
		this.contentType = contentType;
	}

	public String getContentType() {
		return contentType;
	}
	
	@Override
	public String toString() {
		return getValue().toString();
	}

}
