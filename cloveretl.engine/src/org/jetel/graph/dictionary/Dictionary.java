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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.apache.log4j.Logger;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelException;
import org.jetel.graph.GraphElement;
import org.jetel.graph.TransformationGraph;

/**
 * @author Martin Zatopek (martin.zatopek@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created Jul 15, 2008
 */
public class Dictionary extends GraphElement {

	/**
	 * This constant is used for distinguish between standard xml attributes and attributes,
	 * which define dictionary value.
	 * 
	 * For instance: 
	 * <Entry contentType="contentType" id="DictionaryEntry1" input="false" name="i1" 
	 * output="false" dictval.value="Serra Angel" type="string"/>
	 */
	public static final String DICTIONARY_VALUE_NAMESPACE = "dictval.";
	
	private static final String DEFAULT_ID = "_DICTIONARY";

	private static final String DEFAULT_DICTIONARY_TYPE_ID = ObjectDictionaryType.TYPE_ID;
	
	private Map<String, DictionaryEntry> dictionary;

	public Dictionary(TransformationGraph graph) {
		super(DEFAULT_ID, graph);
		
		dictionary = new HashMap<String, DictionaryEntry>();
	}

	@Override
	public synchronized void init() throws ComponentNotReadyException {
        if(isInitialized()) return;
		super.init();

		//initialization of all default dictionary values
		for (DictionaryEntry dictionaryEntry : dictionary.values()) {
			dictionaryEntry.init(this);
		}
	}
	
	@Override
	public synchronized void reset() throws ComponentNotReadyException {
		super.reset();
		
		//resets default dictionary entries and removes all the others
		Iterator<DictionaryEntry> iterator = dictionary.values().iterator();
		while (iterator.hasNext()) {
		    final DictionaryEntry entry = iterator.next();
		    if (entry.isDefault()) {
		        entry.reset();
		    } else {
		        iterator.remove();
		    }
		}
	}
	
	public DictionaryEntry getEntry(String key) {
		return dictionary.get(key);
	}
	
	public boolean hasEntry(String key) {
		return dictionary.containsKey(key);
	}
	
	public IDictionaryType getType(String key) {
		DictionaryEntry entry = getEntry(key);
		if (entry == null) {
			return null;
		}
		return getEntry(key).getType();
	}

	public Object getValue(String key) {
		DictionaryEntry entry = getEntry(key);
		if (entry == null) {
			return null;
		}
		return getEntry(key).getValue();
	}

	public void setValue(String key, Object value) throws ComponentNotReadyException {
		DictionaryEntry entry = dictionary.get(key);

		if (entry != null) { //is this key already present in the dictionary?
			if (entry.getType().isValidValue(value)) {
				setValue(key, entry.getType(), value);
			} else { //incompatible dictionary type for the given value
				throw new ComponentNotReadyException(this, "The dictionary key '" + key + "' has asociated incompatible type ('" + value + "' cannot be assigned to " + entry.getType().getTypeId() + " type).");
			}
		} else { // we will safe the given value under default dictionary type
			setValue(key, DictionaryTypeFactory.getDictionaryType(DEFAULT_DICTIONARY_TYPE_ID), value);
		}
	}

	public void setValue(String key, String typeId, Object value) throws ComponentNotReadyException {
		IDictionaryType dictionaryType;
		try {
			dictionaryType = DictionaryTypeFactory.getDictionaryType(typeId);
		} catch(Exception e) {
			throw new ComponentNotReadyException("Dictionary type '" + typeId + "' does not exist (key = " + key + ").");
		}
		setValue(key, dictionaryType, value);
	}

	public void setValueFromProperties(String key, String typeId, Properties properties) throws ComponentNotReadyException, UnsupportedDictionaryOperation {
		IDictionaryType dictionaryType;
		try {
			dictionaryType = DictionaryTypeFactory.getDictionaryType(typeId);
		} catch(Exception e) {
			throw new ComponentNotReadyException("Dictionary type '" + typeId + "' does not exist (key = " + key + ").", e);
		}
		if (!dictionaryType.isParsePropertiesSupported()) {
			throw new UnsupportedDictionaryOperation("Dictionary type '" + typeId + "' cannot be initialized from Properties.");
		}
		try {
			setValue(key, dictionaryType, dictionaryType.parseProperties(properties));
		} catch (AttributeNotFoundException e) {
			throw new ComponentNotReadyException("Dictionary type '" + typeId + "' cannot be initialized by given properties (key = " + key + "): " + properties, e);
		}
	}

	
	private void setValue(String key, IDictionaryType type, Object value) throws ComponentNotReadyException {
		DictionaryEntry entry = dictionary.get(key);

		try {
			if (entry != null) { //is this key already present in the dictionary?
				if (entry.getType().getTypeId().equals(type.getTypeId())) {
					entry.setValue(value);
				} else { //entry type cannot be changed
					throw new ComponentNotReadyException(this, "The dictionary key '" + key + "' has asociated incompatible type (" + entry.getType().getTypeId() + " != " + type.getTypeId() + ").");
				}
			} else { // we will create new dictionary entry record
				entry = new DictionaryEntry(type);
				entry.setValue(value);
				dictionary.put(key, entry);
			}
		} catch (JetelException e) {
			throw new ComponentNotReadyException(this, "The dictionary entry in '" + key + "' cannot store the given value.", e);
		}
		
		if (!isInitialized()) { // is the dictionary already initialized?
			entry.setDefault(true); // if not, dictionary entry will be initialized later
		} else {
			entry.init(this); //in other case, initialize entry immediately
		}
	}
	
	public boolean isInput(String key) {
		DictionaryEntry entry = dictionary.get(key);
		
		return entry != null && entry.isInput();
	}

	public void setAsInput(String key) {
		DictionaryEntry entry = dictionary.get(key);
		
		if (entry == null) { //is this key already present in the dictionary?
			throw new IllegalArgumentException();
		}
		
		if (!isInitialized() || !entry.isDefault()) { // only non-initialized or non-default entries can be updated
			entry.setInput(true);
		} else {
			throw new IllegalStateException();
		}
	}

	public boolean isOutput(String key) {
		DictionaryEntry entry = dictionary.get(key);
		
		return entry != null && entry.isOutput();
	}

	public void setAsOuput(String key) {
		DictionaryEntry entry = dictionary.get(key);
		
		if (entry == null) { //is this key already present in the dictionary?
			throw new IllegalArgumentException();
		}
		
		if (!isInitialized() || !entry.isDefault()) { // only non-initialized or non-default entries can be updated
			entry.setOutput(true);
		} else {
			throw new IllegalStateException();
		}
	}

	public boolean isRequired(String key) {
		DictionaryEntry entry = dictionary.get(key);
		
		return entry != null && entry.isRequired();
	}

	public void setAsRequired(String key) {
		DictionaryEntry entry = dictionary.get(key);
		
		if (entry == null) { //is this key already present in the dictionary?
			throw new IllegalArgumentException();
		}
		
		if (!isInitialized() || !entry.isDefault()) { // only non-initialized or non-default entries can be updated
			entry.setRequired(true);
		} else {
			throw new IllegalStateException();
		}
	}

	public String getContentType(String key) {
		DictionaryEntry entry = dictionary.get(key);
		
		if (entry != null) {
			return entry.getContentType();
		} else {
			return null;
		}
	}

	public void setContentType(String key, String contentType) {
		DictionaryEntry entry = dictionary.get(key);
		
		if (entry == null) { //is this key already present in the dictionary?
			throw new IllegalArgumentException();
		}
		
		if (!isInitialized() || !entry.isDefault()) { // only non-initialized or non-default entries can be updated
			entry.setContentType(contentType);
		} else {
			throw new IllegalStateException();
		}
	}
	
	public Set<String> getKeys() {
		return dictionary.keySet();
	}
	
	public Set<Entry<String, DictionaryEntry>> getEntries() {
		return dictionary.entrySet();
	}
	
	public boolean isEmpty() {
		return dictionary.isEmpty();
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("Dictionary#").append(this.hashCode());
		if (dictionary.isEmpty()) {
			sb.append(" empty");
		} else {
			printContent(sb, null, " Dictionary content:");
		}
		return sb.toString(); 
	}

	/**
	 * Prints out the dictionary content into log at INFO level.
	 * @param logger
	 * @param message
	 */
	public void printContent(Logger logger, String message) {
		printContent(null, logger, message);
	}
	
	/**
	 * Prints out the dictionary content into log at INFO level AND/OR into specified StringBuilder.
	 * 
	 * @param stringBuilder - if it's null, it's ignored; if it's not null, it's filled with dictionary content
	 * @param logger - if it's null, it's ignored; if it's not null, it's used for logging dictionary content
	 * @param message - first message introducing the content
	 */
	private void printContent(StringBuilder sb, Logger logger, String message) {
		if (!dictionary.isEmpty() 
				&& (sb != null || (logger != null && logger.isInfoEnabled()))
				) {
			if (logger != null)
				logger.info(message);
			if (sb != null) {
				sb.append(message).append("\n");
			}
			
			Set<String> keys = this.getKeys();
			for (String key : keys) {
				DictionaryEntry entry = this.getEntry(key);
				String entryValue = null;
				if (entry.getType().isFormatPropertiesSupported()) {
					Properties properties = entry.getType().formatProperties(entry.getValue());
					if (properties != null) {
						entryValue = properties.toString();
					}
				} else {
					entryValue = "<unprintable_value>";
				}
				String s = "DictEntry:" + key + ":" + entry.getType().getTypeId() + ":" + entryValue;
				if (logger != null) {
					logger.info(s);
				}
				if (sb != null) {
					sb.append(s).append("\n");					
				}
			}// for
		}// if not empty
	}
	
}
