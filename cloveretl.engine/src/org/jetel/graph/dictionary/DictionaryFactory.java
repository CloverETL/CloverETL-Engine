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

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Enumeration;
import java.util.Properties;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.dictionary.jaxb.Entry;
import org.jetel.graph.dictionary.jaxb.ObjectFactory;
import org.jetel.graph.dictionary.jaxb.Property;
import org.jetel.util.JAXBContextProvider;
import org.jetel.util.string.StringUtils;

/**
 * Clover engine dictionary object can be built and serialize by this factory class.
 * 
 * @author Martin Zatopek (info@cloveretl.com)
 *         (c) (c) Javlin, a.s. (www.javlin.eu) (www.cloveretl.com)
 *
 * @created 8.2.2010
 */
public class DictionaryFactory {

	private static final String DICTIONARY_PACKAGE_NAME = "org.jetel.graph.dictionary.jaxb";

	private DictionaryFactory() {
		//private constructor
	}
	
	/**
	 * Create new dictionary instance based on xml specification at given InputStream.
	 * @param is
	 * @return
	 * @throws ComponentNotReadyException
	 */
	@SuppressWarnings("unchecked")
	public static Dictionary loadDictionary(InputStream is) throws ComponentNotReadyException {
		JAXBContext jaxbContext;
		try {
			jaxbContext = JAXBContextProvider.getInstance().getContext(DICTIONARY_PACKAGE_NAME, org.jetel.graph.dictionary.jaxb.Dictionary.class.getClassLoader());
			Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
			org.jetel.graph.dictionary.jaxb.Dictionary jaxbDictionary = ((JAXBElement<org.jetel.graph.dictionary.jaxb.Dictionary>) unmarshaller.unmarshal(is)).getValue();
			return loadDictionary(jaxbDictionary);
		} catch (JAXBException e) {
			throw new ComponentNotReadyException("Dictionary parsing error.", e);
		}
	}
	
	private static Dictionary loadDictionary(org.jetel.graph.dictionary.jaxb.Dictionary jaxbDictionary) throws ComponentNotReadyException {
		Dictionary result = new Dictionary(null);
		for (Entry entry : jaxbDictionary.getEntry()) {
			result.setValue(entry.getName(), entry.getType(), null);
			if (entry.isInput()) {
				result.setAsInput(entry.getName());
			}
			if (entry.isOutput()) {
				result.setAsOuput(entry.getName());
			}
			if (entry.isRequired()) {
				result.setAsRequired(entry.getName());
			}
			if (!StringUtils.isEmpty(entry.getContentType())) {
				result.setContentType(entry.getName(), entry.getContentType());
			}
			if (!entry.getProperty().isEmpty() && result.getType(entry.getName()).isParsePropertiesSupported()) {
				Properties valueProperties = new Properties();
				for (Property jaxbProperty : entry.getProperty()) {
					valueProperties.setProperty(jaxbProperty.getKey(), jaxbProperty.getValue());
				}
				try {
					result.getType(entry.getName()).parseProperties(valueProperties);
				} catch (AttributeNotFoundException e) {
					throw new ComponentNotReadyException("Invalid dictionary format", e);
				}
			}
		}
		return result;
	}
	
	/**
	 * Serializes the given dictionary in XML form into given OutputStream.
	 * @param dictionary
	 * @param os
	 * @throws ComponentNotReadyException
	 */
	public static void saveDictionary(Dictionary dictionary, OutputStream os) throws ComponentNotReadyException {
		org.jetel.graph.dictionary.jaxb.Dictionary jaxbDictionary = saveDictionary(dictionary);
		JAXBContext jaxbContext;
		try {
			jaxbContext = JAXBContextProvider.getInstance().getContext(DICTIONARY_PACKAGE_NAME, org.jetel.graph.dictionary.jaxb.Dictionary.class.getClassLoader());
			Marshaller marshaller = jaxbContext.createMarshaller();
			marshaller.marshal((new ObjectFactory()).createDictionary(jaxbDictionary), os);
		} catch (JAXBException e) {
			throw new ComponentNotReadyException("Dictionary formatting error.", e);
		}
	
	}
	
	private static org.jetel.graph.dictionary.jaxb.Dictionary saveDictionary(Dictionary dictionary) {
		org.jetel.graph.dictionary.jaxb.Dictionary jaxbDictionary = new org.jetel.graph.dictionary.jaxb.Dictionary();
		for (String key : dictionary.getKeys()) {
			DictionaryEntry entry = dictionary.getEntry(key);
			Entry jaxbEntry = new Entry();
			jaxbEntry.setName(key);
			jaxbEntry.setType(entry.getType().getTypeId());
			jaxbEntry.setInput(entry.isInput());
			jaxbEntry.setOutput(entry.isOutput());
			jaxbEntry.setRequired(entry.isRequired());
			jaxbEntry.setContentType(entry.getContentType());
			jaxbDictionary.getEntry().add(jaxbEntry);
			
			if (entry.getValue() != null) {
				if (entry.getType().isFormatPropertiesSupported()) {
					Properties valueProperties = entry.getType().formatProperties(entry.getValue());
					
					for (Enumeration<?> e = valueProperties.propertyNames(); e.hasMoreElements();) {
						String propertyKey = (String) e.nextElement();
						Property property = new Property();
						property.setKey(propertyKey);
						property.setValue(valueProperties.getProperty(propertyKey));
						jaxbEntry.getProperty().add(property);
					}
				}
			}
		}
		
		return jaxbDictionary;
	}
	
}
