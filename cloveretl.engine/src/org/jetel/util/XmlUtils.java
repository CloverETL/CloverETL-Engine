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
package org.jetel.util;

import java.io.StringReader;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.Map;
import java.util.Properties;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.xmlbeans.impl.common.XMLChar;
import org.jetel.exception.JetelException;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.util.string.StringUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.InputSource;

/**
 * Set of utilities for manipulating with XML documents.
 * 
 * @author Martin Zatopek (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 20.6.2011
 */
public class XmlUtils {

	/**
	 * Only private constructor, this class should not be instantiated at all.
	 */
	private XmlUtils() {
	}
	
    /**
     * Creates org.w3c.dom.Document object from the given String.
     * 
     * @param inString
     * @return
     * @throws XMLConfigurationException
     */
    public static Document createDocumentFromString(String inString) throws JetelException {
        InputSource is = new InputSource(new StringReader(inString));
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        try {
            return dbf.newDocumentBuilder().parse(is);
        } catch (Exception e) {
            throw new JetelException("XML document building failed.", e);
        }
    }

    /**
     * Creates org.w3c.dom.Document object from the given ReadableByteChannel.
     * 
     * @param readableByteChannel
     * @return
     * @throws XMLConfigurationException
     */
    public static Document createDocumentFromChannel(ReadableByteChannel readableByteChannel) throws JetelException {
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        try {
            return dbf.newDocumentBuilder().parse(Channels.newInputStream(readableByteChannel));
        } catch (Exception e) {
            throw new JetelException("XML document building failed.", e);
        }
    }
    
    /**
     * Returns true if the argument can be used
     * as a name of an XML element (or attribute).
     * 
     * Uses Apache XMLBeans library.
     * 
     * @param name the string to be checked
     * @return <code>true</code> if <code>name</code> is a valid XML element or attribute name
     */
    public static boolean isValidElementName(String name) {
    	return XMLChar.isValidName(name);
    }
    
    /**
	 * Expands a prefixed element or attribute name to a universal name.
	 * I.e. the namespace prefix is replaced by augmented URI.
	 * The URIs are taken from the namespaceBindings parameter.
	 * 
	 * @param prefixedName XML element or attribute name e.g. <code>mov:movies</code>
	 * 
	 * @return Universal XML name in the form: <code>{http://www.javlin.eu/movies}title</code>
	 */
	public static String createQualifiedName(String prefixedName, Map<String, String> namespaceBindings) {
		if (prefixedName == null || prefixedName.isEmpty()) {
			return prefixedName;
		}
		
		// check if universal XML name exists
		int indexOfOpenBracket = prefixedName.indexOf("{");
		if (-1<indexOfOpenBracket && indexOfOpenBracket<prefixedName.indexOf("}")) {
			return prefixedName;
		}
		
		final String[] parsed = prefixedName.split(":");
		
		if (parsed.length < 2) {
			return "{}" + parsed[0];
		}
		
		/*
		 * Prefixed element:
		 * Get the URI (already in Clark's notation) and use it to create qualified name
		 */
		String namespaceURI = namespaceBindings.get(parsed[0]);
		namespaceURI = namespaceURI == null ? "{}" : namespaceURI;
		
		
		return namespaceURI + parsed[1];
	}
	
	/**
	 * Creates org.w3c.dom.Document with single root element, which has attributes
	 * assembled from the given properties.
	 * Example result:
	 * <myRoot property1="value1" property2="value2" property3="value3"/>
	 * 	
	 * @param rootElement name of root element
	 * @param properties properties which will be converted to attributes of the only root element
	 * @return XML document with described structure 
	 */
	public static Document createDocumentFromProperties(String rootElement, Properties properties) {
		if (!StringUtils.isEmpty(rootElement) && properties != null) {
			try {
				// create new Document
				DocumentBuilderFactory dbfac = DocumentBuilderFactory.newInstance();
				DocumentBuilder docBuilder = null;
				docBuilder = dbfac.newDocumentBuilder();
				Document doc = docBuilder.newDocument();
		
				// create the root element and add it to the document
				Element root = doc.createElement(rootElement);
				doc.appendChild(root);
				
				for (String propertyName : properties.stringPropertyNames()) {
					root.setAttribute(propertyName, properties.getProperty(propertyName));
				}
				return doc;
			} catch (ParserConfigurationException e) {
				throw new JetelRuntimeException(e);
			}
		} else {
			throw new NullPointerException("rootElement=" + rootElement + "; properties=" + properties);
		}
	}
	
}
