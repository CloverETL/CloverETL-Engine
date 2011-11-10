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

import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.xmlbeans.impl.common.XMLChar;
import org.jetel.exception.JetelException;
import org.jetel.exception.XMLConfigurationException;
import org.w3c.dom.Document;
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
    
}
