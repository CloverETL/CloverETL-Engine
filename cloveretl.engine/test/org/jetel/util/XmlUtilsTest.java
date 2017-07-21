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

import java.util.Properties;

import org.jetel.test.CloverTestCase;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

/**
 * @author Martin Zatopek (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 5.7.2010
 */
public class XmlUtilsTest extends CloverTestCase {
	
	private static void checkValid(String name) {
		assertTrue(name, XmlUtils.isValidElementName(name));
	}

	private static void checkInvalid(String name) {
		assertFalse(name, XmlUtils.isValidElementName(name));
	}

	public void testIsValidElementName() {
		checkValid("elementName");
		checkValid("_elementName");
		checkValid("_elementname");
		checkValid("element:name");
		checkValid("element.name");
		
		checkInvalid(".elementName");
		checkInvalid("1elementName");
		checkInvalid("element name");
	}
	
	public void testCreateDocumentFromProperties() {
		try {
			XmlUtils.createDocumentFromProperties(null, null);
			assertTrue(false);
		} catch (NullPointerException e) {
			//OK
		}
		try {
			XmlUtils.createDocumentFromProperties(null, new Properties());
			assertTrue(false);
		} catch (NullPointerException e) {
			//OK
		}
		try {
			XmlUtils.createDocumentFromProperties("myRoot", null);
			assertTrue(false);
		} catch (NullPointerException e) {
			//OK
		}
		try {
			XmlUtils.createDocumentFromProperties("", new Properties());
			assertTrue(false);
		} catch (NullPointerException e) {
			//OK
		}
		
		Document document;
		Properties properties;
		Element rootElement;
		
		properties = new Properties();
		document = XmlUtils.createDocumentFromProperties("myRoot", properties);
		rootElement = (Element) document.getFirstChild();
		assertEquals("myRoot", rootElement.getNodeName());
		assertFalse(rootElement.hasAttributes());
		
		properties.setProperty("nonEmptyAttr", "value1");
		document = XmlUtils.createDocumentFromProperties("myRoot", properties);
		rootElement = (Element) document.getFirstChild();
		assertEquals("myRoot", rootElement.getNodeName());
		assertEquals(1, rootElement.getAttributes().getLength());
		assertEquals("value1", rootElement.getAttribute("nonEmptyAttr"));

		properties.setProperty("emptyAttr", "");
		document = XmlUtils.createDocumentFromProperties("myRoot", properties);
		rootElement = (Element) document.getFirstChild();
		assertEquals("myRoot", rootElement.getNodeName());
		assertEquals(2, rootElement.getAttributes().getLength());
		assertEquals("value1", rootElement.getAttribute("nonEmptyAttr"));
		assertEquals("", rootElement.getAttribute("emptyAttr"));
	}
	
}
