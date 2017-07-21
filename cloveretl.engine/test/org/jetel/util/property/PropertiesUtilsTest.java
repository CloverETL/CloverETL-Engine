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
package org.jetel.util.property;

import java.util.Properties;

import org.jetel.test.CloverTestCase;

/**
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 12.6.2012
 */
public class PropertiesUtilsTest extends CloverTestCase {

	private static final String LINE_SEPARATOR = System.getProperties().getProperty("line.separator");
	
	@Override
	protected void setUp() throws Exception {
		super.setUp();
		initEngine();
	}

	public void testParseFormatProperties() {
		String s;
		Properties properties = new Properties();
		
		assertNull(s = PropertiesUtils.formatProperties(null));
		assertNull(PropertiesUtils.parseProperties(s));

		assertEquals("", s = PropertiesUtils.formatProperties(properties));
		assertEquals(properties, PropertiesUtils.parseProperties(s));

		properties.setProperty("martin", "kokon");
		assertEquals("martin=kokon" + LINE_SEPARATOR, s = PropertiesUtils.formatProperties(properties));
		assertEquals(properties, PropertiesUtils.parseProperties(s));
		
		properties.setProperty("key1", "");
		s = PropertiesUtils.formatProperties(properties);
		assertEquals(properties, PropertiesUtils.parseProperties(s));

		properties.setProperty("", "neco");
		s = PropertiesUtils.formatProperties(properties);
		assertEquals(properties, PropertiesUtils.parseProperties(s));

		properties.setProperty("", "");
		s = PropertiesUtils.formatProperties(properties);
		assertEquals(properties, PropertiesUtils.parseProperties(s));
	}
	
	public void testCopy() {
		Properties empty = new Properties();
		Properties empty1 = new Properties();

		PropertiesUtils.copy(null, null);
		
		PropertiesUtils.copy(null, empty);
		assertTrue(empty.size() == 0);
		
		PropertiesUtils.copy(empty, empty1);
		assertTrue(empty.size() == 0);
		assertTrue(empty1.size() == 0);

		Properties prop = new Properties();
		prop.setProperty("neco", "neco1");
		PropertiesUtils.copy(empty, prop);
		assertTrue(empty.size() == 0);
		assertTrue(prop.size() == 1);
		assertEquals("neco1", prop.getProperty("neco"));
		
		PropertiesUtils.copy(prop, prop);
		assertTrue(prop.size() == 1);
		assertEquals("neco1", prop.getProperty("neco"));
		
		Properties prop1 = new Properties();
		PropertiesUtils.copy(prop, prop1);
		assertTrue(prop.size() == 1);
		assertEquals("neco1", prop.getProperty("neco"));
		assertTrue(prop1.size() == 1);
		assertEquals("neco1", prop1.getProperty("neco"));
		
		prop.setProperty("dalsi", "neco2");
		prop.setProperty("dalsi2", "");
		prop1.setProperty("xxx", "yyy");
		PropertiesUtils.copy(prop, prop1);
		assertTrue(prop.size() == 3);
		assertEquals("neco1", prop.getProperty("neco"));
		assertEquals("neco2", prop.getProperty("dalsi"));
		assertEquals("", prop.getProperty("dalsi2"));
		assertEquals(null, prop.getProperty("dalsi3"));
		assertTrue(prop1.size() == 4);
		assertEquals("neco1", prop1.getProperty("neco"));
		assertEquals("yyy", prop1.getProperty("xxx"));
		assertEquals("neco2", prop1.getProperty("dalsi"));
		assertEquals("", prop1.getProperty("dalsi2"));
		assertEquals(null, prop1.getProperty("dalsi3"));
	}
	
	public void testDuplicate() {
		assertEquals(null, PropertiesUtils.duplicate(null));
		
		Properties prop = PropertiesUtils.duplicate(new Properties());
		assertTrue(prop.size() == 0);

		prop = PropertiesUtils.duplicate(prop);
		assertTrue(prop.size() == 0);

		prop.setProperty("xxx", "yyy");
		Properties prop1 = PropertiesUtils.duplicate(prop);
		assertTrue(prop.size() == 1);
		assertEquals("yyy", prop.getProperty("xxx"));
		assertTrue(prop1.size() == 1);
		assertEquals("yyy", prop1.getProperty("xxx"));

		prop.setProperty("xxx", "yyy");
		prop.setProperty("neco", "");
		prop1 = PropertiesUtils.duplicate(prop);
		assertTrue(prop.size() == 2);
		assertEquals("yyy", prop.getProperty("xxx"));
		assertEquals("", prop.getProperty("neco"));
		assertTrue(prop1.size() == 2);
		assertEquals("yyy", prop1.getProperty("xxx"));
		assertEquals("", prop1.getProperty("neco"));
	}
	
}
