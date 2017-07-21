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
	
}
