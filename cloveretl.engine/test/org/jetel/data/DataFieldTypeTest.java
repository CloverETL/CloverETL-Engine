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
package org.jetel.data;

import java.util.Date;
import java.util.Map;

import org.jetel.data.primitive.Decimal;
import org.jetel.metadata.DataFieldType;
import org.jetel.test.CloverTestCase;
import org.jetel.util.string.CloverString;

import com.sun.xml.bind.v2.schemagen.xmlschema.List;

/**
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 30 Jan 2012
 */
public class DataFieldTypeTest extends CloverTestCase {

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		initEngine();
	}
	
	public void testGetName() {
		assertEquals("list", DataFieldType.LIST.getName());
		assertEquals("map", DataFieldType.MAP.getName());
		assertEquals("string", DataFieldType.STRING.getName());
		assertEquals("date", DataFieldType.DATE.getName());
		assertEquals("number", DataFieldType.NUMBER.getName());
		assertEquals("integer", DataFieldType.INTEGER.getName());
		assertEquals("long", DataFieldType.LONG.getName());
		assertEquals("decimal", DataFieldType.DECIMAL.getName());
		assertEquals("byte", DataFieldType.BYTE.getName());
		assertEquals("cbyte", DataFieldType.CBYTE.getName());
		assertEquals("boolean", DataFieldType.BOOLEAN.getName());
	}

	public void testGetInternalValueClass() {
		assertEquals(List.class, DataFieldType.LIST.getInternalValueClass());
		assertEquals(Map.class, DataFieldType.MAP.getInternalValueClass());
		assertEquals(CloverString.class, DataFieldType.STRING.getInternalValueClass());
		assertEquals(Date.class, DataFieldType.DATE.getInternalValueClass());
		assertEquals(Double.class, DataFieldType.NUMBER.getInternalValueClass());
		assertEquals(Integer.class, DataFieldType.INTEGER.getInternalValueClass());
		assertEquals(Long.class, DataFieldType.LONG.getInternalValueClass());
		assertEquals(Decimal.class, DataFieldType.DECIMAL.getInternalValueClass());
		assertEquals(byte[].class, DataFieldType.BYTE.getInternalValueClass());
		assertEquals(byte[].class, DataFieldType.CBYTE.getInternalValueClass());
		assertEquals(Boolean.class, DataFieldType.BOOLEAN.getInternalValueClass());
	}

	public void testIsNumeric() {
		assertEquals(false, DataFieldType.LIST.isNumeric());
		assertEquals(false, DataFieldType.MAP.isNumeric());
		assertEquals(false, DataFieldType.STRING.isNumeric());
		assertEquals(false, DataFieldType.DATE.isNumeric());
		assertEquals(true, DataFieldType.NUMBER.isNumeric());
		assertEquals(true, DataFieldType.INTEGER.isNumeric());
		assertEquals(true, DataFieldType.LONG.isNumeric());
		assertEquals(true, DataFieldType.DECIMAL.isNumeric());
		assertEquals(false, DataFieldType.BYTE.isNumeric());
		assertEquals(false, DataFieldType.CBYTE.isNumeric());
		assertEquals(false, DataFieldType.BOOLEAN.isNumeric());
	}

	public void testIsTrimType() {
		assertEquals(false, DataFieldType.LIST.isTrimType());
		assertEquals(false, DataFieldType.MAP.isTrimType());
		assertEquals(false, DataFieldType.STRING.isTrimType());
		assertEquals(true, DataFieldType.DATE.isTrimType());
		assertEquals(true, DataFieldType.NUMBER.isTrimType());
		assertEquals(true, DataFieldType.INTEGER.isTrimType());
		assertEquals(true, DataFieldType.LONG.isTrimType());
		assertEquals(true, DataFieldType.DECIMAL.isTrimType());
		assertEquals(false, DataFieldType.BYTE.isTrimType());
		assertEquals(false, DataFieldType.CBYTE.isTrimType());
		assertEquals(true, DataFieldType.BOOLEAN.isTrimType());
	}

	public void testFromName() {
		assertEquals(DataFieldType.LIST, DataFieldType.fromName("list"));
		assertEquals(DataFieldType.MAP, DataFieldType.fromName("map"));
		assertEquals(DataFieldType.STRING, DataFieldType.fromName("string"));
		assertEquals(DataFieldType.DATE, DataFieldType.fromName("date"));
		assertEquals(DataFieldType.NUMBER, DataFieldType.fromName("number"));
		
		assertEquals(DataFieldType.NUMBER, DataFieldType.fromName("numeric"));
		
		assertEquals(DataFieldType.INTEGER, DataFieldType.fromName("integer"));
		assertEquals(DataFieldType.LONG, DataFieldType.fromName("long"));
		assertEquals(DataFieldType.DECIMAL, DataFieldType.fromName("decimal"));
		assertEquals(DataFieldType.BYTE, DataFieldType.fromName("byte"));
		assertEquals(DataFieldType.CBYTE, DataFieldType.fromName("cbyte"));
		assertEquals(DataFieldType.BOOLEAN, DataFieldType.fromName("boolean"));
		
		try { DataFieldType.fromName(null); assertTrue(false); } catch (IllegalArgumentException e) { /*OK*/ }
		try { DataFieldType.fromName(""); assertTrue(false); } catch (IllegalArgumentException e) { /*OK*/ }
		try { DataFieldType.fromName("neco"); assertTrue(false); } catch (IllegalArgumentException e) { /*OK*/ }
		try { DataFieldType.fromName("List"); assertTrue(false); } catch (IllegalArgumentException e) { /*OK*/ }
		try { DataFieldType.fromName("DATE"); assertTrue(false); } catch (IllegalArgumentException e) { /*OK*/ }
		try { DataFieldType.fromName("sTrinG"); assertTrue(false); } catch (IllegalArgumentException e) { /*OK*/ }
	}

}
