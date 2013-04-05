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
package org.jetel.metadata;

import static org.jetel.metadata.DataFieldFormatType.BINARY;
import static org.jetel.metadata.DataFieldFormatType.EXCEL;
import static org.jetel.metadata.DataFieldFormatType.JAVA;
import static org.jetel.metadata.DataFieldFormatType.JODA;
import static org.jetel.metadata.DataFieldFormatType.getFormatType;
import static org.jetel.metadata.DataFieldFormatType.isExistingPrefix;

import org.jetel.test.CloverTestCase;
import org.jetel.util.string.StringUtils;

/**
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 13 Jan 2012
 */
public class DataFieldFormatTypeTest extends CloverTestCase {

		public void testIsExistingPrefix() {
		assertTrue(isExistingPrefix("java"));
		assertTrue(isExistingPrefix("Java"));
		assertTrue(isExistingPrefix("JAVA"));

		assertTrue(isExistingPrefix("JODA"));
		assertTrue(isExistingPrefix("jodA"));

		assertTrue(isExistingPrefix("BINAry"));
		assertTrue(isExistingPrefix("binary"));

		assertTrue(isExistingPrefix("eXcel"));
		
		assertFalse(isExistingPrefix("exceljava"));
		assertFalse(isExistingPrefix("excel:"));
		assertFalse(isExistingPrefix(" binary"));
		assertFalse(isExistingPrefix(null));
		assertFalse(isExistingPrefix(""));
	}
	
	public void testGetLongName() {
		for (DataFieldFormatType formatType : DataFieldFormatType.values()) {
			assertTrue(!StringUtils.isEmpty(formatType.getLongName()));
		}
	}
	
	public void testGetFormatPrefix() {
		assertEquals("java", JAVA.getFormatPrefix());
		assertEquals("joda", JODA.getFormatPrefix());
		assertEquals("binary", BINARY.getFormatPrefix());
		assertEquals("excel", EXCEL.getFormatPrefix());
	}
	
	public void testPrependFormatPrefix() {
		assertEquals("java:neco", JAVA.prependFormatPrefix("neco"));
		assertEquals("excel:java:neco", EXCEL.prependFormatPrefix("java:neco"));
		assertEquals("binary:", BINARY.prependFormatPrefix(""));
		assertEquals("joda:", JODA.prependFormatPrefix(null));
	}
	
	public void testGetFormatPrefixWithDelimiter() {
		assertEquals("java:", JAVA.getFormatPrefixWithDelimiter());
		assertEquals("joda:", JODA.getFormatPrefixWithDelimiter());
		assertEquals("binary:", BINARY.getFormatPrefixWithDelimiter());
		assertEquals("excel:", EXCEL.getFormatPrefixWithDelimiter());
	}
	
	public void testGetFormatType() {
		assertEquals(JAVA, getFormatType("neco"));
		assertEquals(null, getFormatType(""));
		assertEquals(null, getFormatType(null));
		assertEquals(JAVA, getFormatType("java:"));
		assertEquals(JODA, getFormatType("joda:"));
		assertEquals(BINARY, getFormatType("Binary:"));
		assertEquals(EXCEL, getFormatType("EXCEL:"));
		
		assertEquals(JAVA, getFormatType(":EXCEL:"));
		assertEquals(JAVA, getFormatType(" joda: "));
	}
	
	public void testGetFormat() {
		assertEquals("neco", JAVA.getFormat("java:neco"));
		assertEquals("neco", JAVA.getFormat("neco"));
		assertEquals("", JAVA.getFormat(""));
		assertEquals("", JAVA.getFormat(null));
		
		assertEquals("", JODA.getFormat("joda:"));
		assertEquals("", BINARY.getFormat("joda"));
		assertEquals("", EXCEL.getFormat("joda:neco"));
		assertEquals("neco", JODA.getFormat("joda:neco"));
		assertEquals("", JODA.getFormat("java:neco"));
		assertEquals("", BINARY.getFormat("java:"));
		assertEquals("java", JAVA.getFormat("java"));
		assertEquals("", JODA.getFormat(""));
		assertEquals("", JODA.getFormat(null));
	}
	
}
