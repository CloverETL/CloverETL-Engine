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

import org.jetel.test.CloverTestCase;

/**
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 7.12.2012
 */
public class EdgeDebugUtilsTest extends CloverTestCase {
	
	public void testGetDebugFileName() {
		assertEquals("0-0-edge.dbg", EdgeDebugUtils.getDebugFileName(0, 0, "edge"));
		assertEquals("1234-0-eee.dbg", EdgeDebugUtils.getDebugFileName(1234, 0, "eee"));
		assertEquals("1234-32-eee-1.dbg", EdgeDebugUtils.getDebugFileName(1234, 32, "eee__1"));
		assertEquals("12-1234-edsaee-1435.dbg", EdgeDebugUtils.getDebugFileName(12, 1234, "edsaee__1435"));
		assertEquals("0-456-eee__.dbg", EdgeDebugUtils.getDebugFileName(0, 456, "eee__"));

		try {
			assertEquals("1234-e.e-e.dbg", EdgeDebugUtils.getDebugFileName(1234, 34, "e.e-e"));
			assertTrue(false);
		} catch (Exception e) {
			
		}
		try {
			assertEquals("", EdgeDebugUtils.getDebugFileName(-1, 123, ""));
			assertTrue(false);
		} catch (Exception e) {
			
		}
		try {
			assertEquals("", EdgeDebugUtils.getDebugFileName(23, -456, ""));
			assertTrue(false);
		} catch (Exception e) {
			
		}
		try {
			assertEquals("", EdgeDebugUtils.getDebugFileName(1234, 23, null));
			assertTrue(false);
		} catch (Exception e) {
			
		}
		try {
			assertEquals("", EdgeDebugUtils.getDebugFileName(23, 1234, "eee__-1"));
			assertTrue(false);
		} catch (Exception e) {
			
		}
	}

	public void testAssembleUniqueEdgeId() {
		assertEquals("edge1__0", EdgeDebugUtils.assembleUniqueEdgeId("edge1", 0));	
		assertEquals("edge1__123", EdgeDebugUtils.assembleUniqueEdgeId("edge1", 123));	
		assertEquals("edge1", EdgeDebugUtils.assembleUniqueEdgeId("edge1", -1));	
		assertEquals("edge1", EdgeDebugUtils.assembleUniqueEdgeId("edge1", -123));	
		assertEquals("edge1", EdgeDebugUtils.assembleUniqueEdgeId("edge1", null));	
	}
	
	public void testIsDebugFileName() {
		assertTrue(EdgeDebugUtils.isDebugFileName("0-edge.dbg") == true);
		assertTrue(EdgeDebugUtils.isDebugFileName("-123-edge.dbg") == false);
		assertTrue(EdgeDebugUtils.isDebugFileName("123-edge.dbg") == true);
		assertTrue(EdgeDebugUtils.isDebugFileName("123-.dbg") == false);
		assertTrue(EdgeDebugUtils.isDebugFileName("123-eee.dbg") == true);
		assertTrue(EdgeDebugUtils.isDebugFileName("123-e.ee.dbg") == false);
		assertTrue(EdgeDebugUtils.isDebugFileName("123-edge-1.dbg") == true);
		assertTrue(EdgeDebugUtils.isDebugFileName("123-edge-123.dbg") == true);
		assertTrue(EdgeDebugUtils.isDebugFileName("123-edge-0.dbg") == true);
		assertTrue(EdgeDebugUtils.isDebugFileName("123-edge--1.dbg") == false);
		assertTrue(EdgeDebugUtils.isDebugFileName("123-edge-.dbg") == false);
	}
	
	public void testIsDebugFileName1() {
		assertTrue(EdgeDebugUtils.isDebugFileName("0-edge.dbg", 0, "edge") == true);
		assertTrue(EdgeDebugUtils.isDebugFileName("0-edge.dbg", 1, "edge") == false);
		assertTrue(EdgeDebugUtils.isDebugFileName("0-edge.dbg", 0, "ege") == false);
		
		assertTrue(EdgeDebugUtils.isDebugFileName("-123-edge.dbg", -123, "edge") == false);
		assertTrue(EdgeDebugUtils.isDebugFileName("-123-edge.dbg", 123, null) == false);

		assertTrue(EdgeDebugUtils.isDebugFileName("123-edge.dbg", 123, "edge") == true);
		assertTrue(EdgeDebugUtils.isDebugFileName("123-edge.dbg", 1, "edge") == false);
		assertTrue(EdgeDebugUtils.isDebugFileName("123-edge.dbg", 123, "ege") == false);

		assertTrue(EdgeDebugUtils.isDebugFileName("123-edge-1.dbg", 123, "edge") == true);
		assertTrue(EdgeDebugUtils.isDebugFileName("123-edge-1.dbg", 1, "edge") == false);
		assertTrue(EdgeDebugUtils.isDebugFileName("123-edge-1.dbg", 123, "edge-1") == false);
		assertTrue(EdgeDebugUtils.isDebugFileName("123-edge-1.dbg", 123, "edge__1") == false);
		
		assertTrue(EdgeDebugUtils.isDebugFileName("123-edge-0.dbg", 123, "edge") == true);
		assertTrue(EdgeDebugUtils.isDebugFileName("123-edge-0.dbg", 1, "edge") == false);
		assertTrue(EdgeDebugUtils.isDebugFileName("123-edge-0.dbg", 123, "edge0") == false);
	}

	public void testExtractUniqueEdgeId() {
		assertEquals("edge", EdgeDebugUtils.extractUniqueEdgeId("0-edge.dbg"));
		assertEquals("eee", EdgeDebugUtils.extractUniqueEdgeId("123-eee.dbg"));
		assertEquals("eee__1", EdgeDebugUtils.extractUniqueEdgeId("123-eee-1.dbg"));
		assertEquals("eee__0", EdgeDebugUtils.extractUniqueEdgeId("123-eee-0.dbg"));
		assertEquals("eee__1", EdgeDebugUtils.extractUniqueEdgeId("123-eee__1.dbg"));

		try {
			EdgeDebugUtils.extractUniqueEdgeId("abc");
			assertTrue(false);
		} catch (Exception e) {
			
		}
		try {
			EdgeDebugUtils.extractUniqueEdgeId("-abc.dbg");
			assertTrue(false);
		} catch (Exception e) {
			
		}
		try {
			EdgeDebugUtils.extractUniqueEdgeId("-1-abc.dbg");
			assertTrue(false);
		} catch (Exception e) {
			
		}
		try {
			EdgeDebugUtils.extractUniqueEdgeId("1-ab-c.dbg");
			assertTrue(false);
		} catch (Exception e) {
			
		}
		try {
			EdgeDebugUtils.extractUniqueEdgeId("1-ab-.dbg");
			assertTrue(false);
		} catch (Exception e) {
			
		}
		try {
			EdgeDebugUtils.extractUniqueEdgeId("1-ab--1.dbg");
			assertTrue(false);
		} catch (Exception e) {
			
		}
	}

	public void testExtractWriterRunId() {
		assertEquals(0, EdgeDebugUtils.extractWriterRunId("0-edge.dbg"));
		assertEquals(123, EdgeDebugUtils.extractWriterRunId("123-eee.dbg"));
		assertEquals(123, EdgeDebugUtils.extractWriterRunId("123-eee-0.dbg"));
		assertEquals(123, EdgeDebugUtils.extractWriterRunId("123-eee-123.dbg"));

		assertEquals(0, EdgeDebugUtils.extractWriterRunId("0-0-edge.dbg"));
		assertEquals(123, EdgeDebugUtils.extractWriterRunId("123-0-eee.dbg"));
		assertEquals(123, EdgeDebugUtils.extractWriterRunId("123-456-eee-0.dbg"));
		assertEquals(123, EdgeDebugUtils.extractWriterRunId("123-1-eee-123.dbg"));

		try {
			EdgeDebugUtils.extractWriterRunId("abc");
			assertTrue(false);
		} catch (Exception e) {
			
		}
		try {
			EdgeDebugUtils.extractWriterRunId("-abc.dbg");
			assertTrue(false);
		} catch (Exception e) {
			
		}
		try {
			EdgeDebugUtils.extractWriterRunId("-1-abc.dbg");
			assertTrue(false);
		} catch (Exception e) {
			
		}
		try {
			EdgeDebugUtils.extractWriterRunId("1-ab-c.dbg");
			assertTrue(false);
		} catch (Exception e) {
			
		}
	}

	public void testExtractReaderRunId() {
		assertEquals(0, EdgeDebugUtils.extractReaderRunId("0-edge.dbg"));
		assertEquals(123, EdgeDebugUtils.extractReaderRunId("123-eee.dbg"));
		assertEquals(123, EdgeDebugUtils.extractReaderRunId("123-eee-0.dbg"));
		assertEquals(123, EdgeDebugUtils.extractReaderRunId("123-eee-123.dbg"));

		assertEquals(0, EdgeDebugUtils.extractReaderRunId("0-0-edge.dbg"));
		assertEquals(0, EdgeDebugUtils.extractReaderRunId("123-0-eee.dbg"));
		assertEquals(456, EdgeDebugUtils.extractReaderRunId("123-456-eee-0.dbg"));
		assertEquals(1, EdgeDebugUtils.extractReaderRunId("123-1-eee-123.dbg"));

		try {
			EdgeDebugUtils.extractReaderRunId("abc");
			assertTrue(false);
		} catch (Exception e) {
			
		}
		try {
			EdgeDebugUtils.extractReaderRunId("-abc.dbg");
			assertTrue(false);
		} catch (Exception e) {
			
		}
		try {
			EdgeDebugUtils.extractReaderRunId("-1-abc.dbg");
			assertTrue(false);
		} catch (Exception e) {
			
		}
		try {
			EdgeDebugUtils.extractReaderRunId("1-ab-c.dbg");
			assertTrue(false);
		} catch (Exception e) {
			
		}
	}

	public void testExtractEdgeIdFromUniqueEdgeId() {
		assertEquals("edge1", EdgeDebugUtils.extractEdgeIdFromUniqueEdgeId("edge1"));
		assertEquals("edge1", EdgeDebugUtils.extractEdgeIdFromUniqueEdgeId("edge1__0"));
		assertEquals("edge1", EdgeDebugUtils.extractEdgeIdFromUniqueEdgeId("edge1__123"));
	}
	
}
