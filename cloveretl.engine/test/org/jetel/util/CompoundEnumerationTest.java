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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.NoSuchElementException;

import org.jetel.test.CloverTestCase;

/**
 * @author martin (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 25. 11. 2016
 */
public class CompoundEnumerationTest extends CloverTestCase {

	public void testNull() {
		try {
			new CompoundEnumeration<String>(null);
			fail();
		} catch (NullPointerException e) {
			//OK
		}
	}

	public void testEmpty() {
		List<Enumeration<String>> enumerations = new ArrayList<Enumeration<String>>();
		
		CompoundEnumeration<String> compoundEnumeration = new CompoundEnumeration<String>(enumerations);
		assertFalse(compoundEnumeration.hasMoreElements());
		try {
			compoundEnumeration.nextElement();
			fail();
		} catch (NoSuchElementException e) {
			//OK
		}
		assertFalse(compoundEnumeration.hasMoreElements());
		try {
			compoundEnumeration.nextElement();
			fail();
		} catch (NoSuchElementException e) {
			//OK
		}
	}

	public void testEmpty1() {
		List<Enumeration<String>> enumerations = new ArrayList<Enumeration<String>>();
		
		CompoundEnumeration<String> compoundEnumeration = new CompoundEnumeration<String>(enumerations);
		try {
			compoundEnumeration.nextElement();
			fail();
		} catch (NoSuchElementException e) {
			//OK
		}
		assertFalse(compoundEnumeration.hasMoreElements());
		try {
			compoundEnumeration.nextElement();
			fail();
		} catch (NoSuchElementException e) {
			//OK
		}
	}

	public void testEmpty2() {
		List<Enumeration<String>> enumerations = new ArrayList<Enumeration<String>>();
		enumerations.add(Collections.enumeration(Collections.<String>emptyList()));
		CompoundEnumeration<String> compoundEnumeration = new CompoundEnumeration<String>(enumerations);

		assertFalse(compoundEnumeration.hasMoreElements());
		try {
			compoundEnumeration.nextElement();
			fail();
		} catch (NoSuchElementException e) {
			//OK
		}
	}

	public void testEmpty3() {
		List<Enumeration<String>> enumerations = new ArrayList<Enumeration<String>>();
		enumerations.add(Collections.enumeration(Collections.<String>emptyList()));
		enumerations.add(Collections.enumeration(Collections.<String>emptyList()));
		enumerations.add(Collections.enumeration(Collections.<String>emptyList()));
		CompoundEnumeration<String> compoundEnumeration = new CompoundEnumeration<String>(enumerations);

		assertFalse(compoundEnumeration.hasMoreElements());
		try {
			compoundEnumeration.nextElement();
			fail();
		} catch (NoSuchElementException e) {
			//OK
		}
	}

	public void test1() {
		List<Enumeration<String>> enumerations = new ArrayList<Enumeration<String>>();
		enumerations.add(Collections.enumeration(Arrays.asList((String) null)));
		CompoundEnumeration<String> compoundEnumeration = new CompoundEnumeration<String>(enumerations);

		assertTrue(compoundEnumeration.hasMoreElements());
		assertNull(compoundEnumeration.nextElement());
		try {
			compoundEnumeration.nextElement();
			fail();
		} catch (NoSuchElementException e) {
			//OK
		}
		assertFalse(compoundEnumeration.hasMoreElements());
	}

	public void test2() {
		List<Enumeration<String>> enumerations = new ArrayList<Enumeration<String>>();
		enumerations.add(Collections.enumeration(Arrays.asList("a", "b", "c")));
		CompoundEnumeration<String> compoundEnumeration = new CompoundEnumeration<String>(enumerations);

		assertTrue(compoundEnumeration.hasMoreElements());
		assertEquals("a", compoundEnumeration.nextElement());
		assertTrue(compoundEnumeration.hasMoreElements());
		assertEquals("b", compoundEnumeration.nextElement());
		assertTrue(compoundEnumeration.hasMoreElements());
		assertEquals("c", compoundEnumeration.nextElement());
		assertFalse(compoundEnumeration.hasMoreElements());
		try {
			compoundEnumeration.nextElement();
			fail();
		} catch (NoSuchElementException e) {
			//OK
		}
	}

	public void test3() {
		List<Enumeration<String>> enumerations = new ArrayList<Enumeration<String>>();
		enumerations.add(Collections.enumeration(Arrays.asList((String) null)));
		enumerations.add(Collections.enumeration(Arrays.asList((String) null)));
		enumerations.add(Collections.enumeration(Arrays.asList((String) null)));
		CompoundEnumeration<String> compoundEnumeration = new CompoundEnumeration<String>(enumerations);

		assertTrue(compoundEnumeration.hasMoreElements());
		assertNull(compoundEnumeration.nextElement());
		assertTrue(compoundEnumeration.hasMoreElements());
		assertNull(compoundEnumeration.nextElement());
		assertTrue(compoundEnumeration.hasMoreElements());
		assertNull(compoundEnumeration.nextElement());
		assertFalse(compoundEnumeration.hasMoreElements());
		try {
			compoundEnumeration.nextElement();
			fail();
		} catch (NoSuchElementException e) {
			//OK
		}
	}

	public void test4() {
		List<Enumeration<String>> enumerations = new ArrayList<Enumeration<String>>();
		enumerations.add(Collections.enumeration(Arrays.asList((String) null, (String) null)));
		enumerations.add(Collections.enumeration(Arrays.asList("a")));
		enumerations.add(Collections.enumeration(Arrays.asList((String) null, "b", "c")));
		CompoundEnumeration<String> compoundEnumeration = new CompoundEnumeration<String>(enumerations);

		assertTrue(compoundEnumeration.hasMoreElements());
		assertEquals(null, compoundEnumeration.nextElement());
		assertTrue(compoundEnumeration.hasMoreElements());
		assertEquals(null, compoundEnumeration.nextElement());
		assertTrue(compoundEnumeration.hasMoreElements());
		assertEquals("a", compoundEnumeration.nextElement());
		assertTrue(compoundEnumeration.hasMoreElements());
		assertEquals(null, compoundEnumeration.nextElement());
		assertTrue(compoundEnumeration.hasMoreElements());
		assertEquals("b", compoundEnumeration.nextElement());
		assertTrue(compoundEnumeration.hasMoreElements());
		assertEquals("c", compoundEnumeration.nextElement());

		assertFalse(compoundEnumeration.hasMoreElements());
		try {
			compoundEnumeration.nextElement();
			fail();
		} catch (NoSuchElementException e) {
			//OK
		}
	}

	public void test5() {
		List<Enumeration<String>> enumerations = new ArrayList<Enumeration<String>>();
		enumerations.add(Collections.enumeration(Arrays.asList((String) null, (String) null)));
		enumerations.add(Collections.enumeration(Collections.<String>emptyList()));
		enumerations.add(Collections.enumeration(Arrays.asList((String) null, "b", "c")));
		CompoundEnumeration<String> compoundEnumeration = new CompoundEnumeration<String>(enumerations);

		assertTrue(compoundEnumeration.hasMoreElements());
		assertEquals(null, compoundEnumeration.nextElement());
		assertTrue(compoundEnumeration.hasMoreElements());
		assertEquals(null, compoundEnumeration.nextElement());
		assertTrue(compoundEnumeration.hasMoreElements());
		assertEquals(null, compoundEnumeration.nextElement());
		assertTrue(compoundEnumeration.hasMoreElements());
		assertEquals("b", compoundEnumeration.nextElement());
		assertTrue(compoundEnumeration.hasMoreElements());
		assertEquals("c", compoundEnumeration.nextElement());

		assertFalse(compoundEnumeration.hasMoreElements());
		try {
			compoundEnumeration.nextElement();
			fail();
		} catch (NoSuchElementException e) {
			//OK
		}
	}
	

}
