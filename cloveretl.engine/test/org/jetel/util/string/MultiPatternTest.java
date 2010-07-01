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
package org.jetel.util.string;

import junit.framework.TestCase;

/**
 * @author csochor (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created Jun 30, 2010
 */
public class MultiPatternTest extends TestCase {

	public void testNull() {
		try {
			MultiPattern.parse(null);
		} catch (IllegalArgumentException e) {
			// OK
			return;
		}
		fail("Exception expected");
	}

	public void testEmpty() {
		try {
			MultiPattern.parse("");
		} catch (IllegalArgumentException e) {
			// OK
			return;
		}
		fail("Exception expected");
	}

	public void test1Char() {
		final MultiPattern mp = MultiPattern.parse("/");
		assertEquals(1, mp.size());
		assertEquals("/", mp.getString(0));
	}

	public void testNoSeparator() {
		final MultiPattern mp = MultiPattern.parse("fas|d");
		assertEquals(1, mp.size());
		assertEquals("fas|d", mp.getString(0));
	}

	public void testOnlySeparators() {
		final MultiPattern mp = MultiPattern.parse("pp");
		assertEquals(1, mp.size());
		assertEquals("", mp.getString(0));
	}
	
	public void test1Pattern() {
		final MultiPattern mp = MultiPattern.parse("pkrokodylp");
		assertEquals(1, mp.size());
		assertEquals("krokodyl", mp.getString(0));
	}

	public void test1PatternWithSpace() {
		final MultiPattern mp = MultiPattern.parse("pkrokodylp ");
		assertEquals(1, mp.size());
		assertEquals("pkrokodylp ", mp.getString(0));
	}
	
	public void test2Patterns() {
		final MultiPattern mp = MultiPattern.parse("pkropkodylp");
		assertEquals(2, mp.size());
		assertEquals("kro", mp.getString(0));
		assertEquals("kodyl", mp.getString(1));
	}
	
	public void test2EmptyPatterns() {
		final MultiPattern mp = MultiPattern.parse("fff");
		assertEquals(2, mp.size());
		assertEquals("", mp.getString(0));
		assertEquals("", mp.getString(1));
	}
	
	public void testEmptyAndPipePatterns() {
		final MultiPattern mp = MultiPattern.parse("ff|f");
		assertEquals(2, mp.size());
		assertEquals("", mp.getString(0));
		assertEquals("|", mp.getString(1));
	}
	
	public void test4Patterns() {
		final MultiPattern mp = MultiPattern.parse("/A/B/C/D/");
		assertEquals(4, mp.size());
		assertEquals("A", mp.getString(0));
		assertEquals("B", mp.getString(1));
		assertEquals("C", mp.getString(2));
		assertEquals("D", mp.getString(3));
	}
	
	public void testRealAnoNePatterns() {
		final MultiPattern mp = MultiPattern.parse("!Ano!.*!Ano!Ne!");
		assertEquals(4, mp.size());
		assertEquals("Ano", mp.getString(0));
		assertEquals(".*", mp.getString(1));
		assertEquals("Ano", mp.getString(2));
		assertEquals("Ne", mp.getString(3));
	}
	
}
