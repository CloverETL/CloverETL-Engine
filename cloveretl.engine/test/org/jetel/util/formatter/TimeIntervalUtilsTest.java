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
package org.jetel.util.formatter;

import static org.jetel.util.formatter.TimeIntervalUtils.formatInterval;
import static org.jetel.util.formatter.TimeIntervalUtils.parseInterval;
import junit.framework.TestCase;

import org.jetel.util.formatter.TimeIntervalUtils.DefaultUnit;

/**
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created Oct 9, 2012
 */
public class TimeIntervalUtilsTest extends TestCase {
	
	private static final long ms = 1;
	private static final long s = 1000 * ms;
	private static final long m = 60 * s;
	private static final long h = 60 * m;
	private static final long d = 24 * h;
	private static final long w = 7 * d;
	
	/**
	 * Test method for {@link org.jetel.util.formatter.TimeIntervalUtils#parseInterval(java.lang.String)}.
	 */
	public void testParseInterval() {
		assertEquals(1*w + 15*h + 2*m + 10*s + 123*ms, parseInterval("1w 15h 2m 10s 123ms"));
		assertEquals(1*w + 23*m, parseInterval("1w 23m"));
		assertEquals(1*h + 3*m + 23*ms, parseInterval("1h 3m 23ms"));
		assertEquals(1*h + 23*ms, parseInterval("1h 23ms"));
		assertEquals(23*ms, parseInterval("23ms"));
		assertEquals(1*m + 23*ms, parseInterval("1m 23ms"));
		assertEquals(1*w + 23*ms, parseInterval("1w 23ms"));
		assertEquals(10*ms, parseInterval("10"));
		assertEquals(10*ms, parseInterval("10", DefaultUnit.MILLISECOND));
		assertEquals(10*s, parseInterval("10", DefaultUnit.SECOND));
		assertEquals(10*m, parseInterval("10", DefaultUnit.MINUTE));
		assertEquals(10*h, parseInterval("10", DefaultUnit.HOUR));
		assertEquals(10*d, parseInterval("10", DefaultUnit.DAY));
		assertEquals(10*w, parseInterval("10", DefaultUnit.WEEK));
		
		try {
			parseInterval("1.5h");
			fail();
		} catch (IllegalArgumentException ex) {
			System.err.println(ex.getMessage());
		}

		try {
			parseInterval("1h 10");
			fail();
		} catch (IllegalArgumentException ex) {
			System.err.println(ex.getMessage());
		}

		try {
			parseInterval("3s 1h");
			fail();
		} catch (IllegalArgumentException ex) {
			System.err.println(ex.getMessage());
		}

		try {
			parseInterval("3ms 1h");
			fail();
		} catch (IllegalArgumentException ex) {
			System.err.println(ex.getMessage());
		}

		try {
			parseInterval("3ms 1s");
			fail();
		} catch (IllegalArgumentException ex) {
			System.err.println(ex.getMessage());
		}

		try {
			parseInterval("3s 1h");
			fail();
		} catch (IllegalArgumentException ex) {
			System.err.println(ex.getMessage());
		}

		try {
			TimeIntervalUtils.parseInterval(null);
			fail();
		} catch (NullPointerException ex) {
			System.err.println(ex.getMessage());
		}

		try {
			TimeIntervalUtils.parseInterval("");
			fail();
		} catch (IllegalArgumentException ex) {
			System.err.println(ex.getMessage());
		}

	}

	/**
	 * Test method for {@link org.jetel.util.formatter.TimeIntervalUtils#formatInterval(long)}.
	 */
	public void testFormatInterval() {
		assertEquals("1w 15h 2m 10s 123ms", formatInterval(1*w + 15*h + 2*m + 10*s + 123*ms));
		assertEquals("1w 23ms", formatInterval(1*w + 23*ms));
		assertEquals(null, formatInterval(null));
		assertEquals(null, formatInterval(-1L));
	}

}
