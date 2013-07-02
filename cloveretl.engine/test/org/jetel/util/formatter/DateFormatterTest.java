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

import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

import org.jetel.test.CloverTestCase;

/**
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created May 28, 2013
 */
public class DateFormatterTest extends CloverTestCase {
	
	private DateFormatter formatter;
	private Calendar calendar;

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		initEngine();
		calendar = Calendar.getInstance();
		calendar.set(Calendar.MILLISECOND, 0);
	}

	@Override
	protected void tearDown() throws Exception {
		super.tearDown();
		formatter = null;
		calendar = null;
	}
	
	public void testParseDate() throws Exception {
		Date parsed = null;
		Date expected = null;

		// base case, set on metadata field
		formatter = DateFormatterFactory.getFormatter("dd.MM.yyyy HH:mm:ss", (Locale) null, "GMT+4");
		parsed = formatter.parseDate("28.5.2013 15:34:12");
		expected = getTime(28,5,2013, 5,34,12, "GMT-6");
		assertEquals(expected, parsed);

		// base case with Joda, set on metadata field
		formatter = DateFormatterFactory.getFormatter("joda:dd.MM.yyyy HH:mm:ss", (Locale) null, "joda:+04:00");
		parsed = formatter.parseDate("28.5.2013 15:34:12");
		expected = getTime(28,5,2013, 5,34,12, "GMT-6");
		assertEquals(expected, parsed);

		// Joda format, but no Joda time zone
		try {
			formatter = DateFormatterFactory.getFormatter("joda:dd.MM.yyyy HH:mm:ss", (Locale) null, "GMT+4");
			fail("IllegalStateException expected");
		} catch (IllegalStateException ex) {}
		
		// Java format, but no Java time zone
		try {
			formatter = DateFormatterFactory.getFormatter("dd.MM.yyyy HH:mm:ss", (Locale) null, "joda:+04:00");
			fail("IllegalStateException expected");
		} catch (IllegalStateException ex) {}

		// Java format, default timezone
		formatter = DateFormatterFactory.getFormatter("dd.MM.yyyy HH:mm:ss", (Locale) null, null);

		// Joda format, default timezone
		formatter = DateFormatterFactory.getFormatter("joda:dd.MM.yyyy HH:mm:ss", (Locale) null, null);

		// Java format, both timezone formats
		formatter = DateFormatterFactory.getFormatter("dd.MM.yyyy HH:mm:ss", (Locale) null, "GMT+4;joda:+04:00");
		parsed = formatter.parseDate("28.05.2013 15:34:12");
		expected = getTime(28,05,2013, 5,34,12, "GMT-6");
		assertEquals(expected, parsed);

		// Joda format, both timezone formats
		formatter = DateFormatterFactory.getFormatter("joda:dd.MM.yyyy HH:mm:ss", (Locale) null, "GMT+4;joda:+04:00");
		parsed = formatter.parseDate("28.05.2013 15:34:12");
		expected = getTime(28,05,2013, 5,34,12, "GMT-6");
		assertEquals(expected, parsed);

		// Java format, both timezone formats, quoted
		formatter = DateFormatterFactory.getFormatter("dd.MM.yyyy HH:mm:ss", (Locale) null, "'GMT+4';'joda:+04:00'");
		parsed = formatter.parseDate("28.05.2013 15:34:12");
		expected = getTime(28,05,2013, 5,34,12, "GMT-6");
		assertEquals(expected, parsed);

		// Joda format, both timezone formats, quoted
		formatter = DateFormatterFactory.getFormatter("joda:dd.MM.yyyy HH:mm:ss", (Locale) null, "'GMT+4';'joda:+04:00'");
		parsed = formatter.parseDate("28.05.2013 15:34:12");
		expected = getTime(28,05,2013, 5,34,12, "GMT-6");
		assertEquals(expected, parsed);
	}
	
	public void testFormat() throws Exception {
		Date date = null;
		String result = null;
		String expected = null;
		
		formatter = DateFormatterFactory.getFormatter("dd.MM.yyyy HH:mm:ss Z", (Locale) null, "GMT+4");
		date = getTime(28,05,2013, 5,34,12, "GMT-6");
		result = formatter.format(date);
		expected = "28.05.2013 15:34:12 +0400";
		assertEquals(expected, result);

		formatter = DateFormatterFactory.getFormatter("joda:dd.MM.yyyy HH:mm:ss Z", (Locale) null, "joda:+04:00");
		date = getTime(28,05,2013, 5,34,12, "GMT-6");
		result = formatter.format(date);
		expected = "28.05.2013 15:34:12 +0400";
		assertEquals(expected, result);
	}
	
	private Date getTime(int day, int month, int year, int hour, int minute, int second, String timeZone) {
		calendar.setTimeZone(TimeZone.getTimeZone(timeZone));
		calendar.set(year, month-1, day, hour, minute, second);
		return calendar.getTime();
	}

}
