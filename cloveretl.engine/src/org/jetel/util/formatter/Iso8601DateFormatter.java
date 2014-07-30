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

import java.util.Date;
import java.util.Locale;

import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

/**
 * Represents the date formatter for ISO8601 standard.
 * 
 * @author jan.michalica (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 1.4.2014
 */
class Iso8601DateFormatter implements DateFormatter {
	
	private DateTimeFormatter parser;
	private DateTimeFormatter printer;
	
	private String pattern;
	private Locale locale;
	
	public Iso8601DateFormatter(String format, Locale locale, DateTimeZone dateTimeZone) {
		switch (format) {
        	case "date":
        		parser = ISODateTimeFormat.dateParser().withZone(dateTimeZone);
        		printer = ISODateTimeFormat.date().withZone(dateTimeZone);
        		break;
        	case "time":
        		parser = ISODateTimeFormat.timeParser().withZone(dateTimeZone);
        		printer = ISODateTimeFormat.time().withZone(dateTimeZone);
        		break;
        	default:
        		// "dateTime"
        		parser = ISODateTimeFormat.dateTimeParser().withZone(dateTimeZone);
        		printer = ISODateTimeFormat.dateTime().withZone(dateTimeZone);
		}
		
		this.locale = locale;
		this.pattern = format;
		
		if (locale != null) {
			parser = parser.withLocale(locale);
			printer = printer.withLocale(locale);
		}
	}
	
	@Override
	public String format(Date value) {
		return printer.print(value.getTime());
	}

	@Override
	public Date parseDate(String value) {
		return new Date(parser.parseMillis(value));
	}

	@Override
	public Date parseDateStrict(String value) {
		return parseDate(value);
	}

	@Override
	public long parseMillis(String value) {
		return parser.parseMillis(value);
	}

	@Override
	public String getPattern() {
		return pattern;
	}

	@Override
	public Locale getLocale() {
		return locale;
	}

	@Override
	public boolean tryParse(String value) {
		if (value == null) {
			return false;
		}
		try {
			parseMillis(value);
			return true;
		} catch (IllegalArgumentException e) {
			return false;
		}
	}

	@Override
	public void setLenient(boolean lenient) {}
}
