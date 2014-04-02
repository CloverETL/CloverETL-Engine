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

import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

/**
 * ISO-8601 date formatter suitable for processing dates from XML documents.
 * 
 * @author jan.michalica (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 1.4.2014
 */
enum Iso8601DateFormatter implements DateFormatter {
	
	date(ISODateTimeFormat.dateParser(), ISODateTimeFormat.date()),
	time(ISODateTimeFormat.timeParser(), ISODateTimeFormat.time()),
	dateTime(ISODateTimeFormat.dateTimeParser(), ISODateTimeFormat.dateTime());
	
	private final DateTimeFormatter parser;
	private final DateTimeFormatter printer;
	
	private Iso8601DateFormatter(DateTimeFormatter parser, DateTimeFormatter printer) {
		this.parser = parser;
		this.printer = printer;
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
		return name();
	}

	@Override
	public Locale getLocale() {
		return null;
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
