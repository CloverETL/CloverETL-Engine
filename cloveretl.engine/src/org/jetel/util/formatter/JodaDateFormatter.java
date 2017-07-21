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

import org.jetel.data.Defaults;
import org.jetel.util.MiscUtils;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * Represents the Joda Time based date formatters.
 *
 * @author Martin Janik, Javlin a.s. &lt;martin.janik@javlin.eu&gt;
 *
 * @version 17th August 2009
 * @since 10th August 2009
 */
class JodaDateFormatter implements DateFormatter {

	/** Joda-Time date time formatter */
	private final DateTimeFormatter dateTimeFormatter;
	private String pattern;
	private Locale locale;

	public JodaDateFormatter() {
		this(null);
	}

	public JodaDateFormatter(Locale locale) {
		this(null, locale);
	}

	public JodaDateFormatter(String pattern, Locale locale) {
		this(pattern, locale, null);
	}

	public JodaDateFormatter(String pattern, Locale locale, DateTimeZone timeZone) {
		if (pattern == null) {
			pattern = Defaults.DEFAULT_DATE_FORMAT;
		}
		if (locale == null) {
			locale = MiscUtils.getDefaultLocale();
		}
		DateTimeFormatter formatter = DateTimeFormat.forPattern(pattern).withLocale(locale);
		if (timeZone != null) {
			formatter = formatter.withZone(timeZone);
		}
		this.dateTimeFormatter = formatter;
		this.pattern = pattern;
		this.locale = locale;
	}

	@Override
	public String format(Date value) {
		return dateTimeFormatter.print(value.getTime());
	}

	@Override
	public Date parseDate(String value) {
		return new Date(parseMillis(value));
	}

	@Override
	public long parseMillis(String value) {
		return dateTimeFormatter.parseMillis(value);
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
		try{
			dateTimeFormatter.parseMillis(value);
		}catch(IllegalArgumentException ex){
			return false;
		}
		return true;
	}

	@Override
	public void setLenient(boolean lenient) {
		//DO NOTHING - no lenient parsing option
	}

}