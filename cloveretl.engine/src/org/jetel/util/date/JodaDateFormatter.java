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
package org.jetel.util.date;

import java.util.Date;
import java.util.Locale;

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

	public JodaDateFormatter() {
		this.dateTimeFormatter = DateTimeFormat.mediumDate();
	}

	public JodaDateFormatter(Locale locale) {
		this.dateTimeFormatter = DateTimeFormat.mediumDate().withLocale(locale);
	}

	public JodaDateFormatter(String pattern, Locale locale) {
		this.dateTimeFormatter = DateTimeFormat.forPattern(pattern).withLocale(locale);
		this.pattern=pattern;
	}

	public String format(Date value) {
		return dateTimeFormatter.print(value.getTime());
	}

	public Date parseDate(String value) {
		return new Date(parseMillis(value));
	}

	public long parseMillis(String value) {
		return dateTimeFormatter.parseMillis(value);
	}

	public String getPattern() {
		return pattern;
	}

	public boolean tryParse(String value) {
		try{
			dateTimeFormatter.parseMillis(value);
		}catch(IllegalArgumentException ex){
			return false;
		}
		return true;
	}

	public void setLenient(boolean lenient) {
		//DO NOTHING - no lenient parsing option
	}

}