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

import java.util.Locale;

import org.jetel.util.MiscUtils;

/**
 * Factory for internally used date formatters.
 *
 * @author Martin Janik, Javlin a.s. &lt;martin.janik@javlin.eu&gt;
 * @author David Pavlis, Javlin a.s. &lt;david.pavlis@javlin.eu&gt;
 *
 * @version 16th April 2010
 * @created 17th August 2009
 */
public final class DateFormatterFactory {

	/** the Java prefix specifying date format strings used by the Java's DateFormat class */
	public static final String JAVA_FORMAT_PREFIX = "java:";
	/** the Joda-Time prefix specifying date format strings used by the Joda-Time's DateTimeFormatter class */
	public static final String JODA_FORMAT_PREFIX = "joda:";

	public static DateFormatter getFormatter(String formatString, Locale locale) {
		if (locale == null) {
			locale = Locale.getDefault();
		}

		if (formatString == null) {
			return new JodaDateFormatter(locale);
		} else if (formatString.startsWith(JAVA_FORMAT_PREFIX)) {
			return new JavaDateFormatter(formatString.substring(JAVA_FORMAT_PREFIX.length()), locale);
		} else {
			if (formatString.startsWith(JODA_FORMAT_PREFIX)) {
				formatString = formatString.substring(JODA_FORMAT_PREFIX.length());
			}

			return new JodaDateFormatter(formatString, locale);
		}
	}

	public static DateFormatter getFormatter(String formatString, String localeString) {
		return getFormatter(formatString, MiscUtils.createLocale(localeString));
	}

	public static DateFormatter getFormatter(String formatString) {
		return getFormatter(formatString, Locale.getDefault());
	}

	public static DateFormatter getFormatter(Locale locale) {
		return new JavaDateFormatter(locale);
	}

	public static DateFormatter getFormatter() {
		return new JavaDateFormatter();
	}

	private DateFormatterFactory() {
		throw new UnsupportedOperationException();
	}

}
