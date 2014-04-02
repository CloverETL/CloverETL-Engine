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
import java.util.TimeZone;

import org.jetel.metadata.DataFieldFormatType;
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
	
	public static DateFormatter getFormatter(String formatString, Locale locale, String timeZoneId) {
		TimeZoneProvider timeZoneProvider = new TimeZoneProvider(timeZoneId);
		final DataFieldFormatType formatType = DataFieldFormatType.getFormatType(formatString);
		if (formatType == DataFieldFormatType.JODA) {
			return new JodaDateFormatter(DataFieldFormatType.JODA.getFormat(formatString), locale, timeZoneProvider.getJodaTimeZone());
		} else if (formatType == DataFieldFormatType.ISO_8601) {
			return Iso8601DateFormatter.valueOf(DataFieldFormatType.ISO_8601.getFormat(formatString));
		} else {
			TimeZone tz = timeZoneProvider.getJavaTimeZone();
			if (DataFieldFormatType.getFormatType(formatString) == DataFieldFormatType.JAVA) {
				return new JavaDateFormatter(DataFieldFormatType.JAVA.getFormat(formatString), locale, tz);
			} else {
				return new JavaDateFormatter(locale, tz);
			}
		}
		
	}

	public static DateFormatter getFormatter(String formatString, Locale locale) {
		return getFormatter(formatString, locale, null);
	}

	public static DateFormatter getFormatter(String formatString, String localeString) {
		return getFormatter(formatString, localeString, null);
	}

	public static DateFormatter getFormatter(String formatString, String localeString, String timeZoneString) {
		return getFormatter(formatString, MiscUtils.createLocale(localeString), timeZoneString);
	}

	public static DateFormatter getFormatter(String formatString) {
		return getFormatter(formatString, (Locale) null);
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
