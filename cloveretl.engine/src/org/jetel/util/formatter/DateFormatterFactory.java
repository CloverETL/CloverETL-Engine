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

import org.jetel.exception.JetelRuntimeException;
import org.jetel.metadata.DataFieldFormatType;
import org.jetel.util.MiscUtils;
import org.threeten.bp.ZoneId;
import org.threeten.bp.format.DateTimeFormatter;
import org.threeten.bp.format.DateTimeFormatterBuilder;

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
	
	public static DateFormatter getFormatter(String format, Locale locale, TimeZoneProvider timeZoneProvider) {
		if (timeZoneProvider == null) {
			timeZoneProvider = new TimeZoneProvider();
		}
		final DataFieldFormatType formatType = DataFieldFormatType.getFormatType(format);
		if (formatType == DataFieldFormatType.JODA) {
			return new JodaDateFormatter(DataFieldFormatType.JODA.getFormat(format), locale, timeZoneProvider.getJodaTimeZone());
		} else if (formatType == DataFieldFormatType.ISO_8601) {
			return new Iso8601DateFormatter(DataFieldFormatType.ISO_8601.getFormat(format), locale, timeZoneProvider.getJodaTimeZone());
		} else {
			TimeZone tz = timeZoneProvider.getJavaTimeZone();
			if (DataFieldFormatType.getFormatType(format) == DataFieldFormatType.JAVA) {
				return new JavaDateFormatter(DataFieldFormatType.JAVA.getFormat(format), locale, tz);
			} else {
				return new JavaDateFormatter(locale, tz);
			}
		}
		
	}

	public static DateFormatter getFormatter(String format, Locale locale, String timeZoneString) {
		return getFormatter(format, locale, new TimeZoneProvider(timeZoneString));
	}

	public static DateFormatter getFormatter(String format, Locale locale) {
		return getFormatter(format, locale, (TimeZoneProvider) null);
	}

	public static DateFormatter getFormatter(String format, String localeString) {
		return getFormatter(format, localeString, null);
	}

	public static DateFormatter getFormatter(String format, String localeString, String timeZoneString) {
		return getFormatter(format, MiscUtils.createLocale(localeString), timeZoneString);
	}

	public static DateFormatter getFormatter(String format) {
		return getFormatter(format, (Locale) null);
	}

	public static DateFormatter getFormatter(Locale locale) {
		return new JavaDateFormatter(locale);
	}

	public static DateFormatter getFormatter() {
		return new JavaDateFormatter();
	}

	public static DateTimeFormatter getNanoDateFormatter(String format, String localeString, String timeZoneString) {
		return getNanoDateFormatter(format, MiscUtils.createLocale(localeString), new TimeZoneProvider(timeZoneString));
	}

	public static DateTimeFormatter getNanoDateFormatter(String format, Locale locale, TimeZoneProvider timeZoneProvider) {
		final DataFieldFormatType formatType = DataFieldFormatType.getFormatType(format);
		if (formatType == DataFieldFormatType.JAVA8) {
	        DateTimeFormatterBuilder builder = new DateTimeFormatterBuilder();
	        builder.appendPattern(DataFieldFormatType.JAVA8.getFormat(format));
	        DateTimeFormatter formatter;
	        if (locale != null) {
	        	formatter = builder.toFormatter(locale);
	        } else {
	        	formatter = builder.toFormatter();
	        }
	        ZoneId zoneId = timeZoneProvider != null ? timeZoneProvider.getJava8TimeZone() : null;
	        if (zoneId != null) {
	        	formatter = formatter.withZone(zoneId);
	        }
			return formatter;
		} else {
			throw new JetelRuntimeException("missing java8 date format pattern");
		}
	}
	
	private DateFormatterFactory() {
		throw new UnsupportedOperationException();
	}

}
