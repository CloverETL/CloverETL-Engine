/*
 * jETeL/Clover.ETL - Java based ETL application framework.
 * Copyright (C) 2002-2009  David Pavlis <david.pavlis@javlin.eu>
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
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
package org.jetel.util.date;

/**
 * Date utility class.
 *
 * @author Martin Janik, Javlin a.s. &lt;martin.janik@javlin.eu&gt;
 *
 * @version 16th April 2010
 * @since 17th August 2009
 */
public final class DateUtils {

	/**
	 * Strips the format prefix (currently <i>java:</i> or <i>joda:</i>) from the beginning of the given format string.
	 * If no prefix is found, the format string is left unchanged.
	 *
	 * @param formatString a format string to be stripped of the prefix
	 *
	 * @return the format string with the prefix stripped
	 */
	public static String stripFormatPrefix(String formatString) {
		if (formatString == null) {
			return null;
		}

		if (formatString.startsWith(DateFormatterFactory.JAVA_FORMAT_PREFIX)) {
			return formatString.substring(DateFormatterFactory.JAVA_FORMAT_PREFIX.length());
		}

		if (formatString.startsWith(DateFormatterFactory.JODA_FORMAT_PREFIX)) {
			return formatString.substring(DateFormatterFactory.JODA_FORMAT_PREFIX.length());
		}

		return formatString;
	}

	private DateUtils() {
		throw new UnsupportedOperationException();
	}

}
