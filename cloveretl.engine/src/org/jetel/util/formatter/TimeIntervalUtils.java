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

import org.joda.time.Duration;
import org.joda.time.MutablePeriod;
import org.joda.time.format.PeriodFormatter;
import org.joda.time.format.PeriodFormatterBuilder;
import org.joda.time.format.PeriodParser;

/**
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created Oct 9, 2012
 */
public class TimeIntervalUtils {
	
	/**
	 * The parser uses "x" instead of "ms",
	 * which collides with "m".
	 * 
	 * It is thread-safe.
	 */
	private static final PeriodParser parser = new PeriodFormatterBuilder()
		.rejectSignedValues(true)
		.appendWeeks().appendSuffix("w")
		.appendSeparator(" ")
		.appendDays().appendSuffix("d")
		.appendSeparator(" ")
		.appendHours().appendSuffix("h")
		.appendSeparator(" ")
		.appendMinutes().appendSuffix("m")
		.appendSeparator(" ")
		.appendSeconds().appendSuffix("s")
		.appendSeparator(" ")
		.appendMillis().appendSuffix("x").toParser(); // "x" is a substitution for "ms" 
	
	/**
	 * The formatter is thread-safe.
	 */
	private static final PeriodFormatter formatter = new PeriodFormatterBuilder()
		.rejectSignedValues(true)
		.appendWeeks().appendSuffix("w")
		.appendSeparator(" ")
		.appendDays().appendSuffix("d")
		.appendSeparator(" ")
		.appendHours().appendSuffix("h")
		.appendSeparator(" ")
		.appendMinutes().appendSuffix("m")
		.appendSeparator(" ")
		.appendSeconds().appendSuffix("s")
		.appendSeparator(" ")
		.appendMillis().appendSuffix("ms").toFormatter();
	
	private static final Locale locale = new Locale("en");
	
	public enum DefaultUnit {
		MILLISECOND(1),
		SECOND(1000 * MILLISECOND.value),
		MINUTE(60 * SECOND.value),
		HOUR(60 * MINUTE.value),
		DAY(24 * HOUR.value),
		WEEK(7 * DAY.value);

		public final long value;
		
		private DefaultUnit(long value) {
			this.value = value;
		}
	}
	
	private static long parseLong(String input, DefaultUnit unit, int errorPosition) throws IllegalArgumentException {
		try {
			// try parsing it just as a plain number and multiply it with the default unit
			return Long.parseLong(input) * unit.value;
		} catch (NumberFormatException nfe) {
			// use the errorPosition returned from PeriodParser.parseInto() to throw a nicer exception
			StringBuilder sb = new StringBuilder(input.length());
			if (errorPosition < input.length()) {
				sb.append(input.substring(0, errorPosition));
				sb.append('>');
				sb.append(input.substring(errorPosition));
			} else {
				sb.append(input);
			}
			// translate back the error message
			throw new IllegalArgumentException(sb.toString().replace("x", "ms"));
		}
	}
	
	/**
	 * Parses the input as a time interval in JIRA-like syntax.
	 * Plain numbers without a unit are treated using the provided <code>unit</code>.
	 * When <code>input</code> is <code>null</code> or an empty string,
	 * <code>null</code> is returned.
	 * 
	 * Returns the duration of the interval in milliseconds.
	 * 
	 * @param input a time interval formatted using JIRA-like syntax
	 * @param unit the default unit
	 * @return duration of the interval in milliseconds
	 * @throws IllegalArgumentException
	 */
	public static long parseInterval(String input, DefaultUnit unit) throws IllegalArgumentException {
		if (input == null) {
			throw new NullPointerException("input");
		}
		if (input.isEmpty()) {
			throw new IllegalArgumentException("empty string");
		}
		input = input.replace("ms", "x"); // replace "ms", which collides with "m", with non-conflicting "x"
		
		MutablePeriod parsedPeriod = new MutablePeriod();
		
		int parseResult = parser.parseInto(parsedPeriod, input, 0, locale);
		if (parseResult < 0) {
			int position = ~parseResult; // bitwise complement to get the position of the error
			return parseLong(input, unit, position);
		} else if (parseResult < input.length()) { // the input has not been read to its end
			return parseLong(input, unit, parseResult);
		}
		return parsedPeriod.toPeriod().toStandardDuration().getMillis();
	}

	/**
	 * Parses the input as a time interval in JIRA-like syntax.
	 * Plain numbers without a unit are considered to be milliseconds.
	 * When <code>input</code> is <code>null</code> or an empty string,
	 * <code>null</code> is returned.
	 * 
	 * Returns the duration of the interval in milliseconds.
	 * 
	 * @see #parseInterval(String, DefaultUnit)
	 * 
	 * @param input a time interval formatted using JIRA-like syntax
	 * @return duration of the interval in milliseconds
	 * @throws IllegalArgumentException
	 */
	public static long parseInterval(String input) throws IllegalArgumentException {
		return parseInterval(input, DefaultUnit.MILLISECOND);
	}
	
	/**
	 * Formats the given time interval in milliseconds
	 * using JIRA-like syntax.
	 * 
	 * @param interval
	 * @return formatted interval
	 */
	public static String formatInterval(Long interval) {
		if ((interval == null) || (interval < 0)) {
			return null;
		}
		
		return formatter.print(new Duration(interval).toPeriod().normalizedStandard());
	}

	public static void main(String[] args) {
		String example = "1w 15h 1m 70s 123ms";

		long millis = parseInterval(example);
		System.out.println(formatInterval(millis));

		System.out.println("period in milliseconds: " + millis);
	}
	

}
