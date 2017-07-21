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

import org.jetel.util.CloverPublicAPI;

/**
 * Unified interface of internally used date formatters.
 *
 * @author Martin Janik, Javlin a.s. &lt;martin.janik@javlin.eu&gt;
 * @author David Pavlis, Javlin a.s. &lt;david.pavlis@javlin.eu&gt;
 *
 * @version 16th April 2010
 * @created 10th August 2009
 */
@CloverPublicAPI
public interface DateFormatter {

	/**
	 * Formats the given date value.
	 *
	 * @param value a date value to be formatted
	 *
	 * @return a string representation of the date value
	 */
	public String format(Date value);

	/**
	 * Parses the string containing a date value.
	 *
	 * @param value a string representation of a date value
	 *
	 * @return the date value
	 *
	 * @throws IllegalArgumentException if the string value has invalid format
	 */
	public Date parseDate(String value);
	
	/**
	 * Parses the string containing a date value. Fails also when there's invalid text after the parsed date.
	 *
	 * @param value a string representation of a date value
	 *
	 * @return the date value
	 *
	 * @throws IllegalArgumentException if the string value has invalid format
	 * or if there's invalid extra text after the parsed value
	 */
	public Date parseDateStrict(String value);
	
	/**
	 * Parses the date from string using {@link #parseDateStrict()} and does additional validation.
	 * 
	 * This parsing method first parses the string and then formats the Date to another string. Formatted string is
	 * compared to the input string and when they are not equal an exception is thrown. This is the most strict version of
	 * parsing currently possible.
	 *
	 * @param value a string representation of a date value
	 *
	 * @return the date value
	 *
	 * @throws IllegalArgumentException if the string value has invalid format
	 * or if there's invalid extra text after the parsed value or if the parsed string does not match pattern exactly
	 */
	public Date parseDateExactMatch(String value);
	

	/**
	 * Parses the string containing a date value.
	 *
	 * @param value a string representation of a date value
	 *
	 * @return the date value in milliseconds
	 *
	 * @throws IllegalArgumentException if the string value has invalid format
	 */
	public long parseMillis(String value);
	
	/**
	 * Returns the pattern string used for creating this formatter.
	 * 
	 * @return pattern string
	 */
	public String getPattern();
	
	/**
	 * Returns the local used for creating this formatter.
	 * @return locale definition
	 */
	public Locale getLocale();
	
	/**
	 * Try to parse input value to Date
	 * 
	 * @param value
	 * @return true if input can be successfully parsed, otherwise false
	 */
	public boolean tryParse(String value);

    /**
     * Specify whether or not date/time parsing is to be lenient.  With
     * lenient parsing, the parser may use heuristics to interpret inputs that
     * do not precisely match this object's format.  With strict parsing,
     * inputs must match this object's format.
     */
	public void setLenient(boolean lenient);
	
}