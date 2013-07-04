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

import java.text.DateFormat;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.jetel.data.Defaults;
import org.jetel.util.MiscUtils;

/**
 * Represents the Java based date formatters.
 *
 * @author Martin Janik, Javlin a.s. &lt;martin.janik@javlin.eu&gt;
 *
 * @version 17th August 2009
 * @since 10th August 2009
 */
class JavaDateFormatter implements DateFormatter {

	/** classic Java date format */
	private final DateFormat dateFormat;
	private ParsePosition position=new ParsePosition(0);
	private Locale locale;
	
	public JavaDateFormatter() {
		this(null);
	}

	public JavaDateFormatter(Locale locale) {
		this(null, locale);
	}

	public JavaDateFormatter(String pattern, Locale locale) {
		if (locale == null) {
			locale = MiscUtils.createLocale(Defaults.DEFAULT_LOCALE);
		}
		if (pattern == null) {
			pattern = Defaults.DEFAULT_DATE_FORMAT;
		}
		this.dateFormat = new SimpleDateFormat(pattern, locale);
		this.dateFormat.setLenient(false);
		this.locale = locale;
	}

	@Override
	public String format(Date value) {
		return dateFormat.format(value);
	}

	@Override
	public Date parseDate(String value) {
		position.setIndex(0);
		final Date date=dateFormat.parse(value,position);
		if (position.getIndex()==0)
			throw new IllegalArgumentException("Unparseable date: \"" + value + "\" at position "+
	                position.getErrorIndex());
		return date;
	}
	
	@Override
	public Date parseDateStrict(String value) {
		position.setIndex(0);
		final Date date=dateFormat.parse(value,position);
		if (position.getIndex()==0)
			throw new IllegalArgumentException("Unparseable date: \"" + value + "\" at position "+
			        position.getErrorIndex());
		if (position.getIndex() != value.length()) {
			throw new IllegalArgumentException("Invalid string \"" + value.substring(position.getIndex()) + "\" at the end of the parsed value");
		}
		return date;
	}

	@Override
	public long parseMillis(String value) {
		return parseDate(value).getTime();
	}

	@Override
	public String getPattern() {
		return ((SimpleDateFormat)dateFormat).toPattern();
	}

	@Override
	public Locale getLocale() {
		return locale;
	}
	
	@Override
	public boolean tryParse(String value) {
		position.setIndex(0);
		dateFormat.parse(value,position);
		if (position.getIndex()==0)
			return false;
		else
			return true;
	}

	@Override
	public void setLenient(boolean lenient) {
		dateFormat.setLenient(lenient);
	}
	
}