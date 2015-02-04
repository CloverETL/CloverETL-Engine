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

import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.ParsePosition;

import org.jetel.util.ExceptionUtils;

/**
 * @author csochor (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created Jun 29, 2010
 */
public class JavaNumericFormatter implements NumericFormatter {

	private final String formatPattern;
	
	private final NumberFormat numberFormat;
	
	private final ParsePosition tempParsePosition;

	@Override
	public String toString() {
		return "[JavaNumericFormatter]" + getFormatPattern();
	}

	public JavaNumericFormatter(String formatPattern, NumberFormat numberFormat) {
		if (numberFormat == null) {
			throw new IllegalArgumentException("numberFormat is required");
		}
		this.formatPattern = formatPattern;
		this.numberFormat = numberFormat;
		this.tempParsePosition = new ParsePosition(0);
	}

	@Override
	public int parseInt(CharSequence seq) throws ParseException {
		final ParsePosition parsePosition = getTempParsePostion();
		
		//try to parse the given string to number
		Number resultNumber = numberFormat.parse(seq.toString(), parsePosition); //this toString() conversion is potential performance issue
		if (resultNumber == null || parsePosition.getErrorIndex() != -1 || parsePosition.getIndex() != seq.length()) {
			//parsing failed
			throw new ParseException("Integer parsing error.", -1);
		}
		
		if (resultNumber instanceof Long) { // result is long - seems to be ok
			long resultLong = resultNumber.longValue();
			if (resultLong <= Integer.MAX_VALUE && resultLong > Integer.MIN_VALUE) {
				return (int) resultLong;
			} else { // result is out of range
				throw new ParseException("Out of integer range.", -1);
			}
		} else { // result is double or something like this
			throw new ParseException("String does not represent an integer value.", -1);
		}
	}

	@Override
	public String formatInt(int value) {
		return numberFormat.format(value);
	}

	@Override
	public String formatLong(long value) {
		return numberFormat.format(value);
	}

	@Override
	public long parseLong(CharSequence seq) throws ParseException {
		final ParsePosition parsePosition = getTempParsePostion();
		
		//try to parse the given string to number
		Number resultNumber = numberFormat.parse(seq.toString(), parsePosition); //this toString() conversion is potential performance issue
		if (resultNumber == null || parsePosition.getErrorIndex() != -1 || parsePosition.getIndex() != seq.length()) {
			//parsing failed
			throw new ParseException("Long parsing error.", -1);
		}
		
		if (resultNumber instanceof Long) { // result is long - seems to be ok
			long resultLong = resultNumber.longValue();
			if (resultLong != Long.MIN_VALUE) {
				return resultLong;
			} else { // result is out of range
				throw new ParseException("Out of long range.", -1);
			}
		} else { // result is double or something like this
			throw new ParseException("String does not represent a long value.", -1);
		}
	}

	@Override
	public String formatDouble(double value) {
		return numberFormat.format(value);
	}

	@Override
	public double parseDouble(CharSequence seq) throws ParseException {
		final ParsePosition parsePosition = getTempParsePostion();
		
		//try to parse the given string to number
		Number resultNumber = numberFormat.parse(seq.toString(), parsePosition); //this toString() conversion is potential performance issue
		if (resultNumber == null || parsePosition.getErrorIndex() != -1 || parsePosition.getIndex() != seq.length()) {
			//parsing failed
			throw new ParseException("Double parsing error.", -1);
		}

		return resultNumber.doubleValue();
	}

	@Override
	public String formatBigDecimal(BigDecimal bd) {
		if (bd == null) {
			return "NaN";
		} else {
			return numberFormat.format(bd);
		}
	}

	/**
	 * !!! Underlying NumberFormat has to be switched to ParseBigDecimal mode.
	 * !!! {@link DecimalFormat#setParseBigDecimal(boolean)}
	 */
	@Override
	public BigDecimal parseBigDecimal(CharSequence seq) throws ParseException {
		try {
			return (BigDecimal) numberFormat.parse(seq.toString());
		} catch (NumberFormatException e) {
			throw new ParseException(ExceptionUtils.getMessage(e), -1);
		}
	}

	private ParsePosition getTempParsePostion() {
		tempParsePosition.setIndex(0);
		tempParsePosition.setErrorIndex(-1);
		return tempParsePosition;
	}

	@Override
	public String getFormatPattern() {
		return formatPattern;
	}
	
}
