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
package org.jetel.ctl;

import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.jetel.util.formatter.TimeZoneProvider;

/**
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created May 29, 2013
 */
public class LiteralParser {
	
	private static final DateFormat DATE_FORMATTER_TEMPLATE;
	private static final DateFormat DATETIME_FORMATTER_TEMPLATE;
	
    private static final Calendar CALENDAR = Calendar.getInstance();

    static {
		DATE_FORMATTER_TEMPLATE = new SimpleDateFormat("yyyy-MM-dd");
		DATE_FORMATTER_TEMPLATE.setLenient(false);
		
		DATETIME_FORMATTER_TEMPLATE = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		DATETIME_FORMATTER_TEMPLATE.setLenient(false);
	}
	
    // if performance is insufficient, use a pool to store and reuse SimpleDateFormat instances
    private final DateFormat dateFormatter;
    private final DateFormat datetimeFormatter;
    
	public LiteralParser() {
		TimeZoneProvider timeZone = new TimeZoneProvider();
		
		// SimpleDateFormat creation is expensive, cloning is slightly faster
    	dateFormatter = (DateFormat) DATE_FORMATTER_TEMPLATE.clone();
    	dateFormatter.setTimeZone(timeZone.getJavaTimeZone());
    	
    	datetimeFormatter = (DateFormat) DATETIME_FORMATTER_TEMPLATE.clone();
    	datetimeFormatter.setTimeZone(timeZone.getJavaTimeZone());
	}

	public int parseInt(String valueImage) throws NumberFormatException {
		int valueObj;
		
		if (valueImage.startsWith("0x")) {
			// hexadecimal literal -> skip 0x
			valueObj = Integer.parseInt(valueImage.substring(2),16);
		} else if (valueImage.startsWith("-0x")) { 
			// negative hexadecimal literal -> skip -0x
			valueObj = -Integer.parseInt(valueImage.substring(3),16);
		} else if (valueImage.startsWith("-0")) {
			// negative octal literal
			valueObj = -Integer.parseInt(valueImage.substring(2),8);
		} else if (valueImage.startsWith("0")) {
			// octal literal
			valueObj = Integer.parseInt(valueImage,8);
		} else {
			// decimal literal
			valueObj = Integer.parseInt(valueImage);
		}
		
		return valueObj;
	}
	
	public long parseLong(String valueImage) throws NumberFormatException {
		long valueObj;
		
		valueImage = stripDistincter(valueImage,'l','L');
		if (valueImage.startsWith("0x")) {
			// hexadecimal literal -> remove 0x prefix
			valueObj = Long.parseLong(valueImage.substring(2),16);
		} else if (valueImage.startsWith("-0x")) {
			valueObj = -Long.parseLong(valueImage.substring(3),16);
	    } else if (valueImage.startsWith("0")) {
			// octal literal -> 0 is the distincter, but Java handles that correctly
			valueObj = Long.parseLong(valueImage,8);
	    } else if (valueImage.startsWith("-0")) {
			// negative octal literal
			valueObj = -Long.parseLong(valueImage.substring(2),8);
		} else {
			valueObj = Long.parseLong(valueImage);
		}
		
		return valueObj;
	}
	
	public BigDecimal parseBigDecimal(String valueImage) throws NumberFormatException, ArithmeticException {
		BigDecimal valueObj;
		
		valueObj = new BigDecimal(stripDistincter(valueImage,'d','D'),TransformLangExecutor.MAX_PRECISION);

		return valueObj;
	}
	
	public double parseDouble(String valueImage) throws NumberFormatException {
		double valueObj;
		
        valueObj = Double.parseDouble(stripDistincter(valueImage,'f','F'));
		
		return valueObj;
	}
	
	public boolean parseBoolean(String valueImage) {
		boolean valueObj;
		
		valueObj = Boolean.parseBoolean(valueImage);
		
		return valueObj;
	}
	
	/**
	 * This method is not thread-safe, uses {@link SimpleDateFormat} and {@link Calendar}.
	 * 
	 * @param valueImage
	 * @return
	 * @throws ParseException
	 */
	public Date parseDate(String valueImage) throws ParseException {
		Date valueObj;
		
		final ParsePosition p1 = new ParsePosition(0);
		Date date = dateFormatter.parse(valueImage, p1);
		if (date == null) {
			throw new ParseException("Date literal '" + valueImage + "' could not be parsed.", 0);
		}
		if (p1.getIndex() < valueImage.length() - 1) {
			throw new ParseException("Date literal '" + valueImage + "' has invalid format.", p1.getErrorIndex()); 
		}
		
		synchronized (CALENDAR) { // Calendar is not thread-safe and we're calling multiple methods
			CALENDAR.setTime(date);
			// set all time fields to zero
			CALENDAR.set(Calendar.HOUR, 0);
			CALENDAR.set(Calendar.MINUTE, 0);
			CALENDAR.set(Calendar.SECOND, 0);
			CALENDAR.set(Calendar.MILLISECOND, 0);
			valueObj = CALENDAR.getTime();
		}
		
		return valueObj;
	}
	
	/**
	 * This method is not thread-safe, uses {@link SimpleDateFormat}.
	 * 
	 * @param valueImage
	 * @return
	 * @throws ParseException
	 */
	public Date parseDateTime(String valueImage) throws ParseException {
		Date valueObj;
		
		final ParsePosition p2 = new ParsePosition(0);
		valueObj = datetimeFormatter.parse(valueImage,p2);

		if (valueObj == null) {
			throw new ParseException("Date-time literal '" + valueImage + "' could not be parsed.", 0);
		}
		if (p2.getIndex() < valueImage.length() - 1) {
			throw new ParseException("Date-time literal '" + valueImage + "' has invalid format.", p2.getErrorIndex());
		}
		
		return valueObj;
	}

	private String stripDistincter(String input, char... dist) {
		final char c = input.charAt(input.length()-1);
		for (int i=0; i<dist.length; i++) {
			if (c == dist[i]) {
				return input.substring(0,input.length()-1);
			}
		}
		
		return input;
	}
}
