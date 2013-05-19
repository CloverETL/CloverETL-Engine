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
package org.jetel.component.validator.utils.convertors;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

import org.jetel.component.validator.rules.DateValidationRule;
import org.jetel.data.Defaults;
import org.jetel.metadata.DataFieldFormatType;
import org.jetel.util.string.CloverString;
import org.jetel.util.string.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * Implementation of converter for converting something into date.
 * Supports JODA and JAVA formatting syntax.
 * 
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 15.1.2013
 * @see DateValidationRule
 */
public class DateConverter implements Converter {
	private String format;
	private Locale locale;
	private TimeZone timezone;
	
	private DateConverter(String format, Locale locale, TimeZone timezone) {
		this.format = format;
		this.locale = locale;
		this.timezone = timezone;
	}
	
	/**
	 * Create instance of date converter
	 * @param format Formatting mask used for parsing the date from input
	 * @param locale Locale to be used for parsing
	 * @param timezone Timezone to be used for parsing
	 */	
	public static DateConverter newInstance(String format, Locale locale, TimeZone timezone) {
		return new DateConverter(format, locale, timezone);
	}

	@Override
	public Date convert(Object o) {
		if (o instanceof CloverString || o instanceof String) {
			String tempInput = o.toString();
			DataFieldFormatType formatType = DataFieldFormatType.getFormatType(format);
			if(formatType == DataFieldFormatType.JAVA || formatType == null) {
				try {
					SimpleDateFormat dateFormat = new SimpleDateFormat(formatType.getFormat(format), locale);
					dateFormat.setTimeZone(timezone);
					
					Date parsedDate = dateFormat.parse(tempInput);
					if(!dateFormat.format(parsedDate).equals(tempInput.trim())) {
						return null;
					}
					return parsedDate;
				} catch (Exception ex) {
					return null;	
				}
			} else {
				try {
					DateTimeFormatter formatter = DateTimeFormat.forPattern(formatType.getFormat(format));
					formatter = formatter.withLocale(locale).withZone(DateTimeZone.forID(timezone.getID()));
					DateTime parsedDate = formatter.parseDateTime(tempInput);
					if(!parsedDate.toString(formatter).equals(tempInput.trim())) {
						return null;
					}
					return parsedDate.toDate();
				} catch (Exception ex) {
					return null;
				}
			}
		}
		return null;
	}
	
	/**
	 * Inspired by {@link CreateFiles#fromXML}
	 */
	@Override
	public Date convertFromCloverLiteral(String o) {
		SimpleDateFormat format = new SimpleDateFormat(Defaults.DEFAULT_DATETIME_FORMAT);
		format.setTimeZone(TimeZone.getTimeZone("UTC"));
    	if (!StringUtils.isEmpty(o)) {
    		try {
    			return format.parse(o);
    		} catch (ParseException ex) {
    			format = new SimpleDateFormat(Defaults.DEFAULT_DATE_FORMAT);
    			format.setTimeZone(TimeZone.getTimeZone("UTC"));
    			try {
    				return format.parse(o);
    			} catch (ParseException ex2) {
    				return null;
    			}
    		}
    	}
    	return null;
	}

}
