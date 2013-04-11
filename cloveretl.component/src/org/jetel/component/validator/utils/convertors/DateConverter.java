/*
 * CloverETL Engine - Java based ETL application framework.
 * Copyright (c) Javlin, a.s. (info@cloveretl.com).  Use is subject to license terms.
 *
 * www.cloveretl.com
 */
package org.jetel.component.validator.utils.convertors;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

import org.jetel.data.Defaults;
import org.jetel.metadata.DataFieldFormatType;
import org.jetel.util.string.CloverString;
import org.jetel.util.string.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 15.1.2013
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
	 * {@link CreateFiles#fromXML}
	 */
	@Override
	public Date convertFromCloverLiteral(String o) {
		SimpleDateFormat format = new SimpleDateFormat(Defaults.DEFAULT_DATETIME_FORMAT);
    	if (!StringUtils.isEmpty(o)) {
    		try {
    			return format.parse(o);
    		} catch (ParseException ex) {
                return null;
    		}
    	}
    	return null;
	}

}
