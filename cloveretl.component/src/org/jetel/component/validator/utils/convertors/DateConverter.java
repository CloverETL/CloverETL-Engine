/*
 * CloverETL Engine - Java based ETL application framework.
 * Copyright (c) Javlin, a.s. (info@cloveretl.com).  Use is subject to license terms.
 *
 * www.cloveretl.com
 */
package org.jetel.component.validator.utils.convertors;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.jetel.component.validator.ValidationNode.State;
import org.jetel.data.Defaults;
import org.jetel.data.primitive.Decimal;
import org.jetel.data.primitive.DecimalFactory;
import org.jetel.metadata.DataFieldFormatType;
import org.jetel.util.string.CloverString;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 15.1.2013
 */
public class DateConverter implements Converter {
	private String format;
	private boolean strict;
	private Locale locale;
	
	private DateConverter(String format, boolean strict, Locale locale) {
		this.format = format;
		this.strict = strict;
		this.locale = locale;
	}
	
	public static DateConverter newInstance(String format, boolean strict, Locale locale) {
		return new DateConverter(format, strict, locale);
	}

	@Override
	public Date convert(Object o) {
		if (o instanceof CloverString || o instanceof String) {
			String tempInput = o.toString();
			DataFieldFormatType formatType = DataFieldFormatType.getFormatType(format);
			if(formatType == DataFieldFormatType.JAVA || formatType == null) {
				try {
					SimpleDateFormat dateFormat;
					if (formatType == null) {
						dateFormat = new SimpleDateFormat(Defaults.DEFAULT_DATETIME_FORMAT, locale);
					} else {
						 dateFormat = new SimpleDateFormat(formatType.getFormat(format), locale);
					}
					
					Date parsedDate = dateFormat.parse(tempInput);
					if(strict && !dateFormat.format(parsedDate).equals(tempInput.trim())) {
						return null;
					}
					return parsedDate;
				} catch (Exception ex) {
					return null;	
				}
			} else {
				try {
					DateTimeFormatter formatter = DateTimeFormat.forPattern(formatType.getFormat(format));
					formatter = formatter.withLocale(locale);
					DateTime parsedDate = formatter.parseDateTime(tempInput);
					if(strict && !parsedDate.toString(formatter).equals(tempInput.trim())) {
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

}
