/*
 * CloverETL Engine - Java based ETL application framework.
 * Copyright (c) Javlin, a.s. (info@cloveretl.com).  Use is subject to license terms.
 *
 * www.cloveretl.com
 */
package org.jetel.component.validator.utils.convertors;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.Date;
import java.util.Locale;

import org.jetel.data.primitive.Decimal;
import org.jetel.data.primitive.DecimalFactory;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.string.CloverString;

/**
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 25.1.2013
 */
public class DoubleConverter implements Converter {
	
	private String format;
	private Locale locale;
	
	private DoubleConverter(String format, Locale locale) {
		this.format = format;
		this.locale = locale;
	}
	
	public static DoubleConverter newInstance(String format, Locale locale) {
		return new DoubleConverter(format, locale);
	}

	@Override
	public Double convert(Object o) {
		if (o instanceof Double) {
			return (Double)o;
		}
		if (o instanceof BigDecimal) {
			// Possible precision loss
			return null;
		}
		if (o instanceof BigInteger) {
			// Possible overflow -> unwanted value
			return null;
		}
		if (o instanceof Integer) {
			return ((Integer) o).doubleValue();
		}
		if (o instanceof Long) {
			return ((Long) o).doubleValue();
		}
		if (o instanceof Date) {
			return Long.valueOf(((Date) o).getTime()).doubleValue();
		}
		if (o instanceof CloverString) {
			try {
				DecimalFormat format = (DecimalFormat) DecimalFormat.getInstance(locale);
				format.applyLocalizedPattern(this.format);
				return format.parse(((CloverString) o).toString()).doubleValue(); 
			} catch (Exception e) {
				return null;
			}
		}
		if (o instanceof String) {
			try {
				DecimalFormat format = (DecimalFormat) DecimalFormat.getInstance(locale);
				format.applyLocalizedPattern(this.format);
				return format.parse((String) o).doubleValue(); 
			} catch (ParseException e) {
				return null;
			}
		}
		if(o instanceof byte[]) {
			return Double.parseDouble("" +((byte[]) o).length);
		}
		if(o instanceof Date) {
			
		}
		// TODO: add cbyte, byte, date, list, map
		return null;
	}
	
	/**
	 * {@link ComponentXMLAttributes#getDouble(String)}
	 */
	@Override
	public Double convertFromCloverLiteral(String o) {
		if (o.equalsIgnoreCase(ComponentXMLAttributes.STR_MIN_DOUBLE)) {
			return Double.MIN_VALUE;
		} else if (o.equalsIgnoreCase(ComponentXMLAttributes.STR_MAX_DOUBLE)) {
			return Double.MAX_VALUE;
		}
		try {
			return Double.parseDouble(o);
		} catch (Exception ex) {
			return null;
		}
	}

}
