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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.util.Date;
import java.util.Locale;

import org.jetel.component.validator.rules.NumberValidationRule;
import org.jetel.component.validator.utils.CommonFormats;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.string.CloverString;

/**
 * Implementation of convertor for converting something into double.
 * 
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 25.1.2013
 * @see NumberValidationRule
 */
public class DoubleConverter implements Converter {
	
	private String format;
	private Locale locale;
	
	private DoubleConverter(String format, Locale locale) {
		this.format = format;
		this.locale = locale;
	}
	
	/**
	 * Create instance of double converter
	 * @param format Formatting mask used for parsing the number from input
	 * @param locale Locale to be used for parsing
	 */	
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
		if (o instanceof CloverString || o instanceof String) {
			String input;
			if(o instanceof CloverString) {
				input = ((CloverString) o).toString();
			} else {
				input = (String) o;
			}
			try {
				DecimalFormat format;
				if(this.format.equals(CommonFormats.INTEGER)) {
					return Long.valueOf(input).doubleValue();
				} if(this.format.equals(CommonFormats.NUMBER)) {
					format = (DecimalFormat) DecimalFormat.getInstance(locale);
				} else {
					format = (DecimalFormat) DecimalFormat.getInstance(locale);
					format.applyPattern(this.format);
				}
				return format.parse(input).doubleValue(); 
			} catch (Exception e) {
				e.printStackTrace();
				System.err.println("HER");
				return null;
			}
		}
		if(o instanceof byte[]) {
			// On byte arrays take its size into account rather than content
			return Double.parseDouble("" +((byte[]) o).length);
		}
		if(o instanceof Date) {
			return (double) ((Date) o).getTime();
		}
		return null;
	}
	
	/**
	 * Inspired by {@link ComponentXMLAttributes#getDouble(String)}
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
