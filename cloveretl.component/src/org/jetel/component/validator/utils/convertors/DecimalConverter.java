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
import java.util.Date;
import java.util.Locale;

import org.jetel.component.validator.utils.CommonFormats;
import org.jetel.data.primitive.Decimal;
import org.jetel.data.primitive.DecimalFactory;
import org.jetel.data.primitive.NumericFormat;
import org.jetel.util.string.CloverString;

/**
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 15.1.2013
 */
public class DecimalConverter implements Converter {
	private String format;
	private Locale locale;
	
	private DecimalConverter(String format, Locale locale) {
		this.format = format;
		this.locale = locale;
	}
	
	public static DecimalConverter newInstance(String format, Locale locale) {
		return new DecimalConverter(format, locale);
	}

	@Override
	public Decimal convert(Object o) {
		if (o instanceof Decimal) {
			return (Decimal)o;
		}
		if (o instanceof BigDecimal) {
			BigDecimal bd = (BigDecimal)o;
			return DecimalFactory.getDecimal(bd);
		}
		if (o instanceof BigInteger) {
			BigInteger bi = (BigInteger)o;
			return DecimalFactory.getDecimal(new BigDecimal(bi));
		}
		if (o instanceof Integer) {
			Number number = (Number)o;
			return DecimalFactory.getDecimal(number.intValue());
		}
		if (o instanceof Number) {
			Number number = (Number)o;
			return DecimalFactory.getDecimal(number.doubleValue());
		}
		if (o instanceof Long) {
			Number number = (Number)o;
			return DecimalFactory.getDecimal(number.longValue());
		}
		if (o instanceof CloverString || o instanceof String) {
			String input;
			if(o instanceof CloverString) {
				input = ((CloverString) o).toString();
			} else {
				input = (String) o;
			}
			try {
				NumericFormat nf;
				if(this.format.equals(CommonFormats.INTEGER)) {
					return DecimalFactory.getDecimal(Long.valueOf(input));
				} else if(this.format.equals(CommonFormats.NUMBER)) {
						nf = new NumericFormat(locale);
				} else {  
					nf = new NumericFormat(locale);
					nf.applyPattern(format);
				}
				return DecimalFactory.getDecimal(input, nf);
			} catch (NumberFormatException e) {
				return null;
			}
		}
		if (o instanceof byte[]) {
			return DecimalFactory.getDecimal(((byte[]) o).length);
		}
		if (o instanceof Date) {
			return DecimalFactory.getDecimal(((Date) o).getTime());
		}
		return null;
	}
	
	@Override
	public Decimal convertFromCloverLiteral(String o) {
		try {
			return DecimalFactory.getDecimal(o);
		} catch (Exception ex) {
			return null;
		}
	}

}
