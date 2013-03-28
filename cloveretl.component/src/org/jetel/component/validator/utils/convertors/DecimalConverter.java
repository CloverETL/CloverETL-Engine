/*
 * CloverETL Engine - Java based ETL application framework.
 * Copyright (c) Javlin, a.s. (info@cloveretl.com).  Use is subject to license terms.
 *
 * www.cloveretl.com
 */
package org.jetel.component.validator.utils.convertors;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

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
		if (o instanceof CloverString) {
			try {
				NumericFormat nf = new NumericFormat(locale);
				nf.applyPattern(format);
				return DecimalFactory.getDecimal(((CloverString)o).toString(), nf);
			} catch (NumberFormatException e) {
				return null;
			}
		}
		if (o instanceof String) {
			try {
				NumericFormat nf = new NumericFormat(locale);
				nf.applyPattern(format);
				return DecimalFactory.getDecimal((String)o, nf);
			} catch (Exception e) {
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
