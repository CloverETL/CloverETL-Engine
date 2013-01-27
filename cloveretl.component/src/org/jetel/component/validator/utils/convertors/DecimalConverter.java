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

import org.jetel.data.primitive.Decimal;
import org.jetel.data.primitive.DecimalFactory;

/**
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 15.1.2013
 */
public class DecimalConverter implements Converter {
	
	private static DecimalConverter instance;
	private DecimalConverter() {}
	
	public static DecimalConverter getInstance() {
		if(instance == null) {
			instance = new DecimalConverter();
		}
		return instance;
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
		if (o instanceof String) {
			try {
				return DecimalFactory.getDecimal((String)o);
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

}
