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
import org.jetel.util.string.CloverString;

/**
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 25.1.2013
 */
public class DoubleConverter implements Converter {
	
	private static DoubleConverter instance;
	private DoubleConverter() {}
	
	public static DoubleConverter getInstance() {
		if(instance == null) {
			instance = new DoubleConverter();
		}
		return instance;
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
				return Double.parseDouble(((CloverString) o).toString());
			} catch (NumberFormatException e) {
				return null;
			}
		}
		if (o instanceof String) {
			try {
				return Double.parseDouble((String) o);
			} catch (NumberFormatException e) {
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

}
