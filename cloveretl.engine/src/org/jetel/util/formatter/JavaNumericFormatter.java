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
package org.jetel.util.formatter;

import java.math.BigDecimal;
import java.text.NumberFormat;
import java.text.ParseException;

import org.jetel.data.primitive.NumericFormat;

/**
 * @author csochor (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created Jun 29, 2010
 */
public class JavaNumericFormatter implements NumericFormatter {

	private final NumberFormat numberFormat;

	@Override
	public String toString() {
		return "(JavaNumericFormatter)" + numberFormat.toString();
	}

	public JavaNumericFormatter(NumberFormat numberFormat) {
		if (numberFormat == null) {
			throw new IllegalArgumentException("numberFormat is required");
		}
		this.numberFormat = numberFormat;
	}

	@Override
	public int parseInt(CharSequence seq) throws ParseException {
		return numberFormat.parse(seq.toString()).intValue();
	}

	@Override
	public String formatInt(int value) {
		return numberFormat.format(value);
	}

	@Override
	public String formatLong(long value) {
		return numberFormat.format(value);
	}

	@Override
	public long parseLong(CharSequence seq) throws ParseException {
		return numberFormat.parse(seq.toString()).longValue();
	}

	@Override
	public String formatDouble(double value) {
		return numberFormat.format(value);
	}

	@Override
	public double parseDouble(CharSequence seq) throws ParseException {
		return numberFormat.parse(seq.toString()).doubleValue();
	}

	@Override
	public String format(BigDecimal bd) {
		if (bd == null) {
			return "NaN";
		} else {
			return numberFormat.format(bd);
		}
	}

	@Override
	public BigDecimal parseBigDecimal(CharSequence seq) {
		return ((NumericFormat)numberFormat).parse(seq);
	}

}
