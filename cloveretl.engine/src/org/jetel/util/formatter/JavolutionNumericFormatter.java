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
import java.math.MathContext;
import java.math.RoundingMode;
import java.text.ParseException;

import javolution.text.TypeFormat;

import org.jetel.ctl.TransformLangExecutor;

/**
 * @author csochor (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created Jun 29, 2010
 */
public class JavolutionNumericFormatter implements NumericFormatter {
	
	private MathContext precision;
	
	public JavolutionNumericFormatter() {
		this(TransformLangExecutor.MAX_PRECISION);
	}

	public JavolutionNumericFormatter(int precision) {
		this(new MathContext(precision, RoundingMode.DOWN));
	}

	private JavolutionNumericFormatter(MathContext precision) {
		this.precision = precision;
	}

	@Override
	public String toString() {
		return "[JavolutionNumericFormatter]" + getFormatPattern();
	}

	@Override
	public int parseInt(CharSequence seq) {
		// a workaround for https://java.net/jira/browse/JAVOLUTION-117 - javolution throws incorrect exception if the
		// sequence does not start with a digit
		firstDigitCheck(seq);
		return TypeFormat.parseInt(seq);
	}

	/**
	 * throws an exception if the sequence does not start with a digit, '+', or '-'
	 * @param seq
	 */
	private void firstDigitCheck(CharSequence seq) {
		if (seq.length() > 0) {
			char c = seq.charAt(0);
			if (c != '-' && c != '+' && (c < '0' || c > '9')) {
				throw new NumberFormatException("Invalid integer representation for " + seq.toString());
			}
		}
	}

	@Override
	public String formatInt(int value) {
		return Integer.toString(value);
	}

	@Override
	public String formatLong(long value) {
		return Long.toString(value);
	}

	@Override
	public long parseLong(CharSequence seq) throws ParseException {
		// a workaround for https://java.net/jira/browse/JAVOLUTION-117 - javolution throws incorrect exception if the
		// sequence does not start with a digit
		firstDigitCheck(seq);
		return TypeFormat.parseLong(seq);
	}

	@Override
	public String formatDouble(double value) {
		return Double.toString(value);
	}

	@Override
	public double parseDouble(CharSequence seq) throws ParseException {
		return TypeFormat.parseDouble(seq);
	}

	@Override
	public String formatBigDecimal(BigDecimal bd) {
		if (bd == null) {
			return "NaN";
		} else {
			return bd.toString();
		}
	}

	@Override
	public BigDecimal parseBigDecimal(CharSequence seq) {
		return new BigDecimal(seq.toString(), precision);
	}

	@Override
	public String getFormatPattern() {
		return "";
	}
}
