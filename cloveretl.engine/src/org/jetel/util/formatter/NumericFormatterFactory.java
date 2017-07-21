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

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.NumberFormat;
import java.util.Locale;

import org.jetel.util.MiscUtils;
import org.jetel.util.string.StringUtils;

/**
 * @author csochor (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created Jun 29, 2010
 */
public class NumericFormatterFactory {

	private static final JavolutionNumericFormatter PLAIN_FORMATTER = new JavolutionNumericFormatter();

	public static NumericFormatter getFormatter(String formatString, String localeString) {
		NumberFormat numberFormat = createNumberFormatter(formatString, localeString);
		
		if (numberFormat != null) {
			return new JavaNumericFormatter(formatString, numberFormat);
		} else {
			return getPlainFormatterInstance();
		}
	}
	
	public static NumericFormatter getDecimalFormatter(String formatString, String localeString, int length, int scale) {
		NumberFormat numberFormat = createNumberFormatter(formatString, localeString);
		
		if (numberFormat != null) {
			if( numberFormat instanceof DecimalFormat){
				((DecimalFormat) numberFormat).setParseBigDecimal(true);
			}
			return new JavaNumericFormatter(formatString, numberFormat);
		} else {
			return new JavolutionNumericFormatter(length);
		}
	}

	public static NumericFormatter getDecimalFormatter(String formatString, String localeString) {
		NumberFormat numberFormat = createNumberFormatter(formatString, localeString);
		
		if (numberFormat != null) {
			if( numberFormat instanceof DecimalFormat){
				((DecimalFormat) numberFormat).setParseBigDecimal(true);
			}
			return new JavaNumericFormatter(formatString, numberFormat);
		} else {
			return getPlainFormatterInstance();
		}
		
// This implementation exploit our NumericFormat, which is unfortunately pretty buggy 
//		- its only advantage is that is able to parse CharSequence instead String what is faster in our case 
		
//		// handle locale
//		final Locale locale;
//		if (!StringUtils.isEmpty(localeString)) {
//			locale = MiscUtils.createLocale(localeString);
//		} else {
//			locale = null;
//		}
//
//		// handle formatString
//		NumberFormat numericFormat = null;
//		if (!StringUtils.isEmpty(formatString)) {
//			if (locale != null) {
//				numericFormat = new NumericFormat(formatString,
//						new DecimalFormatSymbols(locale));
//			} else {
//				numericFormat = new NumericFormat(formatString);
//			}
//		} else if (locale != null) {
//			numericFormat = new NumericFormat(locale);
//		}
//
//		if (numericFormat != null) {
//			return new JavaNumericFormatter(numericFormat);
//		} else {
//			return createPlainFormatter();
//		}

	}

	public static NumericFormatter getPlainFormatterInstance() {
		return PLAIN_FORMATTER;
	}

	private NumericFormatterFactory() {
		throw new UnsupportedOperationException();
	}

	private static NumberFormat createNumberFormatter(String formatString, String localeString) {
		// handle locale
		final Locale locale;
		if (!StringUtils.isEmpty(localeString)) {
			locale = MiscUtils.createLocale(localeString);
		} else {
			locale = null;
		}

		// handle formatString
		NumberFormat numberFormat = null;
		if (!StringUtils.isEmpty(formatString)) {
			if (locale != null) {
				numberFormat = new DecimalFormat(formatString,
						new DecimalFormatSymbols(locale));
			} else {
				numberFormat = new DecimalFormat(formatString,
						new DecimalFormatSymbols(MiscUtils.getDefaultLocale()));
			}
		} else if (locale != null) {
			numberFormat = DecimalFormat.getInstance(locale);
		}

		return numberFormat;
	}
	
}
