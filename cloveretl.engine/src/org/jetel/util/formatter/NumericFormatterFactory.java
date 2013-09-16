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
		return getFormatter(formatString, createLocale(localeString));
	}

	public static NumericFormatter getFormatter(String formatString, Locale locale) {
		NumberFormat numberFormat = createNumberFormatter(formatString, locale);
		
		if (numberFormat != null) {
			return new JavaNumericFormatter(formatString, numberFormat);
		} else {
			return getPlainFormatterInstance();
		}
	}
	
	public static NumericFormatter getDecimalFormatter(String formatString, String localeString, int length, int scale) {
		return getDecimalFormatter(formatString, createLocale(localeString), length, scale);
	}
	public static NumericFormatter getDecimalFormatter(String formatString, Locale locale, int length, int scale) {
		NumberFormat numberFormat = createNumberFormatter(formatString, locale);
		
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
		return getDecimalFormatter(formatString, createLocale(localeString));
	}

	public static NumericFormatter getDecimalFormatter(String formatString, Locale locale) {
		NumberFormat numberFormat = createNumberFormatter(formatString, locale);
		
		if (numberFormat != null) {
			if( numberFormat instanceof DecimalFormat){
				((DecimalFormat) numberFormat).setParseBigDecimal(true);
			}
			return new JavaNumericFormatter(formatString, numberFormat);
		} else {
			return getPlainFormatterInstance();
		}
	}

	public static NumericFormatter getPlainFormatterInstance() {
		return PLAIN_FORMATTER;
	}

	private NumericFormatterFactory() {
		throw new UnsupportedOperationException();
	}
	
	private static Locale createLocale(String localeString) {
		Locale locale;
		if (!StringUtils.isEmpty(localeString)) {
			locale = MiscUtils.createLocale(localeString);
		} else {
			locale = null;
		}
		return locale;
	}

	private static NumberFormat createNumberFormatter(String formatString, Locale locale) {

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
