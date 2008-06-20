/*
 *    jETeL/Clover - Java based ETL application framework.
 *    Copyright (C) 2005-06  Javlin Consulting <info@javlinconsulting.cz>
 *    
 *    This library is free software; you can redistribute it and/or
 *    modify it under the terms of the GNU Lesser General Public
 *    License as published by the Free Software Foundation; either
 *    version 2.1 of the License, or (at your option) any later version.
 *    
 *    This library is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU    
 *    Lesser General Public License for more details.
 *    
 *    You should have received a copy of the GNU Lesser General Public
 *    License along with this library; if not, write to the Free Software
 *    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 */
package org.jetel.util;

import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Locale;

import org.jetel.data.Defaults;
import org.jetel.metadata.DataFieldMetadata;

public final class MiscUtils {

	private MiscUtils() {
	}

	/**
     * Creates locale from clover internal format - <language_identifier>[.<country_identifier>]
     * Examples:
     *  en
     *  en.GB
     *  fr
     *  fr.FR
     *  cs
     *  cs.CZ
	 * 
	 * @param localeStr
	 * @return
	 */
	public static Locale createLocale(String localeStr) {
		Locale locale = null;

		if (localeStr == null) {
			locale = Locale.getDefault();
		} else {
			String[] localeLC = localeStr.split(Defaults.DEFAULT_LOCALE_STR_DELIMITER_REGEX);
			if (localeLC.length > 1) {
				locale = new Locale(localeLC[0], localeLC[1]);
			} else {
				locale = new Locale(localeLC[0]);
			}
		}

		return locale;
	}

	/**
	 * Creates Decimal/Date format depending on data field type
	 * 
	 * @param formatType
	 *            field type; for <i>DateDataField</i> there is created DateFormat , for all numeric fields there is
	 *            created DecimalFormat, for other fields returns null
	 * @param locale
	 *            locale in Clover internat format
	 * @param format
	 *            format string
	 * @return DecimalFormat, DateFormat or null
	 */
	public static Format createFormatter(char formatType, String locale, String format) {
		Locale loc = createLocale(locale);
		if (format != null || locale != null) {
			switch (formatType) {
			case DataFieldMetadata.DATE_FIELD:
			case DataFieldMetadata.DATETIME_FIELD:
				return new SimpleDateFormat(format, loc);
			case DataFieldMetadata.DECIMAL_FIELD:
			case DataFieldMetadata.NUMERIC_FIELD:
			case DataFieldMetadata.INTEGER_FIELD:
			case DataFieldMetadata.LONG_FIELD:
				return new DecimalFormat(format, new DecimalFormatSymbols(loc));
			}
		} else {
			switch (formatType) {
			case DataFieldMetadata.DATE_FIELD:
			case DataFieldMetadata.DATETIME_FIELD:
				return DateFormat.getDateInstance(DateFormat.DEFAULT, loc);
			case DataFieldMetadata.DECIMAL_FIELD:
			case DataFieldMetadata.NUMERIC_FIELD:
			case DataFieldMetadata.INTEGER_FIELD:
			case DataFieldMetadata.LONG_FIELD:
				return DecimalFormat.getInstance(loc);
			}
		}
		return null;
	}
}
