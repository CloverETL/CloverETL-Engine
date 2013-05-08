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
package org.jetel.component.validator.utils;

import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;

import org.jetel.component.validator.params.LanguageSetting;

/**
 * Helper class with predefined number and dates formats, timezone and locales.
 * 
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 12.3.2013
 * @see LanguageSetting
 */
public class CommonFormats {
	
	/**
	 * Special value for INTEGERS as it needs special treating
	 */
	public static final String INTEGER = "INTEGER";
	/**
	 * Special value for NUMBERS as it needs special treating
	 */
	public static final String NUMBER = "NUMBER";
	
	/**
	 * @see (NumericFormatAttributeType)
	 */
	public static final String[] numbers = {
		INTEGER,
		NUMBER,
		"#",
		"#.#",
		"#.###",
		"000",
		"000.#",
		"#.### %",
		"### \u00A4",
		"### a string",
		"-###",
	};
	public static final String defaultNumber = numbers[0];
	
	/**
	 * @see (DateFormatAttributeType)
	 */
	public static final String[] dates = {
		"yyyy-MM-dd HH:mm:ss",
		"yyyy-MM-dd",
		"HH:mm:ss",
		"dd.MM.yy",
		"dd/MM/yy",
		"dd.MM.yyyy",
		"MM.dd.yyyy",
		"yyyy-MM-dd hh:mm:ss 'text'",
		"yyyy.MM.dd HH:mm:ss.SSS z",
		"EEE, MMM d, yy",
		"joda:yyyy-MM-dd HH:mm:ss",
		"joda:yyyy-MM-dd",
		"joda:HH:mm:ss",
		"joda:dd.MM.yy",
		"joda:dd/MM/yy",
		"joda:dd.MM.yyyy",
		"joda:MM.dd.yyyy",
		"joda:yyyy-MM-dd hh:mm:ss 'text'",
		"joda:EEE, MMM d, yy",
	};
	public static final String defaultDate = dates[0];
	
	/** 
	 * @see (LocaleAttributeType)
	 */
	public static final String[] locales;
	
	static {
		Locale[] availableLocales = DateFormat.getAvailableLocales();
		List<String> result = new ArrayList<String>();
		for (Locale locale : availableLocales) {
			String localeString;
			if (locale.getCountry().equals("")) {
				localeString = locale.getLanguage();
			} else {
				localeString = locale.getLanguage() + "." + locale.getCountry();
			}
			result.add(localeString);
		}
		
		Collections.sort(result);

		locales = (String[]) result.toArray(new String[result.size()]); 
	}
	
	/**
	 * List of all available timezones
	 */
	public static final String[] timezones = TimeZone.getAvailableIDs();
		
}
