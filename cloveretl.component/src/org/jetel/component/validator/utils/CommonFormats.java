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
	public static final String INTEGER = "INTEGER"; //$NON-NLS-1$
	/**
	 * Special value for NUMBERS as it needs special treating
	 */
	public static final String NUMBER = "NUMBER"; //$NON-NLS-1$
	
	/**
	 * @see (NumericFormatAttributeType)
	 */
	public static final String[] numbers = {
		INTEGER,
		NUMBER,
		"#", //$NON-NLS-1$
		"#.#", //$NON-NLS-1$
		"#.###", //$NON-NLS-1$
		"000", //$NON-NLS-1$
		"000.#", //$NON-NLS-1$
		"#.### %", //$NON-NLS-1$
		"### \u00A4", //$NON-NLS-1$
		"### a string", //$NON-NLS-1$
		"-###", //$NON-NLS-1$
	};
	public static final String defaultNumber = numbers[0];
	
	/**
	 * @see (DateFormatAttributeType)
	 */
	public static final String[] dates = {
		"yyyy-MM-dd HH:mm:ss", //$NON-NLS-1$
		"yyyy-MM-dd", //$NON-NLS-1$
		"HH:mm:ss", //$NON-NLS-1$
		"dd.MM.yy", //$NON-NLS-1$
		"dd/MM/yy", //$NON-NLS-1$
		"dd.MM.yyyy", //$NON-NLS-1$
		"MM.dd.yyyy", //$NON-NLS-1$
		"yyyy-MM-dd hh:mm:ss 'text'", //$NON-NLS-1$
		"yyyy.MM.dd HH:mm:ss.SSS z", //$NON-NLS-1$
		"EEE, MMM d, yy", //$NON-NLS-1$
		"joda:yyyy-MM-dd HH:mm:ss", //$NON-NLS-1$
		"joda:yyyy-MM-dd", //$NON-NLS-1$
		"joda:HH:mm:ss", //$NON-NLS-1$
		"joda:dd.MM.yy", //$NON-NLS-1$
		"joda:dd/MM/yy", //$NON-NLS-1$
		"joda:dd.MM.yyyy", //$NON-NLS-1$
		"joda:MM.dd.yyyy", //$NON-NLS-1$
		"joda:yyyy-MM-dd hh:mm:ss 'text'", //$NON-NLS-1$
		"joda:EEE, MMM d, yy", //$NON-NLS-1$
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
			if (locale.getCountry().equals("")) { //$NON-NLS-1$
				localeString = locale.getLanguage();
			} else {
				localeString = locale.getLanguage() + "." + locale.getCountry(); //$NON-NLS-1$
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
	
	public static final String[] phoneNumbers = {
		"", //$NON-NLS-1$
		"+1 DDD.DDD.DDDD", //$NON-NLS-1$
		"+1 (DDD) DDD-DDDD", //$NON-NLS-1$
		"DDD DDD DDD", //$NON-NLS-1$
		"+D{7,15}", //$NON-NLS-1$
		"+D{1,5} D{3} D{3} D{3,6}", //$NON-NLS-1$
		"8 DDDD DD-DD-DD", //$NON-NLS-1$
		"(02x) DDDD DDDD" //$NON-NLS-1$
	};

}
