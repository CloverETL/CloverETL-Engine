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
package org.jetel.util;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Locale;

import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.graph.OutputPort;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.string.StringUtils;

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

		if (StringUtils.isEmpty(localeStr)) {
			locale = MiscUtils.createLocale(Defaults.DEFAULT_LOCALE);
		} else {
			String[] localeLC = localeStr.split("\\.");
			if (localeLC.length > 1) {
				locale = new Locale(localeLC[0], localeLC[1]);
			} else {
				locale = new Locale(localeLC[0]);
			}
		}

		return locale;
	}

	/**
	 * Converts given locale to clover standard format. This is symmetric method to {@link #createLocale(String)}. 
	 * @param locale
	 * @return
	 */
	public static String localeToString(Locale locale) {
		if (locale == null) {
			throw new NullPointerException("Locale cannot be null.");
		}
		
		String language = locale.getLanguage();
		String country = locale.getCountry();
		
		if (StringUtils.isEmpty(language)) {
			throw new IllegalArgumentException("Given locale '" + locale.getDisplayName() + "' does not have language specified. Unexpected error.");
		}
		return language 
			+ (!StringUtils.isEmpty(country) ? ("." + country) : "");
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

	/**
	 * Converts stack trace of a given throwable to a string.
	 *
	 * @param throwable a throwable
	 *
	 * @return stack trace of the given throwable as a string
	 */
	public static String stackTraceToString(Throwable throwable) {
		if (throwable == null) {
			return null;
		} else {
			StringWriter stringWriter = new StringWriter();
			throwable.printStackTrace(new PrintWriter(stringWriter));
	
			return stringWriter.toString();
		}
	}

	/**
	 * Utility method. Compares two objects for equality,
	 * handles <code>null</code> values.
	 * 
	 * @param o1
	 * @param o2
	 * @return <code>true</code> iff <code>o1</code> and <code>o2</code> are equal.
	 */
    public static boolean equals(Object o1, Object o2) {
        return o1 == null ? o2 == null : o1.equals(o2);
    }

    /**
     * Extract message from the given exception chain. All messages from all exceptions are concatenated
     * to the resulted string.
     * @param message prefixed message text which will be in the start of resulted string
     * @param exception converted exception
     * @return resulted overall message
     */
    public static String exceptionChainToMessage(String message, Throwable exception) {
    	StringBuffer result = new StringBuffer();
    	if (message != null) {
    		result.append(message);
    	}
    	if (exception == null) {
    		return result.toString();
    	}
    	Throwable exceptionIterator = exception;
    	String lastMessage = "";
    	while (true) {
    		if (!StringUtils.isEmpty(exceptionIterator.getMessage())
    				&& !lastMessage.equals(exceptionIterator.getMessage())) {
    			lastMessage = exceptionIterator.getMessage();
	    		if (!StringUtils.isEmpty(result)) {
	    			result.append("\nCaused by: ");
	    		}
	    		result.append(lastMessage);
    		}

    		if (exceptionIterator.getCause() == null || exceptionIterator.getCause() == exceptionIterator) {
    			break;
    		} else {
    			exceptionIterator = exceptionIterator.getCause();
    		}
    	}
    	return result.toString();
    }
    
	/**
	 * @return array of metadata derived from the given array of records
	 */
	public static DataRecordMetadata[] extractMetadata(DataRecord[] records) {
		DataRecordMetadata[] result = new DataRecordMetadata[records.length];
		
		int i = 0;
		for (DataRecord record : records) {
			if (record != null) {
				result[i] = record.getMetadata();
			}
			i++;
		}
		
		return result;
	}
	
	/**
	 * Just send the given record to the given port.
	 */
	public static void sendRecordToPort(OutputPort outputPort, DataRecord record) throws InterruptedException {
		try {
			outputPort.writeRecord(record);
		} catch (IOException e) {
			throw new JetelRuntimeException(e);
		}
	}

	/**
	 * Resets all given records.
	 */
	public static void resetRecords(DataRecord[] records) {
		for (DataRecord record : records) {
			if (record != null) {
				record.reset();
			}
		}
	}

}
