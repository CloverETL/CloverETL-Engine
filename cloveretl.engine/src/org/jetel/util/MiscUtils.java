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
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.graph.ContextProvider;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPort;
import org.jetel.graph.runtime.GraphRuntimeContext;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.formatter.TimeZoneProvider;
import org.jetel.util.string.StringUtils;

public final class MiscUtils {

	private static final Logger logger = Logger.getLogger(MiscUtils.class);
	
	private MiscUtils() {
	}

	/**
	 * Default timezone is timezone from either {@link GraphRuntimeContext#getTimeZone()}
	 * or from {@link Defaults#DEFAULT_TIME_ZONE}
	 * @return default timezone
	 */
	public static String getDefaultTimeZone() {
		GraphRuntimeContext context = ContextProvider.getRuntimeContext();
		String timeZone = context != null ? context.getTimeZone() : null;
		return !StringUtils.isEmpty(timeZone) ? timeZone : Defaults.DEFAULT_TIME_ZONE;
	}

	/**
	 * Returns the default locale,
	 * either set in the {@link GraphRuntimeContext}
	 * or as {@link Defaults#DEFAULT_LOCALE}.
	 * 
	 * @return
	 */
	public static Locale getDefaultLocale() {
		return MiscUtils.createLocale(getDefaultLocaleId());
	}
	
	/**
	 * Returns the string representing the default locale,
	 * either set in the {@link GraphRuntimeContext}
	 * or as {@link Defaults#DEFAULT_LOCALE}.
	 * 
	 * @return
	 */
	public static String getDefaultLocaleId() {
		GraphRuntimeContext context = ContextProvider.getRuntimeContext();
		String locale = context != null ? context.getLocale() : null;
		locale = (!StringUtils.isEmpty(locale)) ? locale : Defaults.DEFAULT_LOCALE;
		return (!StringUtils.isEmpty(locale)) ? locale : MiscUtils.localeToString(Locale.getDefault());
	}
	
	/**
	 * @return use {@link MiscUtils#getDefaultLocaleId()} instead
	 */
	@Deprecated
	public static String getDefautLocaleId() {
		return getDefaultLocaleId();
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
			locale = getDefaultLocale();
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
	 * Replacement for {@link Calendar#getInstance(java.util.TimeZone, Locale)},
	 * uses the locale and time zone defined in
	 * {@link Defaults} or {@link GraphRuntimeContext}
	 * if either if the parameters is <code>null</code>.
	 * 
	 * @param localeStr
	 * @param timeZoneStr
	 * @return
	 */
	public static Calendar createCalendar(String localeStr, String timeZoneStr) {
		Locale locale = MiscUtils.createLocale(localeStr);
		TimeZoneProvider timeZone = new TimeZoneProvider(timeZoneStr);
		return createCalendar(locale, timeZone);
	}
	
	/**
	 * Creates a {@link Calendar} instance from the given
	 * {@link Locale} and {@link TimeZoneProvider}.
	 * 
	 * Assumes the arguments are not <code>null</code>.
	 * 
	 * @param locale
	 * @param timeZone
	 * @return new {@link Calendar} instance.
	 */
	public static Calendar createCalendar(Locale locale, TimeZoneProvider timeZone) {
		return Calendar.getInstance(timeZone.getJavaTimeZone(), locale);
	}
	
	/**
	 * Replacement for {@link Calendar#getInstance()},
	 * uses the locale and time zone defined in
	 * {@link Defaults} or {@link GraphRuntimeContext}.
	 * 
	 * @return
	 */
	public static Calendar getDefaultCalendar() {
		return createCalendar((String) null, (String) null);
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

	/**
	 * Wrapper for method {@link System#getenv(String)} method.
	 * Possible {@link SecurityException} is caught and null is returned.
	 * Null value is returned even for null variable name.
	 */
	public static String getEnvSafe(String envVariableName) {
		if (envVariableName == null) {
			return null;
		}
		try {
			return System.getenv(envVariableName);
		} catch (SecurityException e) {
			logger.debug("Could not resolve environment variable '" + envVariableName + "' due security manager.", e);
			return null;
		}
	}
	
	/**
	 * Returns information about number of received and sent records from/to the given component.
	 * The resulted string could look like 'In0: 15, In1: 30, Out0:5'
	 */
	public static String getInOutRecordsMessage(Node component) {
    	//collect information from input ports
    	StringBuilder recordsMessage = new StringBuilder();
    	Map<Integer, InputPort> inPorts = component.getInputPorts();
    	for (Entry<Integer, InputPort> inPort : inPorts.entrySet()) {
    		if (recordsMessage.length() > 0) {
    			recordsMessage.append(", ");
    		}
    		recordsMessage.append("In" + inPort.getKey() + ": " + inPort.getValue().getInputRecordCounter() + " recs");
    	}
    	
    	//collect information from output ports
    	Map<Integer, OutputPort> outPorts = component.getOutputPorts();
    	for (Entry<Integer, OutputPort> outPort : outPorts.entrySet()) {
    		if (recordsMessage.length() > 0) {
    			recordsMessage.append(", ");
    		}
    		recordsMessage.append("Out" + outPort.getKey() + ": " + outPort.getValue().getOutputRecordCounter() + " recs");
    	}
    	
    	return recordsMessage.toString();
	}

}
