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
package org.jetel.metadata;

import org.jetel.util.string.StringUtils;

/**
 * @author Pavel Simecek (pavel.simecek@javlin.eu)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 11.1.2012
 */
public enum DataFieldFormatType {
	JAVA("java", "Default system date/time/number format"),
	JODA("joda", "Joda date/time format"),
	ISO_8601("iso-8601", "ISO-8601 date/time format"),
	EXCEL("excel", "Excel number/date/time format"),
	BINARY("binary", "Binary format");

	public static final DataFieldFormatType DEFAULT_FORMAT_TYPE = JAVA;

	private final String formatPrefix;
	private final String longName;
	private static final String prefixDelimiter = ":";
	
	DataFieldFormatType(String formatPrefix, String longName) {
		this.formatPrefix = formatPrefix;
		this.longName = longName;
	}

	public static boolean isExistingPrefix(String prefix) {
		return guessFormatType(prefix) != null;
	}
	
	public String getLongName() { 
		return longName;
	}
	
	public String getFormatPrefix() {
		return formatPrefix;
	}
	
	public String prependFormatPrefix(String formatStr) {
		return this.formatPrefix + prefixDelimiter + (!StringUtils.isEmpty(formatStr) ? formatStr : "");
	}
	
	public String getFormatPrefixWithDelimiter() {
		return formatPrefix + prefixDelimiter;
	}
	
	/**
	 * Extracts a format type from a prefix of returns null if formatString
	 * has no prefix with an existing format type.
	 * 
	 * @param formatString
	 * @return
	 */
	private static DataFieldFormatType getFormatTypeFromPrefix(String formatString) {
		if (formatString!=null) {
	    	int delimiterPos = formatString.indexOf(prefixDelimiter);
	    	if (delimiterPos != -1) {
		    	String potentialPrefix = formatString.substring(0, delimiterPos).toLowerCase();
		    	return guessFormatType(potentialPrefix);
	    	}
		}
		
		return null;
	}
	
	/**
	 * @return format type instance based on the given prefix or null if does not exist
	 */
	private static DataFieldFormatType guessFormatType(String prefix) {
		for (DataFieldFormatType dataFieldFormat : DataFieldFormatType.values()) {
			if (dataFieldFormat.formatPrefix.equalsIgnoreCase(prefix)) {
				return dataFieldFormat;
			}
		}
		return null;
	}
	
	/**
	 * Returns a type of format extracted from a prefix of a formatting string.
	 *  
	 * @param formatString
	 * @return format type for non-empty formatString, null for null or empty formatString
	 */
	public static DataFieldFormatType getFormatType(String formatString) {
		DataFieldFormatType result = getFormatTypeFromPrefix(formatString);
		if (result!=null) {
			return result;
		} else {
			if (formatString!=null && !formatString.isEmpty()) {
				return DEFAULT_FORMAT_TYPE;
			} else {
				return null;
			}
		}
	}
	
	private String getFormatStringWithoutPrefix(String formatString, String formatPrefix) {
		int formatPrefixPlusDelimiterLength = formatPrefix.length() + prefixDelimiter.length();
		
		return formatString.substring(formatPrefixPlusDelimiterLength);
	}
	
	/**
	 * A method that extract a formatting string with respect to matching
	 * prefix of formatString.
	 * 
	 * @param formatString
	 * @return
	 *      Formatting string with respect to a format type.
	 *      When an instance of DataFieldFormatType does not match a format type detected
	 *      from a formatString prefix, it either returns an empty string or tries to
	 *      convert the formatting string to a desired format type.
	 */
	public String getFormat(String formatString) {
		if (formatString!=null) {
			DataFieldFormatType formatTypeFromFormatString = getFormatTypeFromPrefix(formatString);
			if (this == formatTypeFromFormatString) {
				return getFormatStringWithoutPrefix(formatString, this.formatPrefix);
			} else if (formatTypeFromFormatString == null && this == DEFAULT_FORMAT_TYPE) {
				return formatString;
			}
			else {
				return "";
			}
		}
		return "";
	}
	

}
