/*
 *    Copyright (c) 2004-2008 Javlin Consulting s.r.o. (info@javlinconsulting.cz)
 *    All rights reserved.
 */

package org.jetel.metadata.extraction;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.jetel.data.Defaults;
import org.jetel.metadata.DataFieldFormatType;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataFieldType;
import org.jetel.metadata.extraction.FieldTypeGuess;
import org.jetel.util.formatter.DateFormatter;
import org.jetel.util.formatter.DateFormatterFactory;

/**
 * 
 * Helper static class - guessing of field type from string
 * 
 * @author Jakub Lehotsky (jakub.lehotsky@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @since Feb 26, 2008
 */
public class FieldTypeGuesser {

	public static final String[] DATE_FORMAT_ITEMS = new String[] {
		"",
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
		DataFieldFormatType.EXCEL.prependFormatPrefix("General"),
		DataFieldFormatType.EXCEL.prependFormatPrefix("M/D/YY"),
		DataFieldFormatType.EXCEL.prependFormatPrefix("D-MMM-YY"),
		DataFieldFormatType.EXCEL.prependFormatPrefix("h:mm AM/PM"),
		DataFieldFormatType.EXCEL.prependFormatPrefix("M/D/YY h:mm"),
		DataFieldFormatType.EXCEL.prependFormatPrefix("h:mm"),
		DataFieldFormatType.EXCEL.prependFormatPrefix("mm:ss"),
		DataFieldFormatType.EXCEL.prependFormatPrefix("[h]:mm:ss"),
		DataFieldFormatType.EXCEL.prependFormatPrefix("mm:ss.0"),
	};
	
	private static final List<String> NUMBER_TYPES_PRIORITY = new ArrayList<String>(4);
	private static final List<DateFormatter> DATE_FORMATERS = new ArrayList<DateFormatter>();
	
	static {
		for (String pattern: DATE_FORMAT_ITEMS) {
			try {
				DATE_FORMATERS.add(DateFormatterFactory.getFormatter(pattern));
			} catch (IllegalArgumentException e) {
				//failed to create date formatter for given pattern - never mind
			}
		}
		NUMBER_TYPES_PRIORITY.add(DataFieldType.INTEGER.getName());
		NUMBER_TYPES_PRIORITY.add(DataFieldType.LONG.getName());
		NUMBER_TYPES_PRIORITY.add(DataFieldType.DECIMAL.getName());
		NUMBER_TYPES_PRIORITY.add(DataFieldType.NUMBER.getName());
	}
	
	private FieldTypeGuesser() {	
	}
	
	/**
	 * @param string Value of metadata field.
	 * @return Name of metadata field type as detected from given value of metadata field.
	 */
	public static FieldTypeGuess guessFieldType(String string) {

		if (string != null) {
			try {
				Integer.parseInt(string);
				return new FieldTypeGuess(DataFieldType.INTEGER);
			} catch (NumberFormatException e) {
				// do nothing, we continue trying other types
			}
			try {
				Long.parseLong(string);
				return new FieldTypeGuess(DataFieldType.LONG);
			} catch (NumberFormatException e) {
				// do nothing, we continue trying other types
			}
			try {
				double number = Double.parseDouble(string);
				String[] split = String.valueOf(number).split("\\.");
				if (split.length == 2 && split[1].length() <= 3) {
					Properties props = new Properties();
					props.put(DataFieldMetadata.SCALE_ATTR, String.valueOf(split[1].length()));
					props.put(DataFieldMetadata.LENGTH_ATTR, String.valueOf(Defaults.DataFieldMetadata.DECIMAL_LENGTH));
					return new FieldTypeGuess(DataFieldType.DECIMAL, props);
				}

				return new FieldTypeGuess(DataFieldType.NUMBER);
			} catch (NumberFormatException e) {
				// do nothing, we continue trying other types
			}
			for (DateFormatter dateFormat : DATE_FORMATERS) {
				try {
					dateFormat.parseDate(string);
					Properties props = new Properties();
					props.put(MetadataModelGuess.FORMAT_PROPERTY, dateFormat.getPattern());
					return new FieldTypeGuess(DataFieldType.DATE, props);
				} catch (IllegalArgumentException e) {
					// try other formatters
				}
			}
			
			if (string.equalsIgnoreCase("true") || string.equalsIgnoreCase("false")) {
				return new FieldTypeGuess(DataFieldType.BOOLEAN);
			}
		}
		return new FieldTypeGuess(DataFieldType.STRING);
	}
	
	/**
	 * Guesses type of field.
	 * 
	 * @param detectedTypes Types detected from values of the field.
	 * 
	 * @return Type of metadata field.
	 */
	public static FieldTypeGuess guessFieldType(FieldTypeGuess[] detectedTypes) {
		
		FieldTypeGuess guessType = FieldTypeGuess.defaultGuess();
		if (detectedTypes != null) {
			guessType = detectedTypes[0];
			boolean valueChanged = false;;
			for (int i = 0; i < detectedTypes.length; i++) {
				if (guessType == null) {
					guessType = detectedTypes[i];
				}
				if (detectedTypes[i] != null && !guessType.equals(detectedTypes[i])) {
					if (isNumberTypeUpgrade(guessType, detectedTypes[i])) {
						guessType = detectedTypes[i];
					} else if (isNumberTypeDowngrade(guessType, detectedTypes[i])) {
						//do nothing - keep type as detected
					} else if (!valueChanged) {
						valueChanged = true;
						guessType = detectedTypes[i];
					} else if (DataFieldType.DECIMAL.equals(guessType.getType()) && 
							DataFieldType.DECIMAL.equals(detectedTypes[i].getType())) {
						FieldTypeGuess unioned = unionScale(guessType, detectedTypes[i]);
						if (unioned != null) {
							guessType = unioned;
						} else {
							return FieldTypeGuess.defaultGuess();
						}
					} else {
						return FieldTypeGuess.defaultGuess();
					}
				}
			}
		}
		return guessType;
	}
	
	private static FieldTypeGuess unionScale(FieldTypeGuess decimalA, FieldTypeGuess decimalB) {
		
		try {
			int scaleA = Integer.parseInt(decimalA.getTypeProperties().getProperty(DataFieldMetadata.SCALE_ATTR));
			int scaleB = Integer.parseInt(decimalB.getTypeProperties().getProperty(DataFieldMetadata.SCALE_ATTR));
			
			Properties newProperties = new Properties();
			newProperties.put(DataFieldMetadata.SCALE_ATTR, String.valueOf(scaleA > scaleB ? scaleA : scaleB));
			
			return new FieldTypeGuess(DataFieldType.DECIMAL, newProperties);
			
		} catch (NumberFormatException e) {
			//never mind just return null
		}
		return null;
	}

	private static boolean isNumberTypeUpgrade(FieldTypeGuess originalType, FieldTypeGuess newType) {
		
		if (NUMBER_TYPES_PRIORITY.indexOf(originalType.getType()) < 0 || 
				NUMBER_TYPES_PRIORITY.indexOf(newType.getType()) < 0) {
			//not a number
			return false;
		}
		return NUMBER_TYPES_PRIORITY.indexOf(originalType.getType()) < NUMBER_TYPES_PRIORITY.indexOf(newType.getType());
	}
	
	private static boolean isNumberTypeDowngrade(FieldTypeGuess originalType, FieldTypeGuess newType) {
		
		if (NUMBER_TYPES_PRIORITY.indexOf(originalType.getType()) < 0 || 
				NUMBER_TYPES_PRIORITY.indexOf(newType.getType()) < 0) {
			//not a number
			return false;
		}
		return NUMBER_TYPES_PRIORITY.indexOf(originalType.getType()) > NUMBER_TYPES_PRIORITY.indexOf(newType.getType());
	}

}
