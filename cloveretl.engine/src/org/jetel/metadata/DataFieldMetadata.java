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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

import org.jetel.data.DataField;
import org.jetel.data.DataFieldFactory;
import org.jetel.data.Defaults;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.ConfigurationStatus.Priority;
import org.jetel.exception.ConfigurationStatus.Severity;
import org.jetel.exception.InvalidGraphObjectNameException;
import org.jetel.util.formatter.BooleanFormatter;
import org.jetel.util.formatter.BooleanFormatterFactory;
import org.jetel.util.formatter.DateFormatter;
import org.jetel.util.formatter.DateFormatterFactory;
import org.jetel.util.formatter.ParseBooleanException;
import org.jetel.util.primitive.TypedProperties;
import org.jetel.util.string.StringUtils;

/**
 * A class that represents metadata describing one particular data field.<br>
 * Handles encoding of characters.
 *
 * @author David Pavlis, Javlin a.s. &lt;david.pavlis@javlin.eu&gt;
 * @author Martin Janik, Javlin a.s. &lt;martin.janik@javlin.eu&gt;
 *
 * @version 13th March 2009
 * @since 26th March 2002
 *
 * @see org.jetel.metadata.DataRecordMetadata
 *
 * @revision $Revision$
 */
public class DataFieldMetadata implements Serializable {

	private static final long serialVersionUID = -880873886732472663L;

	public static final int INTEGER_LENGTH = 9;
	public static final int LONG_LENGTH = 18;
	public static final int DOUBLE_SCALE = 323;
	public static final int DOUBLE_LENGTH = DOUBLE_SCALE + 615;

	public static final String BINARY_FORMAT_STRING = "binary";
	public static final String BLOB_FORMAT_STRING = "blob";

	public static final String LENGTH_ATTR = "length";
	public static final String SCALE_ATTR = "scale";
	public static final String SIZE_ATTR = "size";

	/** Characters that can be contained in format of date. */
	private static final Pattern DATE_ONLY_PATTERN = Pattern.compile("[GyMwWDdFE]");
	/** Characters that can be contained in format of time. */
	private static final Pattern TIME_ONLY_PATTERN = Pattern.compile("[aHhKkmsSzZ]");
	
	public static final String EMPTY_NAME = "_";
	
	/** Parent data record metadata. */
	private DataRecordMetadata dataRecordMetadata;

	/** Ordinal number of the field within data record metadata. */
	private int number;
	/** Name of the field. */
	protected String name;
	/** Description of the field. */
	private String description;
	/** Original name of the field. */
	private String label;

	/** The type of the field. */
	private DataFieldType type = DataFieldType.UNKNOWN;

	/** Delimiter of the field (could be empty if field belongs to fixLength record). */
	private String delimiter = null;
	/** If this switch is set to true, EOF works as delimiter for this field. It's useful for last field in the record. */
	private boolean eofAsDelimiter = false;
	/** Format of Number, Date, DateTime, String (RegExp) or empty if not applicable. */
	private String formatStr = null;
	/** Length of the field (in bytes) if the field belongs to fixLength record. */
	private short size = 0;

	/** Relative shift of the beginning of the field. */
	private short shift = 0;
	/** Indicates if when reading from file try to trim string value to obtain value */
	private boolean trim = false;
	/** Fields can assume null value by default. */
	private boolean nullable = true;
	/** String value that is considered as null (in addition to null itself). */
	private String nullValue = null;

	/** The default value. */
	private Object defaultValue;
	/** The default value as a string. */
	private String defaultValueStr;
	/** The auto-filling value. */
	private String autoFilling;

	/**
	 * Field can be populated by execution of Java code which can include references to fields from input records. The
	 * code corresponds to a body of a method which has to return a value that has a type of the field type.
	 * 
	 * The syntax for the field references is as follows:
	 * 
	 * [record name].[field name]
	 */
	private transient TypedProperties fieldProperties;
	/**
	 * Locale string. Both language and country can be specified - if both are specified then language string & country
	 * string have to be delimited by "." (dot) -> e.g. "en.UK" , "fr.CA". If only language should be specified, then
	 * use language code according to ISO639 -> e.g. "en" , "de".
	 * 
	 * @see java.util.Locale
	 */
	private String localeStr = null;

	/**
	 * See Collator.setStregth(String strength). It is used only in string fields.
	 */
	private String collatorSensitivity = null;

	/**
	 * Container type of data field - SINGLE, LIST, MAP.
	 */
	private DataFieldContainerType containerType = DataFieldContainerType.SINGLE;
	
	/**
	 * Constructor for a delimited type of field.
	 * 
	 * @param name the name of the field
	 * @param fieldType the type of this field
	 * @param delimiter a string to be used as a delimiter for this field
	 * @deprecated use {@link DataFieldMetadata#DataFieldMetadata(String, DataFieldType, String)} instead
	 */
	@Deprecated
	public DataFieldMetadata(String name, char fieldType, String delimiter) {
		this(name, DataFieldType.fromChar(fieldType), delimiter);
	}

	/**
	 * Constructor for a delimited type of field.
	 * @param name
	 * @param fieldType
	 * @param delimiter
	 */
	public DataFieldMetadata(String name, DataFieldType fieldType, String delimiter) {
		setName(name);

		this.type = fieldType;
		this.delimiter = delimiter;

		if (isTrimType()) {
			this.trim = true;
		}

		setFieldProperties(new Properties());
	}

	/**
	 * Constructor for a delimited type of field with specified container type.
	 * @param name
	 * @param fieldType
	 * @param delimiter
	 * @param containerType 
	 */
	public DataFieldMetadata(String name, DataFieldType fieldType, String delimiter, DataFieldContainerType containerType) {
		this(name, fieldType, delimiter);
		setContainerType(containerType);
	}

	/**
	 * Constructor for a default (String) delimited type of field.
	 * 
	 * @param name the name of the field
	 * @param delimiter a string to be used as a delimiter for this field
	 */
	public DataFieldMetadata(String name, String delimiter) {
		this(name, DataFieldType.STRING, delimiter);
	}

	/**
	 * Constructor for a fixed-length type of field.
	 * 
	 * @param name the name of the field
	 * @param fieldType the type of this field
	 * @param size the size of the field (in bytes)
	 * @deprecated use {@link DataFieldMetadata#DataFieldMetadata(String, DataFieldType, short)} instead
	 */
	@Deprecated
	public DataFieldMetadata(String name, char fieldType, short size) {
		this(name, DataFieldType.fromChar(fieldType), size);
	}

	/**
	 * Constructor for a fixed-length type of field.
	 * @param name
	 * @param fieldType
	 * @param size
	 */
	public DataFieldMetadata(String name, DataFieldType fieldType, short size) {
		setName(name);

		this.type = fieldType;
		this.size = size;

		if (isTrimType()) {
			this.trim = true;
		}

		setFieldProperties(new Properties());
	}

	/**
	 * Constructor for a default (String) fixed-length type of field.
	 * 
	 * @param name the name of the field
	 * @param size the size of the field (in bytes)
	 */
	public DataFieldMetadata(String name, short size) {
		this(name, DataFieldType.STRING, size);
	}

	/**
	 * Private constructor used in the duplicate() method.
	 */
	private DataFieldMetadata() {
	}

	/**
	 * Sets the parent data record metadata.
	 *
	 * @param dataRecordMetadata the new parent data record metadata
	 */
	public void setDataRecordMetadata(DataRecordMetadata dataRecordMetadata) {
		this.dataRecordMetadata = dataRecordMetadata;
	}

	/**
	 * @return the parent data record metadata
	 */
	public DataRecordMetadata getDataRecordMetadata() {
		return dataRecordMetadata;
	}

	/**
	 * Sets the ordinal number of the data field.
	 *
	 * @param number the new ordinal number
	 */
	public void setNumber(int number) {
		this.number = number;
	}

	/**
	 * @return the ordinal number of the data field
	 */
	public int getNumber() {
		return number;
	}

	/**
	 * Sets the name of the field.
	 *
	 * @param name the new name of the field
	 */
	public void setName(String name) {
		if (!StringUtils.isValidObjectName(name)) {
			throw new InvalidGraphObjectNameException(name, "FIELD");
		}

		this.name = name;
	}

	/**
	 * @return the name of the field
	 */
	public String getName() {
		return name;
	}

	/**
	 * Sets the description of the field.
	 *
	 * @param name the new description of the field
	 */
	public void setDescription(String description) {
		this.description = description;
	}

	/**
	 * @return the description of the field
	 */
	public String getDescription() {
		return description;
	}
	
	/**
	 * Sets the original name of the field.
	 * 
	 * @param label the original name of the field
	 */
	public void setLabel(String label) {
		this.label = label;
	}

	/**
	 * Returns the original name of the field.
	 * 
	 * @return original name of the field
	 */
	public String getLabel() {
		return label;
	}

	/**
	 * Returns the label of the field.
	 * If it is not set, returns the name of the field.
	 * 
	 * @return label of the field
	 */
	public String getLabelOrName() {
		if (label == null) {
			return getName();
		}
		return label;
	}
	
	/**
	 * Sets the type of the data field.
	 * 
	 * @param type the new type of the data field
	 *
	 * @since 30th October 2002
	 * @deprecated use {@link #setDataType(DataFieldType)} instead
	 */
	@Deprecated
	public void setType(char type) {
		setDataType(DataFieldType.fromChar(type));
	}

	/**
	 * Sets the type of the data field.
	 * @param type
	 */
	public void setDataType(DataFieldType type) {
		this.type = type;
	}
	
	/**
	 * @return the type of the data field
	 *
	 * @since 30th October 2002
	 * @deprecated use {@link #getDataType()} instead
	 */
	@Deprecated
	public char getType() {
		return getDataType().getObsoleteIdentifier();
	}

	/**
	 * @return the type of the data field
	 */
	public DataFieldType getDataType() {
		return type;
	}
	
	/**
	 * Sets the type of the data field using the full string form.
	 * 
	 * @param type the new type of the data field as a string
	 * @deprecated use {@link #setDataType(DataFieldType) in combination with {@link DataFieldType#fromName(String)} instead
	 */
	@Deprecated
	public void setTypeAsString(String type) {
		setDataType(DataFieldType.fromName(type));
	}

	/**
	 * @return the type of the data field as a string
	 * @deprecated use {@link #getDataType()} and {@link DataFieldType#getName()} instead
	 */
	@Deprecated
	public String getTypeAsString() {
		return getDataType().getName();
	}

	/**
	 * @return container type of this field - SINGLE, LIST, MAP
	 */
	public DataFieldContainerType getContainerType() {
		return containerType;
	}
	
	/**
	 * Sets container type of this field - SINGLE, LIST, MAP.
	 * @param containerType
	 */
	public void setContainerType(DataFieldContainerType containerType) {
		if (containerType == null) {
			throw new NullPointerException("Data field container type cannot be null.");
		}
		this.containerType = containerType;
	}
	
	/**
	 * @return <code>true</code> if this data field is numeric, <code>false</code> otherwise
	 * @deprecated use {@link #getDataType()} and {@link DataFieldType#isNumeric()} instead
	 */
	@Deprecated
	public boolean isNumeric() {
		return getDataType().isNumeric();
	}

	/**
	 * @deprecated use {@link #getDataType()} and {@link DataFieldType#isTrimType()} instead
	 */
	@Deprecated
	private boolean isTrimType() {
		return getDataType().isTrimType();
	}
	
	/**
	 * Sets the delimiter string.
	 * 
	 * @param delimiter the new delimiter string
	 */
	public void setDelimiter(String delimiter) {
		this.delimiter = delimiter;
	}

	/**
	 * @return the delimiter string
	 */
	public String getDelimiter() {
		return delimiter;
	}

	/**
	 * Returns an array of all field delimiters assigned to this field. In case no field delimiters are defined, default
	 * field delimiters from parent metadata are returned. Delimiters for last field are extended by a record delimiter.
	 * 
	 * @return the array of all field delimiters
	 */
	public String[] getDelimiters() {
		if (isDelimited()) {
			String[] delimiters = null;

			if (delimiter != null) {
				delimiters = delimiter.split(Defaults.DataFormatter.DELIMITER_DELIMITERS_REGEX);

				if (isLastNonAutoFilledField()) { // if field is last
					if (getDataRecordMetadata().isSpecifiedRecordDelimiter()) {
						List<String> tempDelimiters = new ArrayList<String>();

						for (int i = 0; i < delimiters.length; i++) {
							// combine each field delimiter with each record delimiter
							String[] recordDelimiters = getDataRecordMetadata().getRecordDelimiters();

							for (int j = 0; j < recordDelimiters.length; j++) {
								tempDelimiters.add(delimiters[i] + recordDelimiters[j]);
							}
						}

						delimiters = tempDelimiters.toArray(new String[tempDelimiters.size()]);
					}
				}
			} else {
				if (!isLastNonAutoFilledField()) { // if the field is not last
					delimiters = getDataRecordMetadata().getFieldDelimiters();
				} else {
					delimiters = getDataRecordMetadata().getRecordDelimiters();

					if (delimiters == null) {
						delimiters = getDataRecordMetadata().getFieldDelimiters();
					}
				}
			}

			return delimiters;
		}

		return null;
	}

	/**
	 * @return <code>true</code> if any field delimiter contains a carriage return, <code>false</code> otherwise
	 */
	public boolean containsCarriageReturnInDelimiters() {
		String[] delimiters = getDelimiters();

		if (delimiters != null) {
			for (String delimiter : delimiters) {
				if (delimiter.indexOf('\r') >= 0) {
					return true;
				}
			}
		}

		return false;
	}

	/**
	 * Sets the OEF-as-delimiter flag.
	 *
	 * @param eofAsDelimiter the new value of the flag
	 */
	public void setEofAsDelimiter(boolean eofAsDelimiter) {
		this.eofAsDelimiter = eofAsDelimiter;
	}

	/**
	 * @return the value of the OEF-as-delimiter flag
	 */
	public boolean isEofAsDelimiter() {
		return eofAsDelimiter;
	}

	/**
	 * Sets the format pattern of this data field.
	 *
	 * @param formatStr the new format pattern (acceptable value depends on the type of the data field)
	 */
	public void setFormatStr(String formatStr) {
		this.formatStr = formatStr;
	}

	/**
	 * This method should not be called directly, use {@link #getFormat(DataFieldFormatType)} instead.
	 * @return the format pattern which will be used when outputting field's value as a string, or <code>null</code> if
	 * no format pattern is set for this field
	 */
	@SuppressWarnings("deprecation")
	public String getFormatStr() {
		if (formatStr != null) {
			return formatStr;
		}

		if (dataRecordMetadata != null) {
			if (getDataType().isNumeric()) {
				return dataRecordMetadata.getNumberFormatStr();
			}

			if (type == DataFieldType.DATE || type == DataFieldType.DATETIME) {
				return dataRecordMetadata.getDateFormatStr();
			}
		}

		return null;
	}

	/**
	 * This method checks if type of field is date or datetime and if formatString isn't <code>null</code> or empty.
	 *
	 * @return <code>true</code> if type of field is date or datetime, <code>false</code> otherwise
	 *
	 * @since 24th August 2007
	 * @see org.jetel.component.DataFieldmetadata.isTimeFormat(CharSequence)
	 */
	@SuppressWarnings("deprecation")
	private boolean isDateOrTimeFieldWithFormatStr() {
		if (type != DataFieldType.DATE && type != DataFieldType.DATETIME) {
			return false;
		}

		return !StringUtils.isEmpty(formatStr);
	}

	/**
	 * This method checks if formatString has a format of date. If formatString is <code>null</code> or empty then
	 * formatString hasn't a format of date.
	 * Note: formatString can has a format of date and format of time at the same time.
	 *
	 * @return <code>true</code> if formatString has a format of date, <code>false</code> otherwise
	 *
	 * @since 24th August 2007
	 * @see org.jetel.component.DataFieldmetadata.isTimeFormat(CharSequence)
	 */
	public boolean isDateFormat() {
		if (!isDateOrTimeFieldWithFormatStr()) {
			return false;
		}

		return DATE_ONLY_PATTERN.matcher(formatStr).find();
	}

	/**
	 * This method checks if formatString has a format of time. If formatString is <code>null</code> or empty then
	 * formatString hasn't a format of time.
	 * Note: formatString can has a format of date and format of time at the same time.
	 *
	 * @return <code>true</code> if formatString has a format of time, <code>false</code> otherwise
	 *
	 * @since 24th August 2007
	 * @see org.jetel.component.DataFieldmetadata.isDateFormat(CharSequence)
	 */
	public boolean isTimeFormat() {
		if (!isDateOrTimeFieldWithFormatStr()) {
			return false;
		}

		return TIME_ONLY_PATTERN.matcher(formatStr).find();
	}

	/**
	 * Sets the maximum field size (used only when dealing with a fixed-size type of record).
	 *
	 * @param size the new size of the data field
	 */
	public void setSize(short size) {
		this.size = size;
	}

	/**
	 * @return the specified maximum field size (used only when dealing with a fixed-size type of record)
	 */
	public short getSize() {
		return size;
	}

	/**
	 * @return <code>true</code> if this data field is delimited, <code>false</code> otherwise
	 */
	public boolean isDelimited() {
		return (size == 0);
	}

	/**
	 * @return <code>true</code> if this data field is fixed-length, <code>false</code> otherwise
	 */
	public boolean isFixed() {
		return (size != 0);
	}
	
	/**
	 * Determines whether the field is byte-based.
	 * 
	 * @return <code>true</code> if this data field is byte-based, <code>false</code> otherwise
	 */
	public boolean isByteBased() {
		if(BinaryFormat.isBinaryFormat(formatStr)) {
			return true; 
		}
		return (type == DataFieldType.BYTE || type == DataFieldType.CBYTE);
	}
	
	/**
	 * @return true iff formatStr is not null and is not empty
	 */
	public boolean hasFormat() {
		return !StringUtils.isEmpty(formatStr);
	}
	
	/**
	 * Returns a type of a data field format obtained by analysis of
	 * a prefix of getFormarStr(). No prefix with a format type
	 * results in a default format type (DataFieldFormatType.DEFAULT_FORMAT_TYPE).
	 * 
	 * Returns null for getFormatStr() being null or an empty string.
	 *   
	 * @return a type of a data field format (or null if the field has no format)
	 */
	public DataFieldFormatType getFormatType() {
		return DataFieldFormatType.getFormatType(getFormatStr());
	}
	
	/**
	 * Returns a formatting string of a given type. A purpose of this method is to prevent
	 * usage of incompatible formatting string types.
	 * 
	 * The type of a formatting string is checked (using its prefix) 
	 *   - If the formatting string stored in meta-data has a different type, either
	 *     an empty string is returned or conversion is performed.
	 *   - If the formatting string stored in meta-data matches a type given in argument,
	 *     the formatting string with no prefix is returned
	 * 
	 * @param dataFieldFormat
	 * @return
	 */
	public String getFormat(DataFieldFormatType dataFieldFormat) {
		return dataFieldFormat.getFormat(getFormatStr());
	}
	
	/**
	 * Returns a formatting string of the default type ({@link DataFieldFormatType#DEFAULT_FORMAT_TYPE}).
	 * A purpose of this method is to prevent usage of incompatible formatting string types.
	 * <p>
	 * Equivalent call:<br>
	 * {@link #getFormat(DataFieldFormatType.DEFAULT_FORMAT_TYPE)}
	 * 
	 * 
	 * @param dataFieldFormat
	 * @return
	 * @see #getFormat(DataFieldFormatType)
	 */
	public String getFormat() {
		return getFormat(DataFieldFormatType.DEFAULT_FORMAT_TYPE);
	}
	
	/**
	 * Prepare a DateFormatter instance based on format string and locale.
	 * Available only for date field metadata.
	 * @return DateFormatter instance for this date field metadata
	 */
	public DateFormatter createDateFormatter() {
		if (type != DataFieldType.DATE) {
			throw new UnsupportedOperationException("DateFormatter is available only for date field metadata.");
		}
		return DateFormatterFactory.getFormatter(getFormatStr(), getLocaleStr());
	}

	/**
	 * Sets the position of the field in a data record (used only when dealing with fixed-size type of record).
	 * 
	 * @param shift the new position of the field in a data record
	 */
	public void setShift(short shift) {
		this.shift = shift;
	}

	/**
	 * @return the position of the field in a data record (used only when dealing with fixed-size type of record)
	 */
	public short getShift() {
		return shift;
	}

	/**
	 * Sets the trim flag.
	 *
	 * @param trim the new value of the trim flag
	 */
	public void setTrim(boolean trim) {
		this.trim = trim;
	}

	/**
	 * @return the value of the trim flag
	 */
	public boolean isTrim() {
		return trim;
	}

	/**
	 * Sets the nullable flag.
	 * 
	 * @param nullable the new value of the nullable flag
	 */
	public void setNullable(boolean nullable) {
		this.nullable = nullable;
	}

	/**
	 * @return the value of the nullable flag
	 */
	public boolean isNullable() {
		return nullable;
	}

	/**
	 * Sets a string value that will be considered as <code>null</code> (in addition to <code>null</code> itself).
	 *
	 * @param nullValue the string value to be considered as null, or <code>null</code> if an empty string should be used
	 */
	public void setNullValue(String nullValue) {
		this.nullValue = nullValue;
	}

	/**
	 * @return the string value that is considered as <code>null</code>, never returns <code>null</code>
	 */
	public String getNullValue() {
		if (nullValue != null) {
			return nullValue;
		}

		if (dataRecordMetadata != null) {
			return dataRecordMetadata.getNullValue();
		}

		return DataRecordMetadata.DEFAULT_NULL_VALUE;
	}

	/**
	 * Sets the default value.
	 *
	 * @param defaultValue the new default value
	 */
	public void setDefaultValue(Object defaultValue) {
		this.defaultValue = defaultValue;
	}

	/**
	 * @return the default value
	 */
	public Object getDefaultValue() {
		return defaultValue;
	}

	/**
	 * Sets the default string value.
	 *
	 * @param defaultValueStr the new default string value
	 *
	 * @since 30th October 2002
	 */
	public void setDefaultValueStr(String defaultValueStr) {
		this.defaultValueStr = defaultValueStr;
	}

	/**
	 * @return the default string value
	 *
	 * @since 30th October 2002
	 */
	public String getDefaultValueStr() {
		if (defaultValueStr != null) {
			return defaultValueStr;
		} else if (defaultValue != null) {
			return defaultValue.toString();
		}

		return null;
	}

	/**
	 * @return <code>true</code> if the default value is set, <code>false</code> otherwise
	 */
	public boolean isDefaultValueSet() {
		return (!StringUtils.isEmpty(defaultValueStr) || defaultValue != null);
	}

	/**
	 * Sets the auto-filling value.
	 *
	 * @param autoFilling the new auto-filling value
	 */
	public void setAutoFilling(String autoFilling) {
		this.autoFilling = autoFilling;
	}

	/**
	 * @return the auto-filling value
	 */
	public String getAutoFilling() {
		return autoFilling;
	}

	/**
	 * @return true if the data field is auto filled, <code>false</code> otherwise
	 */
	public boolean isAutoFilled() {
		return !StringUtils.isEmpty(autoFilling);
	}

	/**
	 * @return <code>true</code> if this data field is the last non-autofilled field within the data record metadata,
	 * <code>false</code> otherwise.
	 */
	private boolean isLastNonAutoFilledField() {
		if (isAutoFilled()) {
			return false;
		}

		DataRecordMetadata metadata = getDataRecordMetadata();
		DataFieldMetadata[] fields = metadata.getFields();

		for (int i = getNumber() + 1; i < metadata.getNumFields(); i++) {
			if (!fields[i].isAutoFilled()) {
				return false;
			}
		}

		return true;
	}

	/**
	 * Sets the fieldProperties attribute of the DataRecordMetadata object. Field properties allows defining additional
	 * parameters for individual fields. These parameters (key-value pairs) are NOT normally handled by CloverETL, but
	 * can be used in user's code or Components - thus allow for greater flexibility.
	 * 
	 * @param properties The new recordProperties value
	 */
	public void setFieldProperties(Properties properties) {
		fieldProperties = new TypedProperties(properties);

		if (type == DataFieldType.DECIMAL) {
			if (fieldProperties.getProperty(LENGTH_ATTR) == null) {
				fieldProperties.setProperty(LENGTH_ATTR, Integer.toString(Defaults.DataFieldMetadata.DECIMAL_LENGTH));
			}

			if (fieldProperties.getProperty(SCALE_ATTR) == null) {
				fieldProperties.setProperty(SCALE_ATTR, Integer.toString(Defaults.DataFieldMetadata.DECIMAL_SCALE));
			}
		}
	}

	/**
	 * Gets the fieldProperties attribute of the DataFieldMetadata object.<br>
	 * These properties are automatically filled-in when parsing XML (.fmt) file containing data record metadata. Any
	 * attribute not directly recognized by Clover is stored within properties object.<br>
	 * Example:
	 * 
	 * <pre>
	 * &lt;Field name=&quot;Field1&quot; type=&quot;numeric&quot; delimiter=&quot;;&quot; myOwn1=&quot;1&quot; myOwn2=&quot;xyz&quot; /&gt;
	 * </pre>
	 * 
	 * @return The fieldProperties value
	 */
	public TypedProperties getFieldProperties() {
		return fieldProperties;
	}

	/**
	 * Sets a value of the property with the given key.
	 *
	 * @param key the key of the property
	 * @param value the value to be set
	 */
	public void setProperty(String key, String value) {
		if (fieldProperties == null) {
			setFieldProperties(new Properties());
		}

		fieldProperties.setProperty(key, value);
	}

	/**
	 * Returns a value of the property with the given key.
	 * 
	 * @param key the key of the property
	 *
	 * @return the value of the property
	 */
	public String getProperty(String key) {
		return fieldProperties.getProperty(key);
	}

	/**
	 * Sets the locale code string.<br>
	 * Formatters/Parsers are generated based on this field's value.
	 *
	 * @param localeStr the locale code (eg. "en", "fr", ...)
	 */
	public void setLocaleStr(String localeStr) {
		this.localeStr = localeStr;
	}

	/**
	 * @return the locale code string, or <code>null</code> if no locale string is set
	 */
	public String getLocaleStr() {
		if (localeStr != null) {
			return localeStr;
		}

		if (dataRecordMetadata != null) {
			return dataRecordMetadata.getLocaleStr();
		}

		return null;
	}

	/**
	 * Set collator sensitivity string.	
	 * @param collatorSensitivity
	 */
	public void setCollatorSensitivity(String collatorSensitivity) {
		this.collatorSensitivity = collatorSensitivity;
	}

	/**
	 * Returns collator sensitivity as string according to CollatorSensitivityType class.
	 * @return
	 */
	public String getCollatorSensitivity() {
		if (collatorSensitivity != null) {
			return collatorSensitivity;
		} else {
			return dataRecordMetadata.getCollatorSensitivity();
		}
	}

	/**
	 * This method checks if value from this field can be put safe in another field.
	 *
	 * @param anotherField the another field to be checked
	 *
	 * @return <code>true</code> if conversion is save, <code>false</code> otherwise
	 */
	public boolean isSubtype(DataFieldMetadata anotherField) {
		switch (type) {
		case INTEGER:
			switch (anotherField.getDataType()) {
			case DECIMAL:
				int anotherFieldLength = Integer.valueOf(anotherField.getProperty(LENGTH_ATTR));
				int anotherFieldScale = Integer.valueOf(anotherField.getProperty(SCALE_ATTR));

				return (anotherFieldLength - anotherFieldScale >= INTEGER_LENGTH);
			}
			break;
		case LONG:
			switch (anotherField.getDataType()) {
			case DECIMAL:
				int anotherFieldLength = Integer.valueOf(anotherField.getProperty(LENGTH_ATTR));
				int anotherFieldScale = Integer.valueOf(anotherField.getProperty(SCALE_ATTR));

				return (anotherFieldLength - anotherFieldScale >= LONG_LENGTH);
			}
			break;
		case NUMBER:
			switch (anotherField.getDataType()) {
			case DECIMAL:
				int anotherFieldLength = Integer.valueOf(anotherField.getProperty(LENGTH_ATTR));
				int anotherFieldScale = Integer.valueOf(anotherField.getProperty(SCALE_ATTR));

				return (anotherFieldLength >= DOUBLE_LENGTH && anotherFieldScale >= DOUBLE_SCALE);
			}
			break;
		case DECIMAL:
			switch (anotherField.getDataType()) {
			case DECIMAL:
				int anotherFieldLength = Integer.valueOf(anotherField.getProperty(LENGTH_ATTR));
				int anotherFieldScale = Integer.valueOf(anotherField.getProperty(SCALE_ATTR));

				return (anotherFieldLength >= Integer.valueOf(fieldProperties.getProperty(LENGTH_ATTR)) && anotherFieldScale >= Integer.valueOf(fieldProperties.getProperty(SCALE_ATTR)));
			case NUMBER:
				return (Integer.valueOf(fieldProperties.getProperty(LENGTH_ATTR)) <= DOUBLE_LENGTH && Integer.valueOf(fieldProperties.getProperty(SCALE_ATTR)) <= DOUBLE_SCALE);
			case INTEGER:
				return (Integer.valueOf(fieldProperties.getProperty(LENGTH_ATTR)) - Integer.valueOf(fieldProperties.getProperty(SCALE_ATTR)) <= INTEGER_LENGTH);
			case LONG:
				return (Integer.valueOf(fieldProperties.getProperty(LENGTH_ATTR)) - Integer.valueOf(fieldProperties.getProperty(SCALE_ATTR)) <= LONG_LENGTH);
			}
			break;
		}
		return type.isSubtype(anotherField.getDataType());
	}

	/**
	 * Creates a deep copy of this data field metadata object.
	 *
	 * @return an exact copy of current data field metadata object
	 */
	public DataFieldMetadata duplicate() {
		DataFieldMetadata dataFieldMetadata = new DataFieldMetadata();

		dataFieldMetadata.setNumber(number);
		dataFieldMetadata.setName(name);
		dataFieldMetadata.setLabel(label);
		dataFieldMetadata.setDescription(description);
		dataFieldMetadata.setDataType(type);
		dataFieldMetadata.setContainerType(containerType);
		dataFieldMetadata.setDelimiter(delimiter);
		dataFieldMetadata.setEofAsDelimiter(eofAsDelimiter);
		dataFieldMetadata.setFormatStr(formatStr);
		dataFieldMetadata.setSize(size);
		dataFieldMetadata.setShift(shift);
		dataFieldMetadata.setTrim(trim);
		dataFieldMetadata.setNullable(nullable);
		dataFieldMetadata.setDefaultValueStr(defaultValueStr);
		dataFieldMetadata.setAutoFilling(autoFilling);
		dataFieldMetadata.setFieldProperties(fieldProperties);
		dataFieldMetadata.setLocaleStr(localeStr);
		dataFieldMetadata.setCollatorSensitivity(collatorSensitivity);

		return dataFieldMetadata;
	}

	@Override
	public boolean equals(Object object) {
		return equals(object, true);
	}

	public boolean equals(Object object, boolean checkFixDelType) {
		if (object == this) {
			return true;
		}

		if (!(object instanceof DataFieldMetadata)) {
			return false;
		}

		DataFieldMetadata dataFieldMetadata = (DataFieldMetadata) object;

		if (this.type == dataFieldMetadata.getDataType()) {
			if (isFixed() && dataFieldMetadata.isFixed()) {
				// both fixed
				return (getSize() == dataFieldMetadata.getSize());
			} else if (!isFixed() && !dataFieldMetadata.isFixed()) {
				// both delimited
				if (this.type == DataFieldType.DECIMAL) {
					return (getProperty(LENGTH_ATTR).equals(dataFieldMetadata.getProperty(LENGTH_ATTR))
							&& getProperty(SCALE_ATTR).equals(dataFieldMetadata.getProperty(SCALE_ATTR)));
				} else {
					// the same type and both delimited
					return true;
				}
			} else {
				// one fixed and the second delimited
				return !checkFixDelType;
			}
		}

		return false;
	}

	@Override
	public int hashCode() {
		//FIXME this is not correct implementation, at least decimal type with different scale and length can be considered as different
		return this.type.hashCode(); 
	}

	/**
	 * Checks if the meta is valid.
	 *
	 * @param status
	 * @return
	 */
	public void checkConfig(ConfigurationStatus status) {
		//check data type
		if (type == null) {
			status.add(new ConfigurationProblem("Data type is not specified.", Severity.ERROR, null, Priority.NORMAL));
		}
		
		// verify default value - approved by kokon
		if (defaultValue != null || defaultValueStr != null) {
			DataField dataField = DataFieldFactory.createDataField(this, true);
			try {
				dataField.setToDefaultValue();
			} catch (RuntimeException e) {
				status.add(new ConfigurationProblem("Wrong default value '" + getDefaultValueStr() + "' for field '" + name + "' in the record metadata element '" + dataRecordMetadata.getName() + "'.",
						Severity.ERROR, null, Priority.NORMAL));
			}
		}
		
		if (DataFieldType.BOOLEAN == type && !StringUtils.isEmpty(formatStr)) {
			final BooleanFormatter bf = BooleanFormatterFactory.createFormatter(formatStr);

			final String trueO = bf.formatBoolean(true);
			try {
				if (true != bf.parseBoolean(trueO)) {
					status.add(new ConfigurationProblem("Wrong boolean format '" + formatStr + "' for field '" + name + "' in the record metadata element '" + dataRecordMetadata.getName() + "' - reverse check for true output string defined by the format '" + trueO + "' will return incorrect value.",
							Severity.WARNING, null, Priority.NORMAL));
				}
			} catch (ParseBooleanException e) {
				status.add(new ConfigurationProblem("Wrong boolean format '" + formatStr + "' for field '" + name + "' in the record metadata element '" + dataRecordMetadata.getName() + "' - reverse check for true output string defined by the format '" + trueO + "' will not be parsable (" + e.getMessage() + ").",
						Severity.WARNING, null, Priority.NORMAL));
			}

			final String falseO = bf.formatBoolean(false);
			try {
				if (false != bf.parseBoolean(falseO)) {
					status.add(new ConfigurationProblem("Wrong boolean format '" + formatStr + "' for field '" + name + "' in the record metadata element '" + dataRecordMetadata.getName() + "' - reverse check for true output string defined by the format '" + falseO + "' will return incorrect value.",
							Severity.WARNING, null, Priority.NORMAL));
				}
			} catch (ParseBooleanException e) {
				status.add(new ConfigurationProblem("Wrong boolean format '" + formatStr + "' for field '" + name + "' in the record metadata element '" + dataRecordMetadata.getName() + "' - reverse check for true output string defined by the format '" + falseO + "' will not be parsable (" + e.getMessage() + ").",
						Severity.WARNING, null, Priority.NORMAL));
			}
		}
	}

	@Override
	public String toString(){
		return "Field [name:" + this.name + ", type:" + this.type.toString(getContainerType()) +
				(containerType != DataFieldContainerType.SINGLE ? ", containerType:" + containerType.getDisplayName() : "") +
				", position:" + this.number  + "]";
	}
	
	/**
	 * @deprecated use {@link DataFieldType#STRING} instead
	 */
	@Deprecated
	public static final char STRING_FIELD = 'S';
	/**
	 * @deprecated use {@link DataFieldType#STRING} instead
	 */
	@Deprecated
	public static final String STRING_TYPE = "string";

	/**
	 * @deprecated use {@link DataFieldType#DATE} instead
	 */
	@Deprecated
	public static final char DATE_FIELD = 'D';
	/**
	 * @deprecated use {@link DataFieldType#DATE} instead
	 */
	@Deprecated
	public static final String DATE_TYPE = "date";

	/**
	 * @deprecated use {@link DataFieldType#DATE} instead
	 */
	@Deprecated
	public static final char DATETIME_FIELD = 'T';
	/**
	 * @deprecated use {@link DataFieldType#DATE} instead
	 */
	@Deprecated
	public static final String DATETIME_TYPE = "datetime";

	/**
	 * @deprecated use {@link DataFieldType#NUMBER} instead
	 */
	@Deprecated
	public static final char NUMERIC_FIELD = 'N';
	/**
	 * @deprecated use {@link DataFieldType#NUMBER} instead
	 */
	@Deprecated
	public static final String NUMERIC_TYPE = "number";
	/**
	 * @deprecated use {@link DataFieldType#NUMBER} instead
	 */
	@Deprecated
	public static final String NUMERIC_TYPE_DEPRECATED = "numeric";

	/**
	 * @deprecated use {@link DataFieldType#INTEGER} instead
	 */
	@Deprecated
	public static final char INTEGER_FIELD = 'i';
	/**
	 * @deprecated use {@link DataFieldType#INTEGER} instead
	 */
	@Deprecated
	public static final String INTEGER_TYPE = "integer";

	/**
	 * @deprecated use {@link DataFieldType#LONG} instead
	 */
	@Deprecated
	public static final char LONG_FIELD = 'l';
	/**
	 * @deprecated use {@link DataFieldType#LONG} instead
	 */
	@Deprecated
	public static final String LONG_TYPE = "long";

	/**
	 * @deprecated use {@link DataFieldType#DECIMAL} instead
	 */
	@Deprecated
	public static final char DECIMAL_FIELD = 'd';
	/**
	 * @deprecated use {@link DataFieldType#DECIMAL} instead
	 */
	@Deprecated
	public static final String DECIMAL_TYPE = "decimal";

	/**
	 * @deprecated use {@link DataFieldType#BYTE} instead
	 */
	@Deprecated
	public static final char BYTE_FIELD = 'B';
	/**
	 * @deprecated use {@link DataFieldType#BYTE} instead
	 */
	@Deprecated
	public static final String BYTE_TYPE = "byte";

	/**
	 * @deprecated use {@link DataFieldType#BOOLEAN} instead
	 */
	@Deprecated
	public static final char BOOLEAN_FIELD = 'b';
	/**
	 * @deprecated use {@link DataFieldType#BOOLEAN} instead
	 */
	@Deprecated
	public static final String BOOLEAN_TYPE = "boolean";

	/**
	 * @deprecated use {@link DataFieldType#CBYTE} instead
	 */
	@Deprecated
	public static final char BYTE_FIELD_COMPRESSED = 'Z';
	/**
	 * @deprecated use {@link DataFieldType#CBYTE} instead
	 */
	@Deprecated
	public static final String BYTE_COMPRESSED_TYPE = "cbyte";

	/**
	 * @deprecated should not be used at all
	 */
	@Deprecated
	public static final char SEQUENCE_FIELD = 'q';
	/**
	 * @deprecated should not be used at all
	 */
	@Deprecated
	public static final String SEQUENCE_TYPE = "sequence";

	/**
	 * @deprecated use {@link DataFieldType#NULL} instead
	 */
	@Deprecated
	public static final char NULL_FIELD = 'n';
	/**
	 * @deprecated use {@link DataFieldType#NULL} instead
	 */
	@Deprecated
	public static final String NULL_TYPE = "null";

	/**
	 * @deprecated use {@link DataFieldType#UNKNOWN} instead
	 */
	@Deprecated
	public static final char UNKNOWN_FIELD = ' ';
	/**
	 * @deprecated use {@link DataFieldType#UNKNOWN} instead
	 */
	@Deprecated
	public static final String UNKNOWN_TYPE = "unknown";

	/**
	 * Converts a type of a data field into its full string form.
	 *
	 * @param type the type of a data field
	 *
	 * @return the type of the data field as a string
	 * @deprecated use {@link DataFieldType#getName()} instead
	 */
	@Deprecated
	public static String type2Str(char type) {
		try {
			return DataFieldType.fromChar(type).getName();
		} catch (IllegalArgumentException e) {
			return DataFieldType.UNKNOWN.getName();
		}
	}

	/**
	 * Converts a type of a data field in a full string form into its character form.
	 *
	 * @param type the type of the data field as a string
	 *
	 * @return the type of a data field
	 * @deprecated use {@link DataFieldType#fromName(String)} instead
	 */
	@Deprecated
	public static char str2Type(String dataTypeName) {
		try {
			return DataFieldType.fromName(dataTypeName).getObsoleteIdentifier();
		} catch (IllegalArgumentException e) {
			return DataFieldType.UNKNOWN.getObsoleteIdentifier();
		}
	}

}
