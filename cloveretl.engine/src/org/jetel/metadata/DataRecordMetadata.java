/*
 * jETeL/Clover.ETL - Java based ETL application framework.
 * Copyright (C) 2002-2009  David Pavlis <david.pavlis@javlin.eu>
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
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
package org.jetel.metadata;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.jetel.data.Defaults;
import org.jetel.data.RecordKey;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.InvalidGraphObjectNameException;
import org.jetel.exception.ConfigurationStatus.Priority;
import org.jetel.exception.ConfigurationStatus.Severity;
import org.jetel.util.primitive.BitArray;
import org.jetel.util.primitive.TypedProperties;
import org.jetel.util.string.StringUtils;

import edu.umd.cs.findbugs.annotations.SuppressWarnings;

/**
 * A class that represents meta data describing a data record.
 * 
 * @author David Pavlis, Javlin a.s. &lt;david.pavlis@javlin.eu&gt;
 * @author Martin Janik, Javlin a.s. &lt;martin.janik@javlin.eu&gt;
 * 
 * @version 28th May 2009
 * @since 26th March 2002
 * 
 * @see DataFieldMetadata
 * @see org.jetel.data.DataRecord
 * @see org.jetel.data.DataField
 * 
 * @revision $Revision$
 */
public class DataRecordMetadata implements Serializable, Iterable<DataFieldMetadata> {

	private static final long serialVersionUID = 7032218607804024730L;

	public static final char DELIMITED_RECORD = 'D';
	public static final char FIXEDLEN_RECORD = 'F';
	public static final char MIXED_RECORD = 'M';

	public static final String BYTE_MODE_ATTR = "byteMode";

	/** Name of the data record. */
	private String name;
	/** Description of the data record. */
	private String description;

	/** The type of the data record. */
	private char recType;

	private int skipSourceRows;

	private String recordDelimiter;
	private String fieldDelimiter;
	private int recordSize = -1;

	@SuppressWarnings("Se")
	private List<DataFieldMetadata> fields = new ArrayList<DataFieldMetadata>();
	@SuppressWarnings("Se")
	private Map<String, Integer> fieldNamesMap = new HashMap<String, Integer>();
	@SuppressWarnings("Se")
	private Map<Integer, String> fieldTypes = new HashMap<Integer, String>();
	@SuppressWarnings("Se")
	private Map<String, Integer> fieldOffset = new HashMap<String, Integer>();

	/** an array of field names specifying a primary key */
	private String[] keyFieldNames = null;

	private BitArray fieldNullSwitch = new BitArray();
	private short numNullableFields = 0;

	private TypedProperties recordProperties = new TypedProperties();
	private String localeStr = null;

	/** a format string for numbers */
	private String numberFormatStr = null;
	/** a format string for dates */
	private String dateFormatStr = null;

	/**
	 * Constructs data record meta data with given name.
	 *
	 * @param name the name of the data record
	 *
	 * @since 2nd May 2002
	 */
	public DataRecordMetadata(String name) {
		setName(name);
	}

	/**
	 * Constructs data record meta data with given name and type.
	 *
	 * @param name the name of the data record
	 * @param recType the type of the data record
	 *
	 * @since 2nd May 2002
	 */
	public DataRecordMetadata(String name, char recType) {
		this(name);

		this.recType = recType;
	}

	/**
	 * Sets the name of the data record.
	 *
	 * @param name the new name of the data record
	 */
	public void setName(String name) {
		if (!StringUtils.isValidObjectName(name)) {
			throw new InvalidGraphObjectNameException(name, "RECORD");
		}

		this.name = name;
	}

	/**
	 * @return the name of the data record
	 */
	public String getName() {
		return name;
	}

	/**
	 * Sets the description of the data record.
	 *
	 * @param description the new description of the data record
	 */
	public void setDescription(String description) {
		this.description = description;
	}

	/**
	 * @return the description of the data record
	 */
	public String getDescription() {
		return description;
	}

	/**
	 * Sets the type of record (delimited/fixed-length).
	 *
	 * @param recType the new type of record
	 *
	 * @since 3rd May 2002
	 */
	public void setRecType(char recType) {
		this.recType = recType;
	}

	/**
	 * @return the type of record (delimited/fixed-length)
	 *
	 * @since 3rd May 2002
	 */
	public char getRecType() {
		return recType;
	}

	/**
	 * Sets the value of the skipSourceRows.
	 */
	public void setSkipSourceRows(int skipSourceRows) {
		this.skipSourceRows = skipSourceRows;
	}

	/**
	 * @return the value of the skipSourceRows
	 */
	public int getSkipSourceRows() {
		return skipSourceRows;
	}

	/**
	 * Sets the field delimiter.
	 *
	 * @param fieldDelimiter the new field delimiter
	 */
	public void setFieldDelimiter(String fieldDelimiter) {
		this.fieldDelimiter = fieldDelimiter;
	}

	/**
	 * @return the field delimiter
	 */
	public String getFieldDelimiter() {
		return fieldDelimiter;
	}

	/**
	 * @return an array of field delimiters
	 */
	public String[] getFieldDelimiters() {
		if (!StringUtils.isEmpty(fieldDelimiter)) {
			return fieldDelimiter.split(Defaults.DataFormatter.DELIMITER_DELIMITERS_REGEX);
		}

		return null;
	}

	/**
	 * @return <code>true</code> if the field delimiter is specified, <code>false</code> otherwise
	 */
	public boolean isSpecifiedFieldDelimiter() {
		return (getFieldDelimiters() != null);
	}

	/**
	 * Sets the data record delimiter.
	 *
	 * @param recordDelimiter the new data record delimiter
	 */
	public void setRecordDelimiter(String recordDelimiter) {
		this.recordDelimiter = recordDelimiter;
	}

	/**
	 * @return the data record delimiter
	 */
	public String getRecordDelimiter() {
		return recordDelimiter;
	}

	/**
	 * @return an array of data record delimiters
	 */
	public String[] getRecordDelimiters() {
		if (!StringUtils.isEmpty(recordDelimiter)) {
			return recordDelimiter.split(Defaults.DataFormatter.DELIMITER_DELIMITERS_REGEX);
		}

		return null;
	}

	/**
	 * @return <code>true</code> if the data record delimiter is specified, <code>false</code> otherwise
	 */
	public boolean isSpecifiedRecordDelimiter() {
		return (getRecordDelimiters() != null);
	}

	/**
	 * Sets the size of the data record.
	 *
	 * @param recordSize the new size
	 */
	public void setRecordSize(int recordSize) {
		this.recordSize = recordSize;
	}

	/**
	 * @return the size of the data record
	 */
	public int getRecordSize() {
		if (recType != FIXEDLEN_RECORD) {
			return -1; // unknown size
		}

		if (recordSize > -1) {
			return recordSize;
		}

		return getRecordSizeStripAutoFilling();
	}

	/**
	 * @return the size of the data record with auto-filling stripped off
	 */
	public int getRecordSizeStripAutoFilling() {
		if (recType != FIXEDLEN_RECORD) {
			return -1; // unknown size
		}

		// compute size of fixed record (without trailing filler)
		int recSize = 0;
		int prevEnd = 0;

		for (DataFieldMetadata field : fields) {
			if (field.getAutoFilling() == null) {
				prevEnd += field.getSize() + field.getShift();
				recSize = Math.max(recSize, prevEnd);
			}
		}

		return recSize;
	}

	/**
	 * @return an array of data field meta data objects
	 */
	public DataFieldMetadata[] getFields() {
		return fields.toArray(new DataFieldMetadata[fields.size()]);
	}

	/**
	 * @return the number of data fields within the data record
	 */
	public int getNumFields() {
		return fields.size();
	}

	/**
	 * Returns a <code>DataFieldMetadata</code> reference based on the field's position within a data record.
	 *
	 * @param fieldNumber the ordinal number of the requested field
	 *
	 * @return a <code>DataFieldMetadata</code> reference
	 */
	public DataFieldMetadata getField(int fieldNumber) {
		if (fieldNumber < 0 || fieldNumber >= fields.size()) {
			return null;
		}

		return fields.get(fieldNumber);
	}

	/**
	 * Returns a <code>DataFieldMetadata</code> reference based on the field's name.
	 *
	 * @param fieldName the name of the requested field
	 *
	 * @return a <code>DataFieldMetadata</code> reference
	 */
	public DataFieldMetadata getField(String fieldName) {
		int position = getFieldPosition(fieldName);

		if (position >= 0) {
			return getField(position);
		}

		return null;
	}

	/**
	 * Returns the type of a field based on the field's position within a data record.
	 *
	 * @param fieldNumber the ordinal number of the requested field
	 *
	 * @return the type of the field
	 */
	public char getFieldType(int fieldNumber) {
		DataFieldMetadata field = getField(fieldNumber);

		if (field != null) {
			return field.getType();
		}

		return DataFieldMetadata.UNKNOWN_FIELD;
	}

	/**
	 * Returns the type of a field based on the field's name.
	 * 
	 * @param fieldName the name of the requested field
	 *
	 * @return the type of the field
	 */
	public char getFieldType(String fieldName) {
		DataFieldMetadata field = getField(fieldName);

		if (field != null) {
			return field.getType();
		}

		return DataFieldMetadata.UNKNOWN_FIELD;
	}

	/**
	 * Returns the type of a field based on the field's position within a data record as a string.
	 *
	 * @param fieldNumber the ordinal number of the requested field
	 *
	 * @return the type of the field as a string
	 */
	public String getFieldTypeAsString(int fieldNumber) {
		DataFieldMetadata field = getField(fieldNumber);

		if (field != null) {
			return field.getTypeAsString();
		}

		return DataFieldMetadata.UNKNOWN_TYPE;
	}

	/**
	 * Returns the position of a field based on the field's name.
	 * 
	 * @param fieldName the name of the requested field
	 *
	 * @return the position of the field within the data record or -1 if no such field exists
	 */
	public int getFieldPosition(String fieldName) {
		if (fieldNamesMap.isEmpty()) {
			updateFieldNamesMap();
		}

		Integer position = fieldNamesMap.get(fieldName);

		if (position != null) {
			return position;
		}

		return -1;
	}

	/**
	 * Adds a field to the data record.
	 *
	 * @param field a <code>DataFieldMetadata</code> reference
	 */
	public void addField(DataFieldMetadata field) {
		addField(fields.size(), field);
	}

	/**
	 * Adds a field to the data record at the specified index.
	 *
	 * @param index the index to be used to add the field
	 * @param field a <code>DataFieldMetadata</code> reference
	 */
	public void addField(int index, DataFieldMetadata field) {
		field.setDataRecordMetadata(this);
		fields.add(index, field);

		structureChanged();
	}

	/**
	 * Deletes a data field identified by the given field number.
	 *
	 * @param fieldNumber the ordinal number of the field to be deleted
	 */
	public void delField(int fieldNumber) {
		if (fieldNumber >= 0 && fieldNumber < fields.size()) {
			fields.remove(fieldNumber);
			structureChanged();
		}
	}

	/**
	 * Deletes a data field identified by the given field number.
	 *
	 * @param fieldName the name of the field to be deleted
	 */
	public void delField(String fieldName) {
		int position = getFieldPosition(fieldName);

		if (position >= 0) {
			delField(position);
		}
	}

	/**
	 * Deletes all fields in this data field meta data.
	 */
	public void delAllFields() {
		fields.clear();
		structureChanged();
	}

	/**
	 * Call if the structure of the meta data changes (a field was added/removed).
	 */
	private void structureChanged() {
		recordSize = -1;

		fieldNamesMap.clear();
		fieldTypes.clear();
		fieldOffset.clear();

		fieldNullSwitch.resize(fields.size());
		numNullableFields = 0;

		int count = 0;

		for (DataFieldMetadata fieldMeta : fields) {
			fieldMeta.setNumber(count);

			if (fieldMeta.isNullable()) {
				fieldNullSwitch.set(count);
				numNullableFields++;
			}

			count++;
		}
	}

	/**
	 * @return an array of field names sorted by the fields' ordinal numbers
	 */
	public String[] getFieldNamesArray() {
		String[] fieldNamesArray = new String[fields.size()];

		for (int i = 0; i < fields.size(); i++) {
			fieldNamesArray[i] = fields.get(i).getName();
		}

		return fieldNamesArray;
	}

	/**
	 * @return a map mapping field names to field ordinal numbers
	 *
	 * @since 2nd May 2002
	 */
	public Map<String, Integer> getFieldNamesMap() {
		if (fieldNamesMap.isEmpty()) {
			updateFieldNamesMap();
		}

		return new HashMap<String, Integer>(fieldNamesMap);
	}

	/**
	 * Used to populate the fieldNamesMap map if empty.
	 */
	private void updateFieldNamesMap() {
		assert (fieldNamesMap.isEmpty());

		for (int i = 0; i < fields.size(); i++) {
			fieldNamesMap.put(fields.get(i).getName(), i);
		}
	}

	/**
	 * @return a map mapping field ordinal numbers to field types
	 *
	 * @since 2nd May 2002
	 */
	public Map<Integer, String> getFieldTypes() {
		if (fieldTypes.isEmpty()) {
			for (int i = 0; i < fields.size(); i++) {
				fieldTypes.put(i, Character.toString(fields.get(i).getType()));
			}
		}

		return new HashMap<Integer, String>(fieldTypes);
	}

	/**
	 * Returns the offset of a field based on the field's name.
	 * 
	 * @param fieldName the name of the requested field
	 *
	 * @return the offset of the field within the data record or -1 if no such field exists
	 */
	public int getFieldOffset(String fieldName) {
		if (recType != FIXEDLEN_RECORD) {
			return -1;
		}

		if (fieldOffset.isEmpty()) {
			int offset = 0;

			for (DataFieldMetadata field : fields) {
				fieldOffset.put(field.getName(), offset + field.getShift());
				offset += field.getSize();
			}
		}

		Integer offset = fieldOffset.get(fieldName);

		if (offset != null) {
			return offset;
		}

		return -1;
	}

	/**
	 * Sets the array of field names specifying a primary key of the data record.
	 *
	 * @param keyFieldNames the array of field names to be used
	 */
	public void setKeyFieldNames(String[] keyFieldNames) {
		this.keyFieldNames = keyFieldNames;
	}

	/**
	 * @return an array of field names specifying a primary key of the data record
	 */
	public String[] getKeyFieldNames() {
		return keyFieldNames;
	}

	/**
	 * Creates and initializes a record key for the specified key field names.
	 *
	 * @return a new initialized record key or <code>null</code> no key field names are specified
	 *
	 * @throw RuntimeException if any of the field names is invalid
	 */
	public RecordKey getRecordKey() {
		if (keyFieldNames == null) {
			return null;
		}

		RecordKey recordKey = new RecordKey(keyFieldNames, this);
		recordKey.init();

		return recordKey;
	}

	/**
	 * @return a <code>BitArray</code> where bits are set for fields which may contain a NULL value.
	 *
	 * @since 18th January 2007
	 */
	public BitArray getFieldsNullSwitches() {
		return fieldNullSwitch;
	}

	/**
	 * @return the number of nullable fields
	 *
	 * @since 18th January 2007
	 */
	public short getNumNullableFields() {
		return numNullableFields;
	}

	/**
	 * @return <code>true</code> if a data record described by this meta data contains at least one field which may
	 * contain a NULL value, <code>false</code> otherwise
	 *
	 * @since 18th January 2007
	 */
	public boolean isNullable() {
		return (numNullableFields != 0);
	}

	/**
	 * Sets the recordProperties attribute of the DataRecordMetadata object Record properties allows defining additional
	 * parameters for record. These parameters (key-value pairs) are NOT normally handled by CloverETL, but can be used
	 * in user's code or specialized Components.
	 * 
	 * @param properties The new recordProperties value
	 */
	public void setRecordProperties(Properties properties) {
		recordProperties = new TypedProperties(properties);
	}

	/**
	 * Gets the recordProperties attribute of the DataRecordMetadata object.
	 *
	 * @return The recordProperties value
	 */
	public TypedProperties getRecordProperties() {
		return recordProperties;
	}

	/**
	 * Sets a value of the property with the given key.
	 *
	 * @param key the key of the property
	 * @param value the value to be set
	 */
	public String getProperty(String attrName) {
		return recordProperties.getProperty(attrName);
	}

	/**
	 * Sets the locale code string.
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
		return localeStr;
	}

	/**
	 * Sets the number format pattern as a default format string for numeric data fields.
	 *
	 * @param numberFormatStr the new number format pattern
	 */
	public void setNumberFormatStr(String numberFormatStr) {
		this.numberFormatStr = numberFormatStr;
	}

	/**
	 * @return the number format pattern as a default format string for numeric data fields, or <code>null</code> if no
	 * format pattern is set
	 */
	public String getNumberFormatStr() {
		return numberFormatStr;
	}

	/**
	 * Sets the date format pattern as a default format string for date/time data fields, or <code>null</code> if no
	 * format pattern is set
	 *
	 * @param dateFormatStr the new date format pattern
	 */
	public void setDateFormatStr(String dateFormatStr) {
		this.dateFormatStr = dateFormatStr;
	}

	/**
	 * @return the date format pattern as a default format string for date/time data fields
	 */
	public String getDateFormatStr() {
		if (dateFormatStr != null) {
			return dateFormatStr;
		}

		return Defaults.DEFAULT_DATE_FORMAT;
	}

	/**
	 * Creates a deep copy of this data record meta data object.
	 *
	 * @return an exact copy of current data record meta data object
	 */
	public DataRecordMetadata duplicate() {
		DataRecordMetadata dataRecordMetadata = new DataRecordMetadata(name, recType);

		dataRecordMetadata.setName(name);
		dataRecordMetadata.setDescription(description);
		dataRecordMetadata.setRecType(recType);
		dataRecordMetadata.setSkipSourceRows(skipSourceRows);
		dataRecordMetadata.setRecordDelimiter(recordDelimiter);
		dataRecordMetadata.setFieldDelimiter(fieldDelimiter);
		dataRecordMetadata.setRecordSize(recordSize);

		for (DataFieldMetadata field : fields) {
			dataRecordMetadata.addField(field.duplicate());
		}

		dataRecordMetadata.setRecordProperties(recordProperties);
		dataRecordMetadata.setLocaleStr(localeStr);

		return dataRecordMetadata;
	}

	/**
	 * Checks if the meta is valid.
	 *
	 * @param status
	 * @return
	 */
	public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		// verify count of fields
		if (fields.size() == 0) {
			status.add(new ConfigurationProblem("No field elements for '" + name + "' have been found!",
					Severity.ERROR, null, Priority.NORMAL));
		}
		
		// verify recordType
		if (recType != DELIMITED_RECORD && recType != FIXEDLEN_RECORD && recType != MIXED_RECORD) {
			status.add(new ConfigurationProblem("Unknown record type '" + recType + "' in the record metadata element '"
					+ name + "'.", Severity.ERROR, null, Priority.NORMAL));
		}

		// verify delimiters - field delimiters
		verifyDelimitersAndSizes(status); // cannot move to field metadata yet because of mixed field metadata
		
		// verify field names
		verifyFieldNames(status);
		
		// call checkConfig at meta fields
		for (DataFieldMetadata field: fields) {
			field.checkConfig(status);
		}
		
		return status;
	}

	/**
	 * Verifies field names.
	 * @param status
	 */
	private void verifyFieldNames(ConfigurationStatus status) {
		// the same names
		Set<String> setName = new HashSet<String>();
		String sName;
		for (DataFieldMetadata field: fields) {
			sName = field.getName();
			if (setName.contains(sName)) {
				status.add(new ConfigurationProblem("Field name '" + field.getName() + "' in the record element '"
						+ name + "' is defined more than once!", Severity.ERROR, null, Priority.NORMAL));
			} else {
				setName.add(sName);
			}
		}
	}

	/**
	 * Verifies field delimiters and sizes.
	 * @param status
	 */
	private void verifyDelimitersAndSizes(ConfigurationStatus status) {
		switch (recType) {
		case DELIMITED_RECORD:
			for (DataFieldMetadata field: fields) {
				verifyFieldDelimiter(field, status);
			}
			break;
			
		case FIXEDLEN_RECORD:
			for (DataFieldMetadata field: fields) {
				verifyFieldSize(field, status);
			}
			break;

		case MIXED_RECORD:
			for (DataFieldMetadata field: fields) {
				// delimited field
				if (field.isDelimited()) verifyFieldDelimiter(field, status);
				// field size
				else verifyFieldSize(field, status);
			}
			break;

		default:
			break;
		}
	}
	
	/**
	 * Verifies field size.
	 * @param status
	 */
	private void verifyFieldSize(DataFieldMetadata field, ConfigurationStatus status) {
		if (field.getSize() <= 0) {
			status.add(new ConfigurationProblem("Field size '" + field.getSize() + "' for the field '" + field.getName()
					+ "' in the record element '" + name + "' has wrong number!", Severity.ERROR, null, Priority.NORMAL));
		}
	}
	
	/**
	 * Verifies field delimiter.
	 * @param field
	 * @param status
	 */
	private void verifyFieldDelimiter(DataFieldMetadata field, ConfigurationStatus status) {
		if (field.isEofAsDelimiter()) return;
		String[] fieldDelimiters = field.getDelimiters();
		if (fieldDelimiters == null || fieldDelimiters.length == 0) {
			status.add(new ConfigurationProblem("Field delimiter for the field '" + field.getName() +
					"' in the record element '" + name + "' not found!", Severity.ERROR, null, Priority.NORMAL));
		}
	}

	/**
	 * Finds a field with a given autoFilling function.
	 * 
	 * @param autoFilling the autoFilling function.
	 *
	 * @return index of the field or -1 if no such field exists
	 */
	public int findAutoFilledField(String autoFilling) {
		int fieldNumber = 0;

		for (DataFieldMetadata field : fields) {
			if (autoFilling.equals(field.getAutoFilling())) {
				return fieldNumber;
			}

			fieldNumber++;
		}

		return -1;
	}

	/**
	 * Returns field ordinal numbers for all specified field names.
	 *
	 * @param fieldNames an array of field names
	 *
	 * @return an array of corresponding field ordinal numbers
	 */
	public int[] fieldsIndices(String... fieldNames) {
		int[] indices = new int[fieldNames.length];

		for (int i = 0; i < fieldNames.length; i++) {
			indices[i] = getFieldPosition(fieldNames[i]);

			if (indices[i] < 0) {
				throw new RuntimeException("No such field name found for: '" + name + "'");
			}
		}

		return indices;
	}

	/**
	 * @return header with field names ended by field delimiters; used by flat file writers for file header describing data
	 */
	public String getFieldNamesHeader() {
		StringBuilder ret = new StringBuilder();
		DataFieldMetadata dataFieldMetadata;
		short fieldSize;
		int headerSize;
		char blank = ' ';

		for (int i = 0; i < getNumFields(); i++) {
			dataFieldMetadata = getField(i);
			if (dataFieldMetadata.isDelimited()) {
				// delim: add field name and delimiter
				ret.append(dataFieldMetadata.getName());
				ret.append(dataFieldMetadata.getDelimiters()[0]);
			} else {
				// fixlen: strip header name or add blank spaces
				fieldSize = dataFieldMetadata.getSize();
				if (fieldSize <= (headerSize = dataFieldMetadata.getName().length())) {
					ret.append(dataFieldMetadata.getName().substring(0, fieldSize));
				} else {
					ret.append(dataFieldMetadata.getName());
					for (int j = fieldSize - headerSize; j > 0; j--) {
						ret.append(blank);
					}
				}
			}
		}
		// add record delimiter for fixlen
		if (getField(getNumFields() - 1).isFixed()) {
			String[] delimiters = getRecordDelimiters();
			if (delimiters != null) {
				for (String delim : delimiters) {
					if (delim != null) {
						ret.append(delim);
					}
				}
			}
		}

		return ret.toString();
	}

	/**
	 * This method is used by GUI to prepopulate record meta info with default fields and user defined sizes. It also
	 * adjusts the number of fields and their sizes based on user selection.
	 * 
	 * @param fieldWidths
	 */
	public void bulkLoadFieldSizes(short[] fieldWidths) {
		DataFieldMetadata aField = null;
		int fieldsNumber = fields.size();

		// if no fields then create default fields with sizes as in objects
		if (fieldsNumber == 0) {
			for (int i = 0; i < fieldWidths.length; i++) {
				addField(new DataFieldMetadata("Field" + Integer.toString(i), fieldWidths[i]));
			}
		} else {
			// fields exist
			for (int i = 0; i < fieldWidths.length; i++) {
				if (i < fieldsNumber) {
					// adjust the sizes
					aField = getField(i);
					aField.setSize(fieldWidths[i]);
				} else {
					// insert new fileds, if any
					addField(new DataFieldMetadata("Field" + Integer.toString(i), fieldWidths[i]));
				}
			}
			if (fieldsNumber > fieldWidths.length) {
				// remove deleted fields, if any
				for (int i = fieldWidths.length; i < fieldsNumber; i++) {
					delField(i);
				}
			}
		}

	}

	public boolean equals(Object object) {
		return equals(object, true);
	}

	public boolean equals(Object object, boolean checkFixDelType) {
		if (object == this) {
			return true;
		}

		if (!(object instanceof DataRecordMetadata)) {
			return false;
		}

		DataRecordMetadata dataRecordMetadata = (DataRecordMetadata) object;

		if (getNumFields() != dataRecordMetadata.getNumFields()) {
			return false;
		}

		for (int i = 0; i < getNumFields(); i++) {
			if (!getField(i).equals(dataRecordMetadata.getField(i), checkFixDelType)) {
				return false;
			}
		}

		return true;
	}

	public int hashCode() {
		int hashCode = 17;

		for (DataFieldMetadata field : fields) {
			hashCode = 37 * hashCode + field.hashCode();
		}

		return hashCode;
	}

	public String toString() {
		StringBuffer buffer = new StringBuffer();
		buffer.append("DataRecordMetadata[");
		buffer.append("fields = ").append(fields);
		buffer.append(", fieldNamesMap = ").append(getFieldNamesMap());
		buffer.append(", fieldTypes = ").append(getFieldTypes());
		buffer.append(", name = ").append(name);
		buffer.append(", recType = ").append(recType);
		buffer.append(", localeStr = ").append(localeStr);
		buffer.append(", skipSourceRows = ").append(skipSourceRows);
		buffer.append(", recordProperties = ").append(recordProperties);
		buffer.append(", DELIMITED_RECORD = ").append(DELIMITED_RECORD);
		buffer.append(", FIXEDLEN_RECORD = ").append(FIXEDLEN_RECORD);
		buffer.append("]");

		return buffer.toString();
	}

	/**
	 * Iterator for contained field meta data.
	 *
	 * @see java.lang.Iterable#iterator()
	 */
	public Iterator<DataFieldMetadata> iterator() {
		return fields.iterator();
	}

	/**
	 * Returns true if metadata contains at least one field without auto-filling.
	 * @return
	 */
	public boolean hasFieldWithoutAutofilling() {
		for (DataFieldMetadata dataFieldMetadata : getFields()) {
			if (!dataFieldMetadata.isAutoFilled()) {
				return true;
			}
		}
		return false;
	}

}

