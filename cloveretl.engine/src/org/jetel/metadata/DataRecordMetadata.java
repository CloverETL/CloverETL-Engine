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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.DataRecordNature;
import org.jetel.data.Defaults;
import org.jetel.data.RecordKey;
import org.jetel.data.Token;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.ConfigurationStatus.Priority;
import org.jetel.exception.ConfigurationStatus.Severity;
import org.jetel.exception.InvalidGraphObjectNameException;
import org.jetel.graph.JobType;
import org.jetel.graph.TransformationGraph;
import org.jetel.util.primitive.BitArray;
import org.jetel.util.primitive.TypedProperties;
import org.jetel.util.string.QuotingDecoder;
import org.jetel.util.string.StringUtils;

import edu.umd.cs.findbugs.annotations.SuppressWarnings;

/**
 * A class that represents metadata describing a data record.
 * 
 * For possible concurrency problem see {@link #structureChanged()}.
 * 
 * @author David Pavlis, Javlin a.s. &lt;david.pavlis@javlin.eu&gt;
 * @author Martin Janik, Javlin a.s. &lt;martin.janik@javlin.eu&gt;
 * 
 * @version 20th January 2010
 * @created 26th March 2002
 * 
 * @see DataFieldMetadata
 * @see org.jetel.data.DataRecord
 * @see org.jetel.data.DataField
 * 
 * @revision $Revision$
 */
public class DataRecordMetadata implements Serializable, Iterable<DataFieldMetadata> {

	private static final long serialVersionUID = 7032218607804024730L;
	
	/** The default string value that is considered as null. */
	public static final String DEFAULT_NULL_VALUE = "";
	
	public static final String EMPTY_NAME = "_";

	/** Parent graph of this metadata */
	private TransformationGraph graph;
	/** Name of the data record. */
	private String name;
	/** Description of the data record. */
	private String description;
	/** Original name of the data record. */
	private String label;

	/** The type of the flat file parsing - delimited, fixedlen or mixed */
	private DataRecordParsingType parsingType;

	private int skipSourceRows = -1;
	
	private boolean quotedStrings;
	private Character quoteChar;

	private String recordDelimiter;
	private String fieldDelimiter;
	private int recordSize = -1;

	@SuppressWarnings("Se")
	private List<DataFieldMetadata> fields = new ArrayList<DataFieldMetadata>();
	@SuppressWarnings("Se")
	private Map<String, Integer> fieldNamesMap = new HashMap<String, Integer>();
	@SuppressWarnings("Se")
	private Map<String, Integer> fieldLabelsMap = new HashMap<String, Integer>();
	@SuppressWarnings("Se")
	private Map<Integer, DataFieldType> fieldTypes = new HashMap<Integer, DataFieldType>();
	@SuppressWarnings("Se")
	private Map<String, Integer> fieldOffset = new HashMap<String, Integer>();
	
	private List<DataFieldMetadata> keyFields = new ArrayList<DataFieldMetadata>();

	/** an array of field names specifying a primary key */
	private List<String> keyFieldNames = new ArrayList<String>();

	private short numNullableFields = 0;

	private TypedProperties recordProperties = new TypedProperties();
	private String localeStr = null;

	/** a format string for numbers */
	private String numberFormatStr = null;
	/** a format string for dates */
	private String dateFormatStr = null;

	/** String value that is considered as null (in addition to null itself). */
	private String nullValue = DEFAULT_NULL_VALUE;

	/**
	 * Default collator sensitivity for string fields. Can be overridden by DataFieldMetadata.
	 * See Collator.setStregth(String strength).
	 */
	private String collatorSensitivity = null;
	
	/**
	 * Metadadata nature should correspond with graph nature, see {@link #checkConfig(ConfigurationStatus)}.
	 * Different implementations of DataRecord are used for various natures.
	 * Nature {@link DataRecordNature#DATA_RECORD} is represented by {@link DataRecord}.
	 * Nature {@link DataRecordNature#TOKEN} is represented by {@link Token}.
	 * Record nature is by default derived from graph nature. The {@link GraphNature#ETL_GRAPH}
	 * corresponds with {@link DataRecordNature#DATA_RECORD} and the {@link GraphNature#JOBFLOW}
	 * corresponds with {@link DataRecordNature#TOKEN}. 
	 * @see DataRecordFactory
	 */
	private DataRecordNature nature = null;
	
	/**
	 * @deprecated use {@link DataRecordParsingType#DELIMITED} instead
	 */
	@Deprecated
	public static final char DELIMITED_RECORD = 'D';
	/**
	 * @deprecated use {@link DataRecordParsingType#FIXEDLEN} instead
	 */
	@Deprecated
	public static final char FIXEDLEN_RECORD = 'F';
	/**
	 * @deprecated use {@link DataRecordParsingType#MIXED} instead
	 */
	@Deprecated
	public static final char MIXED_RECORD = 'M';

	/**
	 * Constructs data record metadata with given name.
	 *
	 * @param name the name of the data record
	 *
	 * @since 2nd May 2002
	 */
	public DataRecordMetadata(String name) {
		this(name, null);
	}

	/**
	 * Constructs data record metadata with given name and type.
	 *
	 * @param name the name of the data record
	 * @param recType the type of the data record
	 *
	 * @since 2nd May 2002
	 * @deprecated use {@link DataRecordMetadata#DataRecordMetadata(String, DataRecordParsingType)} instead
	 */
	@Deprecated
	public DataRecordMetadata(String name, char recType) {
		this(name, DataRecordParsingType.fromChar(recType));
	}

	/**
	 * Constructs data record metadata with given name and type.
	 * @param name
	 * @param flatDataType
	 */
	public DataRecordMetadata(String name, DataRecordParsingType parsingType) {
		setName(name);
		this.parsingType = parsingType;
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
	 * Sets the original name of the data record.
	 * 
	 * @param label the original name of the record
	 */
	public void setLabel(String label) {
		this.label = label;
	}

	/**
	 * Returns the original name of the data record.
	 * 
	 * @return the original name of the record 
	 */
	public String getLabel() {
		return label;
	}

	/**
	 * Returns the label of the data record.
	 * If it is not set, returns the name of the record.
	 * 
	 * @return the label of the record 
	 */
	public String getLabelOrName() {
		if (label == null) {
			return getName();
		}
		return label;
	}

	/**
	 * Sets the type of record (delimited/fixed-length).
	 *
	 * @param recType the new type of record
	 *
	 * @since 3rd May 2002
	 * @deprecated use {@link #setParsingType(DataRecordParsingType)} instead
	 */
	@Deprecated
	public void setRecType(char recType) {
		setParsingType(DataRecordParsingType.fromChar(recType));
	}

	/**
	 * Sets the type of flat parsing (delimited/fixed-length/mixed).
	 * @param flatDataType
	 */
	public void setParsingType(DataRecordParsingType parsingType) {
		this.parsingType = parsingType;
	}
	
	/**
	 * @return the type of record (delimited/fixed-length)
	 *
	 * @since 3rd May 2002
	 * @deprecated use {@link #getParsingType()} instead
	 */
	@Deprecated
	public char getRecType() {
		if (parsingType != null) {
			return parsingType.getObsoleteIdnetifier();
		} else {
			return 0;
		}
	}

	/**
	 * @return the type of flat parsing (delimited/fixed-length/mixed)
	 */
	public DataRecordParsingType getParsingType() {
		return parsingType;
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
	 * @return the quote character
	 */
	public Character getQuoteChar() {
		return quoteChar;
	}
	
	/**
	 * @param quoteChar strings quote character
	 */
	public void setQuoteChar(Character quoteChar) {
		this.quoteChar = quoteChar;
	}
	
	/**
	 * @return are strings quoted?
	 */
	public boolean isQuotedStrings() {
		return quotedStrings;
	}
	
	/**
	 * @param quotedStrings are strings quoted?
	 */
	public void setQuotedStrings(boolean quotedStrings) {
		this.quotedStrings = quotedStrings;
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
	 * @return <code>true</code> if any field/record delimiter contains a carriage return, <code>false</code> otherwise
	 */
	public boolean containsCarriageReturnInDelimiters() {
		for (DataFieldMetadata field : fields) {
			if (field.containsCarriageReturnInDelimiters()) {
				return true;
			}
		}

		return false;
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
		if (parsingType != DataRecordParsingType.FIXEDLEN) {
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
		if (parsingType != DataRecordParsingType.FIXEDLEN) {
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
	 * @return an array of data field metadata objects
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
	 * Returns a <code>DataFieldMetadata</code> reference based on the field's label.
	 *
	 * @param label the label of the requested field
	 *
	 * @return a <code>DataFieldMetadata</code> reference
	 */
	public DataFieldMetadata getFieldByLabel(String label) {
		int position = getFieldPositionByLabel(label);

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
	 * @deprecated use {@link #getDataFieldType(int)} instead
	 */
	@Deprecated
	public char getFieldType(int fieldNumber) {
		DataFieldMetadata field = getField(fieldNumber);

		if (field != null) {
			return field.getType();
		}

		return DataFieldMetadata.UNKNOWN_FIELD;
	}

	/**
	 * Returns the type of a field based on the field's position within a data record.
	 *
	 * @param fieldNumber the ordinal number of the requested field
	 *
	 * @return the type of the field
	 */
	public DataFieldType getDataFieldType(int fieldNumber) {
		DataFieldMetadata field = getField(fieldNumber);

		if (field != null) {
			return field.getDataType();
		}

		return DataFieldType.UNKNOWN;
		
	}
	
	/**
	 * Returns the type of a field based on the field's name.
	 * 
	 * @param fieldName the name of the requested field
	 *
	 * @return the type of the field
	 * @deprecated use {@link #getDataFieldType(String)} instead
	 */
	@Deprecated
	public char getFieldType(String fieldName) {
		DataFieldMetadata field = getField(fieldName);

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
	@Deprecated
	public DataFieldType getDataFieldType(String fieldName) {
		DataFieldMetadata field = getField(fieldName);

		if (field != null) {
			return field.getDataType();
		}

		return DataFieldType.UNKNOWN;
	}

	/**
	 * Returns the type of a field based on the field's position within a data record as a string.
	 *
	 * @param fieldNumber the ordinal number of the requested field
	 *
	 * @return the type of the field as a string
	 * @deprecated use {@link #getFieldDataType(int)} and {@link DataFieldType#getName()} instead
	 */
	@Deprecated
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

		Integer position = fieldNamesMap.get(fieldName);

		if (position != null) {
			return position;
		}

		return -1;
	}
	
	/**
	 * Returns the position of a field based on the field's label.
	 * 
	 * If the label is not unique, returns the position
	 * of the first occurence.
	 * 
	 * If no such label exists,
	 * try to find a field by its name instead.
	 * 
	 * @param label the label of the requested field
	 *
	 * @return the position of the field within the data record or -1 if no such field exists
	 */
	public int getFieldPositionByLabel(String label) {

		Integer position = fieldLabelsMap.get(label);

		if (position != null) {
			return position;
		}

		// the label may not exist, but we may find a field by name
		return getFieldPosition(label);
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
	 * Deletes all fields in this data field metadata.
	 */
	public void delAllFields() {
		fields.clear();
		structureChanged();
	}

	/**
	 * Call if the structure of the metadata changes (a field was added/removed).
	 * 
	 * Indexes are re-created by this call. Common use case is that DataRecordMedatada is first filled with all data,
	 * indexes are created and then data are only retrieved.
	 * 
	 * Current implementation can cause concurrency problem as unsynchronized {@link java.util.HashMap} is used for
	 * indexes.
	 * 
	 * Problems would most probably manifest when some thread will not see index in state left by previous
	 * {@link #structureChanged()} call done by other thread but in some older/inconsistent state. As time between
	 * initialization of metadata
	 * structure and using the indexes shall be long enough for HashMap to synchronize without explicit synchronization,
	 * probability of occurrence of this problem is considered to be small. Therefore, risk of this failure is accepted.
	 * 
	 * Reasons are for using unsynchronized HashMap:
	 * 
	 * <li>performance (among wrapping by {@link java.util.Collections#synchronizedMap(Map)}
	 * 
	 * <li>easines of implementation (among using {@link java.util.concurrent.ConcurrentHashMap} which does not allow
	 * null values - a wrapper would be needed but it is too complex to implement)
	 * 
	 */
	private synchronized void structureChanged() {
		recordSize = -1;

		updateFieldNamesMap();
		updateFieldLabelsMap();
		updateFieldTypes();
		updateFieldOffset();
		updateFieldNumbers();
		updateKeyFields();
	}

	private void updateFieldNumbers() {
		int count = 0;

		for (DataFieldMetadata fieldMeta : fields) {
			fieldMeta.setNumber(count++);
		}
	}

	private void updateFieldTypes() {
		fieldTypes.clear();

		for (int i = 0; i < fields.size(); i++) {
			fieldTypes.put(i, fields.get(i).getDataType());
		}
	}

	private void updateKeyFields() {
		keyFields.clear();
		for (String keyFieldName : keyFieldNames) {
			DataFieldMetadata keyField = this.getField(keyFieldName);
			if (keyField != null) {
				keyFields.add(keyField);
			}
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
		return new HashMap<String, Integer>(fieldNamesMap);
	}

	/**
	 * Used to populate the fieldNamesMap map if empty.
	 */
	private void updateFieldNamesMap() {
		fieldNamesMap.clear();

		for (int i = 0; i < fields.size(); i++) {
			fieldNamesMap.put(fields.get(i).getName(), i);
		}
	}

	/**
	 * Used to populate the fieldLabelsMap map if empty.
	 */
	private void updateFieldLabelsMap() {
		fieldLabelsMap.clear();
		
		for (int i = 0; i < fields.size(); i++) {
			String label = fields.get(i).getLabelOrName();
			if (!fieldLabelsMap.containsKey(label)) {
				fieldLabelsMap.put(label, i);
			}
		}
	}
	
	/**
	 * @return a map mapping field labels to field ordinal numbers
	 *
	 * @since 8nd Dec 2011
	 */
	public Map<String, Integer> getFieldLabelsMap() {
		return new HashMap<String, Integer>(fieldLabelsMap);
	}

	/**
	 * @return a map mapping field ordinal numbers to field types
	 *
	 * @since 2nd May 2002
	 */
	public Map<Integer, DataFieldType> getFieldTypes() {
		return new HashMap<Integer, DataFieldType>(fieldTypes);
	}

	/**
	 * Returns the offset of a field based on the field's name.
	 * 
	 * @param fieldName the name of the requested field
	 *
	 * @return the offset of the field within the data record or -1 if no such field exists
	 */
	public int getFieldOffset(String fieldName) {
		if (parsingType != DataRecordParsingType.FIXEDLEN) {
			return -1;
		}

		Integer offset = fieldOffset.get(fieldName);

		if (offset != null) {
			return offset;
		}

		return -1;
	}

	private void updateFieldOffset() {
		int offset = 0;
		fieldOffset.clear();

		for (DataFieldMetadata field : fields) {
			fieldOffset.put(field.getName(), offset + field.getShift());
			offset += field.getSize();
		}
	}

	/**
	 * Sets field names specifying a primary key of the data record.
	 * 
	 * The names can then be retrieved back by getKeyFieldNames(),
	 * or they can be resolved into field metadata by getKeyFields().
	 *
	 * @param keyFieldNames the array of field names to be used
	 */
	public void setKeyFieldNames(List<String> keyFieldNames) {
		this.keyFieldNames.clear();
		this.keyFieldNames.addAll(keyFieldNames);
		updateKeyFields();
	}

	/**
	 * Returns a list of field names specifying a primary key of the data record.
	 * 
	 * Note that it is not guaranteed that all field names point to an existing field. On the other hand it is checked in configCheck()
	 * 
	 * @return a list of field names specifying a primary key of the data record
	 */
	public List<String> getKeyFieldNames() {
		return Collections.unmodifiableList(keyFieldNames);
	}

	/**
	 * Returns a list of fields specifying a primary key of the data record
	 * 
	 * Note that it may return shorter list than getKeyFieldNames, if some of field names do not point to an existing field.
	 * Matching of field names is checked in checkConfig. 
	 * 
	 * @return a list of fields specifying a primary key of the data record
	 */
	public List<DataFieldMetadata> getKeyFields() {
		return Collections.unmodifiableList(keyFields);
	}

	
	/**
	 * Creates and initializes a record key for the specified key field names.
	 *
	 * @return a new initialized record key or <code>null</code> no key field names are specified
	 *
	 * @throw RuntimeException if any of the field names is invalid
	 */
	public RecordKey getRecordKey() {
		if (keyFieldNames.isEmpty()) {
			return null;
		}

		RecordKey recordKey = new RecordKey(keyFieldNames.toArray(new String[keyFieldNames.size()]), this);
		recordKey.init();

		return recordKey;
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
	 * @return <code>true</code> if a data record described by this metadata contains at least one field which may
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
	 * Sets a string value that will be considered as <code>null</code> (in addition to <code>null</code> itself).
	 *
	 * @param nullValue the string value to be considered as null, or <code>null</code> if the default null value
	 * should be used
	 */
	public void setNullValue(String nullValue) {
		this.nullValue = (nullValue != null) ? nullValue : DEFAULT_NULL_VALUE;
	}

	/**
	 * @return the string value that is considered as <code>null</code>, never returns <code>null</code>
	 */
	public String getNullValue() {
		return nullValue;
	}

	/**
	 * Creates a deep copy of this data record metadata object.
	 *
	 * @return an exact copy of current data record metadata object
	 */
	public DataRecordMetadata duplicate() {
		DataRecordMetadata dataRecordMetadata = new DataRecordMetadata(name, parsingType);

		dataRecordMetadata.setName(name);
		dataRecordMetadata.setLabel(label);
		dataRecordMetadata.setDescription(description);
		dataRecordMetadata.setParsingType(parsingType);
		dataRecordMetadata.setSkipSourceRows(skipSourceRows);
		dataRecordMetadata.setRecordDelimiter(recordDelimiter);
		dataRecordMetadata.setFieldDelimiter(fieldDelimiter);
		dataRecordMetadata.setQuoteChar(quoteChar);
		dataRecordMetadata.setQuotedStrings(quotedStrings);
		dataRecordMetadata.setRecordSize(recordSize);
		//nature of duplicate is preserve
		dataRecordMetadata.setNature(getNature());

		for (DataFieldMetadata field : fields) {
			dataRecordMetadata.addField(field.duplicate());
		}

		dataRecordMetadata.setRecordProperties(recordProperties);
		dataRecordMetadata.setLocaleStr(localeStr);
		dataRecordMetadata.setCollatorSensitivity(collatorSensitivity);
		
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
		
		// verify delimiters - field delimiters
		verifyDelimitersAndSizes(status); // cannot move to field metadata yet because of mixed field metadata
		
		// verify field names
		verifyFieldNames(status);
		
		//verify field names that define a primary key
		verifyKeyFieldNames(status);
		
		//verify job type - has to be same as job type of parent graph
		TransformationGraph parentGraph = getGraph();
		if (parentGraph != null) {
			if (parentGraph.getJobType() == JobType.JOBFLOW && getNature() != DataRecordNature.TOKEN) {
				status.add(new ConfigurationProblem("Invalid metadata '" + name + "'. Token metadata nature is required.",
						Severity.ERROR, null, Priority.NORMAL));
			}
		}
		
		// call checkConfig at meta fields
		for (DataFieldMetadata field: fields) {
			field.checkConfig(status);
		}
		
		return status;
	}

	/**
	 * Verifies that all field names that define a primary key of a record are names of
	 * existing fields
	 * 
	 * @param status
	 */
	private void verifyKeyFieldNames(ConfigurationStatus status) {
		if (!keyFieldNames.isEmpty()) {
			Set<String> fieldNames = new HashSet<String>();
			for (DataFieldMetadata field: fields) {
				fieldNames.add(field.getName());
			}
			
			for (String keyFieldName : keyFieldNames) {
				if (!fieldNames.contains(keyFieldName)) {
					status.add(new ConfigurationProblem("Field with name '" + keyFieldName + "' that is listed in a record key " +
							"does not exist", Severity.ERROR, null, Priority.NORMAL));
				}
			}
		}
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
		switch (parsingType) {
		case DELIMITED:
			for (DataFieldMetadata field: fields) {
				verifyFieldDelimiter(field, status);
			}
			break;
			
		case FIXEDLEN:
			for (DataFieldMetadata field: fields) {
				verifyFieldSize(field, status);
			}
			break;

		case MIXED:
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

	public int[] fieldsIndicesComplement(String... excludedFieldNames) {
		int numberOfFieldIndices = getNumFields();
		BitArray excludeFieldFlags = new BitArray(numberOfFieldIndices);

		if (excludedFieldNames != null) {
			numberOfFieldIndices -= excludedFieldNames.length;

			for (String fieldName : excludedFieldNames) {
				int fieldIndex = getFieldPosition(fieldName);

				if (fieldIndex < 0) {
					throw new IllegalArgumentException("Invalid field name: " + fieldName);
				}

				excludeFieldFlags.set(fieldIndex);
			}
		}

		int[] fieldIndices = new int[numberOfFieldIndices];

		for (int i = 0, index = 0; i < excludeFieldFlags.length(); i++) {
			if (!excludeFieldFlags.isSet(i)) {
				fieldIndices[index] = i;
				index++;
			}
		}

		return fieldIndices;
	}

	/**
	 * @return header with field names ended by field delimiters; used by flat file writers for file header describing data
	 */
	public String getFieldNamesHeader(String[] excludedFieldNames, boolean quotedString, Character quoteChar) {
		StringBuilder ret = new StringBuilder();
		DataFieldMetadata dataFieldMetadata;
		int fieldSize;
		int headerSize;
		char blank = ' ';
		String label;

		int[] includedFieldIndices = fieldsIndicesComplement(excludedFieldNames);
		int lastIncludedFieldIndex = includedFieldIndices[includedFieldIndices.length - 1];

		for (int i : includedFieldIndices) {
			dataFieldMetadata = getField(i);
			label = dataFieldMetadata.getLabelOrName();
			if (dataFieldMetadata.isDelimited()) {
				// delim: add field name and delimiter
				if (quotedString) {
					QuotingDecoder decoder = new QuotingDecoder();
					decoder.setQuoteChar(quoteChar);
					label = decoder.encode(label).toString();
				}
				ret.append(label);

				if (i == lastIncludedFieldIndex) {
					dataFieldMetadata = getField(getNumFields() - 1);
				}

				ret.append(dataFieldMetadata.getDelimiters()[0]);
			} else {
				// fixlen: strip header name or add blank spaces
				fieldSize = dataFieldMetadata.getSize();
				if (fieldSize <= (headerSize = label.length())) {
					ret.append(label.substring(0, fieldSize));
				} else {
					ret.append(label);
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
	 * @return header with field names ended by field delimiters; used by flat file writers for file header describing data
	 */
	public String getFieldNamesHeader(boolean quotedString, Character quoteChar) {
		return getFieldNamesHeader(null, quotedString, quoteChar);
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

	/**
	 * Two metadata objects are equal if and only if both have same number of fields,
	 * record types (delimited/fixed) and field types (string, integer, ...).
	 * Field names are ignored.  
	 */
	@Override
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

	@Override
	public int hashCode() {
		int hashCode = 17;

		for (DataFieldMetadata field : fields) {
			hashCode = 37 * hashCode + field.hashCode();
		}

		return hashCode;
	}

	@Override
	public String toString() {
		StringBuffer buffer = new StringBuffer();
		buffer.append("DataRecordMetadata[");
		buffer.append("fields = ").append(fields);
		buffer.append(", fieldNamesMap = ").append(getFieldNamesMap());
		buffer.append(", fieldTypes = ").append(getFieldTypes());
		buffer.append(", name = ").append(name);
		buffer.append(", recType = ").append(parsingType);
		buffer.append(", localeStr = ").append(localeStr);
		buffer.append(", skipSourceRows = ").append(skipSourceRows);
		buffer.append(", quotedStrings = ").append(quotedStrings);
		buffer.append(", quoteChar = ").append(quoteChar);
		buffer.append(", recordProperties = ").append(recordProperties);
		buffer.append("]");

		return buffer.toString();
	}

	/**
	 * Iterator for contained field metadata.
	 *
	 * @see java.lang.Iterable#iterator()
	 */
	@Override
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

	/**
	 * Returns true if metadata contains at least one field of unknown type.
	 */
	public boolean hasFieldOfUnknownType() {
		for (DataFieldMetadata dataFieldMetadata : getFields()) {
			if (dataFieldMetadata.getDataType() == DataFieldType.UNKNOWN) {
				return true;
			}
		}
		return false;
	}
	
	/**
	 * Set collator sensitivity string as a default value for all string data fields.	
	 * @param collatorSensitivity
	 */
	public void setCollatorSensitivity(String collatorSensitivity) {
		this.collatorSensitivity = collatorSensitivity;
	}

	/**
	 * Returns collator sensitivity as string according to CollatorSensitivityType class.
	 * Default value for all string data fields.
	 * @return
	 */
	public String getCollatorSensitivity() {
		return collatorSensitivity;
	}
	
	/**
	 * Sets metadadata nature which should correspond with graph job type, see {@link #checkConfig(ConfigurationStatus)}.
	 * Different implementations of DataRecord are used for various natures.<br>
	 * Nature {@link JobType#ETL_GRAPH} is represented by {@link DataRecord}.<br>
	 * Nature {@link JobType#JOBFLOW} is represented by {@link Token}.<br>
	 * @param nature nature of this metadata
	 */
	public void setNature(DataRecordNature nature) {
		this.nature = nature;
	}
	
	/**
	 * Record nature is by default derived from graph job type if is available.
	 * Local specification of nature is used only if the parent graph is not available.
	 * @return nature associated with this metadata
	 * @see #setNature(DataRecordNature)
	 */
	public DataRecordNature getNature() {
		if (getGraph() != null) {
			return DataRecordNature.fromJobType(getGraph().getJobType());
		} else {
			return nature != null ? nature : DataRecordNature.DEFAULT;
		}
	}
	
	/**
	 * @return the parent graph of this metadata or null if no parent graph is specified
	 */
	public TransformationGraph getGraph() {
		return graph;
	}

	/**
	 * Sets the parent graph of this metadata
	 * @param graph the parent graph to set
	 */
	public void setGraph(TransformationGraph graph) {
		this.graph = graph;
	}

	/**
	 * The main method for record and field names normalization.
	 * 
	 * First add all the fields to the record and set their labels: {@link #setLabel(String)}.
	 * Then call {@link #normalize()} to set the names
	 * to their normalized versions and clear the labels, if applicable. 
	 */
	public void normalize() {
		normalizeMetadata(this);
	}
	
	/**
	 * Sets the original names for the record and its fields.
	 * 
	 * The names are first normalized and made unique.
	 * 
	 * The normalized names are set as the new names.
	 * 
	 * Then if the normalized names are different from the original names,
	 * the original names are set as the new labels. Otherwise
	 * the labels are set to null.
	 * 
	 * @param recordName new full name of the record
	 * @param fieldNames new full names of the fields
	 */
	public void setNames(String recordName, String... fieldNames) {
		setNames(this, recordName, fieldNames);
	}
	
	/**
	 * @see #setNames(String, String...)
	 * 
	 * @param metadata
	 * @param recordName
	 * @param fieldNames
	 */
	public static void setNames(DataRecordMetadata metadata, String recordName, String... fieldNames) {
		if (metadata == null) {
			return;
		}
		if (recordName != null) {
			String normalizedRecordName = StringUtils.normalizeName(recordName);
			metadata.setName(normalizedRecordName);
			metadata.setLabel(normalizedRecordName.equals(recordName) ? null : recordName);
		}
		int numFields = metadata.getNumFields(); 
		if ((fieldNames != null) && (numFields == fieldNames.length)) {
			String[] normalizedNames = StringUtils.normalizeNames(fieldNames);
			boolean equal = Arrays.equals(fieldNames, normalizedNames);
			for (int i = 0; i < numFields; i++) {
				DataFieldMetadata field = metadata.getField(i);
				field.setName(normalizedNames[i]);
				field.setLabel(equal ? null : fieldNames[i]);
			}
		}
		metadata.structureChanged();
	}

	/**
	 * @see #normalize()
	 * 
	 * @param metadata the metadata to normalize
	 */
	public static void normalizeMetadata(DataRecordMetadata metadata) {
		if (metadata == null) {
			return;
		}
		String newRecordName = metadata.getLabel();
		if (newRecordName == null) {
			newRecordName = metadata.getName();
		}
		
		int numFields = metadata.getNumFields();
		
		String[] originalNames = new String[numFields];
		for (int i = 0; i < numFields; i++) {
			DataFieldMetadata field = metadata.getField(i); 
			String label = field.getLabel();
			if (label == null) {
				label = field.getName();
			}
			originalNames[i] = label;
		}
		
		metadata.setNames(newRecordName, originalNames);
	}
	
}

