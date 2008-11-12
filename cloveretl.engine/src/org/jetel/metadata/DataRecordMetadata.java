/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2002-04  David Pavlis <david_pavlis@hotmail.com>
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
// FILE: c:/projects/jetel/org/jetel/metadata/DataRecordMetadata.java

package org.jetel.metadata;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.jetel.data.Defaults;
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
 *  A class that represents metadata describing DataRecord
 *
 * @author      D.Pavlis
 * @since       March 26, 2002
 * @revision    $Revision$
 * @see         DataFieldMetadata
 * @see         org.jetel.data.DataRecord
 * @see         org.jetel.data.DataField
 */
public class DataRecordMetadata implements Serializable, Iterable<DataFieldMetadata> {

	private static final long serialVersionUID = 7032218607804024730L;

    // Associations
	@SuppressWarnings("Se")
	private List<DataFieldMetadata> fields;
	private BitArray fieldNullSwitch;
    
	@SuppressWarnings("Se")
	private Map<String, Integer> fieldNames;
	@SuppressWarnings("Se")
	private Map<Integer, String> fieldTypes;
	@SuppressWarnings("Se")
	private Map<String, Integer> fieldOffset;

	private String name;
	private char recType;
	private String recordDelimiters;
	private String fieldDelimiters;
	private int recordSize=-1;
	private String localeStr;
    private short numNullableFields;
	private boolean skipFirstLine;

	private TypedProperties recordProperties;

	/**  Description of the Field */
	public final static char DELIMITED_RECORD = 'D';
	/**  Description of the Field */
	public final static char FIXEDLEN_RECORD = 'F';
	/**  Description of the Field */
	public final static char MIXED_RECORD = 'M';

    public final static String BYTE_MODE_ATTR = "byteMode";

	// Operations

	/**
	 *  Constructor for the DataRecordMetadata object
	 *
	 * @param  _name  Name of the Data Record
	 * @param  _type  Type of record - delimited/fix-length
	 * @since         May 2, 2002
	 */
	public DataRecordMetadata(String _name, char _type) {
		this(_name);
		this.recType = _type;
	}


	/**
	 *  Constructor for the DataRecordMetadata object
	 *
	 * @param  _name  Name of the Data Record
	 * @since         May 2, 2002
	 */
	public DataRecordMetadata(String _name) {
		if (!StringUtils.isValidObjectName(_name)){
			throw new InvalidGraphObjectNameException(_name,"RECORD");
		}
		this.name = _name;
		this.fields = new ArrayList<DataFieldMetadata>();
		fieldNames = new HashMap();
		fieldTypes = new HashMap();
		fieldOffset = new HashMap<String, Integer>();
		recordProperties = new TypedProperties();
		localeStr=null;
        numNullableFields=0;
        fieldNullSwitch=new BitArray();
	}

	/**
	 * Creates deep copy of existing metadata. 
	 * 
	 * @return new metadata (exact copy of current metatada)
	 */
	public DataRecordMetadata duplicate() {
	    DataRecordMetadata ret = new DataRecordMetadata(getName(), getRecType());

		ret.setRecordDelimiters(getRecordDelimiterStr());
		ret.setFieldDelimiter(getFieldDelimiterStr());
		ret.setLocaleStr(getLocaleStr());
		ret.setRecordSize(getRecordSize());
		ret.setSkipFirstLine(isSkipFirstLine());

		//copy record properties
        ret.setRecordProperties(getRecordProperties());
		
		//copy fields
		DataFieldMetadata[] sourceFields = getFields();
		for(int i = 0; i < sourceFields.length; i++) {
		    ret.addField(sourceFields[i].duplicate());
		}
		
	    return ret;
	}

	/**
	 *  An operation that sets Record Name
	 *
	 * @param  _name  The new Record Name
	 * @since
	 */
	public void setName(String _name) {
		if (!StringUtils.isValidObjectName(_name)){
			throw new InvalidGraphObjectNameException(_name,"RECORD");
		}
		this.name = _name;
	}


	/**
	 *  An operation that returns Record Name
	 *
	 * @return    The Record Name
	 * @since
	 */
	public String getName() {
		return name;
	}


	/**
	 * @return Returns the localeStr.
	 */
	public String getLocaleStr() {
		return localeStr;
	}
	/**
	 * @param localeStr The localeStr to set.
	 */
	public void setLocaleStr(String localeStr) {
		this.localeStr = localeStr;
	}
	/**
	 *  An operation that returns number of Data Fields within Data Record
	 *
	 * @return    Number of Data Fields defined for the record
	 * @since
	 */
	public int getNumFields() {
		return fields.size();
	}


	/**
	 *  An operation that returns DataFieldMetadata reference based on field's
	 *  position within record
	 *
	 * @param  _fieldNum  ordinal number of requested field
	 * @return            DataFieldMetadata reference
	 * @since
	 */
	public DataFieldMetadata getField(int _fieldNum) {

		try {
			return (DataFieldMetadata) fields.get(_fieldNum);
		} catch (IndexOutOfBoundsException e) {
			return null;
		}

	}


	/**
	 *  An operation that returns DataFieldMetadata reference based on field's name
	 *
	 * @param  _fieldName  name of the field requested
	 * @return             DataFieldMetadata reference
	 * @since
	 */
	public DataFieldMetadata getField(String _fieldName) {
		Integer position;
		if (fieldNames.isEmpty()) {
			updateFieldNamesMap();
		}

		position = (Integer) fieldNames.get(_fieldName);
		if (position != null) {
			return getField(position.intValue());
		} else {
			return null;
		}

	}


	/**
	 *  Gets the fieldPosition attribute of the DataRecordMetadata object
	 *
	 * @param  fieldName  Description of the Parameter
	 * @return            The position of the field within record or -1 if
     * such field does not exist.
	 */
	public int getFieldPosition(String fieldName) {
		Integer position;
		if (fieldNames.isEmpty()) {
			updateFieldNamesMap();
		}

		position = (Integer) fieldNames.get(fieldName);
		if (position != null) {
			return position.intValue();
		} else {
			return -1;
		}
	}

	public int getFieldOffset(String fieldName){
		if (getRecType() != FIXEDLEN_RECORD ) return -1;
		
		if (fieldOffset.isEmpty()) {
			updateFieldOffsetMap();
		}
		Integer offset = fieldOffset.get(fieldName);
		if (offset != null) {
			return offset.intValue();
		} else {
			return -1;
		}
	}

	/**
	 *  Gets the fieldType attribute of the DataFieldMetadata identified by fieldName
	 *
	 * @param  fieldName  Description of the Parameter
	 * @return            The field type
	 */
	public char getFieldType(String fieldName) {
		Integer position;
		if (fieldNames.isEmpty()) {
			updateFieldNamesMap();
		}

		position = (Integer) fieldNames.get(fieldName);
		if (position != null) {
			DataFieldMetadata fieldMetadata = getField(position.intValue());
			return fieldMetadata.getType();
		} else {
			return DataFieldMetadata.UNKNOWN_FIELD;
		}
	}


	/**
	 *  Gets the fieldType attribute of the DataFieldMetadata identified by fieldName
	 *
	 * @param  fieldNo      Description of the Parameter
	 * @return              The field type
	 */
	public char getFieldType(int fieldNo) {
		DataFieldMetadata fieldMetadata = getField(fieldNo);
		if (fieldMetadata != null) {
			return fieldMetadata.getType();
		} else {
			return DataFieldMetadata.UNKNOWN_FIELD;
		}
	}

	/**
	 *  Gets the fieldType attribute of the DataFieldMetadata identified by fieldName
	 *
	 * @param  fieldNo      Description of the Parameter
	 * @return              The field type as String
	 */
	public String getFieldTypeAsString(int fieldNo) {
		DataFieldMetadata fieldMetadata = getField(fieldNo);
		if (fieldMetadata != null) {
			return fieldMetadata.getTypeAsString();
		} else {
			return DataFieldMetadata.type2Str(DataFieldMetadata.UNKNOWN_FIELD);
		}
	}


	/**
	 *  Gets the Map where keys are FieldNames and values Field Order Numbers
	 *
	 * @return    Map object {FieldName->Order Number}
	 * @since     May 2, 2002
	 */
	public Map getFieldNames() {
		if (fieldNames.isEmpty()) {
			updateFieldNamesMap();
		}
		return new HashMap(fieldNames);
	}



	/**
	 *  Gets the Map where keys are FieldNames and values Field Types
	 *
	 * @return    Map object {FieldName->Order Number}
	 * @since     May 2, 2002
	 */
	public Map getFieldTypes() {
		if (fieldTypes.isEmpty()) {
			updateFieldTypesMap();
		}
		return new HashMap(fieldTypes);
	}

	/**
	 * Gets array of data field metadata objects.
	 * @return
	 */
	public DataFieldMetadata[] getFields() {
		return (DataFieldMetadata[]) fields.toArray(new DataFieldMetadata[fields.size()]);
	}

	/**
	 * Call if structure of metedata changes (add or remove some field).
	 */
	private void structureChanged() {
		recordSize = -1;
	    fieldNames.clear();
	    fieldTypes.clear();
	    fieldOffset.clear();
        numNullableFields=0;
        fieldNullSwitch.resize(fields.size());
        int count=0;
        for(DataFieldMetadata fieldMeta: fields) {
        	fieldMeta.setNumber(count);
            if (fieldMeta.isNullable()){
                numNullableFields++;
                fieldNullSwitch.set(count);
            }
            count++;
        }
	}
	
	/**  Description of the Method */
	private void updateFieldTypesMap() {
		DataFieldMetadata field;
		// fieldNames.clear(); - not necessary as it is called only if Map is empty
	
		for (int i = 0; i < fields.size(); i++) {
			field = (DataFieldMetadata) fields.get(i);
			fieldTypes.put(Integer.valueOf(i), String.valueOf(field.getType()));
		}
	}



	/**  Description of the Method */
	private void updateFieldNamesMap() {
		DataFieldMetadata field;
		// fieldNames.clear(); - not necessary as it is called only if Map is empty
		for (int i = 0; i < fields.size(); i++) {
			field = (DataFieldMetadata) fields.get(i);
			fieldNames.put(field.getName(), Integer.valueOf(i));
		}
	}
	
	private void updateFieldOffsetMap(){
		if (getRecType() != FIXEDLEN_RECORD) return;
		
		int offset = 0;
		for (DataFieldMetadata field : fields) {
			fieldOffset.put(field.getName(), offset + field.getShift());
			offset += field.getSize();
		}
	}

	/**
	 *  Sets the Record Type (Delimited/Fix-length)
	 *
	 * @param  c  The new recType value
	 * @since     May 3, 2002
	 */
	public void setRecType(char type) {
		recType = type;
	}


	/**
	 *  Gets the Record Type (Delimited/Fix-length)
	 *
	 * @return    The Record Type
	 * @since     May 3, 2002
	 */
	public char getRecType() {
		return recType;
	}

	public void setRecordSize(int recSize) {
		this.recordSize = recSize;
	}
	
	public int getRecordSize() {
		if (recType != FIXEDLEN_RECORD) {
			return -1;	// unknown size
		}

		if (recordSize > -1) {
			return recordSize;
		}		
		
		return getRecordSizeStripAutoFilling();
	}

	public int getRecordSizeStripAutoFilling() {
		if (recType != FIXEDLEN_RECORD) {
			return -1;	// unknown size
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
	 *  Gets the recordProperties attribute of the DataRecordMetadata object
	 *
	 * @return    The recordProperties value
	 */
	public TypedProperties getRecordProperties() {
		return recordProperties;
	}

    /**
     * Gets the one property value from the recordProperties attribute according given attribute name.
     * @param attrName
     * @return
     * @see this.getRecordProperties()
     */
    public String getProperty(String attrName) {
        return recordProperties.getProperty(attrName);
    }

	/**
	 *  Sets the recordProperties attribute of the DataRecordMetadata object
	 *  Record properties allows defining additional parameters for record.
	 *  These parameters (key-value pairs) are NOT normally handled by CloverETL, but
	 *  can be used in user's code or specialised Components. 
	 *
	 * @param  properties  The new recordProperties value
	 */
	public void setRecordProperties(Properties properties) {
		recordProperties = new TypedProperties(properties);
	}

	/**
	 *  An operation that adds DataField (metadata) into DataRecord
	 *
	 * @param  _field  DataFieldMetadata reference
	 * @since
	 */
	public void addField(DataFieldMetadata _field) {
		_field.setDataRecordMetadata(this);
		fields.add(_field);
		structureChanged();
	}

	public void addField(int index, DataFieldMetadata field){
		field.setDataRecordMetadata(this);
		fields.add(index, field);
		structureChanged();
	}
	
	/**
	 *  An operation that deletes data field identified by index
	 *
	 * @param  _fieldNum  ordinal number of the field to be deleted
	 * @since
	 */
	public void delField(int _fieldNum) {
		try {
			fields.remove(_fieldNum);
			structureChanged();
		} catch (IndexOutOfBoundsException e) {
			// do nothing - may-be singnalize error
		}
	}


	/**
	 *  An operation that deletes field identified by name
	 *
	 * @param  _fieldName  Description of Parameter
	 * @since
	 */
	public void delField(String _fieldName) {
		Integer position;
		if (fieldNames.isEmpty()) {
			updateFieldNamesMap();
		}

		position = (Integer) fieldNames.get(_fieldName);
		if (position != null) {
			delField(position.intValue());
		}
	}

	/**
	 * Deletes all fields in metadata.
	 */
	public void delAllFields() {
	    fields.clear();
	    structureChanged();
	}
	
	/**
	 * This method is used by gui to prepopulate record meta info with
	 * default fields and user defined sizes.  It also adjusts the number of fields
	 * and their sizes based on user selection.
	 *
	 * @param  fieldWidths  Description of the Parameter
	 */
	public void bulkLoadFieldSizes(short[] fieldWidths) {
		DataFieldMetadata aField = null;
		int fieldsNumber = fields.size();

		//if no fields then create default fields with sizes as in objects
		if (fieldsNumber == 0) {
			for (int i = 0; i < fieldWidths.length; i++) {
				addField(new DataFieldMetadata("Field" + Integer.toString(i), fieldWidths[i]));
			}
		} else {
			//fields exist
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

	public void setFieldDelimiter(String fieldDelimiters) {
		this.fieldDelimiters = fieldDelimiters;
	}

	public boolean isSpecifiedFieldDelimiter() {
		return getFieldDelimiters() != null;
	}

	public String[] getFieldDelimiters() {
        if(!StringUtils.isEmpty(fieldDelimiters)) {
        	return fieldDelimiters.split(Defaults.DataFormatter.DELIMITER_DELIMITERS_REGEX);
        }
		return null;
	}
	
	public String getFieldDelimiterStr() {
		return fieldDelimiters;
	}
	
	public void setRecordDelimiters(String recordDelimiters) {
		this.recordDelimiters = recordDelimiters;
	}

	public boolean isSpecifiedRecordDelimiter() {
		return getRecordDelimiters() != null;
	}

	public String[] getRecordDelimiters() {
        if(!StringUtils.isEmpty(recordDelimiters)) {
        	return recordDelimiters.split(Defaults.DataFormatter.DELIMITER_DELIMITERS_REGEX);
        }
		return null;
	}

	public String getRecordDelimiterStr() {
		return recordDelimiters;
	}
	
    /**
     * Returns header with field names ended by field delimiters.
     * Used in flat file writers for file header describing data.
     * @return
     */
    public String getFieldNamesHeader() {
        StringBuilder ret = new StringBuilder();
        DataFieldMetadata dataFieldMetadata;
        short fieldSize;
        int headerSize;
        char blank = ' ';
        
        for (int i = 0; i < getNumFields(); i++) {
        	dataFieldMetadata = getField(i);
            if(dataFieldMetadata.isDelimited()) {
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
            		for (int j=fieldSize-headerSize; j>0; j--) {
                		ret.append(blank);
            		}
            	}
            }
        }
        // add record delimiter for fixlen
        if (getField(getNumFields()-1).isFixed()) {
        	String[] delimiters = getRecordDelimiters();
        	if (delimiters != null) {
            	for (String delim: delimiters) {
            		if (delim != null) {
                		ret.append(delim);
            		}
            	}
        	}
        }
        
        return ret.toString();      
    }

    /**
     * toString method: creates a String representation of the object
     * @return the String representation
     */
    public String toString() {
        StringBuffer buffer = new StringBuffer();
        buffer.append("DataRecordMetadata[");
        buffer.append("fields = ").append(fields);
        buffer.append(", fieldNames = ").append(getFieldNames());
        buffer.append(", fieldTypes = ").append(getFieldTypes());
        buffer.append(", name = ").append(name);
        buffer.append(", recType = ").append(recType);
        buffer.append(", localeStr = ").append(localeStr);
        buffer.append(", skipFirstLine = ").append(skipFirstLine);
        buffer.append(", recordProperties = ").append(recordProperties);
        buffer.append(", DELIMITED_RECORD = ").append(DELIMITED_RECORD);
        buffer.append(", FIXEDLEN_RECORD = ").append(FIXEDLEN_RECORD);
        buffer.append("]");
        return buffer.toString();
    }
    
    public boolean equals(Object o){
    	return equals(o, true);
    }
    
    public boolean equals(Object o, boolean checkFixDelType){
    	if (o == null) {
    		return false;
    	}
    	if (!(o instanceof DataRecordMetadata)) {
    		return false;
    	}
    	DataRecordMetadata metadata = (DataRecordMetadata)o;
    	if (getNumFields() != metadata.getNumFields()){
    		return false;
    	}
    	for (int i=0;i<this.getNumFields();i++){
    		if (!this.getField(i).equals(metadata.getField(i), checkFixDelType)){
    			return false;
    		}
    	}
    	return true;
    }
    
    public int hashCode(){
    	int result = 0;
    	for (int i=0;i<this.getNumFields();i++){
    		result = 37*result + this.getField(i).hashCode();
    	}
    	return result;
    }


    /**
     * Determine whether DataRecord described by this metadata
     * has at least one field which may contain a NULL value.<br>
     * 
     * @return true if at least nullable field is present otherwise false
     * @since 18.1.2007
     */
    public boolean isNullable() {
        return numNullableFields!=0;
    }
        
    public void setSkipFirstLine(boolean isSkipFirstLine) {
    	skipFirstLine = isSkipFirstLine;
    }
    
    public boolean isSkipFirstLine() {
    	return skipFirstLine;
    }


    /**
     * Returns BitArray where bits are set for fields
     * which may contain NULL.
     * 
     * @return the fieldNullSwitch
     * @since 18.1.2007
     */
    public BitArray getFieldsNullSwitches() {
        return fieldNullSwitch;
    }


    /**
     * @return the countNullableFields
     * @since 18.1.2007
     */
    public short getNumNullableFields() {
        return numNullableFields;
    }
    
    /**
     * Finds field with a given autoFilling function.
     * 
     * @param autoFilling
     * @return if field exists, returns index of the field, else -1 
     */
    public int findAutoFilledField(String autoFilling) {
        int i = 0;
        for(DataFieldMetadata field : this) {
            if(autoFilling.equals(field.getAutoFilling())) {
                return i;
            }
            i++;
        }
        return -1;
    }


    /**
     * Iterator for contained field metadata.
     * @see java.lang.Iterable#iterator()
     */
    public Iterator<DataFieldMetadata> iterator() {
        return fields.iterator();
    }
    
    /**
     * Returns field positions for all field names.
     * 
     * @param fieldNames - array of field names
     * @return field positions
     */
    public int[] fieldsIndices(String... fieldNames) {
    	int[] indices = new int[fieldNames.length];
    	int i=0;
    	for (String name: fieldNames) {
    		if ((indices[i++] = getFieldPosition(name)) == -1) {
    			throw new RuntimeException("No such field name found for: '" + name + "'");
    		}
    	}
    	return indices;
    }
    
    public ConfigurationStatus checkConfig(ConfigurationStatus status) {
        // Checks if the metadata contains at least one field without autofilling.
    	for (DataFieldMetadata dataFieldMetadata : getFields()) {
    		if (!dataFieldMetadata.isAutoFilled()) return status;
    	}
    	status.add(new ConfigurationProblem(
    			"No Field elements without autofilling for '" + getName() + "' have been found ! ",
    			Severity.ERROR,  
				null, //TODO this
				Priority.NORMAL));
    	return status;    	
    }
    
}
/*
 *  end class DataRecordMetadata
 */

