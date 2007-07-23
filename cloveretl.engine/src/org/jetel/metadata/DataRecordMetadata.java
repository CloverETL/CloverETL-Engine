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
import org.jetel.exception.InvalidGraphObjectNameException;
import org.jetel.util.BitArray;
import org.jetel.util.StringUtils;
import org.jetel.util.TypedProperties;


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
public class DataRecordMetadata implements Serializable {

	private static final long serialVersionUID = 7032218607804024730L;

    // Associations
	/**
	 * @since
	 */
	private List<DataFieldMetadata> fields;
	private BitArray fieldNullSwitch;
    
	private Map fieldNames;
	private Map fieldTypes;

	private String name;
	private char recType;
	private String[] recordDelimiters;
	private short recordSize;
	private String localeStr;
    private short numNullableFields;

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

		ret.setRecordDelimiter(getRecordDelimiters());
		ret.setLocaleStr(getLocaleStr());
		ret.setRecordSize(getRecordSize());

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
		return (DataFieldMetadata[]) fields.toArray(new DataFieldMetadata[0]);
	}

	/**
	 * Call if structure of metedata changes (add or remove some field).
	 */
	private void structureChanged() {
	    fieldNames.clear();
	    fieldTypes.clear();
        numNullableFields=0;
        fieldNullSwitch.resize(fields.size());
        int count=0;
        for(DataFieldMetadata fieldMeta: fields){
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
			fieldTypes.put(new Integer(i), String.valueOf(field.getType()));
		}
	}



	/**  Description of the Method */
	private void updateFieldNamesMap() {
		DataFieldMetadata field;
		// fieldNames.clear(); - not necessary as it is called only if Map is empty
		for (int i = 0; i < fields.size(); i++) {
			field = (DataFieldMetadata) fields.get(i);
			fieldNames.put(field.getName(), new Integer(i));
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

	public void setRecordSize(short recSize) {
		this.recordSize = recSize;
	}
	
	public short getRecordSize() {
	    return recordSize;
	}

	public int getRecordSizeStripAutoFilling() {
		if (recType != FIXEDLEN_RECORD) {
			return -1;	// unknown size
		}
		// compute size of fixed record (without trailing filler)
		int recSize = 0;
		int prevEnd = 0;
		for (Object obj: fields) {
			DataFieldMetadata field = (DataFieldMetadata)obj;
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
		fields.add(_field);
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
	
	public void setRecordDelimiter(String[] recordDelimiters) {
		this.recordDelimiters = recordDelimiters;
	}

	public boolean isSpecifiedRecordDelimiter() {
		return getRecordDelimiters() != null;
	}

	public String[] getRecordDelimiters() {
        if(recordDelimiters == null) {
            String rd = getRecordProperties().getProperty(DataRecordMetadataXMLReaderWriter.RECORD_DELIMITER_ATTR);
            if(rd != null) {
                setRecordDelimiter(StringUtils.stringToSpecChar(rd).split(Defaults.DataFormatter.DELIMITER_DELIMITERS_REGEX));
            }
        }
		return recordDelimiters;
	}

    public String getRecordDelimiter() {
        return getRecordDelimiters() == null ? "" : recordDelimiters[0];
    }
    
    /**
     * Returns header with field names ended by field delimiters.
     * Used in flat file writers for file header describing data.
     * @return
     */
    public String getFieldNamesHeader() {
        StringBuilder ret = new StringBuilder();
        String fieldDelimiter;
        
        for (int i = 0; i < getNumFields(); i++) {
            ret.append(getField(i).getName());
            fieldDelimiter = getField(i).getDelimiter(); 
            ret.append(StringUtils.isEmpty(fieldDelimiter) ? ";" : fieldDelimiter);
        }
        ret.append(getRecordDelimiter());
        
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
        buffer.append(", recordProperties = ").append(recordProperties);
        buffer.append(", DELIMITED_RECORD = ").append(DELIMITED_RECORD);
        buffer.append(", FIXEDLEN_RECORD = ").append(FIXEDLEN_RECORD);
        buffer.append("]");
        return buffer.toString();
    }
    
    public boolean equals(Object o){
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
    		if (!this.getField(i).equals(metadata.getField(i))){
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
    
}
/*
 *  end class DataRecordMetadata
 */

