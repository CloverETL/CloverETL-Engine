/*
 *  jETeL/Clover - Java based ETL application framework.
 *  Copyright (C) 2002  David Pavlis
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 2 of the License, or
 *  (at your option) any later version.
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
// FILE: c:/projects/jetel/org/jetel/metadata/DataRecordMetadata.java

package org.jetel.metadata;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import java.util.Properties;

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
public class DataRecordMetadata {

	// Associations
	/**
	 * @since
	 */
	private List fields;

	private Map fieldNames;
	private Map fieldTypes;

	private String name;
	private char recType;

	private Properties recordProperties;

	/**  Description of the Field */
	public final static char DELIMITED_RECORD = 'D';
	/**  Description of the Field */
	public final static char FIXEDLEN_RECORD = 'F';


	// Operations

	/**
	 *  Constructor for the DataRecordMetadata object
	 *
	 * @param  _name  Name of the Data Record
	 * @param  _type  Type of record - delimited/fix-length
	 * @since         May 2, 2002
	 */
	public DataRecordMetadata(String _name, char _type) {
		this.name = new String(_name);
		this.recType = _type;
		this.fields = new ArrayList();
		fieldNames = new HashMap();
		fieldTypes = new HashMap();
		recordProperties = null;
	}


	/**
	 *  Constructor for the DataRecordMetadata object
	 *
	 * @param  _name  Name of the Data Record
	 * @since         May 2, 2002
	 */
	public DataRecordMetadata(String _name) {
		this.name = new String(_name);
		this.fields = new ArrayList();
		fieldNames = new HashMap();
		fieldTypes = new HashMap();
		recordProperties = null;
	}


	/**
	 *  An operation that sets Record Name
	 *
	 * @param  _name  The new Record Name
	 * @since
	 */
	public void setName(String _name) {
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
	 * @return            The position of the field within record
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


	/**  Description of the Method */
	private void updateFieldTypesMap() {
		DataFieldMetadata field;
		// fieldNames.clear(); - not necessary as it is called only if Map is empty
		try {
			for (int i = 0; i < fields.size(); i++) {
				field = (DataFieldMetadata) fields.get(i);
				fieldTypes.put(new Integer(i), String.valueOf(field.getType()));
			}
		} catch (IndexOutOfBoundsException e) {
		}
	}



	/**  Description of the Method */
	private void updateFieldNamesMap() {
		DataFieldMetadata field;
		// fieldNames.clear(); - not necessary as it is called only if Map is empty
		try {
			for (int i = 0; i < fields.size(); i++) {
				field = (DataFieldMetadata) fields.get(i);
				fieldNames.put(field.getName(), new Integer(i));
			}
		} catch (IndexOutOfBoundsException e) {
		}
	}


	/**
	 *  Sets the Record Type (Delimited/Fix-length)
	 *
	 * @param  c  The new recType value
	 * @since     May 3, 2002
	 */
	public void setRecType(char c) {
		recType = c;
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


	/**
	 *  Gets the recordProperties attribute of the DataRecordMetadata object
	 *
	 * @return    The recordProperties value
	 */
	public Properties getRecordProperties() {
		return recordProperties;
	}


	/**
	 *  Sets the recordProperties attribute of the DataRecordMetadata object
	 *
	 * @param  properties  The new recordProperties value
	 */
	public void setRecordProperties(Properties properties) {
		recordProperties = properties;
	}


	/**
	 *  An operation that adds DataField (metadata) into DataRecord
	 *
	 * @param  _field  DataFieldMetadata reference
	 * @since
	 */
	public void addField(DataFieldMetadata _field) {
		fields.add(_field);
		fieldNames.clear();
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
			fieldNames.clear();
		} catch (IndexOutOfBoundsException e) {
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

}
/*
 *  end class DataRecordMetadata
 */

