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
// FILE: c:/projects/jetel/org/jetel/metadata/DataFieldMetadata.java

package org.jetel.metadata;

/**
 *  A class that represents metadata describing one particular data field.<br>
 *  Handles encoding of characters.
 *
 * @author     D.Pavlis
 * @since      March 26, 2002
 * @see        org.jetel.metadata.DataRecordMetadata
 */
public class DataFieldMetadata {

	/**
	 *  Name of the field
	 *
	 * @since
	 */
	private String name;
	/**
	 *  Delimiter of the field (could be empty if field belongs to fixLength record)
	 *
	 * @since
	 */
	private String delimiter = null;
	/**
	 *  Format of Number, Date, DateTime or empty if not applicable
	 *
	 * @since
	 */
	private String formatStr;
	/**
	 *  Length of the field (in bytes) if the field belongs to fixLength record.
	 *
	 * @since
	 */
	private short size = 0;

	private char fieldType = ' ';

	/**
	 *  Fields can assume null value by default.
	 *
	 * @since
	 */
	private boolean nullable = true;
	
	private String defaultValueStr;

	/**
	 * Field can be populated by execution of Java code which
	 * can include references to fields from input records.  The
	 * code corresponds to a body of a methodwhich has to return
	 * a value that has a type of the field type.
	 * 
	 * The syntax for the field references is as follows:
	 * 
	 *   [record name].[field name]
	 *  
	 */
	private String codeStr;

	// Attributes
	/**
	 *  Description of the Field
	 *
	 * @since    October 30, 2002
	 */
	public final static char STRING_FIELD = 'S';
	/**
	 *  Description of the Field
	 *
	 * @since    October 30, 2002
	 */
	public final static char DATE_FIELD = 'D';
	/**
	 *  Description of the Field
	 *
	 * @since    October 30, 2002
	 */
	public final static char DATETIME_FIELD = 'T';
	/**
	 *  Description of the Field
	 *
	 * @since    October 30, 2002
	 */
	public final static char NUMERIC_FIELD = 'N';
	/**
	 *  Description of the Field
	 *
	 * @since    October 30, 2002
	 */
	public final static char INTEGER_FIELD = 'i';
	/**
	 *  Description of the Field
	 *
	 * @since    October 30, 2002
	 */
	public final static char DECIMAL_FIELD = 'd';
	/**
	 *  Description of the Field
	 *
	 * @since    October 30, 2002
	 */
	public final static char BYTE_FIELD = 'B';


	/**
	 *  Constructor for delimited type of field
	 *
	 * @param  _name       Name of the field
	 * @param  _delimiter  String to be used as a delimiter for this field
	 * @param  _type       Description of Parameter
	 * @since
	 */
	public DataFieldMetadata(String _name, char _type, String _delimiter) {
		this.name = new String(_name);
		this.delimiter = new String(_delimiter);
		this.fieldType = _type;
	}

	/**
	 *  Constructor for default(String) delimited type of field
	 *
	 * @param  _name       Name of the field
	 * @param  _delimiter  String to be used as a delimiter for this field
	 * @since
	 */
	public DataFieldMetadata(String _name, String _delimiter) {
		this.name = new String(_name);
		this.delimiter = new String(_delimiter);
		this.fieldType = STRING_FIELD;
	}

	/**
	 *  Constructor for default(String) fixLength type of field
	 *
	 * @param  _name   Name of the field
	 * @param  _type   Description of Parameter
	 * @param  size    Description of Parameter
	 * @since
	 */
	public DataFieldMetadata(String _name, short size) {
		this.name = new String(_name);
		this.size = size;
		this.fieldType = STRING_FIELD;
	}

	/**
	 *  Constructor for fixLength type of field
	 *
	 * @param  _name   Name of the field
	 * @param  _type   Description of Parameter
	 * @param  size    Description of Parameter
	 * @since
	 */
	public DataFieldMetadata(String _name, char _type, short size) {
		this.name = new String(_name);
		this.size = size;
		this.fieldType = _type;
	}

	/**
	 *  Sets name of the field
	 *
	 * @param  _name  The new Name value
	 * @since
	 */
	public void setName(String _name) {
		this.name = new String(_name);
	}


	/**
	 *  Sets delimiter string
	 *
	 * @param  _delimiter  The new Delimiter value
	 * @since
	 */
	public void setDelimiter(String _delimiter) {
		this.delimiter = new String(_delimiter);
	}


	/**
	 *  Sets format pattern for the field
	 *
	 * @param  _format  The new format pattern (acceptable value depends on DataField type)
	 * @since
	 */
	public void setFormatStr(String _format) {
		this.formatStr = new String(_format);
	}


	/**
	 *  Sets the DefaultValue defined for this field
	 *
	 * @param  defaultValue  The new DefaultValue value
	 * @since                October 30, 2002
	 */
	public void setDefaultValue(String defaultValue) {
		this.defaultValueStr = defaultValue;
	}



	// Operations
	/**
	 *  An operation that does ...
	 *
	 * @return    The Name value
	 * @since
	 */
	public String getName() {
		return name;
	}


	/**
	 *  Gets the Type attribute of the DataFieldMetadata object
	 *
	 * @return    The Type value
	 * @since     October 30, 2002
	 */
	public char getType() {
		return fieldType;
	}


	/**
	 *  Sets the Type attribute of the DataFieldMetadata object
	 *
	 * @return    The Type value
	 * @since     October 30, 2002
	 */
	public void setType(char c) {
		fieldType = c;
	}

	/**
	 *  Returns the specified maximum field size (used only when dealing with fixed-size type of record)
	 *
	 * @return    The Length value
	 * @since
	 */
	public short getSize() {
		return size;
	}


	/**
	 *  Sets the maximum field size (used only when dealing with fixed-size type of record)
	 * @param s
	 */
	public void setSize(short s) {
		size = s;
	}

	/**
	 *  An operation that does ...
	 *
	 * @return    The Delimiter value
	 * @since
	 */
	public String getDelimiter() {
		return delimiter;
	}

	/**
	 *  Gets Format string specifying pattern which will be used when
	 *  outputing field's value in text form
	 *
	 * @return    The FormatStr value
	 * @since
	 */
	public String getFormatStr() {
		return formatStr;
	}


	/**
	 *  Gets the DefaultValue of the DataFieldMetadata object
	 *
	 * @return    The DefaultValue value
	 * @since     October 30, 2002
	 */
	public String getDefaultValue() {
		return defaultValueStr;
	}
	/**
	 * Sets the nullable attribute of the DataFieldMetadata object
	 * @param nullable
	 */
	public void setNullable(boolean nullable) {
		this.nullable = nullable;
	}
	/**
	 * Gets the nullable of the DataFieldMetadata object
	 * @return
	 */
	public boolean isNullable() {
		return nullable;
	}

	/**
	 * Sets the codeStr attribute of the DataFieldMetadata object
	 * @param codeStr
	 */
	public void setCodeStr(String codeStr) {
		this.codeStr = codeStr;
	}

	/**
	 * Gets the codeStr of the DataFieldMetadata object
	 * @return
	 */
	public String getCodeStr() {
		return codeStr;
	}

}
/*
 *  end class DataFieldMetadata
 */

