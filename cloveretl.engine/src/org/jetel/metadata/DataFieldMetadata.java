/*
 *  jETeL/Clover - Java based ETL application framework.
 *  Copyright (C) 2002-04  David Pavlis <david_pavlis@hotmail.com>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 *  This library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Lesser General Public License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public
 *  License along with this library; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 */
// FILE: c:/projects/jetel/org/jetel/metadata/DataFieldMetadata.java

package org.jetel.metadata;

import java.util.Properties;
import org.jetel.exception.InvalidGraphObjectNameException;
import org.jetel.util.StringUtils;

/**
 *  A class that represents metadata describing one particular data field.<br>
 *  Handles encoding of characters.
 *
 * @author      D.Pavlis
 * @since       March 26, 2002
 * @revision    $Revision$
 * @see         org.jetel.metadata.DataRecordMetadata
 */
public class DataFieldMetadata {

	/**
	 *  Name of the field
	 */
	private String name;
	/**
	 *  Delimiter of the field (could be empty if field belongs to fixLength record)
	 */
	private String delimiter = null;
	/**
	 *  Format of Number, Date, DateTime or empty if not applicable
	 */
	private String formatStr;
	/**
	 *  Length of the field (in bytes) if the field belongs to fixLength record.
	 */
	private short size = 0;

	private char fieldType = ' ';

	/**
	 *  Fields can assume null value by default.
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
	 */
	private String codeStr;

	private Properties fieldProperties;
	
	/**
	 * Locale string. Both language and country can be specified - if both
	 * are specified then language string & country string have to be
	 * delimited by "." (dot) -> e.g. "en.UK" , "fr.CA".
	 * If only language should be specified, then use language
	 * code according to ISO639 -> e.g. "en" , "de". 
	 * @see	java.util.Locale
	 */
	private String localeStr;

	// Attributes

	/**  Description of the Field */
	public final static char STRING_FIELD = 'S';

	/**  Description of the Field */
	public final static char DATE_FIELD = 'D';

	/**  Description of the Field */
	public final static char DATETIME_FIELD = 'T';

	/**  Description of the Field */
	public final static char NUMERIC_FIELD = 'N';

	/**  Description of the Field */
	public final static char INTEGER_FIELD = 'i';

	/**  Description of the Field */
	public final static char DECIMAL_FIELD = 'd';

	/**  Description of the Field */
	public final static char BYTE_FIELD = 'B';

	/**  Description of the Field */
	public final static char SEQUENCE_FIELD = 'q';

	/**  Description of the Field */
	public final static char UNKNOWN_FIELD = ' ';


	/**
	 *  Constructor for delimited type of field
	 *
	 * @param  _name       Name of the field
	 * @param  _delimiter  String to be used as a delimiter for this field
	 * @param  _type       Description of Parameter
	 * @since
	 */
	public DataFieldMetadata(String _name, char _type, String _delimiter) {
		if (!StringUtils.isValidObjectName(_name)) {
			throw new InvalidGraphObjectNameException(_name, "FIELD");
		}
		this.name = _name;
		this.delimiter = _delimiter;
		this.fieldType = _type;
		this.fieldProperties = null;
		this.localeStr=null;
	}


	/**
	 *  Constructor for default(String) delimited type of field
	 *
	 * @param  _name       Name of the field
	 * @param  _delimiter  String to be used as a delimiter for this field
	 * @since
	 */
	public DataFieldMetadata(String _name, String _delimiter) {
		if (!StringUtils.isValidObjectName(_name)) {
			throw new InvalidGraphObjectNameException(_name, "FIELD");
		}
		this.name = _name;
		this.delimiter = _delimiter;
		this.fieldType = STRING_FIELD;
		this.fieldProperties = null;
		this.localeStr=null;
	}


	/**
	 *  Constructor for default(String) fixLength type of field
	 *
	 * @param  _name  Name of the field
	 * @param  size   Description of Parameter
	 * @since
	 */
	public DataFieldMetadata(String _name, short size) {
		if (!StringUtils.isValidObjectName(_name)) {
			throw new InvalidGraphObjectNameException(_name, "FIELD");
		}
		this.name = _name;
		this.size = size;
		this.fieldType = STRING_FIELD;
		this.fieldProperties = null;
		this.localeStr=null;
	}


	/**
	 *  Constructor for fixLength type of field
	 *
	 * @param  _name  Name of the field
	 * @param  _type  Description of Parameter
	 * @param  size   Description of Parameter
	 * @since
	 */
	public DataFieldMetadata(String _name, char _type, short size) {
		if (!StringUtils.isValidObjectName(_name)) {
			throw new InvalidGraphObjectNameException(_name, "FIELD");
		}
		this.name = _name;
		this.size = size;
		this.fieldType = _type;
		this.fieldProperties = null;
		this.localeStr=null;
	}


	/**
	 *  Sets name of the field
	 *
	 * @param  _name  The new Name value
	 * @since
	 */
	public void setName(String _name) {
		if (!StringUtils.isValidObjectName(_name)) {
			throw new InvalidGraphObjectNameException(_name, "FIELD");
		}
		this.name = _name;
	}


	/**
	 *  Sets delimiter string
	 *
	 * @param  _delimiter  The new Delimiter value
	 * @since
	 */
	public void setDelimiter(String _delimiter) {
		this.delimiter = _delimiter;
	}


	/**
	 *  Sets format pattern for the field
	 *
	 * @param  _format  The new format pattern (acceptable value depends on DataField type)
	 * @since
	 */
	public void setFormatStr(String _format) {
		this.formatStr = _format;
	}


	/**
	 * @return Returns the localeStr.
	 */
	public String getLocaleStr() {
		return localeStr;
	}
	/**
	 * Sets the localeStr.<br> Formatters/Parsers
	 * are generated based on this field's value.
	 * @param localeStr The locale code. (eg. "en", "fr",..).
	 */
	public void setLocaleStr(String localeStr) {
		this.localeStr = localeStr;
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
	 * @param  type  The new type value
	 * @since        October 30, 2002
	 */
	public void setType(char type) {
		fieldType = type;
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
	 *
	 * @param  _size  The new size value
	 */
	public void setSize(short _size) {
		size = _size;
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
	 *
	 * @param  nullable
	 */
	public void setNullable(boolean nullable) {
		this.nullable = nullable;
	}


	/**
	 * Gets the nullable of the DataFieldMetadata object
	 *
	 * @return
	 */
	public boolean isNullable() {
		return nullable;
	}


	/**
	 * Sets the codeStr attribute of the DataFieldMetadata object
	 *
	 * @param  codeStr
	 */
	public void setCodeStr(String codeStr) {
		this.codeStr = codeStr;
	}


	/**
	 * Gets the codeStr of the DataFieldMetadata object
	 *
	 * @return
	 */
	public String getCodeStr() {
		return codeStr;
	}


	/**
	 *  Gets the fieldProperties attribute of the DataFieldMetadata object.<br>
	 *  These properties are automatically filled-in when parsing XML (.fmt) file
	 * containing data record metadata. Any attribute not directly recognized by Clover
	 * is stored within properties object.<br>
	 * Example:
	 * <pre>&lt;Field name="Field1" type="numeric" delimiter=";" myOwn1="1" myOwn2="xyz" /&gt;</pre>
	 *
	 * @return    The fieldProperties value
	 */
	public Properties getFieldProperties() {
		return fieldProperties;
	}


	/**
	 *  Sets the fieldProperties attribute of the DataRecordMetadata object.
	 *  Field properties allows defining additional parameters for individual fields.
	 *  These parameters (key-value pairs) are NOT normally handled by CloverETL, but
	 *  can be used in user's code or Components - thus allow for greater flexibility.
	 *
	 * @param  properties  The new recordProperties value
	 */
	public void setFieldProperties(Properties properties) {
		fieldProperties = properties;
	}

}
/*
 *  end class DataFieldMetadata
 */

