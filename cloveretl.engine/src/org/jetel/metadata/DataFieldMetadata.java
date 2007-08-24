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

import java.io.Serializable;
import java.util.Enumeration;
import java.util.Properties;
import java.util.regex.Pattern;

import org.jetel.data.Defaults;
import org.jetel.exception.InvalidGraphObjectNameException;
import org.jetel.util.StringUtils;
import org.jetel.util.TypedProperties;

/**
 *  A class that represents metadata describing one particular data field.<br>
 *  Handles encoding of characters.
 *
 * @author      D.Pavlis
 * @since       March 26, 2002
 * @revision    $Revision$
 * @see         org.jetel.metadata.DataRecordMetadata
 */
public class DataFieldMetadata implements Serializable {
	
	private static final long serialVersionUID = -880873886732472663L;
  
    public static int INTEGER_LENGTH = 9;
	public static int LONG_LENGTH = 18;
	public static int DOUBLE_SCALE = 323;
	public static int DOUBLE_LENGTH = DOUBLE_SCALE + 615;

	/**
	 *  Characters that can be contained in format of date
	 */
	private final static Pattern DATE_ONLY_PATTERN = Pattern.compile("[GyMwWDdFE]");
	
	/**
	 *  Characters that can be contained in format of time
	 */
	private final static Pattern TIME_ONLY_PATTERN = Pattern.compile("[aHhKkmsSzZ]");
	
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

	/**
	 * Relative shift of the beginning of the field
	 */
	private short shift = 0;

	private char fieldType = ' ';
	
	/**
	 * Indicates if when reading from file try to trim string value to obtain value
	 */
	private boolean trim = false;

	/**
	 *  Fields can assume null value by default.
	 */
	private boolean nullable = true;

	private String defaultValueStr;

    private Object defaultValue;
    
    private String autoFilling;
    
    
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
//	private String codeStr;

	private TypedProperties fieldProperties;
	
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
	public final static char LONG_FIELD = 'l';
	
	/**  Description of the Field */
	public final static char DECIMAL_FIELD = 'd';

	/**  Description of the Field */
	public final static char BYTE_FIELD = 'B';

	/**  Description of the Field */
	public final static char BYTE_FIELD_COMPRESSED = 'Z';

	/**  Description of the Field */
	public final static char SEQUENCE_FIELD = 'q';

	/**  Description of the Field */
	public final static char UNKNOWN_FIELD = ' ';

	public final static String LENGTH_ATTR = "length";
	
	public final static String SCALE_ATTR = "scale";

	/**
	 *  Constructor for delimited type of field
	 *
	 * @param  _name       Name of the field
	 * @param  _delimiter  String to be used as a delimiter for this field
	 * @param  _type       Description of Parameter
	 * @param shift   Relative shift of the beginning of the field.
	 * @since
	 */
	public DataFieldMetadata(String _name, char _type, String _delimiter) {
		if (!StringUtils.isValidObjectName(_name)) {
			throw new InvalidGraphObjectNameException(_name, "FIELD");
		}
		this.name = _name;
		this.delimiter = _delimiter;
		this.fieldType = _type;
		setFieldProperties(new TypedProperties());
		this.localeStr=null;
		if (isNumeric() || fieldType == DATE_FIELD || fieldType == DATETIME_FIELD){
			trim = true;
		}
	}


	/**
	 *  Constructor for default(String) delimited type of field
	 *
	 * @param  _name       Name of the field
	 * @param  _delimiter  String to be used as a delimiter for this field
	 * @param shift   Relative shift of the beginning of the field.
	 * @since
	 */
	public DataFieldMetadata(String _name, String _delimiter) {
		if (!StringUtils.isValidObjectName(_name)) {
			throw new InvalidGraphObjectNameException(_name, "FIELD");
		}
		this.name = _name;
		this.delimiter = _delimiter;
		this.fieldType = STRING_FIELD;
        setFieldProperties(new TypedProperties());
		this.localeStr=null;
	}


	/**
	 *  Constructor for default(String) fixLength type of field
	 *
	 * @param  _name  Name of the field
	 * @param  size   Description of Parameter
	 * @param shift   Relative shift of the beginning of the field.
	 * @since
	 */
	public DataFieldMetadata(String _name, short size) {
		if (!StringUtils.isValidObjectName(_name)) {
			throw new InvalidGraphObjectNameException(_name, "FIELD");
		}
		this.name = _name;
		this.size = size;
		this.fieldType = STRING_FIELD;
        setFieldProperties(new TypedProperties());
		this.localeStr=null;
	}

	/**
	 *  Constructor for fixLength type of field
	 *
	 * @param  _name  Name of the field
	 * @param  _type  Description of Parameter
	 * @param  size   Description of Parameter
	 * @param shift   Relative shift of the beginning of the field.
	 * @since
	 */
	public DataFieldMetadata(String _name, char _type, short size) {
		if (!StringUtils.isValidObjectName(_name)) {
			throw new InvalidGraphObjectNameException(_name, "FIELD");
		}
		this.name = _name;
		this.size = size;
		this.fieldType = _type;
        setFieldProperties(new TypedProperties());
		this.localeStr=null;
		if (isNumeric() || fieldType == DATE_FIELD || fieldType == DATETIME_FIELD){
			trim = true;
		}
	}

	private DataFieldMetadata() {
	    //EMPTY
	}

	/**
	 * Creates deep copy of existing field metadata. 
	 * 
	 * @return new metadata (exact copy of current field metatada)
	 */
	public DataFieldMetadata duplicate() {
	    DataFieldMetadata ret = new DataFieldMetadata();

		ret.setName(getName());
	    ret.setDelimiter(getDelimiter());
	    ret.setFormatStr(getFormatStr());
	    ret.setShift(getShift());
	    ret.setSize(getSize());
		ret.setType(getType());
		ret.setNullable(isNullable());
		ret.setDefaultValueStr(getDefaultValueStr());
//		ret.setCodeStr(getCodeStr());
		ret.setLocaleStr(getLocaleStr());

		//copy record properties
		Properties target = new Properties();
		Properties source = getFieldProperties();
		if(source != null) {
		    for(Enumeration e = source.propertyNames(); e.hasMoreElements();) {
		        String key = (String) e.nextElement();
		        target.put(key, source.getProperty(key));
		    }
			ret.setFieldProperties(target);
		}

		return ret;
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
	public void setDefaultValueStr(String defaultValue) {
		this.defaultValueStr = defaultValue;
	}

    public void setDefaultValue(Object defaultValue) {
        this.defaultValue = defaultValue;
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
	 *  Gets the Type attribute of the DataFieldMetadata object
	 *
	 * @return    The Type value in full string form.
	 */
	public String getTypeAsString() {
		return type2Str(fieldType);
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

	public void setType(String type) {
		fieldType = str2Type(type);
	}


	/**
	 *  Returns position of the field in data record (used only when dealing with fixed-size type of record)
	 *
	 * @return    The Length value
	 * @since
	 */
	public short getShift() {
		return shift;
	}


	/**
	 *  Sets position of the field in data record (used only when dealing with fixed-size type of record)
	 *
	 * @param  _size  The new size value
	 */
	public void setShift(short shift) {
		this.shift = shift;
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
	 *  An operation that does ...
	 *
	 * @return    The Delimiter value
	 * @since
	 */
	public String[] getDelimiters() {
		return delimiter.split(Defaults.DataFormatter.DELIMITER_DELIMITERS_REGEX);
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
	public String getDefaultValueStr() {
		if (defaultValueStr != null) {
			return defaultValueStr;
		} else if (defaultValue != null) {
			return defaultValue.toString();
		}
		return null;
	}

    public Object getDefaultValue() {
        return defaultValue;
    }
    
	/**
	 * @return true if default value is set
	 */
	public boolean isDefaultValue() {
	    return !StringUtils.isEmpty(defaultValueStr) || defaultValue != null;    
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

	public void setAutoFilling(String autoFilling) {
		this.autoFilling = autoFilling;
	}

	public String getAutoFilling() {
		return autoFilling;
	}

    public boolean isAutoFilled() {
        return !StringUtils.isEmpty(autoFilling);
    }
    
	/**
	 * Sets the codeStr attribute of the DataFieldMetadata object
	 *
	 * @param  codeStr
	 */
//	public void setCodeStr(String codeStr) {
//		this.codeStr = codeStr;
//	}


	/**
	 * Gets the codeStr of the DataFieldMetadata object
	 *
	 * @return
	 */
//	public String getCodeStr() {
//		return codeStr;
//	}


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
	public TypedProperties getFieldProperties() {
		return fieldProperties;
	}

    /**
     * Gets the one property value from the fieldProperties attribute according given attribute name.
     * @param attrName
     * @return
     * @see this.getFieldProperties()
     */
    public String getProperty(String attrName) {
        return fieldProperties.getProperty(attrName);
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
		fieldProperties = new TypedProperties(properties);
		
		//set default attribute values
		if(fieldType == DECIMAL_FIELD) {
			if(fieldProperties.getProperty(LENGTH_ATTR) == null) {
				fieldProperties.setProperty(LENGTH_ATTR, Integer.toString(Defaults.DataFieldMetadata.DECIMAL_LENGTH));
			}
			if(fieldProperties.getProperty(SCALE_ATTR) == null) {
				fieldProperties.setProperty(SCALE_ATTR, Integer.toString(Defaults.DataFieldMetadata.DECIMAL_SCALE));
			}
		}
	}

	public boolean isDelimited() {
		return (delimiter != null);
	}

	public boolean isFixed() {
		return (delimiter == null);
	}
	
	/**
	 * @return true if data field of this metadata field implements numeric interface; false else
	 */
	public boolean isNumeric() {
	    return fieldType == NUMERIC_FIELD
	    	|| fieldType == INTEGER_FIELD
	    	|| fieldType == LONG_FIELD
	    	|| fieldType == DECIMAL_FIELD;
	}
	
	public static String type2Str(char fieldType) {
		switch (fieldType) {
			case DataFieldMetadata.NUMERIC_FIELD:
				return "numeric";
			case DataFieldMetadata.INTEGER_FIELD:
				return "integer";
			case DataFieldMetadata.STRING_FIELD:
				return "string";
			case DataFieldMetadata.DATE_FIELD:
				return "date";
			case DataFieldMetadata.LONG_FIELD:
				return "long";
			case DataFieldMetadata.DECIMAL_FIELD:
			    return "decimal";
			case DataFieldMetadata.BYTE_FIELD:
			    return "byte";
			case DataFieldMetadata.BYTE_FIELD_COMPRESSED:
			    return "cbyte";
			case DataFieldMetadata.DATETIME_FIELD:
			    return "datetime";
			case DataFieldMetadata.SEQUENCE_FIELD:
			    return "sequence";
			default:
				return "!!! UNKNOWN !!!";
		}
	}
	
	public static char str2Type(String fieldType) {
		if(fieldType.compareToIgnoreCase("numeric") == 0)
		    return DataFieldMetadata.NUMERIC_FIELD;
		else if(fieldType.compareToIgnoreCase("integer") == 0)
		    return DataFieldMetadata.INTEGER_FIELD;
		else if(fieldType.compareToIgnoreCase("string") == 0)
		    return DataFieldMetadata.STRING_FIELD;
		else if(fieldType.compareToIgnoreCase("date") == 0)
		    return DataFieldMetadata.DATE_FIELD;
		else if(fieldType.compareToIgnoreCase("long") == 0)
		    return DataFieldMetadata.LONG_FIELD;
		else if(fieldType.compareToIgnoreCase("decimal") == 0)
		    return DataFieldMetadata.DECIMAL_FIELD;
		else if(fieldType.compareToIgnoreCase("byte") == 0)
		    return DataFieldMetadata.BYTE_FIELD;
		else if(fieldType.compareToIgnoreCase("cbyte") == 0)
		    return DataFieldMetadata.BYTE_FIELD_COMPRESSED;
		else if(fieldType.compareToIgnoreCase("datetime") == 0)
		    return DataFieldMetadata.DATETIME_FIELD;
		else if(fieldType.compareToIgnoreCase("sequence") == 0)
		    return DataFieldMetadata.SEQUENCE_FIELD;
		return DataFieldMetadata.UNKNOWN_FIELD;
	}
	
	public boolean equals(Object o){
		if (!(o instanceof DataFieldMetadata)){
			return false;
		}
		if (this.fieldType==((DataFieldMetadata)o).fieldType){
			if (isFixed() && ((DataFieldMetadata)o).isFixed()) {
				//both fixed
				return getSize() == ((DataFieldMetadata)o).getSize();
			}else if (!isFixed() && !((DataFieldMetadata)o).isFixed()) {
				//both delimited
				if (this.fieldType==DECIMAL_FIELD){
					return (getProperty(LENGTH_ATTR).equals(
							((DataFieldMetadata)o).getProperty(LENGTH_ATTR)) && 
							getProperty(SCALE_ATTR).equals(
									((DataFieldMetadata)o).getProperty(SCALE_ATTR)));
				}else{
					//the same type and both delimited
					return true;
				}
			}else{
				//one fixed and the second delimited
				return false;
			}
		}
		//diffrent types
		return false;
	}
	
	public int hashCode(){
		return (int)this.fieldType;
	}
	
	/**
	 * This method checks if value from this field can be put safe in another field
	 * 
	 * @param anotherField
	 * @return true if conversion is save, false in another case
	 */
	public boolean isSubtype(DataFieldMetadata anotherField){
		int anotherFieldLength;
		int anotherFieldScale;
		switch (fieldType) {
		case BYTE_FIELD:
		case BYTE_FIELD_COMPRESSED:
			switch (anotherField.getType()) {
			case BYTE_FIELD:
			case BYTE_FIELD_COMPRESSED:
				return true;
			default:
				return false;
			}
		case STRING_FIELD:
			switch (anotherField.getType()) {
			case BYTE_FIELD:
			case BYTE_FIELD_COMPRESSED:
			case STRING_FIELD:
				return true;
			default:
				return false;
			}
		case DATE_FIELD:
			switch (anotherField.getType()) {
			case BYTE_FIELD:
			case BYTE_FIELD_COMPRESSED:
			case STRING_FIELD:
			case DATE_FIELD:
			case DATETIME_FIELD:
				return true;
			default:
				return false;
			}
		case DATETIME_FIELD:
			switch (anotherField.getType()) {
			case BYTE_FIELD:
			case BYTE_FIELD_COMPRESSED:
			case STRING_FIELD:
			case DATETIME_FIELD:
				return true;
			default:
				return false;
			}
		case INTEGER_FIELD:
			switch (anotherField.getType()) {
			case BYTE_FIELD:
			case BYTE_FIELD_COMPRESSED:
			case STRING_FIELD:
			case INTEGER_FIELD:
			case LONG_FIELD:
			case NUMERIC_FIELD:
				return true;
			case DECIMAL_FIELD:
				anotherFieldLength = Integer.valueOf(anotherField.getProperty(LENGTH_ATTR));
				anotherFieldScale = Integer.valueOf(anotherField.getProperty(SCALE_ATTR));
				if (anotherFieldLength - anotherFieldScale >= INTEGER_LENGTH) {
					return true;
				}else{
					return false;
				}
			default:
				return false;
			}
		case LONG_FIELD:
			switch (anotherField.getType()) {
			case BYTE_FIELD:
			case BYTE_FIELD_COMPRESSED:
			case STRING_FIELD:
			case LONG_FIELD:
			case NUMERIC_FIELD:
				return true;
			case DECIMAL_FIELD:
				anotherFieldLength = Integer.valueOf(anotherField.getProperty(LENGTH_ATTR));
				anotherFieldScale = Integer.valueOf(anotherField.getProperty(SCALE_ATTR));
				if (anotherFieldLength - anotherFieldScale >= LONG_LENGTH) {
					return true;
				}else{
					return false;
				}
			default:
				return false;
			}
		case NUMERIC_FIELD:
			switch (anotherField.getType()) {
			case BYTE_FIELD:
			case BYTE_FIELD_COMPRESSED:
			case STRING_FIELD:
			case NUMERIC_FIELD:
				return true;
			case DECIMAL_FIELD:
				anotherFieldLength = Integer.valueOf(anotherField.getProperty(LENGTH_ATTR));
				anotherFieldScale = Integer.valueOf(anotherField.getProperty(SCALE_ATTR));
				if (anotherFieldLength >= DOUBLE_LENGTH && anotherFieldScale >= DOUBLE_SCALE) {
					return true;
				}else{
					return false;
				}
			default:
				return false;
			}
		case DECIMAL_FIELD:
			switch (anotherField.getType()) {
			case BYTE_FIELD:
			case BYTE_FIELD_COMPRESSED:
			case STRING_FIELD:
				return true;
			case DECIMAL_FIELD:
				anotherFieldLength = Integer.valueOf(anotherField.getProperty(LENGTH_ATTR));
				anotherFieldScale = Integer.valueOf(anotherField.getProperty(SCALE_ATTR));
				if (anotherFieldLength >= Integer.valueOf(fieldProperties.getProperty(LENGTH_ATTR)) && 
						anotherFieldScale >= Integer.valueOf(fieldProperties.getProperty(SCALE_ATTR))) {
					return true;
				}else{
					return false;
				}
			case NUMERIC_FIELD:
				if (Integer.valueOf(fieldProperties.getProperty(LENGTH_ATTR)) <= DOUBLE_LENGTH && 
						Integer.valueOf(fieldProperties.getProperty(SCALE_ATTR)) <= DOUBLE_SCALE ) {
					return true;
				}else{
					return false;
				}
			case INTEGER_FIELD:
				if (Integer.valueOf(fieldProperties.getProperty(LENGTH_ATTR)) - 
						Integer.valueOf(fieldProperties.getProperty(SCALE_ATTR)) 
						<= INTEGER_LENGTH ) {
					return true;
				}else{
					return false;
				}
			case LONG_FIELD:
				if (Integer.valueOf(fieldProperties.getProperty(LENGTH_ATTR)) - 
						Integer.valueOf(fieldProperties.getProperty(SCALE_ATTR)) 
						<= LONG_LENGTH ) {
					return true;
				}else{
					return false;
				}
			default:
				return false;
			}
		}
		return false;
	}


	public boolean isTrim() {
		return trim;
	}


	public void setTrim(boolean trim) {
		this.trim = trim;
	}

	/**
	 * This method checks if formatString has a format of date.
	 * Note: formatString can has a format of date and format of time at the same time.
	 * @param formatString
	 * @return true if formatString has a format of date.
	 * @since     24.8.2007
     * @see       org.jetel.component.DataFieldmetadata.isTimeFormat(CharSequence)
	 */
	public boolean isDateFormat(CharSequence formatString) {
		return DATE_ONLY_PATTERN.matcher(formatString).find();
	}

	/**
	 * This method checks if formatString has a format of time.
	 * Note: formatString can has a format of date and format of time at the same time.
	 * @param formatString
	 * @return true if formatString has a format of time.
	 * @since     24.8.2007
     * @see       org.jetel.component.DataFieldmetadata.isDateFormat(CharSequence)
	 */
	public boolean isTimeFormat(CharSequence formatString) {
		return TIME_ONLY_PATTERN.matcher(formatString).find();
	}
	
}
/*
 *  end class DataFieldMetadata
 */

