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
// FILE: c:/projects/jetel/org/jetel/data/DateDataField.java

package org.jetel.data;
import java.util.*;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharacterCodingException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;

import org.jetel.exception.BadDataFormatException;
import org.jetel.metadata.DataFieldMetadata;

/**
 *  A class that represents ...
 *
 * @author      D.Pavlis
 * @since       March 28, 2002
 * @revision    $Revision$
 * @created     January 26, 2003
 * @see         OtherClasses
 */
public class DateDataField extends DataField {

	// Attributes
	/**
	 *  An attribute that represents ...
	 *
	 * @since    March 28, 2002
	 */
	private Date value;
	private DateFormat dateFormat;
	private ParsePosition parsePosition;
		
	private final static int FIELD_SIZE_BYTES = 8;// standard size of field


	// Associations

	// Operations
	/**
	 *  Constructor for the DateDataField object
	 *
	 * @param  _metadata  Description of Parameter
	 * @since             April 23, 2002
	 */
	public DateDataField(DataFieldMetadata _metadata) {
		super(_metadata);
		Locale locale;
		// handle locale
		if (_metadata.getLocaleStr()!=null){
			String[] localeLC=_metadata.getLocaleStr().split(Defaults.DEFAULT_LOCALE_STR_DELIMITER_REGEX);
			if (localeLC.length>1){
				locale=new Locale(localeLC[0],localeLC[1]);
			}else{
				locale=new Locale(localeLC[0]);
			}
			//probably wrong locale string defined
			if (locale==null){
				throw new RuntimeException("Can't create Locale based on "+_metadata.getLocaleStr());
			}
		}else{
			locale=null;
		}
		// handle format string
		String formatString;
		formatString = _metadata.getFormatStr();
		if ((formatString != null) && (formatString.length() != 0)) {
			if (locale!=null){
				dateFormat = new SimpleDateFormat(formatString,locale);
			}else{
				dateFormat = new SimpleDateFormat(formatString);
			}
			dateFormat.setLenient(false);
		} else if (locale!=null){
			dateFormat = DateFormat.getDateInstance(DateFormat.DEFAULT,locale);
			dateFormat.setLenient(false);
		}
		if (dateFormat!=null){
		    parsePosition = new ParsePosition(0);
		}
	}


	/**
	 *  Constructor for the DateDataField object
	 *
	 * @param  _metadata  Description of Parameter
	 * @param  _value     Description of Parameter
	 * @since             April 23, 2002
	 */
	public DateDataField(DataFieldMetadata _metadata, Date _value) {
		this(_metadata);
		setValue(_value);
	}

	/**
	 * Private constructor to be used internally when clonning object.
	 * Optimized for performance. Many checks waved.
	 * 
	 * @param _metadata
	 * @param _value
	 * @param _dateFormat
	 */
	private DateDataField(DataFieldMetadata _metadata, Date _value, DateFormat _dateFormat){
	    super(_metadata);
	    setValue(_value);
	    this.dateFormat=_dateFormat;
	    this.parsePosition= (_dateFormat!=null) ? new ParsePosition(0) : null;
	}
	

	/* (non-Javadoc)
	 * @see org.jetel.data.DataField#copy()
	 */
	public DataField duplicate(){
	    DateDataField newField=new DateDataField(metadata,value,dateFormat);
	    newField.setNull(isNull());
	    return newField;
	}
	
	
	/* (non-Javadoc)
	 * @see org.jetel.data.DataField#copyField(org.jetel.data.DataField)
	 */
	public void copyFrom(DataField fromField){
	    if (fromField instanceof DateDataField){
	        if (!fromField.isNull){
	            this.value.setTime(((DateDataField)fromField).value.getTime());
	        }
	        setNull(fromField.isNull);
	    }
	}
	
	/**
	 *  Sets the date represented by DateDataField object
	 *
	 * @param  _value                      The new Value value
	 * @exception  BadDataFormatException  Description of the Exception
	 * @since                              April 23, 2002
	 */
	public void setValue(Object _value) throws BadDataFormatException {
		if (_value == null) {
			if (this.metadata.isNullable()) {
				value = null;
				super.setNull(true);
			} else {
				throw new BadDataFormatException(getMetadata().getName() + " field can not be set to null!(nullable=false)", null);
			}
			return;
		}
		if ( _value instanceof Date ) {
			if (value==null){
				value = new Date(((Date) _value).getTime());
			}else{
				value.setTime(((Date) _value).getTime());
			}

			super.setNull(false);
		} else {
			if (this.metadata.isNullable()) {
				value = null;
				super.setNull(true);
			} else {
				throw new BadDataFormatException(getMetadata().getName() + " field can not be set with this object - " + _value.toString(), _value.toString());
			}
		}
	}


	/**
	 *  Gets the date represented by DateDataField object
	 *
	 * @return    The Value value
	 * @since     April 23, 2002
	 */
	public Object getValue() {
		return value;
	}


	/**
	 *  Gets the date attribute of the DateDataField object
	 *
	 * @return    The date value
	 */
	public Date getDate() {
		return value;
	}


	/**
	 *  Sets the Null value indicator
	 *
	 * @param  isNull  The new Null value
	 * @since          October 29, 2002
	 */
	public void setNull(boolean isNull) {
		super.setNull(isNull);
		if (isNull) {
			setValue(null);
		}
	}


	/**
	 *  Gets the DataField type
	 *
	 * @return    The Type value
	 * @since     April 23, 2002
	 */
	public char getType() {
		return DataFieldMetadata.DATE_FIELD;
	}


	/**
	 *  Formats the internal date value into a string representation.<b> If
	 *  metadata describing DateField contains formating string, that string is
	 *  used to create output. Otherwise the standard format is used.
	 *
	 * @return    Description of the Returned Value
	 * @since     April 23, 2002
	 */
	public String toString() {
		if (value == null) {
			return "";
		}
		if ((dateFormat != null)) {
			return dateFormat.format(value);
		} else {
			return value.toString();
		}
	}


	/**
	 *  Description of the Method
	 *
	 * @param  dataBuffer                    Description of Parameter
	 * @param  decoder                       Description of Parameter
	 * @exception  CharacterCodingException  Description of Exception
	 * @since                                October 31, 2002
	 */
	public void fromByteBuffer(ByteBuffer dataBuffer, CharsetDecoder decoder) throws CharacterCodingException {
		fromString(decoder.decode(dataBuffer).toString());
	}


	/**
	 *  Description of the Method
	 *
	 * @param  dataBuffer                    Description of Parameter
	 * @param  encoder                       Description of Parameter
	 * @exception  CharacterCodingException  Description of Exception
	 * @since                                October 31, 2002
	 */
	public void toByteBuffer(ByteBuffer dataBuffer, CharsetEncoder encoder) throws CharacterCodingException {
		dataBuffer.put(encoder.encode(CharBuffer.wrap(toString())));
	}


	/**
	 *  Parses date value from string representation. If format string is defined,
	 *  then it is used as expected pattern.
	 *
	 * @param  _valueStr  Description of Parameter
	 * @since             April 23, 2002
	 */
	public void fromString(String _valueStr) {
		//parsePosition.setIndex(0);
		if (_valueStr == null || _valueStr.equals("")) {
			if (this.metadata.isNullable()) {
				value = null;
				super.setNull(true);
			} else {
				throw new BadDataFormatException(getMetadata().getName() + " field can not be set to null!(nullable=false)", _valueStr);
			}
			return;
		}
		try {
			if (dateFormat != null) {
				value = dateFormat.parse(_valueStr);//, parsePosition);
			} else {
				value = SimpleDateFormat.getDateInstance().parse(_valueStr);
			}
			super.setNull(false);
		} catch (ParseException e) {
			super.setNull(true);
			throw new BadDataFormatException("not a Date", _valueStr);
		}

	}


	/**
	 *  Performs serialization of the internal value into ByteBuffer (used when
	 *  moving data records between components).
	 *
	 * @param  buffer  Description of Parameter
	 * @since          April 23, 2002
	 */
	public void serialize(ByteBuffer buffer) {
		if (value != null) {
			buffer.putLong(value.getTime());
		} else {
			buffer.putLong(0);
		}
	}


	/**
	 *  Performs deserialization of data
	 *
	 * @param  buffer  Description of Parameter
	 * @since          April 23, 2002
	 */
	public void deserialize(ByteBuffer buffer) {
		long tmpl = buffer.getLong();
		if (tmpl == 0) {
			setValue(null);
			return;
		}
		if (value == null) {
			value = new Date(tmpl);
		} else {
			value.setTime(tmpl);
		}
		setNull(false);
	}


	/**
	 *  Description of the Method
	 *
	 * @param  obj  Description of Parameter
	 * @return      Description of the Returned Value
	 * @since       April 23, 2002
	 */
	public boolean equals(Object obj) {
		return (this.value.equals((((DateDataField) obj).getValue())));
	}


	/**
	 *  Compares this object with the specified object for order
	 *
	 * @param  obj  Description of the Parameter
	 * @return      Description of the Return Value
	 */
	public int compareTo(Object obj) {
		if (obj instanceof Date){
			return value.compareTo(obj);
		}else if (obj instanceof DateDataField){
			return value.compareTo(((DateDataField) obj).getDate());
		}else throw new RuntimeException("Object does not represent a Date value: "+obj);
	}


	/**
	 *  Description of the Method
	 *
	 * @param  obj  Description of the Parameter
	 * @return      Description of the Return Value
	 */
	public int compareTo(java.util.Date obj) {
		return value.compareTo(obj);
	}
	
	public int hashCode(){
		return value.hashCode();
	}
	
	/**
	 *  Gets the size attribute of the IntegerDataField object
	 *
	 * @return    The size value
	 * @see	      org.jetel.data.DataField
	 */
	public int getSizeSerialized() {
		return FIELD_SIZE_BYTES;
	}

}
/*
 *  end class DateDataField
 */

