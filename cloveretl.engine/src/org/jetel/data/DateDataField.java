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
 *@author     D.Pavlis
 *@created    January 26, 2003
 *@since      March 28, 2002
 *@see        OtherClasses
 */
public class DateDataField extends DataField {

	// Attributes
	/**
	 *  An attribute that represents ...
	 *
	 *@since    March 28, 2002
	 */
	private Date value;
	private DateFormat dateFormat;
	private ParsePosition parsePosition;


	// Associations

	// Operations
	/**
	 *  Constructor for the DateDataField object
	 *
	 *@param  _metadata  Description of Parameter
	 *@since             April 23, 2002
	 */
	public DateDataField(DataFieldMetadata _metadata) {
		super(_metadata);
		String formatString;
		formatString = _metadata.getFormatStr();
		if ((formatString != null) && (formatString.length() != 0)) {
			dateFormat = new SimpleDateFormat(formatString);
		} else {
			dateFormat = null;
		}
		parsePosition = new ParsePosition(0);
	}


	/**
	 *  Constructor for the DateDataField object
	 *
	 *@param  _metadata  Description of Parameter
	 *@param  _value     Description of Parameter
	 *@since             April 23, 2002
	 */
	public DateDataField(DataFieldMetadata _metadata, Date _value) {
		super(_metadata);
		String formatString;
		formatString = _metadata.getFormatStr();
		if ((formatString != null) && (formatString.length() != 0)) {
			dateFormat = new SimpleDateFormat(formatString);
		} else {
			dateFormat = null;
		}
		parsePosition = new ParsePosition(0);
		
		setValue(_value);
	}


	/**
	 *  Sets the date represented by DateDataField object
	 *
	 *@param  _value  The new Value value
	 *@since          April 23, 2002
	 */
	public void setValue(Object _value) {
		if(_value==null) {
			if(this.metadata.isNullable()) {
				value = null;
				super.setNull(true);
			} else {
				throw new BadDataFormatException(getMetadata().getName()+ " field can not be set to null!(nullable=false)");
			}
			return;
		}
		if ( _value instanceof Date ) {
			value = new Date(((Date) _value).getTime());
			super.setNull(false);
		} else {
			if(this.metadata.isNullable()) {
				value = null;
				super.setNull(true);
			} else
				throw new BadDataFormatException(getMetadata().getName()+" field can not be set with this object - " +_value.toString());
		}
	}

	
	/**
	 *  Gets the date represented by DateDataField object
	 *
	 *@return    The Value value
	 *@since     April 23, 2002
	 */
	public Object getValue() {
		return value;
	}

	/**
	 *  Sets the Null value indicator
	 *
	 *@param  isNull  The new Null value
	 *@since          October 29, 2002
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
	 *@return    The Type value
	 *@since     April 23, 2002
	 */
	public char getType() {
		return DataFieldMetadata.DATE_FIELD;
	}


	/**
	 *  Formats the internal date value into a string representation.<b> If
	 *  metadata describing DateField contains formating string, that string is
	 *  used to create output. Otherwise the standard format is used.
	 *
	 *@return    Description of the Returned Value
	 *@since     April 23, 2002
	 */
	public String toString() {
		if(value == null) {
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
	 *@param  dataBuffer                    Description of Parameter
	 *@param  decoder                       Description of Parameter
	 *@exception  CharacterCodingException  Description of Exception
	 *@since                                October 31, 2002
	 */
	public void fromByteBuffer(ByteBuffer dataBuffer, CharsetDecoder decoder) throws CharacterCodingException {
		fromString(decoder.decode(dataBuffer).toString());
	}


	/**
	 *  Description of the Method
	 *
	 *@param  dataBuffer                    Description of Parameter
	 *@param  encoder                       Description of Parameter
	 *@exception  CharacterCodingException  Description of Exception
	 *@since                                October 31, 2002
	 */
	public void toByteBuffer(ByteBuffer dataBuffer, CharsetEncoder encoder) throws CharacterCodingException {
		dataBuffer.put(encoder.encode(CharBuffer.wrap(toString())));
	}


	/**
	 *  Parses date value from string representation. If format string is defined,
	 *  then it is used as expected pattern.
	 *
	 *@param  _valueStr  Description of Parameter
	 *@since             April 23, 2002
	 */
	public void fromString(String _valueStr) {
		parsePosition.setIndex(0);
		if(_valueStr == null || _valueStr.equals("")) {
			if(this.metadata.isNullable()) {
				value = null;
				super.setNull(true);
			} else
				throw new BadDataFormatException(getMetadata().getName()+" field can not be set to null!(nullable=false)");
			return;
		}
		try {
			if (dateFormat != null) {
				value = dateFormat.parse(_valueStr); //, parsePosition);
			} else {
				value = SimpleDateFormat.getDateInstance().parse(_valueStr);
			}
			super.setNull(false);
		 }catch (ParseException e) {
			super.setNull(true);
		  	throw new BadDataFormatException("not Date");
		 }
		 
	}


	/**
	 *  Performs serialization of the internal value into ByteBuffer (used when
	 *  moving data records between components).
	 *
	 *@param  buffer  Description of Parameter
	 *@since          April 23, 2002
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
	 *@param  buffer  Description of Parameter
	 *@since          April 23, 2002
	 */
	public void deserialize(ByteBuffer buffer) {
		long tmpl = buffer.getLong();
		if(tmpl == 0) {
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
	 *@param  obj  Description of Parameter
	 *@return      Description of the Returned Value
	 *@since       April 23, 2002
	 */
	public boolean equals(Object obj) {
		return (this.value.equals((((DateDataField) obj).getValue())));
	}


	/**
	 *  Compares this object with the specified object for order
	 *
	 *@param  obj  Description of the Parameter
	 *@return      Description of the Return Value
	 */
	public int compareTo(Object obj) {
		return value.compareTo(obj);
	}
}
/*
 *  end class DateDataField
 */

