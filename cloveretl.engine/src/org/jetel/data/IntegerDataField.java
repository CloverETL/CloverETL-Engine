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
package org.jetel.data;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharacterCodingException;

import org.jetel.exception.BadDataFormatException;
import org.jetel.metadata.DataFieldMetadata;

import java.util.logging.Logger;

/**
 *  A class that represents integer number field (32bit signed)
 *
 *@author      D.Pavlis
 *@created     January 26, 2003
 *@since       March 27, 2002
 *@see         org.jetel.metadata.DataFieldMetadata
 *@revision    $Revision$
 */
public class IntegerDataField extends DataField {

	private int value;
	//private DecimalFormat numberFormat;
	//private ParsePosition parsePosition;

	// Attributes
	static Logger logger = Logger.getLogger("org.jetel.data");


	//private static Locale DEFAULT_LOCALE = Locale.US;

	/**
	 *  Constructor for the NumericDataField object
	 *
	 *@param  _metadata  Metadata describing field
	 *@since             March 28, 2002
	 */
	public IntegerDataField(DataFieldMetadata _metadata) {
		super(_metadata);
	}


	/**
	 *  Constructor for the NumericDataField object
	 *
	 *@param  _metadata  Metadata describing field
	 *@param  value      Value to assign to field
	 *@since             March 28, 2002
	 */
	public IntegerDataField(DataFieldMetadata _metadata, int value) {
		super(_metadata);
		setValue(value);
	}


	/**
	 *  Sets the value of the field
	 *
	 *@param  _value  The new Value value
	 *@since          March 28, 2002
	 */
	public void setValue(Object _value) {
		if (_value == null) {
			if(this.metadata.isNullable()) {
				value = Integer.MIN_VALUE;
				super.setNull(true);
			} else {
				throw new BadDataFormatException(getMetadata().getName()+" field can not be set to null!(nullable=false)");
			}
			return;
		}
		if (_value instanceof Integer) {
			value = ((Integer) _value).intValue();
			setNull(false);
		} else {
			if(this.metadata.isNullable()) {
				value = Integer.MIN_VALUE;
				super.setNull(true);
			} else
				throw new BadDataFormatException(getMetadata().getName()+" field can not be set with this object - " +_value.toString());
		}
	}


	/**
	 *  Sets the value of the field
	 *
	 *@param  value  The new Double value
	 *@since         August 19, 2002
	 */
	public void setValue(double value) {
		this.value = (int) value;
		setNull(false);
	}


	/**
	 *  Sets the value of the field
	 *
	 *@param  value  The new Int value
	 *@since         August 19, 2002
	 */
	public void setValue(int value) {
		this.value = value;
		setNull(false);
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
			value = Integer.MIN_VALUE;
		}
	}


	// Associations

	// Operations

	/**
	 *  Gets the Metadata attribute of the NumericDataField object
	 *
	 *@return    The Metadata value
	 *@since     October 31, 2002
	 */
	public DataFieldMetadata getMetadata() {
		return super.getMetadata();
	}


	/**
	 *  Gets the Field Type
	 *
	 *@return    The Type value
	 *@since     March 28, 2002
	 */
	public char getType() {
		return DataFieldMetadata.INTEGER_FIELD;
	}


	/**
	 *  Gets the decimal value represented by this object (as Decimal object)
	 *
	 *@return    The Value value
	 *@since     March 28, 2002
	 */
	public Object getValue() {
		if( Integer.MIN_VALUE==value ) {
			return null;
		}
		return new Integer(value);
	}


	/**
	 *  Gets the decimal value represented by this object as double primitive
	 *
	 *@return    The Double value
	 *@since     August 19, 2002
	 */
	public double getDouble() {
		return (double) value;
	}


	/**
	 *  Gets the numeric value represented by this object casted to int primitive
	 *
	 *@return    The Int value
	 *@since     August 19, 2002
	 */
	public int getInt() {
		return value;
	}


	/**
	 *  Formats internal decimal value into string representation
	 *
	 *@return    Description of the Returned Value
	 *@since     March 28, 2002
	 */
	public String toString() {
		if( Integer.MIN_VALUE==value ) {
			return "";
		}
		return Integer.toString(value);
	}


	/**
	 *  Parses decimal value from its string representation
	 *
	 *@param  valueStr  Description of Parameter
	 *@since            March 28, 2002
	 */
	public void fromString(String valueStr) {
		if(valueStr == null || valueStr.equals("")) {
			if(this.metadata.isNullable()) {
				value = Integer.MIN_VALUE;
				super.setNull(true);
			} else
				throw new BadDataFormatException(getMetadata().getName()+" field can not be set to null!(nullable=false)");
			return;
		} else {
			try {
				value = Integer.parseInt(valueStr);
			} catch (Exception ex) {
//				logger.info("Error when parsing string: " + valueStr);
				throw new BadDataFormatException(getMetadata().getName()+" cannot be set to " + valueStr);
			}
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
	 *  Performs serialization of the internal value into ByteBuffer (used when
	 *  moving data records between components).
	 *
	 *@param  buffer  Description of Parameter
	 *@since          April 23, 2002
	 */
	public void serialize(ByteBuffer buffer) {
		buffer.putInt(value);
	}


	/**
	 *  Performs deserialization of data
	 *
	 *@param  buffer  Description of Parameter
	 *@since          April 23, 2002
	 */
	public void deserialize(ByteBuffer buffer) {
		value = buffer.getInt();
		if (value == Integer.MIN_VALUE) {
			setNull(true);
		} else {
			setNull(false);
		}
	}


	/**
	 *  Description of the Method
	 *
	 *@param  obj  Description of Parameter
	 *@return      Description of the Returned Value
	 *@since       April 23, 2002
	 */
	public boolean equals(Object obj) {
		Integer numValue = new Integer(this.value);

		return (numValue.equals((((IntegerDataField) obj).getValue())));
	}


	/**
	 *  Description of the Method
	 *
	 *@param  obj  Description of the Parameter
	 *@return      Description of the Return Value
	 */
	public int compareTo(Object obj) {
		int compInt = ((IntegerDataField) obj).getInt();

		if (value > compInt) {
			return 1;
		} else if (value < compInt) {
			return -1;
		} else {
			return 0;
		}
	}


	/**
	 *  Description of the Method
	 *
	 *@param  compInt  Description of the Parameter
	 *@return          Description of the Return Value
	 */
	public int compareTo(int compInt) {
		if (value > compInt) {
			return 1;
		} else if (value < compInt) {
			return -1;
		} else {
			return 0;
		}
	}
}
/*
 *  end class IntegerDataField
 */

