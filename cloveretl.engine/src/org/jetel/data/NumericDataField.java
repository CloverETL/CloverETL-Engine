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
package org.jetel.data;
import java.util.Locale;
import java.text.DecimalFormat;
import java.text.ParsePosition;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharacterCodingException;

import org.jetel.exception.BadDataFormatException;
import org.jetel.metadata.DataFieldMetadata;


/**
 *  A class that represents decimal number field (double precision)
 *
 *@author     D.Pavlis
 *@created    January 26, 2003
 *@since      March 27, 2002
 *@see        org.jetel.metadata.DataFieldMetadata
 */
public class NumericDataField extends DataField {

	private double value;
	private DecimalFormat numberFormat;
	private ParsePosition parsePosition;
	
	private final static int FIELD_SIZE_BYTES = 8;// standard size of field
	// Attributes
	/**
	 *  An attribute that represents ...
	 *
	 *@since
	 */

	private static Locale DEFAULT_LOCALE = Locale.US;


	/**
	 *  Constructor for the NumericDataField object
	 *
	 *@param  _metadata  Metadata describing field
	 *@since             March 28, 2002
	 */
	public NumericDataField(DataFieldMetadata _metadata) {
		super(_metadata);
		String formatString;
		formatString = _metadata.getFormatStr();
		if ((formatString != null) && (formatString.length() != 0)) {
			numberFormat = new DecimalFormat(formatString);
			//numberFormat = new DecimalFormat(formatString,new DecimalFormatSymbols(DEFAULT_LOCALE));
		} else {
			numberFormat = null;
		}
		parsePosition = new ParsePosition(0);
	}


	/**
	 *  Constructor for the NumericDataField object
	 *
	 *@param  _metadata  Metadata describing field
	 *@param  value      Value to assign to field
	 *@since             March 28, 2002
	 */
	public NumericDataField(DataFieldMetadata _metadata, double value) {
		super(_metadata);
		this.value = value;
		String formatString;
		formatString = _metadata.getFormatStr();
		if ((formatString != null) && (formatString.length() != 0)) {
			numberFormat = new DecimalFormat(formatString);
		} else {
			numberFormat = null;
		}
		parsePosition = new ParsePosition(0);
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
				value = Double.NaN;
				super.setNull(true);
			} else {
				throw new BadDataFormatException(getMetadata().getName()+" field can not be set to null!(nullable=false)",null);
			}
			return;
		}
		if (_value instanceof Double) {
			value = ((Double) _value).doubleValue();
		} else {
			if(this.metadata.isNullable()) {
				value = Double.NaN;
				super.setNull(true);
			} else
				throw new BadDataFormatException(getMetadata().getName()+" field can not be set with this object - " +_value.toString(),_value.toString());
		}
	}


	/**
	 *  Sets the value of the field
	 *
	 *@param  value  The new Double value
	 *@since         August 19, 2002
	 */
	public void setValue(double value) {
		if (value == Double.NaN) {
			if(this.metadata.isNullable()) {
				value = Double.NaN;
				super.setNull(true);
			} else {
				throw new BadDataFormatException(getMetadata().getName()+" field can not be set to null!(nullable=false)",null);
			}
			return;
		}
		this.value = value;
	}


	/**
	 *  Sets the value of the field
	 *
	 *@param  value  The new Int value
	 *@since         August 19, 2002
	 */
	public void setValue(int value) {
		if (value == Integer.MIN_VALUE) {
			if(this.metadata.isNullable()) {
				this.value = Double.NaN;
				super.setNull(true);
			} else {
				throw new BadDataFormatException(getMetadata().getName()+" field can not be set to null!(nullable=false)",null);
			}
			return;
		}
		this.value = value;
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
			value = Double.NaN;
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
		return DataFieldMetadata.NUMERIC_FIELD;
	}


	/**
	 *  Gets the decimal value represented by this object (as Decimal object)
	 *
	 *@return    The Value value
	 *@since     March 28, 2002
	 */
	public Object getValue() {
		if( Double.isNaN(value) ) {
			return null;
		}
		return new Double(value);
	}


	/**
	 *  Gets the decimal value represented by this object as double primitive
	 *
	 *@return    The Double value
	 *@since     August 19, 2002
	 */
	public double getDouble() {
		return value;
	}


	/**
	 *  Gets the numeric value represented by this object casted to int primitive
	 *
	 *@return    The Int value
	 *@since     August 19, 2002
	 */
	public int getInt() {
		if( Double.NaN==value ) {
			return Integer.MIN_VALUE;
		}
		return (int) value;
	}


	/**
	 *  Gets the Null value indicator
	 *
	 *@return    The Null value
	 *@since     October 29, 2002
	 */
	public boolean isNull() {
		return super.isNull();
	}


	/**
	 *  Formats internal decimal value into string representation
	 *
	 *@return    Description of the Returned Value
	 *@since     March 28, 2002
	 */
	public String toString() {
		if( Double.isNaN(value) ) {
			return "";
		}
		if (numberFormat != null) {
			return numberFormat.format(value);
		} else {
			return Double.toString(value);
		}
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
				value = Double.NaN;
				super.setNull(true);
			} else
				throw new BadDataFormatException(getMetadata().getName()+" field can not be set to null!(nullable=false)",valueStr);
			return;
		} else {
			try {
				if (numberFormat != null) {
					parsePosition.setIndex(0);
					value = numberFormat.parse(valueStr, parsePosition).doubleValue();
				} else {
					value = Double.parseDouble(valueStr);
				}
			} catch (Exception ex) {
				throw new BadDataFormatException(getMetadata().getName()+" cannot be set to " + valueStr,valueStr);
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
		buffer.putDouble(value);
	}


	/**
	 *  Performs deserialization of data
	 *
	 *@param  buffer  Description of Parameter
	 *@since          April 23, 2002
	 */
	public void deserialize(ByteBuffer buffer) {
		value = buffer.getDouble();
		setNull(Double.isNaN(value));
	}


	/**
	 *  Description of the Method
	 *
	 *@param  obj  Description of Parameter
	 *@return      Description of the Returned Value
	 *@since       April 23, 2002
	 */
	public boolean equals(Object obj) {
		Double numValue = new Double(this.value);

		return (numValue.equals((((NumericDataField) obj).getValue())));
	}


	/**
	 *  Compares this object with the specified object for order.
	 *
	 *@param  obj  Description of the Parameter
	 *@return      Description of the Return Value
	 */
	public int compareTo(Object obj) {
		double compDouble = ((NumericDataField) obj).getDouble();
		
		if (value > compDouble) {
			return 1;
		} else if (value < compDouble) {
			return -1;
		} else {
			return 0;
		}
	}


	/**
	 *  Compares this object with the specified object for order.
	 *
	 *@param  compVal  Description of the Parameter
	 *@return          Description of the Return Value
	 */
	public int compareTo(double compVal) {
		if (value > compVal) {
			return 1;
		} else if (value < compVal) {
			return -1;
		} else {
			return 0;
		}
	}

	/**
	 *  Returns how many bytes will be occupied when this field with current
	 *  value is serialized into ByteBuffer
	 *
	 * @return    The size value
	 * @see	      org.jetel.data.DataField
	 */
	public int getSizeSerialized() {
		return FIELD_SIZE_BYTES;
	}

}
/*
 *  end class NumericDataField
 */

