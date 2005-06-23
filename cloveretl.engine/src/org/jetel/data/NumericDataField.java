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
import java.text.NumberFormat;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
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
public class NumericDataField extends DataField implements Number{

	private double value;
	private NumberFormat numberFormat;
	private ParsePosition parsePosition;

	private final static int FIELD_SIZE_BYTES = 8;// standard size of field
	// Attributes
	/**
	 *  An attribute that represents ...
	 *
	 *@since
	 */

	/**
	 *  Constructor for the NumericDataField object
	 *
	 *@param  _metadata  Metadata describing field
	 *@since             March 28, 2002
	 */
	public NumericDataField(DataFieldMetadata _metadata) {
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
			// probably wrong locale string defined
			if (locale==null){
				throw new RuntimeException("Can't create Locale based on "+_metadata.getLocaleStr());
			}
		}else{
			locale=null;
		}
		// handle formatString
		String formatString;
		formatString = _metadata.getFormatStr();
		 if ((formatString != null) && (formatString.length() != 0)) {
		 	if (locale!=null){
				numberFormat = new DecimalFormat(formatString,new DecimalFormatSymbols(locale));
		 	}else{
		 		numberFormat = new DecimalFormat(formatString);
			}
		 }else if (locale!=null) {
				numberFormat = DecimalFormat.getInstance(locale);
		}
		if (numberFormat!=null){
		    parsePosition = new ParsePosition(0);
		}
		
	}


	/**
	 *  Constructor for the NumericDataField object
	 *
	 *@param  _metadata  Metadata describing field
	 *@param  value      Value to assign to field
	 *@since             March 28, 2002
	 */
	public NumericDataField(DataFieldMetadata _metadata, double value) {
		this(_metadata);
		this.value = value;
	}


	/**
	 * Private constructor to be used internally when clonning object.
	 * Optimized for performance. Many checks waved.
	 * @param _metadata
	 * @param value
	 * @param numberFormat
	 */
	private NumericDataField(DataFieldMetadata _metadata,double value,NumberFormat numberFormat){
	    super(_metadata);
	    this.value=value;
	    this.numberFormat=numberFormat;
	    this.parsePosition= (numberFormat !=null) ? new ParsePosition(0) : null; 
	 }
	
	
	/* (non-Javadoc)
	 * @see org.jetel.data.DataField#copy()
	 */
	public DataField duplicate(){
	    NumericDataField newField=new NumericDataField(metadata,value,numberFormat);
	    newField.setNull(isNull());
	    return newField;
	}
	
	
	/* (non-Javadoc)
	 * @see org.jetel.data.DataField#copyField(org.jetel.data.DataField)
	 */
	public void copyFrom(DataField fromField){
	    if (fromField instanceof NumericDataField){
	        if (!fromField.isNull){
	            this.value=((NumericDataField)fromField).value;
	        }
	        setNull(fromField.isNull);
	    }
	}
	
	/**
	 *  Sets the value of the field - the value is extracted from the passed-in object.
	 * If the object is null, then the field value is set to NULL.
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
	 *  Sets the value of the field. If the passed in value is Double.NaN, then
	 * the value of the field is set to NULL.
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
	 *  Sets the value of the field. If the passed in value is Integer.MIN_VALUE, then
	 * the value of the field is set to NULL.
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
	 *  Sets the value of the field. If the passed in value is Long.MIN_VALUE, then
	 * the value of the field is set to NULL.
	 *
	 *@param  value  The new Int value
	 *@since         August 19, 2002
	 */
	public void setValue(long value) {
		if (value == Long.MIN_VALUE) {
			if(this.metadata.isNullable()) {
				this.value = Double.NaN;
				super.setNull(true);
			} else {
				throw new BadDataFormatException(getMetadata().getName()+" field can not be set to null!(nullable=false)",null);
			}
			return;
		}
		this.value = (double)value;
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
		if( isNull ) {
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
		if (isNull){
		    return Double.NaN;
		}
	    return value;
	}


	/**
	 *  Gets the numeric value represented by this object casted to int primitive
	 *
	 *@return    The Int value
	 *@since     August 19, 2002
	 */
	public int getInt() {
		if( isNull ) {
			return Integer.MIN_VALUE;
		}
		return (int) value;
	}

	/**
	 *  Gets the numeric value represented by this object casted to long primitive
	 *
	 *@return    The Int value
	 *@since     August 19, 2002
	 */
	public long getLong() {
		if( isNull ) {
			return Long.MIN_VALUE;
		}
		return (long) value;
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
		if( isNull ) {
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
	 *  Compares field's internal value to value of passed-in Object
	 *
	 *@param  obj  Object representing numeric value
	 *@return      -1,0,1 if internal value(less-then,equals, greather then) passed-in value
	 */
	public int compareTo(Object obj) {
		
		if (obj instanceof NumericDataField){
			return compareTo(((NumericDataField) obj).getDouble());
		}else if (obj instanceof Double){
			return compareTo(((Double)obj).doubleValue());
		}else if (obj instanceof Integer){
			return compareTo(((Integer)obj).doubleValue());
		}else if (obj instanceof Long){
			return compareTo(((Long)obj).doubleValue());
		}else throw new RuntimeException("Object does not represent a numeric value: "+obj);
	}


	/**
	 *  Compares field's internal value to passed-in value
	 *
	 *@param  compVal  double value against which to compare
	 *@return          -1,0,1 if internal value(less-then,equals, greather then) passed-in value
	 */
	public int compareTo(double compVal) {
		return Double.compare(value,compVal);
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.data.Number#compareTo(org.jetel.data.Number)
	 */
	public int compareTo(Number value){
	    return compareTo(value.getDouble());
	}
	
	
	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	public int hashCode(){
		long v=Double.doubleToLongBits(value);
		return (int)(v^(v>>32));
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

