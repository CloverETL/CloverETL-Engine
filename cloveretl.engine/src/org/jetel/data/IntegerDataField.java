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
package org.jetel.data;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;

import org.jetel.data.primitive.CloverInteger;
import org.jetel.data.primitive.Decimal;
import org.jetel.data.primitive.DecimalFactory;
import org.jetel.data.primitive.Numeric;
import org.jetel.exception.BadDataFormatException;
import org.jetel.metadata.DataFieldMetadata;

/**
 *  A class that represents integer number field (32bit signed)
 *
 * @author      D.Pavlis
 * @since       March 27, 2002
 * @revision    $Revision$
 * @created     January 26, 2003
 * @see         org.jetel.metadata.DataFieldMetadata
 */
public class IntegerDataField extends DataField implements Numeric, Comparable {

	private int value;
	private final static int FIELD_SIZE_BYTES = 4;// standard size of field
	//private DecimalFormat numberFormat;
	//private ParsePosition parsePosition;

	// Attributes

	//private static Locale DEFAULT_LOCALE = Locale.US;

	/**
	 *  Constructor for the NumericDataField object
	 *
	 * @param  _metadata  Metadata describing field
	 * @since             March 28, 2002
	 */
	public IntegerDataField(DataFieldMetadata _metadata) {
		super(_metadata);
	}


	/**
	 *  Constructor for the NumericDataField object
	 *
	 * @param  _metadata  Metadata describing field
	 * @param  value      Value to assign to field
	 * @since             March 28, 2002
	 */
	public IntegerDataField(DataFieldMetadata _metadata, int value) {
		super(_metadata);
		setValue(value);
	}
	
	
	
	/* (non-Javadoc)
	 * @see org.jetel.data.DataField#copy()
	 */
	public DataField duplicate(){
	    IntegerDataField newField= new IntegerDataField(metadata,value);
	    newField.setNull(isNull());
	    return newField;
	}

	/**
	 * @see org.jetel.data.Numeric#duplicateNumeric()
	 */
	public Numeric duplicateNumeric() {
	    return new CloverInteger(value);
	}
    
	/* (non-Javadoc)
	 * @see org.jetel.data.DataField#copyField(org.jetel.data.DataField)
	 */
	public void copyFrom(DataField fromField){
	    if (fromField instanceof IntegerDataField){
	        if (!fromField.isNull){
	            this.value=((IntegerDataField)fromField).value;
	        }
	        setNull(fromField.isNull);
	    }
	}
	
	/**
	 *  Sets the value of the field - the value is extracted from the passed-in object.
	 * If the object is null, then the field value is set to NULL.
	 *
	 * @param  _value  The new Value value
	 * @since          March 28, 2002
	 */
	public void setValue(Object _value) {
		if (_value == null) {
		    setNull(true);
        } else if (_value instanceof Numeric) {
            setValue((Numeric) _value);
		} else if (_value instanceof Number) {
			value = ((Number) _value).intValue();
            setNull(value == Integer.MIN_VALUE);
		} else {
		    throw new BadDataFormatException(getMetadata().getName() + " field can not be set with this object - " + _value.toString(), _value.toString());
		}
	}


	/**
	 *  Sets the value of the field.If the passed in value is Double.NaN, then
	 * the value of the field is set to NULL. Double value is casted to int - possible information loss !!
	 *
	 * @param  value  The new Double value
	 * @since         August 19, 2002
	 */
	public void setValue(double value) {
		if (value == Double.NaN) {
		    setNull(true);
			return;
		}
		this.value = (int) value;
		setNull(false);
	}


	/**
	 *  Sets the value of the field.If the passed in value is Integer.MIN_VALUE, then
	 * the value of the field is set to NULL.
	 *
	 * @param  value  The new Int value
	 * @since         August 19, 2002
	 */
	public void setValue(int value) {
		if (value == Integer.MIN_VALUE) {
		    setNull(true);
			return;
		}
		this.value = value;
		setNull(false);
	}

	/**
	 *  Sets the value of the field.If the passed in value is Long.MIN_VALUE, then
	 * the value of the field is set to NULL. Long value is casted to int - possible information loss !!
	 *
	 * @param  value  The new Int value
	 * @since         August 19, 2002
	 */
	public void setValue(long value) {
		if (value == Long.MIN_VALUE) {
		    setNull(true);
			return;
		}
		this.value = (int) value;
		setNull(false);
	}
	
    /**
     * @see org.jetel.data.Numeric#setValue(org.jetel.data.Numeric)
     */
    public void setValue(Numeric value) {
        if(value == null || value.isNull()) {
            setNull(true);
            return;
        }
        this.value = value.getInt();
        setNull(false);
    }
	
    /**
     * @see org.jetel.data.Numeric#setValue(org.jetel.data.primitive.Decimal)
     */
    public void setValue(Decimal value) {
        if(value == null || value.isNull()) {
            setNull(true);
            return;
        }
        this.value = value.getInt();
        setNull(false);
    }

	/**
	 *  Sets the Null value indicator
	 *
	 * @param  isNull  The new Null value
	 * @since          October 29, 2002
	 */
	public void setNull(boolean isNull) {
		super.setNull(isNull);
		if (this.isNull) {
			value = Integer.MIN_VALUE;
		}
	}


	// Associations

	// Operations

	/**
	 *  Gets the Metadata attribute of the NumericDataField object
	 *
	 * @return    The Metadata value
	 * @since     October 31, 2002
	 */
	public DataFieldMetadata getMetadata() {
		return super.getMetadata();
	}


	/**
	 *  Gets the Field Type
	 *
	 * @return    The Type value
	 * @since     March 28, 2002
	 */
	public char getType() {
		return DataFieldMetadata.INTEGER_FIELD;
	}


	/**
	 *  Gets the decimal value represented by this object (as Decimal object)
	 *
	 * @return    The Value value
	 * @since     March 28, 2002
	 */
	public Object getValue() {
		if (isNull) {
			return null;
		}
		return new Integer(value);
	}


	/**
	 *  Gets the decimal value represented by this object as double primitive
	 *
	 * @return    The Double value
	 * @since     August 19, 2002
	 */
	public double getDouble() {
		if (isNull) {
			return Double.NaN;
		}
		return (double) value;
	}


	/**
	 *  Gets the numeric value represented by this object casted to int primitive
	 *
	 * @return    The Int value
	 * @since     August 19, 2002
	 */
	public int getInt() {
		if (isNull){
		    return Integer.MIN_VALUE;
		}
	    return value;
	}

	/**
	 *  Gets the numeric value represented by this object casted to long primitive
	 *
	 * @return    The Int value
	 * @since     August 19, 2002
	 */
	public long getLong() {
		if (isNull){
		    return Long.MIN_VALUE;
		}
	    return value;
	}

	/**
	 *  Formats internal decimal value into string representation
	 *
	 * @return    Description of the Returned Value
	 * @since     March 28, 2002
	 */
	public String toString() {
		if (isNull) {
			return "";
		}
		return Integer.toString(value);
	}


	/**
	 *  Parses decimal value from its string representation
	 *
	 * @param  valueStr  Description of Parameter
	 * @since            March 28, 2002
	 */
	public void fromString(String valueStr) {
		if (valueStr == null || valueStr.length() == 0) {
		    setNull(true);
			return;
		}
		try {
			value = Integer.parseInt(valueStr);
            setNull(this.value == Integer.MIN_VALUE);
		} catch (Exception ex) {
			throw new BadDataFormatException(getMetadata().getName() + " cannot be set to " + valueStr, valueStr);
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

    @Override
    public void toByteBuffer(ByteBuffer dataBuffer) {
        if(!isNull) {
            dataBuffer.putInt(value);
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
		buffer.putInt(value);
	}


	/**
	 *  Performs deserialization of data
	 *
	 * @param  buffer  Description of Parameter
	 * @since          April 23, 2002
	 */
	public void deserialize(ByteBuffer buffer) {
		value = buffer.getInt();
        setNull(value == Integer.MIN_VALUE);
	}


	/**
	 *  Description of the Method
	 *
	 * @param  obj  Description of Parameter
	 * @return      Description of the Returned Value
	 * @since       April 23, 2002
	 */
	public boolean equals(Object obj) {
	    if (isNull || obj==null) return false;
	    
	    if (obj instanceof IntegerDataField){
	        return value==((IntegerDataField) obj).value;
	        //return (numValue.equals((((IntegerDataField) obj).getValue())));
	    }else if (obj instanceof Numeric){
	        return value==((Numeric) obj).getInt();
	    }else{
	        return false;
	    }
	}


	/**
	 *  Compares field's internal value to value of passed-in Object
	 *
	 * @param  obj  Object representing numeric value
	 * @return      -1,0,1 if internal value(less-then,equals, greather then) passed-in value
	 */
	public int compareTo(Object obj) {
		if (obj==null) return 1;
		if (isNull) return -1;
	    
		if (obj instanceof IntegerDataField){
			return compareTo(((IntegerDataField) obj).value);
		}else if (obj instanceof Integer){
			return compareTo(((Integer)obj).intValue());
		}else if (obj instanceof Long){
		    return compareTo(((Long)obj).intValue());
		}else if (obj instanceof Double){
			return compareTo(((Double)obj).intValue());
		}else throw new ClassCastException("Can't compare IntegerDataField and "+obj.getClass().getName());
	}


	/**
	 *  Compares field's internal value to passed-in value
	 *
	 * @param  compInt  Description of the Parameter
	 * @return          -1,0,1 if internal value(less-then,equals, greather then) passed-in value
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
	
	
	/* (non-Javadoc)
	 * @see org.jetel.data.Number#compareTo(org.jetel.data.Number)
	 */
	public int compareTo(Numeric value){
	    if (isNull) {
	        return -1;
	    }else if (value.isNull()) {
	        return 1;
	    }else {
	        return compareTo(value.getInt());
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

	public int hashCode(){
		return value;
	}


	/**
	 * @see org.jetel.data.Numeric#add(org.jetel.data.Number)
	 */
	public void add(Numeric a) {
        if(isNull) return;
        if(a.isNull())
            setNull(true);
        else
            value += a.getInt();
	}


	/**
	 * @see org.jetel.data.Numeric#sub(org.jetel.data.Number)
	 */
	public void sub(Numeric a) {
        if(isNull) return;
        if(a.isNull())
            setNull(true);
        else
            value -= a.getInt();
	}


	/**
	 * @see org.jetel.data.Numeric#mul(org.jetel.data.Number)
	 */
	public void mul(Numeric a) {
        if(isNull) return;
        if(a.isNull())
            setNull(true);
        else
            value *= a.getInt();
	}


	/**
	 * @see org.jetel.data.Numeric#div(org.jetel.data.Number)
	 */
	public void div(Numeric a) {
        if(isNull) return;
        if(a.isNull())
            setNull(true);
        else
            value /= a.getInt();
	}


	/**
	 * @see org.jetel.data.Numeric#abs()
	 */
	public void abs() {
        if(isNull) return;
		value = Math.abs(value);
	}


	/**
	 * @see org.jetel.data.Numeric#mod(org.jetel.data.Number)
	 */
	public void mod(Numeric a) {
        if(isNull) return;
        if(a.isNull())
            setNull(true);
        else
            value %= a.getInt();
	}


	/**
	 * @see org.jetel.data.Numeric#neg()
	 */
	public void neg() {
        if(isNull) return;
		value *= -1;
	}

	/**
	 * @see org.jetel.data.Numeric#getDecimal()
	 */
	public Decimal getDecimal() {
		return DecimalFactory.getDecimal(value);
	}

	/**
	 * @see org.jetel.data.Numeric#getDecimal()
	 */
	public Decimal getDecimal(int precision, int scale) {
		return DecimalFactory.getDecimal(value, precision, scale);
	}

    /**
     * @see org.jetel.data.Numeric#getBigDecimal()
     */
    public BigDecimal getBigDecimal() {
        if(isNull) 
            return null;
        else 
            return BigDecimal.valueOf(value);
    }

}
/*
 *  end class IntegerDataField
 */

