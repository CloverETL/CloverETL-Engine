/*
 * jETeL/CloverETL - Java based ETL application framework.
 * Copyright (c) Javlin, a.s. (info@cloveretl.com)
 *  
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */
package org.jetel.data;

import java.math.BigDecimal;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;

import org.jetel.data.primitive.CloverInteger;
import org.jetel.data.primitive.Decimal;
import org.jetel.data.primitive.DecimalFactory;
import org.jetel.data.primitive.Numeric;
import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.metadata.BinaryFormat;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.util.formatter.NumericFormatter;
import org.jetel.util.formatter.NumericFormatterFactory;
import org.jetel.util.string.Compare;
import org.jetel.util.string.StringUtils;

/**
 *  A class that represents integer number field (32bit signed)
 *
 * @author      D.Pavlis
 * @since       March 27, 2002
 * @revision    $Revision$
 * @created     January 26, 2003
 * @see         org.jetel.metadata.DataFieldMetadata
 */
public class IntegerDataField extends DataField implements Numeric, Comparable<Object> {

	private static final long serialVersionUID = 3959941332738498948L;
	
	private int value;
	private final NumericFormatter numericFormatter;
	private final static int FIELD_SIZE_BYTES = 4;// standard size of field

	private BinaryFormat binaryFormat = null;

	/**
	 *  Constructor for the NumericDataField object
	 *
	 * @param  _metadata  Metadata describing field
	 * @since             March 28, 2002
	 */
	public IntegerDataField(DataFieldMetadata _metadata) {
		this(_metadata, false);
	}

    
    /**
     * @param _metadata Metadata describing field
     * @param plain create plain data field - no formatters,etc. will be assigned/created
     */
    public IntegerDataField(DataFieldMetadata _metadata, boolean plain) {
        super(_metadata);
        
    	if(_metadata.isByteBased()) {
    		String typeStr = _metadata.getBinaryFormatParams();
    		try {
				binaryFormat = BinaryFormat.valueOf(typeStr);
			} catch (IllegalArgumentException iae) {
				throw new JetelRuntimeException("Invalid binary format: " + typeStr, iae);
			}
    		switch(binaryFormat) {
			case BIG_ENDIAN: case LITTLE_ENDIAN:
				if (metadata.getSize() < FIELD_SIZE_BYTES) {
					throw new BadDataFormatException(String.format("The size of the field is less than %d bytes", FIELD_SIZE_BYTES));
				} else if (metadata.getSize() > FIELD_SIZE_BYTES) {
					throw new BadDataFormatException(String.format("The size of the field is more than %d bytes", FIELD_SIZE_BYTES));
				}
				break;
			case PACKED_DECIMAL:
				break;
			default:
				throw new IllegalArgumentException("Invalid binary format: " + binaryFormat);
    		}
    	}

    	if (plain || _metadata.isByteBased()) {
        	numericFormatter = NumericFormatterFactory.getPlainFormatterInstance();
        } else {
        	numericFormatter = NumericFormatterFactory.getFormatter(_metadata.getFormatStr(), _metadata.getLocaleStr());
        }
        
    }

	/**
	 *  Constructor for the NumericDataField object
	 *
	 * @param  _metadata  Metadata describing field
	 * @param  value      Value to assign to field
	 * @since             March 28, 2002
	 */
	public IntegerDataField(DataFieldMetadata _metadata, int value) {
		this(_metadata);
		setValue(value);
	}
	
	/**
	 * Private constructor to be used internally when clonning object.
	 * Optimized for performance. Many checks waved.
	 * @param _metadata
	 * @param value
	 * @param numberFormat
	 */
	private IntegerDataField(DataFieldMetadata _metadata, int value, NumericFormatter numericFormatter){
	    super(_metadata);
	    this.value = value;
	    this.numericFormatter = numericFormatter;
	 }

	public DataField duplicate(){
	    IntegerDataField newField = new IntegerDataField(metadata, value, numericFormatter);
	    newField.setNull(isNull());
	    newField.binaryFormat = this.binaryFormat;
	    return newField;
	}

	/**
	 * @see org.jetel.data.Numeric#duplicateNumeric()
	 */
	public Numeric duplicateNumeric() {
	    return new CloverInteger(value);
	}
    
	/**
	 * @see org.jetel.data.DataField#copyField(org.jetel.data.DataField)
     * @deprecated use setValue(DataField) instead
	 */
	public void copyFrom(DataField fromField){
	    if (fromField instanceof IntegerDataField){
	        if (!fromField.isNull){
	            this.value = ((IntegerDataField) fromField).value;
	        }
	        setNull(fromField.isNull);
	    } else if(fromField instanceof Numeric) {
            if (!fromField.isNull){
                this.value = ((Numeric) fromField).getInt();
            }
            setNull(fromField.isNull);
        } else {
            super.copyFrom(fromField);
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
            setValue((Number) _value);
        } else if (_value instanceof Decimal) {
            setValue((Decimal) _value);
		} else {
		    throw new BadDataFormatException(getMetadata().getName() + " field can not be set with this object - " + _value.toString(), _value.toString());
		}
	}

    @Override
    public void setValue(DataField fromField) {
        if (fromField instanceof IntegerDataField){
            if (!fromField.isNull){
                this.value = ((IntegerDataField) fromField).value;
            }
            setNull(fromField.isNull);
        } else if(fromField instanceof Numeric) {
            if (!fromField.isNull){
                this.value = ((Numeric) fromField).getInt();
            }
            setNull(fromField.isNull);
        } else {
            super.setValue(fromField);
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
		if (Double.isNaN(value)) {
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
	
    public void setValue(Numeric value) {
        if(value == null || value.isNull()) {
            setNull(true);
            return;
        }
        this.value = value.getInt();
        setNull(false);
    }
	
    public void setValue(Number value) {
        if (value == null) {
            setNull(true);
            return;
        }
        this.value = value.intValue();
        setNull(this.value == Integer.MIN_VALUE);
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

    public void setNull() {
        setNull(true);
    }
    
    
    public void reset(){
        if (metadata.isNullable()){
            setNull(true);
        }else if (metadata.isDefaultValueSet()){
            setToDefaultValue();
        }else{
            value=0;
        }
    }

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
		return Integer.valueOf(value);
	}

    /**
     * @see org.jetel.data.DataField#getValueDuplicate()
     */
    public Object getValueDuplicate() {
        return getValue();
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
			return metadata.getNullValue();
		} else {
			return numericFormatter.formatInt(value);
		}
	}
	
	/**
	 * If the binary format is set, store the data accordingly.
	 * 
	 * Call super otherwise.
	 */
	@Override
	public void toByteBuffer(ByteBuffer dataBuffer, CharsetEncoder encoder) throws CharacterCodingException {
		if(binaryFormat != null) {
			switch (binaryFormat) {
			case PACKED_DECIMAL:
				putPackedDecimal(dataBuffer, this.value);
				break;
			case BIG_ENDIAN: case LITTLE_ENDIAN:
				ByteOrder originalByteOrder = dataBuffer.order();
				dataBuffer.order(binaryFormat.byteOrder); // set the field's byte order
				try {
					dataBuffer.putInt(this.value);
				} catch (BufferOverflowException boe) {
					throw new BadDataFormatException("Failed to store the data, the buffer is full", boe);
				} finally {
					dataBuffer.order(originalByteOrder); // restore the original byte order
				}
				break;
			default:
				throw new JetelRuntimeException("Invalid binary format: " + binaryFormat);
			}
		} else {
			super.toByteBuffer(dataBuffer, encoder);
		}
	}

	public void fromString(CharSequence seq) {
		if (seq == null || Compare.equals(seq, metadata.getNullValue())) {
			setNull(true);
			return;
		}
		try {
			value = numericFormatter.parseInt(seq);
			setNull(this.value == Integer.MIN_VALUE);
		} catch (Exception ex) {
			throw new BadDataFormatException(String.format("%s (%s) cannot be set to \"%s\" - doesn't match defined format \"%s\"" 
					+ (!StringUtils.isEmpty(ex.getMessage()) ? " with reason \"%s\"" : ""),
					getMetadata().getName(), DataFieldMetadata.type2Str(getType()), seq, numericFormatter.getFormatPattern(), ex.getMessage()),
					(new StringBuilder(seq)).toString());
		}
	}
	
	/**
	 * If the binary format is set, interpret the data accordingly.
	 * 
	 * Call super otherwise.
	 */
	@Override
	public void fromByteBuffer(ByteBuffer dataBuffer, CharsetDecoder decoder) throws CharacterCodingException {
		if(binaryFormat != null) {
			switch(binaryFormat) {
			case PACKED_DECIMAL:
				long tmpValue = getPackedDecimal(dataBuffer);
				if(tmpValue < Integer.MIN_VALUE || tmpValue > Integer.MAX_VALUE) {
					throw new BadDataFormatException("The packed decimal value does not fit into Integer range");
				} else {
					this.value = (int) tmpValue;
				}
				break;
			case BIG_ENDIAN: case LITTLE_ENDIAN:
				ByteOrder originalByteOrder = dataBuffer.order();
				dataBuffer.order(binaryFormat.byteOrder); //set the field's byte order
				try {
					this.value = dataBuffer.getInt();
					if(dataBuffer.hasRemaining()) {
						throw new BadDataFormatException(String.format("The buffer contains more than %d bytes", FIELD_SIZE_BYTES));
					}
				} catch(BufferUnderflowException bue) {
					throw new BadDataFormatException(String.format("The buffer contains less than %d bytes", FIELD_SIZE_BYTES), bue);
				} finally {
					dataBuffer.order(originalByteOrder); // restore the original byte order
				}
				break;
			default: 
				throw new JetelRuntimeException("Invalid binary format: " + binaryFormat);
			}
		} else {
			super.fromByteBuffer(dataBuffer, decoder);
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
		try {
			buffer.putInt(value);
    	} catch (BufferOverflowException e) {
    		throw new RuntimeException("The size of data buffer is only " + buffer.limit() + ". Set appropriate parameter in defautProperties file.", e);
    	}
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
	    
		if (obj instanceof Numeric){
			return compareTo((Numeric) obj);
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

	public void add(Numeric a) {
        if(isNull) return;
        if(a.isNull())
            setNull(true);
        else
            value += a.getInt();
	}

	public void sub(Numeric a) {
        if(isNull) return;
        if(a.isNull())
            setNull(true);
        else
            value -= a.getInt();
	}

	public void mul(Numeric a) {
        if(isNull) return;
        if(a.isNull())
            setNull(true);
        else
            value *= a.getInt();
	}

	public void div(Numeric a) {
        if(isNull) return;
        if(a.isNull())
            setNull(true);
        else
            value /= a.getInt();
	}

	public void abs() {
        if(isNull) return;
		value = Math.abs(value);
	}

	public void mod(Numeric a) {
        if(isNull) return;
        if(a.isNull())
            setNull(true);
        else
            value %= a.getInt();
	}

	public void neg() {
        if(isNull) return;
		value *= -1;
	}

	public Decimal getDecimal() {
		return DecimalFactory.getDecimal(value);
	}

	public Decimal getDecimal(int precision, int scale) {
		return DecimalFactory.getDecimal(value, precision, scale);
	}

    public BigDecimal getBigDecimal() {
		if (isNull) {
			return null;
		}

		return BigDecimal.valueOf(value);
    }

}
