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
import java.nio.ByteOrder;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;

import org.jetel.data.primitive.CloverDouble;
import org.jetel.data.primitive.Decimal;
import org.jetel.data.primitive.DecimalFactory;
import org.jetel.data.primitive.Numeric;
import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.metadata.BinaryFormat;
import org.jetel.metadata.DataFieldFormatType;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.util.bytes.CloverBuffer;
import org.jetel.util.formatter.NumericFormatter;
import org.jetel.util.formatter.NumericFormatterFactory;
import org.jetel.util.string.Compare;
import org.jetel.util.string.StringUtils;

/**
 *  A class that represents decimal number field (double precision)
 *
 *@author     D.Pavlis
 *@created    January 26, 2003
 *@since      March 27, 2002
 *@see        org.jetel.metadata.DataFieldMetadata
 */
public class NumericDataField extends DataField implements Numeric, Comparable<Object> {

	private static final long serialVersionUID = -3824088924871267023L;
	
	private double value;
	private final NumericFormatter numericFormatter;

	private final static int FIELD_SIZE_BYTES = 8;// standard size of field

	private BinaryFormat binaryFormat = null;
	
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
    public NumericDataField(DataFieldMetadata _metadata){
        this(_metadata,false);
    }
    
    
	/**
     * Constructor for the NumericDataField object
     * 
	 * @param _metadata Metadata describing field
	 * @param plain    create plain data field - no formatters,etc. will be assigned/created
	 */
	public NumericDataField(DataFieldMetadata _metadata,boolean plain) {
		super(_metadata);

		if (_metadata.isByteBased()) {
    		String typeStr = _metadata.getFormat(DataFieldFormatType.BINARY);
    		try {
				binaryFormat = BinaryFormat.valueOf(typeStr);
			} catch (IllegalArgumentException iae) {
				throw new JetelRuntimeException("Invalid binary format: " + typeStr, iae);
			}
			switch (binaryFormat) {
			case FLOAT_BIG_ENDIAN: case FLOAT_LITTLE_ENDIAN: case DOUBLE_BIG_ENDIAN: case DOUBLE_LITTLE_ENDIAN:
				if (_metadata.getSize() != binaryFormat.size) {
					throw new BadDataFormatException(String.format("The size of the field '%s' (%d bytes) does not correspond with binary format '%s' (%d bytes).", _metadata.getName(), _metadata.getSize(), binaryFormat.getFormatString(), binaryFormat.size));
				}
				break;
			default:
				throw new IllegalArgumentException("Invalid binary format: " + binaryFormat);
    		}
    	}

        if (plain || _metadata.isByteBased()) {
        	numericFormatter = NumericFormatterFactory.getPlainFormatterInstance();
        } else {
        	numericFormatter = NumericFormatterFactory.getFormatter(_metadata.getFormat(), _metadata.getLocaleStr());
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
	private NumericDataField(DataFieldMetadata _metadata, double value, NumericFormatter numericFormatter) {
		super(_metadata);
		this.value = value;
		this.numericFormatter = numericFormatter;
	}

	@Override
	public DataField duplicate() {
		NumericDataField newField = new NumericDataField(metadata, value, numericFormatter);
		newField.setNull(isNull());
		newField.binaryFormat = this.binaryFormat;
		return newField;
	}
	
	@Override
	public Numeric duplicateNumeric() {
	    return new CloverDouble(value);
	}
    
	/**
	 * @see org.jetel.data.DataField#copyField(org.jetel.data.DataField)
     * @deprecated use setValue(DataField) instead
	 */
	@Override
	public void copyFrom(DataField fromField){
	    if (fromField instanceof NumericDataField){
	        if (!fromField.isNull){
	            this.value=((NumericDataField)fromField).value;
	        }
	        setNull(fromField.isNull);
	    } else if (fromField instanceof Numeric){
            if (!fromField.isNull){
                this.value = ((Numeric) fromField).getDouble();
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
	 *@param  _value  The new Value value
	 *@since          March 28, 2002
	 */
	@Override
	public void setValue(Object _value) {
		if (_value == null) {
		    setNull(true);
			return;
		}
	    if (_value instanceof Numeric) {
	        setValue((Numeric) _value);
        } else if (_value instanceof Number) {
            setValue((Number) _value);
        } else if (_value instanceof Decimal) {
            setValue((Decimal) _value);
		} else {
			BadDataFormatException ex = new BadDataFormatException(getMetadata().getName()+" field can not be set with this object - " +_value.toString(),_value.toString());
        	ex.setFieldNumber(getMetadata().getNumber());
        	throw ex;
		}
	}

    @Override
    public void setValue(DataField fromField) {
        if (fromField instanceof NumericDataField){
            if (!fromField.isNull){
                this.value=((NumericDataField)fromField).value;
            }
            setNull(fromField.isNull);
        } else if (fromField instanceof Numeric){
            if (!fromField.isNull){
                this.value = ((Numeric) fromField).getDouble();
            }
            setNull(fromField.isNull);
        } else {
            super.setValue(fromField);
        }
    }

	/**
	 *  Sets the value of the field. If the passed in value is Double.NaN, then
	 * the value of the field is set to NULL.
	 *
	 *@param  value  The new Double value
	 *@since         August 19, 2002
	 */
	@Override
	public void setValue(double value) {
		if (Double.isNaN(value)) {
		    setNull(true);
			return;
		}
		this.value = value;
		setNull(false);
	}


	/**
	 *  Sets the value of the field. If the passed in value is Integer.MIN_VALUE, then
	 * the value of the field is set to NULL.
	 *
	 *@param  value  The new Int value
	 *@since         August 19, 2002
	 */
	@Override
	public void setValue(int value) {
		if (value == Integer.MIN_VALUE) {
		    setNull(true);
			return;
		}
		this.value = value;
		setNull(false);
	}

	/**
	 *  Sets the value of the field. If the passed in value is Long.MIN_VALUE, then
	 * the value of the field is set to NULL.
	 *
	 *@param  value  The new Int value
	 *@since         August 19, 2002
	 */
	@Override
	public void setValue(long value) {
		if (value == Long.MIN_VALUE) {
		    setNull(true);
			return;
		}
		this.value = (double) value;
		setNull(false);
	}

    /**
     * @see org.jetel.data.Numeric#setValue(org.jetel.data.Numeric)
     */
    @Override
	public void setValue(Numeric _value) {
        if (_value == null || _value.isNull()) {
            setNull(true);
            return;
        }
        this.value = _value.getDouble();
        setNull(false);
    }
    
    /**
     * @see org.jetel.data.primitive.Numeric#setValue(java.lang.Number)
     */
    @Override
	public void setValue(Number value) {
        if (value == null) {
            setNull(true);
            return;
        }
        this.value = value.doubleValue();
        setNull(Double.isNaN(this.value));
    }

	/**
	 *  Sets the Null value indicator
	 *
	 *@param  isNull  The new Null value
	 *@since          October 29, 2002
	 */
	@Override
	public void setNull(boolean isNull) {
		super.setNull(isNull);
		if (this.isNull) {
			value = Double.NaN;
		}
	}

    /**
     * @see org.jetel.data.primitive.Numeric#setNull()
     */
    @Override
	public void setNull() {
        setNull(true);
    }

    @Override
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
	 *@return    The Metadata value
	 *@since     October 31, 2002
	 */
	@Override
	public DataFieldMetadata getMetadata() {
		return super.getMetadata();
	}

	/**
	 *  Gets the Field Type
	 *
	 *@return    The Type value
	 *@since     March 28, 2002
	 */
	@Override
	public char getType() {
		return DataFieldMetadata.NUMERIC_FIELD;
	}

	/**
	 *  Gets the decimal value represented by this object (as Decimal object)
	 *
	 *@return    The Value value
	 *@since     March 28, 2002
	 */
	@Override
	public Double getValue() {
		if( isNull ) {
			return null;
		}
		return new Double(value);
	}

    /**
     * @see org.jetel.data.DataField#getValueDuplicate()
     */
    @Override
	public Double getValueDuplicate() {
        return getValue();
    }

	/**
	 *  Gets the decimal value represented by this object as double primitive
	 *
	 *@return    The Double value
	 *@since     August 19, 2002
	 */
	@Override
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
	@Override
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
	@Override
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
	@Override
	public boolean isNull() {
		return super.isNull();
	}


	/**
	 *  Formats internal decimal value into string representation
	 *
	 *@return    Description of the Returned Value
	 *@since     March 28, 2002
	 */
	@Override
	public String toString() {
		if( isNull ) {
			return metadata.getNullValue();
		}
		return numericFormatter.formatDouble(value);
	}

	/**
	 * If the binary format is set, stores the data accordingly.
	 * 
	 * Call super otherwise.
	 */
	@Override
	public int toByteBuffer(CloverBuffer dataBuffer, CharsetEncoder encoder, int maxLength) throws CharacterCodingException {
		if (binaryFormat != null) {
			switch (binaryFormat) {
			case DOUBLE_BIG_ENDIAN: 
			case DOUBLE_LITTLE_ENDIAN: 
			case FLOAT_BIG_ENDIAN: 
			case FLOAT_LITTLE_ENDIAN:
				ByteOrder originalByteOrder = dataBuffer.order();
				dataBuffer.order(binaryFormat.byteOrder); // set the field's byte order
				try {
					if (binaryFormat.size == 4) {
						dataBuffer.putFloat((float) this.value);
					} else if(binaryFormat.size == 8) {
						dataBuffer.putDouble(this.value);
					}
				} catch (BufferOverflowException boe) {
					throw new BadDataFormatException("Failed to store the data, the buffer is full", boe);
				} finally {
					dataBuffer.order(originalByteOrder); // restore the original byte order
				}
				break;
			default:
				throw new JetelRuntimeException("Invalid binary format: " + binaryFormat);
			}
			return 0;
		} else {
			return super.toByteBuffer(dataBuffer, encoder, maxLength);
		}
	}

	@Override
	public void fromString(CharSequence seq) {
		if (seq == null || Compare.equals(seq, metadata.getNullValue())) {
			setNull(true);
			return;
		}
		try {
			value = numericFormatter.parseDouble(seq);
			setNull(Double.isNaN(value));
		} catch (Exception ex) {
			throw new BadDataFormatException(String.format("field \"%s\" (%s) cannot be set to \"%s\" - try using a different number format (currently: \"%s\")",
					getMetadata().getName(), DataFieldMetadata.type2Str(getType()), seq, 
					!StringUtils.isEmpty(numericFormatter.getFormatPattern()) ? numericFormatter.getFormatPattern() : "none"));
		}
	}

	/**
	 * If the binary format is set, interpret the data accordingly.
	 * 
	 * Call super otherwise.
	 */
	@Override
	public void fromByteBuffer(CloverBuffer dataBuffer, CharsetDecoder decoder) throws CharacterCodingException {
		if (binaryFormat != null) {
			switch (binaryFormat) {
			case DOUBLE_BIG_ENDIAN: 
			case DOUBLE_LITTLE_ENDIAN: 
			case FLOAT_BIG_ENDIAN: 
			case FLOAT_LITTLE_ENDIAN:
				ByteOrder originalByteOrder = dataBuffer.order();
				dataBuffer.order(binaryFormat.byteOrder); //set the field's byte order
				try {
					if (binaryFormat.size == 4) {
						this.value = dataBuffer.getFloat();
					} else if(binaryFormat.size == 8) {
						this.value = dataBuffer.getDouble();
					}
				} catch(BufferUnderflowException bue) {
					throw new BadDataFormatException(String.format("The buffer contains less than %d bytes", binaryFormat.size), bue);
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
	 *@param  buffer  Description of Parameter
	 *@since          April 23, 2002
	 */
	@Override
	public void serialize(CloverBuffer buffer) {
		try {
			buffer.putDouble(value);
    	} catch (BufferOverflowException e) {
    		throw new RuntimeException("The size of data buffer is only " + buffer.maximumCapacity() + ". Set appropriate parameter in defaultProperties file.", e);
    	}
	}


	/**
	 *  Performs deserialization of data
	 *
	 *@param  buffer  Description of Parameter
	 *@since          April 23, 2002
	 */
	@Override
	public void deserialize(CloverBuffer buffer) {
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
	@Override
	public boolean equals(Object obj) {
	    if (isNull || obj==null) return false;
	    
	    if (obj instanceof NumericDataField){
	        return Double.doubleToLongBits(value)==Double.doubleToLongBits(((NumericDataField)obj).value);
	    }else if (obj instanceof Double){
	        return Double.doubleToLongBits(value)==Double.doubleToLongBits(((Double)obj).doubleValue());
	    }else{
	        return false;
	    }
	}


	/**
	 *  Compares field's internal value to value of passed-in Object
	 *
	 *@param  obj  Object representing numeric value
	 *@return      -1,0,1 if internal value(less-then,equals, greather then) passed-in value
	 */
	@Override
	public int compareTo(Object obj) {
		if (obj==null) return 1;
		if (isNull) return -1;
	    
		if (obj instanceof Numeric){
			return compareTo((Numeric) obj);
		}else if (obj instanceof Double){
			return compareTo(((Double)obj).doubleValue());
		}else if (obj instanceof Integer){
			return compareTo(((Integer)obj).doubleValue());
		}else if (obj instanceof Long){
			return compareTo(((Long)obj).doubleValue());
		}else throw new ClassCastException("Can't compare NumericDataField and "+obj.getClass().getName());
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

	@Override
	public int compareTo(Numeric value){
	    if (isNull) {
	        return -1;
	    } else if (value == null || value.isNull()) {
	        return 1;
	    } else {
	        return compareTo(value.getDouble());
	    }
	}
	
	@Override
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
	@Override
	public int getSizeSerialized() {
		return FIELD_SIZE_BYTES;
	}


	/**
	 * @see org.jetel.data.Numeric#add(org.jetel.data.Number)
	 */
	@Override
	public void add(Numeric a) {
        if(isNull) return;
        if(a.isNull())
            setNull(true);
        else
            value += a.getDouble();
	}


	/**
	 * @see org.jetel.data.Numeric#sub(org.jetel.data.Number)
	 */
	@Override
	public void sub(Numeric a) {
        if(isNull) return;
        if(a.isNull())
            setNull(true);
        else
            value -= a.getDouble();
	}


	/**
	 * @see org.jetel.data.Numeric#mul(org.jetel.data.Number)
	 */
	@Override
	public void mul(Numeric a) {
        if(isNull) return;
        if(a.isNull())
            setNull(true);
        else
            value *= a.getDouble();
	}


	/**
	 * @see org.jetel.data.Numeric#div(org.jetel.data.Number)
	 */
	@Override
	public void div(Numeric a) {
        if(isNull) return;
        if(a.isNull())
            setNull(true);
        else
            value /= a.getDouble();
	}


	/**
	 * @see org.jetel.data.Numeric#abs()
	 */
	@Override
	public void abs() {
        if(isNull) return;
		value = Math.abs(value);
	}


	/**
	 * @see org.jetel.data.Numeric#mod(org.jetel.data.Number)
	 */
	@Override
	public void mod(Numeric a) {
        if(isNull) return;
        if(a.isNull())
            setNull(true);
        else
            value %= a.getDouble();
	}


	/**
	 * @see org.jetel.data.Numeric#neg()
	 */
	@Override
	public void neg() {
        if(isNull) return;
		value *= -1;
	}

	/**
	 * @see org.jetel.data.Numeric#getDecimal()
	 */
	@Override
	public Decimal getDecimal() {
		return DecimalFactory.getDecimal(value);
	}

	/**
	 * @see org.jetel.data.Numeric#getDecimal()
	 */
	@Override
	public Decimal getDecimal(int precision, int scale) {
		return DecimalFactory.getDecimal(value, precision, scale);
	}

    /**
     * @see org.jetel.data.Numeric#getBigDecimal()
     */
    @Override
	public BigDecimal getBigDecimal() {
		if (isNull) {
			return null;
		}

		return BigDecimal.valueOf(value);
    }

}
