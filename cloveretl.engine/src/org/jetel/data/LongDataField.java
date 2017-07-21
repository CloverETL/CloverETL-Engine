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
import java.math.BigInteger;
import java.nio.BufferOverflowException;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;

import org.jetel.data.primitive.CloverLong;
import org.jetel.data.primitive.Decimal;
import org.jetel.data.primitive.DecimalFactory;
import org.jetel.data.primitive.Numeric;
import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.metadata.BinaryFormat;
import org.jetel.metadata.DataFieldFormatType;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.util.bytes.ByteBufferUtils;
import org.jetel.util.bytes.CloverBuffer;
import org.jetel.util.bytes.PackedDecimal;
import org.jetel.util.formatter.NumericFormatter;
import org.jetel.util.formatter.NumericFormatterFactory;
import org.jetel.util.string.Compare;
import org.jetel.util.string.StringUtils;

/**
 *  A class that represents integer number field (32bit signed)
 *
 * @author      D.Pavlis
 * @since       March 27, 2002
 * @created     January 26, 2003
 * @see         org.jetel.metadata.DataFieldMetadata
 */
public class LongDataField extends DataField implements Numeric, Comparable<Object> {

	private static final long serialVersionUID = 7919485553866177802L;
	
	private long value;
	private final NumericFormatter numericFormatter;
	
	private final static int FIELD_SIZE_BYTES = 8; // standard size of field
	
	private BinaryFormat binaryFormat = null;
	private Integer minLength = null;

	/**
	 *  Constructor for the NumericDataField object
	 *
	 * @param  _metadata  Metadata describing field
	 * @since             March 28, 2002
	 */
	public LongDataField(DataFieldMetadata _metadata) {
		this(_metadata, false);
	}

    /**
     * Constructor for the NumericDataField object
     * 
     * @param _metadata Metadata describing field
     * @param plain create plain data field - no formatters,etc. will be assigned/created
     */
    public LongDataField(DataFieldMetadata _metadata, boolean plain) {
        super(_metadata);
        
    	if (_metadata.isByteBased()) {
    		String typeStr = _metadata.getFormat(DataFieldFormatType.BINARY);
    		try {
				binaryFormat = BinaryFormat.valueOf(typeStr);
			} catch (IllegalArgumentException iae) {
				throw new JetelRuntimeException("Invalid binary format: " + typeStr, iae);
			}
    		switch (binaryFormat) {
			case BIG_ENDIAN: case LITTLE_ENDIAN: case PACKED_DECIMAL:
				if (_metadata.isFixed()) {
					this.minLength = (int) _metadata.getSize();
				} else {
					this.minLength = 0;
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
	 * @param  _metadata  Metadata describing field
	 * @param  value      Value to assign to field
	 * @since             March 28, 2002
	 */
	public LongDataField(DataFieldMetadata _metadata, long value) {
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
	private LongDataField(DataFieldMetadata _metadata, long value, NumericFormatter numericFormatter){
	    super(_metadata);
	    this.value = value;
	    this.numericFormatter = numericFormatter;
	 }

	@Override
	public DataField duplicate(){
	    LongDataField newField = new LongDataField(metadata, value, numericFormatter);
	    newField.setNull(isNull());
	    newField.binaryFormat = this.binaryFormat;
	    return newField;
	}

    /**
     * @see org.jetel.data.Numeric#duplicateNumeric()
     */
    @Override
	public Numeric duplicateNumeric() {
        return new CloverLong(value);
    }
    
	/**
	 * @see org.jetel.data.DataField#copyField(org.jetel.data.DataField)
     * @deprecated use setValue(DataField) instead
	 */
	@Override
	public void copyFrom(DataField fromField){
	    if (fromField instanceof LongDataField){
	        if (!fromField.isNull){
	            this.value = ((LongDataField) fromField).value;
	        }
	        setNull(fromField.isNull);
	    } else if (fromField instanceof Numeric){
            if (!fromField.isNull){
                this.value = ((Numeric) fromField).getLong();
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
			BadDataFormatException ex = new BadDataFormatException(getMetadata().getName() + " field can not be set with this object - " + _value.toString(), _value.toString());
        	ex.setFieldNumber(getMetadata().getNumber());
        	throw ex;
		}
	}

	@Override
	public void setValue(DataField fromField) {
        if (fromField instanceof LongDataField){
            if (!fromField.isNull){
                this.value = ((LongDataField) fromField).value;
            }
            setNull(fromField.isNull);
        } else if (fromField instanceof Numeric){
            if (!fromField.isNull){
                this.value = ((Numeric) fromField).getLong();
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
	 * @param  value  The new Double value
	 * @since         August 19, 2002
	 */
	@Override
	public void setValue(double value) {
		if (Double.isNaN(value)) {
		    setNull(true);
			return;
		}
		this.value = (long) value;
		setNull(false);
	}


	/**
	 *  Sets the value of the field. If the passed in value is Integer.MIN_VALUE, then
	 * the value of the field is set to NULL.
	 *
	 * @param  value  The new Int value
	 * @since         August 19, 2002
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
	 *  Sets the value of the field.If the passed in value is Long.MIN_VALUE, then
	 * the value of the field is set to NULL.
	 *
	 * @param  value  The new Int value
	 * @since         August 19, 2002
	 */
	@Override
	public void setValue(long value) {
		if (value == Long.MIN_VALUE) {
		    setNull(true);
		    return;
		}
		this.value = value;
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
        this.value = _value.getLong();
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
        this.value = value.longValue();
        setNull(this.value == Long.MIN_VALUE);
    }

	/**
	 *  Sets the Null value indicator
	 *
	 * @param  isNull  The new Null value
	 * @since          October 29, 2002
	 */
	@Override
	public void setNull(boolean isNull) {
		super.setNull(isNull);
		if (this.isNull) {
			this.value = Long.MIN_VALUE;
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
	 * @return    The Metadata value
	 * @since     October 31, 2002
	 */
	@Override
	public DataFieldMetadata getMetadata() {
		return super.getMetadata();
	}


	/**
	 *  Gets the Field Type
	 *
	 * @return    The Type value
	 * @since     March 28, 2002
	 */
	@Override
	@Deprecated
	public char getType() {
		return DataFieldMetadata.LONG_FIELD;
	}


	/**
	 *  Gets the decimal value represented by this object (as Decimal object)
	 *
	 * @return    The Value value
	 * @since     March 28, 2002
	 */
	@Override
	public Long getValue() {
		if (isNull) {
			return null;
		}
		return Long.valueOf(value);
	}

    /**
     * @see org.jetel.data.DataField#getValueDuplicate()
     */
    @Override
	public Long getValueDuplicate() {
        return getValue();
    }

	/**
	 *  Gets the decimal value represented by this object as double primitive
	 *
	 * @return    The Double value
	 * @since     August 19, 2002
	 */
	@Override
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
	@Override
	public int getInt() {
	    if (isNull){
	        return Integer.MIN_VALUE;
	    }
		return (int)value;
	}

	/**
	 * Gets the numeric value represented by this object as long primitive
	 * @return the long value of this object
	 */
	@Override
	public long getLong(){
	    if (isNull){
	        return Long.MIN_VALUE;
	    }
	    return this.value;
	}
	

	/**
	 *  Formats internal decimal value into string representation
	 *
	 * @return    Description of the Returned Value
	 * @since     March 28, 2002
	 */
	@Override
	public String toString() {
		if (isNull) {
			return metadata.getNullValue();
		} else {
			return numericFormatter.formatLong(value);
		}
	}
	
	/**
	 * If the binary format is set, store the data accordingly.
	 * 
	 * Call super otherwise.
	 */
	@Override
	public int toByteBuffer(CloverBuffer dataBuffer, CharsetEncoder encoder, int maxLength) throws CharacterCodingException {
		if (binaryFormat != null) {
			switch (binaryFormat) {
			case BIG_ENDIAN: case LITTLE_ENDIAN:
				ByteBufferUtils.encodeValue(dataBuffer, this.value, binaryFormat.byteOrder, minLength);
				break;
			case PACKED_DECIMAL:
				PackedDecimal.putPackedDecimal(dataBuffer, this.value, minLength);
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
			value = numericFormatter.parseLong(seq);
			setNull(this.value == Long.MIN_VALUE);
		} catch (Exception ex) {
			BadDataFormatException e = new BadDataFormatException(String.format("Field %s(%s) cannot be set to " +
					"value \"%s\"; doesn't match the specified format \"%s\"" + 
					(!StringUtils.isEmpty(ex.getMessage()) ? " with reason \"%s\";" : ""), getMetadata().getName(), 
					getMetadata().getDataType().getName(), seq, numericFormatter.getFormatPattern(), ex.getMessage()),
					(new StringBuilder(seq)).toString(), ex);
			e.setAdditionalMessage("(note that for ParallelReader or Server parallel transformation run the record " +
					"number might be incorrect)");
			throw e;
		}
	}
	
	private static final BigInteger LONG_MIN_VALUE = BigInteger.valueOf(Long.MIN_VALUE);
	private static final BigInteger LONG_MAX_VALUE = BigInteger.valueOf(Long.MAX_VALUE);
	
	/**
	 * If the binary format is set, interpret the data accordingly.
	 * 
	 * Call super otherwise.
	 */
	@Override
	public void fromByteBuffer(CloverBuffer dataBuffer, CharsetDecoder decoder) throws CharacterCodingException {
		if (binaryFormat != null) {
			BigInteger tmpValue;
			
			switch (binaryFormat) {
			case BIG_ENDIAN: case LITTLE_ENDIAN:
				tmpValue = ByteBufferUtils.decodeValue(dataBuffer, binaryFormat.byteOrder);
				break;
			case PACKED_DECIMAL:
				tmpValue = PackedDecimal.parseBigInteger(dataBuffer);
				break;
			default: 
				throw new JetelRuntimeException("Invalid binary format: " + binaryFormat);
			}
			
			if ((tmpValue.compareTo(LONG_MIN_VALUE) < 0)
					|| (tmpValue.compareTo(LONG_MAX_VALUE) > 0)) {
				throw new BadDataFormatException("The packed decimal value does not fit into Long range");
			} else {
				this.value = tmpValue.longValue();
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
	@Override
	public void serialize(CloverBuffer buffer) {
		try {
			buffer.putLong(value);
		} catch (BufferOverflowException e) {
			throw new RuntimeException("The size of data buffer is only " + buffer.maximumCapacity() + ". Set appropriate parameter in defaultProperties file.", e);
		}
	}


	/**
	 *  Performs deserialization of data
	 *
	 * @param  buffer  Description of Parameter
	 * @since          April 23, 2002
	 */
	@Override
	public void deserialize(CloverBuffer buffer) {
		this.value = buffer.getLong();
		if (value == Long.MIN_VALUE) {
			setNull(true);
		} else {
			setNull(false);
		}
	}

	@Override
	public boolean equals(Object obj) {
	    if (isNull || obj==null) return false;
	    if (obj instanceof LongDataField){
	        return value==((LongDataField)obj).value;
	    }else if (obj instanceof Numeric){
	        return value==((Numeric)obj).getLong();
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
	@Override
	public int compareTo(Object obj) {
		if (obj==null) return 1;
		if (isNull) return -1;
		
		if (obj instanceof Numeric){
			return compareTo((Numeric) obj);
		}else if (obj instanceof Integer){
			return compareTo(((Integer)obj).longValue());
		}else if (obj instanceof Long){
			return compareTo(((Long)obj).longValue());
		}else if (obj instanceof Double){
			return compareTo(((Double)obj).longValue());
		}else throw new ClassCastException("Can't compare LongDataField and "+obj.getClass().getName());
	}


	/**
	 * Compares field's internal value to passed-in value
	 *
	 * @param  compInt  long value against which to compare
	 * @return           -1,0,1 if internal value(less-then,equals, greather then) passed-in value
	 */
	public int compareTo(long compInt) {
		if (value > compInt) {
			return 1;
		} else if (value < compInt) {
			return -1;
		} else {
			return 0;
		}
	}
	
	@Override
	public int compareTo(Numeric value) {
	    if (isNull) {
	        return -1;
	    }else if (value == null || value.isNull()) {
	        return 1;
	    }else {
	        return compareTo(value.getLong());
	    }
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

	@Override
	public int hashCode(){
		return (int)(value^value>>32);
	}

	@Override
	public void add(Numeric a) {
        if(isNull) return;
        if(a.isNull())
            setNull(true);
        else
            value += a.getLong();
	}

	@Override
	public void sub(Numeric a) {
        if(isNull) return;
        if(a.isNull())
            setNull(true);
        else
            value -= a.getLong();
	}

	@Override
	public void mul(Numeric a) {
        if(isNull) return;
        if(a.isNull())
            setNull(true);
        else
            value *= a.getLong();
	}

	@Override
	public void div(Numeric a) {
        if(isNull) return;
        if(a.isNull())
            setNull(true);
        else
            value /= a.getLong();
	}

	@Override
	public void abs() {
        if(isNull) return;
		value = Math.abs(value);
	}

	@Override
	public void mod(Numeric a) {
        if(isNull) return;
        if(a.isNull())
            setNull(true);
        else
            value %= a.getLong();
	}

	@Override
	public void neg() {
        if(isNull) return;
		value *= -1;
	}
	
	@Override
	public Decimal getDecimal() {
		return DecimalFactory.getDecimal(value);
	}

	@Override
	public Decimal getDecimal(int precision, int scale) {
		return DecimalFactory.getDecimal(value, precision, scale);
	}

    @Override
	public BigDecimal getBigDecimal() {
		if (isNull) {
			return null;
		}

		return BigDecimal.valueOf(value);
    }

}
