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
import java.nio.BufferUnderflowException;
import java.nio.ByteOrder;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;

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


/**
 *  A class that represents decimal number field (precision, scale pair)
 *
 *@author     Martin Zatopek
 *@since      November 30, 2005
 *@see        org.jetel.metadata.DataFieldMetadata
 */
public class DecimalDataField extends DataField implements Numeric, Comparable<Object> {

	private static final long serialVersionUID = -9212721402316376203L;
	
	private Decimal value;
	private int precision;
	private int scale;
	private final NumericFormatter numericFormatter;

	private BinaryFormat binaryFormat = null;
	private Integer minLength = null;
	    
    /**
     * Constructor
     * 
     * @param _metadata Metadata describing field
     * @param precision integer unscaled value - maximum digits this field can contain
     * @param scale number of decimal places
     */
    public DecimalDataField(DataFieldMetadata _metadata, int precision, int scale){
        this(_metadata,precision,scale,false);
    }
    
	/**
     * Constructor
     * 
	 * @param _metadata Metadata describing field
	 * @param precision integer unscaled value - maximum digits this field can contain
	 * @param scale number of decimal places
	 * @param plain create plain data field - no formatters,etc. will be assigned/created
	 */
	public DecimalDataField(DataFieldMetadata _metadata, int precision, int scale, boolean plain) {
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
			case DOUBLE_BIG_ENDIAN:
			case DOUBLE_LITTLE_ENDIAN:
			case FLOAT_BIG_ENDIAN:
			case FLOAT_LITTLE_ENDIAN:
				break;
			default:
				throw new IllegalArgumentException("Invalid binary format: " + binaryFormat);
    		}
    	}

		if (plain || _metadata.isByteBased()) {
        	numericFormatter = NumericFormatterFactory.getPlainFormatterInstance();
        } else {
        	numericFormatter = NumericFormatterFactory.getDecimalFormatter(_metadata.getFormat(), _metadata.getLocaleStr());
        } 
        //instantiate Decimal interface
        this.precision = precision;
        this.scale = scale;
        value = DecimalFactory.getDecimal(precision, scale);
    }


	
	/**
	 * @param _metadata Metadata describing field
	 * @param value decimal value to be assigned to the field
	 * @param precision integer unscaled value - maximum digits this field can contain
	 * @param scale number of decimal places
	 */
	public DecimalDataField(DataFieldMetadata _metadata, Decimal value, int precision, int scale) {
		this(_metadata, precision, scale);
		this.value.setValue(value);
	}


	/**
	 * Private constructor to be used internally when clonning object.
	 * Optimized for performance. Many checks waved.
	 * @param _metadata
	 * @param value
	 * @param numberFormat
	 */
	private DecimalDataField(DataFieldMetadata _metadata, Decimal value, NumericFormatter numericFormatter, int precision, int scale) {
	    super(_metadata);
	    this.value = value.createCopy();
	    this.numericFormatter = numericFormatter;
	    this.precision = precision;
	    this.scale = scale;
	 }
	
	
	/**
	 * @see org.jetel.data.DataField#copy()
	 */
	@Override
	public DataField duplicate() {
	    DecimalDataField newField = new DecimalDataField(metadata, value, numericFormatter, precision, scale);
	    newField.setNull(isNull());
		newField.binaryFormat = this.binaryFormat;
		newField.minLength = this.minLength;
	    return newField;
	}
	
    @Override
	public Numeric duplicateNumeric() {
        return value.duplicateNumeric();
    }
    
	/**
	 * @see org.jetel.data.DataField#copyField(org.jetel.data.DataField)
     * @deprecated use setValue(DataField) instead
	 */
	@Override
	public void copyFrom(DataField fromField){
	    if (fromField instanceof DecimalDataField){
	        if (!fromField.isNull) {
	            this.value.setValue(((DecimalDataField) fromField).value);
	        }
	        setNull(fromField.isNull);
        } else if (fromField instanceof Numeric){
            if (!fromField.isNull) {
                this.value.setValue(((Numeric) fromField).getDecimal());
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
        	BadDataFormatException ex = new BadDataFormatException(getMetadata().getName() + " field can not be set with this object - " + _value.toString(), _value.toString());
        	ex.setFieldNumber(getMetadata().getNumber());
        	throw ex;
		}
	}

	@Override
	public void setValue(DataField fromField) {
        if (fromField instanceof DecimalDataField){
            if (!fromField.isNull) {
                this.value.setValue(((DecimalDataField) fromField).value);
            }
            setNull(fromField.isNull);
        } else if (fromField instanceof Numeric){
            if (!fromField.isNull) {
                this.value.setValue(((Numeric) fromField).getDecimal());
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
	public void setValue(double _value) {
		if (Double.isNaN(_value)) {
		    setNull(true);
			return;
		}
		value.setValue(_value);
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
	public void setValue(int _value) {
		if (_value == Integer.MIN_VALUE) {
		    setNull(true);
			return;
		}
		value.setValue(_value);
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
	public void setValue(long _value) {
		if (_value == Long.MIN_VALUE) {
		    setNull(true);
			return;
		}
		value.setValue(_value);
		setNull(false);
	}

    @Override
	public void setValue(Numeric _value) {
        if (_value == null || _value.isNull()) {
            setNull(true);
            return;
        }
        value.setValue(_value);
        setNull(false);
    }

    @Override
	public void setValue(Number value) {
        if (value == null) {
            setNull(true);
            return;
        }
        this.value.setValue(value);
        setNull(this.value.isNaN());
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
			value.setNaN(true);
		}
	}

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
            value.setValue(0);
        }
    }

	/**
	 *  Gets the Field Type
	 *
	 *@return    The Type value
	 *@since     March 28, 2002
	 */
	@Override
	public char getType() {
		return DataFieldMetadata.DECIMAL_FIELD;
	}


	/**
	 *  Gets the decimal value represented by this object (as Decimal object)
	 *
	 *@return    The Value value
	 *@since     March 28, 2002
	 */
	@Override
	public Decimal getValue() {
		if(isNull) {
			return null;
		}
		return value;
	}

    /**
     * @see org.jetel.data.DataField#getValueDuplicate()
     */
    @Override
	public Decimal getValueDuplicate() {
        if(isNull) {
            return null;
        }
        return value.createCopy();
    }

	/**
	 *  Gets the decimal value represented by this object as double primitive
	 *
	 *@return    The Double value
	 *@since     August 19, 2002
	 */
	@Override
	public double getDouble() {
		if (isNull) {
		    return Double.NaN;
		}
	    return value.getDouble();
	}


	/**
	 *  Gets the numeric value represented by this object casted to int primitive
	 *
	 *@return    The Int value
	 *@since     August 19, 2002
	 */
	@Override
	public int getInt() {
		if(isNull) {
			return Integer.MIN_VALUE;
		}
		return value.getInt();
	}

	/**
	 *  Gets the numeric value represented by this object casted to long primitive
	 *
	 *@return    The Int value
	 *@since     August 19, 2002
	 */
	@Override
	public long getLong() {
		if(isNull) {
			return Long.MIN_VALUE;
		}
		return value.getLong();
	}

	/**
	 *  Formats internal decimal value into string representation
	 *
	 *@return    Description of the Returned Value
	 *@since     March 28, 2002
	 */
	@Override
	public String toString() {
		if(isNull) {
			return metadata.getNullValue();
		}
		return value.toString(numericFormatter);
	}

	/**
	 * If the binary format is set, stores the data accordingly.
	 * 
	 * Call super otherwise.
	 */
	@Override
	public int toByteBuffer(CloverBuffer dataBuffer, CharsetEncoder encoder, int maxLength) throws CharacterCodingException {
		if (binaryFormat != null) {
			BigInteger unscaledValue = this.getBigDecimal().unscaledValue();
			switch (binaryFormat) {
			case BIG_ENDIAN: case LITTLE_ENDIAN:
				ByteBufferUtils.encodeValue(dataBuffer, unscaledValue, binaryFormat.byteOrder, minLength);
				break;
			case PACKED_DECIMAL:
				PackedDecimal.putPackedDecimal(dataBuffer, unscaledValue, minLength);
				break;
			case DOUBLE_BIG_ENDIAN: 
			case DOUBLE_LITTLE_ENDIAN: 
			case FLOAT_BIG_ENDIAN: 
			case FLOAT_LITTLE_ENDIAN:
				ByteOrder originalByteOrder = dataBuffer.order();
				dataBuffer.order(binaryFormat.byteOrder); // set the field's byte order
				try {
					if (binaryFormat.size == 4) {
						dataBuffer.putFloat(getBigDecimal().floatValue());
					} else if(binaryFormat.size == 8) {
						dataBuffer.putDouble(getBigDecimal().doubleValue());
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
			value.fromString(seq, numericFormatter);
			setNull(value.isNaN());
		} catch (Decimal.OutOfPrecisionException outOfPrecision) {
			throw new BadDataFormatException(String.format("%s (%s) cannot be set to \"%s\": %s", getMetadata().getName(), DataFieldMetadata.type2Str(getType()), seq, outOfPrecision.getMessage()));
		} catch (Exception ex) {
			throw new BadDataFormatException(
					String.format("%s (%s) cannot be set to \"%s\" - doesn't match defined format \"%s\"",
							getMetadata().getName(), DataFieldMetadata.type2Str(getType()), seq, numericFormatter.getFormatPattern()), seq.toString());
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
			case BIG_ENDIAN: case LITTLE_ENDIAN:
				setValue(new BigDecimal(ByteBufferUtils.decodeValue(dataBuffer, binaryFormat.byteOrder), scale));
				break;
			case PACKED_DECIMAL:
				setValue(new BigDecimal(PackedDecimal.parseBigInteger(dataBuffer), scale));
				break;
			case DOUBLE_BIG_ENDIAN: 
			case DOUBLE_LITTLE_ENDIAN: 
			case FLOAT_BIG_ENDIAN: 
			case FLOAT_LITTLE_ENDIAN:
				ByteOrder originalByteOrder = dataBuffer.order();
				dataBuffer.order(binaryFormat.byteOrder); //set the field's byte order
				try {
					if (binaryFormat.size == 4) {
						setValue(dataBuffer.getFloat());
					} else if(binaryFormat.size == 8) {
						setValue(dataBuffer.getDouble());
					}
				} catch (BufferUnderflowException bue) {
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
			value.serialize(buffer);
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
		value.deserialize(buffer);
        setNull(value.isNaN());
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
	    if (isNull || obj == null) return false;
	    
	    if (obj instanceof DecimalDataField){
	        return value.equals(((DecimalDataField) obj).value);
	    } else if (obj instanceof Numeric){
	        return value.equals((Numeric) obj);
	    } else {
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
		if (obj == null) return 1;
		if (isNull) return -1;
	    
		return value.compareTo(obj);
	}

	@Override
	public int compareTo(Numeric _value){
	    if (isNull) {
	        return -1;
	    } else if (_value == null || _value.isNull()) {
	        return 1;
	    } else {
	        return value.compareTo(_value);
	    }
	}

	@Override
	public int hashCode(){
        return value.hashCode();
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
		return value.getSizeSerialized();
	}

	@Override
	public void add(Numeric a) {
        if(isNull) return;
        if(a.isNull())
            setNull(true);
        else
            value.add(a);
	}

	@Override
	public void sub(Numeric a) {
        if(isNull) return;
        if(a.isNull())
            setNull(true);
        else
            value.sub(a);
	}

	@Override
	public void mul(Numeric a) {
        if(isNull) return;
        if(a.isNull())
            setNull(true);
        else
            value.mul(a);
	}

	@Override
	public void div(Numeric a) {
        if(isNull) return;
        if(a.isNull())
            setNull(true);
        else
            value.div(a);
	}

	@Override
	public void abs() {
        if(isNull) return;
		value.abs();
	}

	@Override
	public void mod(Numeric a) {
        if(isNull) return;
        if(a.isNull())
            setNull(true);
        else
            value.mod(a);
	}

	@Override
	public void neg() {
        if(isNull) return;
		value.neg();
	}

	@Override
	public Decimal getDecimal() {
		return value;
	}

	@Override
	public Decimal getDecimal(int precision, int scale) {
        if(precision == this.precision && scale == this.scale) {
            return value;
        }
        return DecimalFactory.getDecimal(value, precision, scale);
	}

	@Override
	public BigDecimal getBigDecimal() {
		if (isNull) {
			return null;
		}

		return value.getBigDecimal();
	}

}
