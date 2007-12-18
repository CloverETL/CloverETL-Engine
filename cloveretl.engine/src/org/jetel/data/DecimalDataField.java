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
import java.math.BigDecimal;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.text.DecimalFormatSymbols;
import java.util.Locale;

import org.jetel.data.primitive.Decimal;
import org.jetel.data.primitive.DecimalFactory;
import org.jetel.data.primitive.Numeric;
import org.jetel.data.primitive.NumericFormat;
import org.jetel.exception.BadDataFormatException;
import org.jetel.metadata.DataFieldMetadata;


/**
 *  A class that represents decimal number field (precision, scale pair)
 *
 *@author     Martin Zatopek
 *@since      November 30, 2005
 *@see        org.jetel.metadata.DataFieldMetadata
 */
public class DecimalDataField extends DataField implements Numeric, Comparable {

	private Decimal value;
	private int precision;
	private int scale;
	private NumericFormat numericFormat;

    
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
        if (!plain) {
            Locale locale;
            // handle locale
            if (_metadata.getLocaleStr() != null) {
                String[] localeLC = _metadata.getLocaleStr().split(
                        Defaults.DEFAULT_LOCALE_STR_DELIMITER_REGEX);
                if (localeLC.length > 1) {
                    locale = new Locale(localeLC[0], localeLC[1]);
                } else {
                    locale = new Locale(localeLC[0]);
                }
                // probably wrong locale string defined
                if (locale == null) {
                    throw new RuntimeException("Can't create Locale based on "
                            + _metadata.getLocaleStr());
                }
            } else {
                locale = null;
            }
            // handle formatString
            String formatString;
            formatString = _metadata.getFormatStr();
            if ((formatString != null) && (formatString.length() != 0)) {
                if (locale != null) {
                    numericFormat = new NumericFormat(formatString,
                            new DecimalFormatSymbols(locale));
                } else {
                    numericFormat = new NumericFormat(formatString);
                }
            } else if (locale != null) {
                numericFormat = new NumericFormat(locale);
            }
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
	private DecimalDataField(DataFieldMetadata _metadata, Decimal value, NumericFormat numericFormat, int precision, int scale) {
	    super(_metadata);
	    this.value = value.createCopy();
	    this.numericFormat = numericFormat;
	    this.precision = precision;
	    this.scale = scale;
	 }
	
	
	/**
	 * @see org.jetel.data.DataField#copy()
	 */
	public DataField duplicate() {
	    DecimalDataField newField = new DecimalDataField(metadata, value, numericFormat, precision, scale);
	    newField.setNull(isNull());
	    return newField;
	}
	
    /**
     * @see org.jetel.data.Numeric#duplicateNumeric()
     */
    public Numeric duplicateNumeric() {
        return value.duplicateNumeric();
    }
    
	/**
	 * @see org.jetel.data.DataField#copyField(org.jetel.data.DataField)
     * @deprecated use setValue(DataField) instead
	 */
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
		    throw new BadDataFormatException(getMetadata().getName() + " field can not be set with this object - " + _value.toString(), _value.toString());
		}
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.DataField#setValue(org.jetel.data.DataField)
	 */
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
	public void setValue(double _value) {
		if (_value == Double.NaN) {
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
	public void setValue(long _value) {
		if (_value == Long.MIN_VALUE) {
		    setNull(true);
			return;
		}
		value.setValue(_value);
		setNull(false);
	}

    /**
     * @see org.jetel.data.Numeric#setValue(org.jetel.data.Numeric)
     */
    public void setValue(Numeric _value) {
        if (_value == null || _value.isNull()) {
            setNull(true);
            return;
        }
        value.setValue(_value);
        setNull(false);
    }
    
    /**
     * @see org.jetel.data.primitive.Numeric#setValue(java.lang.Number)
     */
    public void setValue(Number value) {
        if (value == null) {
            setNull(true);
            return;
        }
        if(value instanceof BigDecimal) {
            this.value.setValue((BigDecimal) value);
        } else {
            this.value.setValue(value.doubleValue());
        }
        setNull(this.value.isNaN());
    }

	/**
	 *  Sets the Null value indicator
	 *
	 *@param  isNull  The new Null value
	 *@since          October 29, 2002
	 */
	public void setNull(boolean isNull) {
		super.setNull(isNull);
		if (this.isNull) {
			value.setNaN(true);
		}
	}

    /**
     * @see org.jetel.data.primitive.Numeric#setNull()
     */
    public void setNull() {
        setNull(true);
    }
    
    /* (non-Javadoc)
     * @see org.jetel.data.DataField#reset()
     */
    public void reset(){
        if (metadata.isNullable()){
            setNull(true);
        }else if (metadata.isDefaultValue()){
            setToDefaultValue();
        }else{
            value.setValue(0);
        }
    }

	// Associations

	// Operations

	/**
	 *  Gets the Field Type
	 *
	 *@return    The Type value
	 *@since     March 28, 2002
	 */
	public char getType() {
		return DataFieldMetadata.DECIMAL_FIELD;
	}


	/**
	 *  Gets the decimal value represented by this object (as Decimal object)
	 *
	 *@return    The Value value
	 *@since     March 28, 2002
	 */
	public Object getValue() {
		if(isNull) {
			return null;
		}
		return value;
	}

    /**
     * @see org.jetel.data.DataField#getValueDuplicate()
     */
    public Object getValueDuplicate() {
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
	public String toString() {
		if(isNull) {
			return "";
		}
		return value.toString(numericFormat);
	}


	/* (non-Javadoc)
	 * @see org.jetel.data.DataField#fromString(java.lang.CharSequence)
	 */
	public void fromString(CharSequence seq) {
		if(seq == null || seq.length() == 0) {
		    setNull(true);
			return;
		}
		try {
			value.fromString(seq, numericFormat);
			setNull(value.isNaN());
		} catch (Exception ex) {
			throw new BadDataFormatException(getMetadata().getName() + " cannot be set to " + seq, seq.toString());
		}
	}

	/**
	 *  Description of the Method
	 *
	 *@param  dataBuffer                    Description of ParametervalueStr
	 *@param  decoder                       Description of Parameter
	 *@exception  CharacterCodingException  Description of Exception
	 *@since                                October 31, 2002
	 */
	public void fromByteBuffer(ByteBuffer dataBuffer, CharsetDecoder decoder) throws CharacterCodingException {
		value.fromString(decoder.decode(dataBuffer), numericFormat);
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
		if (isNull) {
			return;
		}
		value.toByteBuffer(dataBuffer, encoder, numericFormat);
	}
	
    @Override
    public void toByteBuffer(ByteBuffer dataBuffer) {
    	if (isNull) {
			return;
		}
        value.toByteBuffer(dataBuffer);
    }

	/**
	 *  Performs serialization of the internal value into ByteBuffer (used when
	 *  moving data records between components).
	 *
	 *@param  buffer  Description of Parameter
	 *@since          April 23, 2002
	 */
	public void serialize(ByteBuffer buffer) {
		try {
			value.serialize(buffer);
    	} catch (BufferOverflowException e) {
    		throw new RuntimeException("The size of data buffer is only " + buffer.limit() + ". Set appropriate parameter in defautProperties file.", e);
    	}
	}


	/**
	 *  Performs deserialization of data
	 *
	 *@param  buffer  Description of Parameter
	 *@since          April 23, 2002
	 */
	public void deserialize(ByteBuffer buffer) {
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
	public int compareTo(Object obj) {
		if (obj == null) return 1;
		if (isNull) return -1;
	    
		return value.compareTo(obj);
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.Number#compareTo(org.jetel.data.Number)
	 */
	public int compareTo(Numeric _value){
	    if (isNull) {
	        return -1;
	    } else if (_value.isNull()) {
	        return 1;
	    } else {
	        return value.compareTo(_value);
	    }
	}
	
	
	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
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
	public int getSizeSerialized() {
		return value.getSizeSerialized();
	}


	/**
	 * @see org.jetel.data.Numeric#add(org.jetel.data.Number)
	 */
	public void add(Numeric a) {
        if(isNull) return;
        if(a.isNull())
            setNull(true);
        else
            value.add(a);
	}


	/**
	 * @see org.jetel.data.Numeric#sub(org.jetel.data.Number)
	 */
	public void sub(Numeric a) {
        if(isNull) return;
        if(a.isNull())
            setNull(true);
        else
            value.sub(a);
	}


	/**
	 * @see org.jetel.data.Numeric#mul(org.jetel.data.Number)
	 */
	public void mul(Numeric a) {
        if(isNull) return;
        if(a.isNull())
            setNull(true);
        else
            value.mul(a);
	}


	/**
	 * @see org.jetel.data.Numeric#div(org.jetel.data.Number)
	 */
	public void div(Numeric a) {
        if(isNull) return;
        if(a.isNull())
            setNull(true);
        else
            value.div(a);
	}


	/**
	 * @see org.jetel.data.Numeric#abs()
	 */
	public void abs() {
        if(isNull) return;
		value.abs();
	}


	/**
	 * @see org.jetel.data.Numeric#mod(org.jetel.data.Number)
	 */
	public void mod(Numeric a) {
        if(isNull) return;
        if(a.isNull())
            setNull(true);
        else
            value.mod(a);
	}


	/**
	 * @see org.jetel.data.Numeric#neg()
	 */
	public void neg() {
        if(isNull) return;
		value.neg();
	}

	/**
	 * @see org.jetel.data.Numeric#getDecimal()
	 */
	public Decimal getDecimal() {
		return value;
	}

	/**
	 * @see org.jetel.data.Numeric#getDecimal()
	 */
	public Decimal getDecimal(int precision, int scale) {
        if(precision == this.precision && scale == this.scale) {
            return value;
        }
        return DecimalFactory.getDecimal(value, precision, scale);
	}

    /**
     * @see org.jetel.data.Numeric#getBigDecimal()
     */
    public BigDecimal getBigDecimal() {
        if(isNull) 
            return null;
        else 
            return value.getBigDecimal();
    }


}
