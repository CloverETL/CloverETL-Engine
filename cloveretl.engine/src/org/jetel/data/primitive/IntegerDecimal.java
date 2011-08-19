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
package org.jetel.data.primitive;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetEncoder;
import java.text.ParseException;

import org.jetel.data.DecimalDataField;
import org.jetel.data.IntegerDataField;
import org.jetel.data.LongDataField;
import org.jetel.data.NumericDataField;
import org.jetel.exception.BadDataFormatException;
import org.jetel.util.formatter.NumericFormatter;
import org.jetel.util.formatter.NumericFormatterFactory;

/**
 * Clover internal decimal value representation.
 * Implements Decimal interface and use "int type" for store value.
 *
 *@author     Martin Zatopek
 *@since      April 12, 2006
 *@see        org.jetel.data.primitive.Decimal
 */
public final class IntegerDecimal implements Decimal {

    private long value;
    private int precision;
    private int scale;
    private boolean nan;

    private static long TENPOWERS[] = {
        1, 
        10, 
        100, 
        1000, 
        10000, 
        100000, 
        1000000, 
        10000000, 
        100000000, 
        1000000000, 
        10000000000L, 
        100000000000L, 
        1000000000000L, 
        10000000000000L, 
        100000000000000L, 
        1000000000000000L, 
        10000000000000000L, 
        100000000000000000L, 
        1000000000000000000L
        };

    /**
     * Constructor. New decimal is represented by integer value and on the start of his existence is not a number.
     * @param precision
     * @param scale
     */
    public IntegerDecimal(int precision, int scale) {
        this(-1, precision, scale, true);
    }
    
    /**
     * Private constructor called for example from createCopy() method.
     * @param value
     * @param precision
     * @param scale
     * @param nan
     */
    private IntegerDecimal(long value, int precision, int scale, boolean nan) {
        this.value = value;
        this.precision = precision;
        this.scale = scale;
        this.nan = nan;
    }

    @Override
    public int getPrecision() {
        return precision;
    }

    @Override
    public int getScale() {
        return scale;
    }

    @Override
    public Decimal createCopy() {
        return new IntegerDecimal(value, precision, scale, nan);
    }

    public void setValue(Decimal decimal) {
        if(decimal == null || decimal.isNaN()) {
            setNaN(true);
            return;
        }
        if(decimal instanceof IntegerDecimal) {
            final IntegerDecimal dec = (IntegerDecimal) decimal;
            final int scaleShift = scale - dec.scale;
            if(scaleShift == 0) {
                value = dec.value;
            } else if(scaleShift > 0) {
                value = dec.value * TENPOWERS[scaleShift];
            } else { //if(scaleShift < 0) 
                value = dec.value / TENPOWERS[-scaleShift];
            }
            setNaN(false);
            if(!satisfyPrecision()) {
                setNaN(true);
                throw new NumberFormatException("Number is out of available precision ["+precision+","+scale+"]. (" + decimal + ")");
            }
        } else {
            BigInteger bi = decimal.getBigDecimal().setScale(scale, BigDecimal.ROUND_DOWN).unscaledValue();
            if(HugeDecimal.precision(bi) > precision) {
                throw new NumberFormatException("Number is out of available precision ["+precision+","+scale+"]. (" + decimal + ")");
            }
            value = bi.longValue();
            setNaN(false);
            //throw new RuntimeException("Incompatible type of " + this.getClass().getName() + "(" + precision + "," + scale + ") and " + dec.getClass().getName() + "(" + dec.getPrecision() + "," + dec.getScale() + ").");
            //throw new RuntimeException("Can't set value " + this.getClass().getName() + " from this Decimal implementation - " + decimal.getClass().getName());
        }
    }

    @Override
    public void setValue(double _value) {
        if(Double.isNaN(_value)) {
            setNaN(true);
            return;
        }
        value = (long) convertToInnerForm(_value);
        setNaN(false);
        if(!satisfyPrecision()) {
            setNaN(true);
            throw new NumberFormatException("Number is out of available precision ["+precision+","+scale+"]. (" + _value + ")");
        }
    }

    @Override
    public void setValue(int _value) {
        if(_value == Integer.MIN_VALUE) {
            setNaN(true);
            return;
        }
        value = convertToInnerForm(_value);
        setNaN(false);
        if(!satisfyPrecision()) {
            setNaN(true);
            throw new NumberFormatException("Number is out of available precision ["+precision+","+scale+"]. (" + _value + ")");
        }
    }

    @Override
    public void setValue(long _value) {
        if(_value == Long.MIN_VALUE) {
            setNaN(true);
            return;
        }
        value = convertToInnerForm(_value);
        setNaN(false);
        if(!satisfyPrecision()) {
            setNaN(true);
            throw new NumberFormatException("Number is out of available precision ["+precision+","+scale+"]. (" + _value + ")");
        }
    }

    @Override
    public void setValue(Numeric _value) {
        if (_value == null || _value.isNull()) {
            setNaN(true);
            return;
        }
        if (_value instanceof CloverInteger) {
        	setValue(_value.getInt());
        } else if (_value instanceof CloverLong) {
        	setValue(_value.getLong());
        } else if (_value instanceof Decimal) {
            setValue((Decimal) _value);
        } else {
        	setValue(_value.getBigDecimal());
        }
    }
    
    @Override
    public void setValue(BigDecimal _value) {
    	if (_value == null) {
    		setNaN(true);
    		return;
    	}
        BigInteger bi = _value.setScale(scale, BigDecimal.ROUND_DOWN).unscaledValue();
        if(HugeDecimal.precision(bi) > precision) {
            throw new NumberFormatException("Too many digits before decimal dot, must be " + (precision - scale) + " at most (" + _value + "); specified precision ["+precision+","+scale+"]");
        }
        value = bi.longValue();
        setNaN(false);
    }
    
    /* (non-Javadoc)
     * @see org.jetel.data.primitive.Numeric#setValue(java.lang.Number)
     */
    public void setValue(Number value) {
       if (value instanceof Long) {
           setValue(((Long) value).longValue());
       } else if (value instanceof Integer) {
           setValue(((Integer) value).intValue());
       } else if (value instanceof BigDecimal) {
           setValue((BigDecimal) value);
       } else if (value instanceof Double) {
           setValue(((Double) value).doubleValue());
       } else if (value instanceof Numeric) {
           setValue((Numeric) value);
       } else {
           setValue(value.doubleValue());
       }
    }

    @Override
    public double getDouble() {
        if(isNaN()) {
            return Double.NaN;
        }
        return getBigDecimal().doubleValue();
    }

    @Override
    public int getInt() {
        if(isNaN()) {
            return Integer.MIN_VALUE;
        }
        return (int) convertToOuterForm(value);
    }

    @Override
    public long getLong() {
        if(isNaN()) {
            return Long.MIN_VALUE;
        }
        return convertToOuterForm(value);
    }

    @Override
    public BigDecimal getBigDecimal() {
        if(isNaN()) {
            return null;
        }
        return BigDecimal.valueOf(value, scale); 
    }

    @Override
    public BigDecimal getBigDecimalOutput() {
        if(isNaN()) {
            return null;
        }
        if(!satisfyPrecision()) return null;
        return BigDecimal.valueOf(value, scale); 
    }
    
    @Override
    public Decimal getDecimal() {
        return createCopy();
    }
    
    @Override
    public Decimal getDecimal(int precision, int scale) {
        return DecimalFactory.getDecimal(this, precision, scale);
    }
    
    @Override
    public Numeric duplicateNumeric() {
        return createCopy();
    }
    
    @Override
    public void setNaN(boolean isNaN) {
        nan = isNaN;
    }

    @Override
    public boolean isNaN() {
        return nan;
    }
    
    @Override
    public boolean isNull() {
        return nan;
    }
    
    
    /* (non-Javadoc)
     * @see org.jetel.data.primitive.Numeric#setNull()
     */
    public void setNull(){
        setNaN(true);
    }

    @Override
    public void add(Numeric a) {
        if(isNull()) return;
        if(a.isNull()) setNaN(true);
        if(a instanceof IntegerDataField || a instanceof CloverInteger) {
            value += convertToInnerForm(a.getInt());
        } else  if(a instanceof LongDataField || a instanceof CloverLong) {
            value += convertToInnerForm(a.getLong());
        } else if(a instanceof NumericDataField || a instanceof CloverDouble) {
            value += convertToInnerForm(a.getDouble());
        } else if(a instanceof DecimalDataField || a instanceof Decimal) {
            Decimal d = (a instanceof Decimal) ? (Decimal) a : a.getDecimal();
            if(d instanceof IntegerDecimal && ((IntegerDecimal) d).scale == scale) {
                value += ((IntegerDecimal) d).value;
            } else {
                setValue(getBigDecimal().add(d.getBigDecimal()));
            }
        } else {
            throw new RuntimeException("Unsupported class of parameter 'add' operation (" + a.getClass().getName() + ").");
        }
    }

    @Override
    public void sub(Numeric a) {
        if(isNull()) return;
        if(a.isNull()) setNaN(true);
        if(a instanceof IntegerDataField || a instanceof CloverInteger) {
            value -= convertToInnerForm(a.getInt());
        } else  if(a instanceof LongDataField || a instanceof CloverLong) {
            value -= convertToInnerForm(a.getLong());
        } else if(a instanceof NumericDataField || a instanceof CloverDouble) {
            value -= convertToInnerForm(a.getDouble());
        } else if(a instanceof DecimalDataField || a instanceof Decimal) {
            Decimal d = (a instanceof Decimal) ? (Decimal) a : a.getDecimal();
            if(d instanceof IntegerDecimal && ((IntegerDecimal) d).scale == scale) {
                value -= ((IntegerDecimal) d).value;
            } else {
                setValue(getBigDecimal().subtract(d.getBigDecimal()));
            }
        } else {
            throw new RuntimeException("Unsupported class of parameter 'sub' operation (" + a.getClass().getName() + ").");
        }
    }

    @Override
    public void mul(Numeric a) {
        if(isNull()) return;
        if(a.isNull()) setNaN(true);
        if(a instanceof IntegerDataField || a instanceof CloverInteger) {
            value *= a.getInt();
        } else  if(a instanceof LongDataField || a instanceof CloverLong) {
            value *= a.getLong();
        } else if(a instanceof NumericDataField || a instanceof CloverDouble) {
            value *= a.getDouble();
        } else if(a instanceof DecimalDataField || a instanceof Decimal) {
            Decimal d = (a instanceof Decimal) ? (Decimal) a : a.getDecimal();
//            if(d instanceof IntegerDecimal && ((IntegerDecimal) d).scale == scale) {
//                value *= convertToOuterForm(((double) ((IntegerDecimal) d).value));
//            } else {
                setValue(getBigDecimal().multiply(d.getBigDecimal()));
//            }
        } else {
            throw new RuntimeException("Unsupported class of parameter 'mul' operation (" + a.getClass().getName() + ").");
        }
    }

    @Override
    public void div(Numeric a) {
        if(isNull()) return;
        if(a.isNull()) setNaN(true);
        if(a instanceof IntegerDataField || a instanceof CloverInteger) {
            value /= a.getInt();
        } else  if(a instanceof LongDataField || a instanceof CloverLong) {
            value /= a.getLong();
        } else if(a instanceof NumericDataField || a instanceof CloverDouble) {
            value /= a.getDouble();
        } else if(a instanceof DecimalDataField || a instanceof Decimal) {
            Decimal d = (a instanceof Decimal) ? (Decimal) a : a.getDecimal();
//            if(d instanceof IntegerDecimal && ((IntegerDecimal) d).scale == scale) {
//                value /= convertToOuterForm(((double) ((IntegerDecimal) d).value));
//            } else {
                setValue(getBigDecimal().divide(d.getBigDecimal(), scale, RoundingMode.HALF_UP));
//            }
        } else {
            throw new RuntimeException("Unsupported class of parameter 'div' operation (" + a.getClass().getName() + ").");
        }
    }

    @Override
    public void abs() {
        if(isNull()) return;
        value = Math.abs(value);
    }

    @Override
    public void mod(Numeric a) {
        if(isNull()) return;
        if(a.isNull()) setNaN(true);
        if(a instanceof IntegerDataField || a instanceof CloverInteger) {
            value %= convertToInnerForm(a.getInt());
        } else  if(a instanceof LongDataField || a instanceof CloverLong) {
            value %= convertToInnerForm(a.getLong());
        } else if(a instanceof NumericDataField || a instanceof CloverDouble) {
            value %= convertToInnerForm(a.getDouble());
        } else if(a instanceof DecimalDataField || a instanceof Decimal) {
            Decimal d = (a instanceof Decimal) ? (Decimal) a : a.getDecimal();
            if(d instanceof IntegerDecimal && ((IntegerDecimal) d).scale == scale) {
                value %= ((IntegerDecimal) d).value;
            } else {
                setValue(getBigDecimal().remainder(d.getBigDecimal()));
            }
        } else {
            throw new RuntimeException("Unsupported class of parameter 'mod' operation (" + a.getClass().getName() + ").");
        }
    }

    @Override
    public void neg() {
        if(isNull()) return;
        value = -value;
    }

    @Override
    public void serialize(ByteBuffer byteBuffer) {
    	try {
    		if(isNaN())
    			byteBuffer.putLong(Long.MIN_VALUE);
    		else
    			byteBuffer.putLong(value);
    	} catch (BufferOverflowException e) {
    		throw new RuntimeException("The size of data buffer is only " + byteBuffer.limit() + ". Set appropriate parameter in defaultProperties file.", e);
    	}
    }

    @Override
    public void deserialize(ByteBuffer byteBuffer) {
        value = byteBuffer.getLong();
        setNaN(value == Long.MIN_VALUE);
    }

    @Override
    public int getSizeSerialized() {
        return Long.SIZE / 8;
    }

    @Override
    public String toString(NumericFormatter numericFormatter) {
        BigDecimal bd = getBigDecimalOutput();
       	return numericFormatter.formatBigDecimal(bd);
    }

    @Override
    public String toString() {
        return toString(NumericFormatterFactory.getPlainFormatterInstance());
    }
    
    @Override
    public void toByteBuffer(ByteBuffer dataBuffer, CharsetEncoder encoder, NumericFormatter numericFormatter) throws CharacterCodingException {
    	try {
    		dataBuffer.put(encoder.encode(CharBuffer.wrap(toString(numericFormatter))));
    	} catch (BufferOverflowException e) {
			throw new RuntimeException("The size of data buffer is only " + dataBuffer.limit() + ". Set appropriate parameter in defaultProperties file.", e);
    	}
    }

    @Override
    public void toByteBuffer(ByteBuffer dataBuffer) {
        if(!isNaN()) {
        	try {
        		dataBuffer.putLong(value);
        		dataBuffer.putInt(scale);
        	} catch (BufferOverflowException e) {
    			throw new RuntimeException("The size of data buffer is only " + dataBuffer.limit() + ". Set appropriate parameter in defaultProperties file.", e);
        	}
        }
    }
    
    @Override
    public void fromString(CharSequence seq, NumericFormatter numericFormatter) {
        if(seq == null || seq.length() == 0) {
            setNaN(true);
            return;
        }

        try {
			setValue(numericFormatter.parseBigDecimal(seq));
		} catch (ParseException e) {
			throw new BadDataFormatException("IntegerDecimal cannot represent '" + seq + "' value.", e);
		}
    }

    public void fromCharBuffer(CharBuffer buffer, NumericFormatter numericFormatter) {
        fromString(buffer.toString(), numericFormatter);
    }

    public int compareTo(Numeric value) {
        if (isNull()) {
            return -1;
        }else if (value.isNull()) {
            return 1;
        }else {
            return compareTo((Object) value.getDecimal());
        }
    }
    
    @Override
    public int compareTo(Object obj) {
        if (obj==null) return 1;
        if (isNaN()) return -1;
        
        if (obj instanceof BigDecimal) {
        	return getBigDecimal().compareTo((BigDecimal)obj);
        }else if (obj instanceof Decimal) {
            if(obj instanceof IntegerDecimal && ((IntegerDecimal) obj).scale == scale) {
                return compareTo(((IntegerDecimal) obj).value);
            }
            return getBigDecimal().compareTo(((Decimal) obj).getBigDecimal());
        } else if (obj instanceof Integer) {
            return compareTo(convertToInnerForm(((Integer) obj).intValue()));
        } else if(obj instanceof Long) {
            return compareTo(convertToInnerForm(((Long) obj).longValue()));
        } else if (obj instanceof Double) { 
            return getBigDecimal().compareTo(BigDecimal.valueOf((Double) obj));
        } else if (obj instanceof IntegerDataField) {
            return compareTo(convertToInnerForm(((IntegerDataField) obj).getInt()));
        } else if(obj instanceof LongDataField) {
            return compareTo(convertToInnerForm(((LongDataField) obj).getLong()));
        } else if (obj instanceof NumericDataField) {
            return getBigDecimal().compareTo(BigDecimal.valueOf(((NumericDataField) obj).getDouble()));
        } else if (obj instanceof DecimalDataField) {
            return compareTo(((DecimalDataField) obj).getValue());
        } else throw new ClassCastException("Can't compare this DecimalDataField and " + obj.getClass().getName());
    }

    private int compareTo(long compLong) {
        if (value > compLong) {
            return 1;
        } else if (value < compLong) {
            return -1;
        } else {
            return 0;
        }
    }

    /**
     * Check if stored value is in dimension defined by precision.
     * @return true if value is shorter than precision; false else
     */
    public boolean satisfyPrecision() {
        if(isNaN()) return true;
        return !(precision < HugeDecimal.longLength(value)); 
    }
    
    @Override
	public boolean equals(Object obj) {
        if(obj instanceof Numeric)
            return compareTo((Numeric) obj) == 0;
        else return false;
    }

    @Override
	public int hashCode() {
        if(isNaN()) return Integer.MIN_VALUE;
        return (int)(value^value>>32);
    }
    
    private long convertToInnerForm(long l) {
        if(scale >= 0) {
            return l * TENPOWERS[scale];
        } else {
            return l / TENPOWERS[-scale];
        }
    }

    private long convertToInnerForm(double d) {
    	BigDecimal bd = BigDecimal.valueOf(d);
        BigInteger bi = bd.setScale(scale, BigDecimal.ROUND_DOWN).unscaledValue();
        return bi.longValue();
    }

    private long convertToOuterForm(long l) {
        if(scale >= 0) {
            return l / TENPOWERS[scale];
        } else {
            return l * TENPOWERS[-scale];
        }
    }

//    private double convertToOuterForm(double d) {
//        if(scale >= 0) {
//            return d / TENPOWERS[scale];
//        } else {
//            return d * TENPOWERS[-scale];
//        }
//    }

}
