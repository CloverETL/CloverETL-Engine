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
package org.jetel.data.primitive;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.text.NumberFormat;
import java.text.ParsePosition;

import org.jetel.data.DecimalDataField;
import org.jetel.data.IntegerDataField;
import org.jetel.data.LongDataField;
import org.jetel.data.Numeric;
import org.jetel.data.NumericDataField;
import org.jetel.util.NumericFormat;

/**
 * Clover internal decimal value representation.
 * Implements Decimal interface and use "int type" for store value.
 *
 *@author     Martin Zatopek
 *@since      April 12, 2006
 *@see        org.jetel.data.primitive.Decimal
 */
public class IntegerDecimal implements Decimal {

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

    /**
     * @see org.jetel.data.Decimal#getPrecision()
     */
    public int getPrecision() {
        return precision;
    }

    /**
     * @see org.jetel.data.Decimal#getScale()
     */
    public int getScale() {
        return scale;
    }

    /**
     * @see org.jetel.data.Decimal#createCopy()
     */
    public Decimal createCopy() {
        return new IntegerDecimal(value, precision, scale, nan);
    }

    /**
     * @see org.jetel.data.Decimal#setValue(org.jetel.data.Decimal)
     */
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
                throw new NumberFormatException("Number is out of available precision. (" + decimal + ")");
            }
        } else {
            BigInteger bi = decimal.getBigDecimal().setScale(scale, BigDecimal.ROUND_DOWN).unscaledValue();
            if(HugeDecimal.precision(bi) > precision) {
                throw new NumberFormatException("Number is out of available precision. (" + decimal + ")");
            }
            value = bi.longValue();
            setNaN(false);
            //throw new RuntimeException("Incompatible type of " + this.getClass().getName() + "(" + precision + "," + scale + ") and " + dec.getClass().getName() + "(" + dec.getPrecision() + "," + dec.getScale() + ").");
            //throw new RuntimeException("Can't set value " + this.getClass().getName() + " from this Decimal implementation - " + decimal.getClass().getName());
        }
    }

    /**
     * @see org.jetel.data.Decimal#setValue(double)
     */
    public void setValue(double _value) {
        if(_value == Double.NaN) {
            setNaN(true);
            return;
        }
        value = (long) (_value * TENPOWERS[scale]);
        setNaN(false);
        if(!satisfyPrecision()) {
            setNaN(true);
            throw new NumberFormatException("Number is out of available precision. (" + _value + ")");
        }
    }

    /**
     * @see org.jetel.data.Decimal#setValue(int)
     */
    public void setValue(int _value) {
        if(_value == Integer.MIN_VALUE) {
            setNaN(true);
            return;
        }
        value = ((long) _value) * TENPOWERS[scale];
        setNaN(false);
        if(!satisfyPrecision()) {
            setNaN(true);
            throw new NumberFormatException("Number is out of available precision. (" + _value + ")");
        }
    }

    /**
     * @see org.jetel.data.Decimal#setValue(long)
     */
    public void setValue(long _value) {
        if(_value == Long.MIN_VALUE) {
            setNaN(true);
            return;
        }
        value = _value * TENPOWERS[scale];
        setNaN(false);
        if(!satisfyPrecision()) {
            setNaN(true);
            throw new NumberFormatException("Number is out of available precision. (" + _value + ")");
        }
    }

    /**
     * @see org.jetel.data.Numeric#setValue(org.jetel.data.Numeric)
     */
    public void setValue(Numeric _value) {
        if(_value.isNull()) {
            setNaN(true);
            return;
        }
        setValue(_value.getBigDecimal());
    }
    
    /**
     * @see org.jetel.data.primitive.Decimal#setValue(java.math.BigDecimal)
     */
    public void setValue(BigDecimal _value) {
        BigInteger bi = _value.setScale(scale, BigDecimal.ROUND_DOWN).unscaledValue();
        if(HugeDecimal.precision(bi) > precision) {
            throw new NumberFormatException("Number is out of available precision. (" + _value + ")");
        }
        value = bi.longValue();
        setNaN(false);
    }

    /**
     * @see org.jetel.data.Decimal#getDouble()
     */
    public double getDouble() {
        if(isNaN()) {
            return Double.NaN;
        }
        return ((double) value) / TENPOWERS[scale];
    }

    /**
     * @see org.jetel.data.Decimal#getInt()
     */
    public int getInt() {
        if(isNaN()) {
            return Integer.MIN_VALUE;
        }
        return (int) (value / TENPOWERS[scale]);
    }

    /**
     * @see org.jetel.data.Decimal#getLong()
     */
    public long getLong() {
        if(isNaN()) {
            return Long.MIN_VALUE;
        }
        return value / TENPOWERS[scale];
    }

    /**
     * @see org.jetel.data.Decimal#getBigDecimal()
     */
    public BigDecimal getBigDecimal() {
        if(isNaN()) {
            return null;
        }
        return BigDecimal.valueOf(value, scale); 
    }

    /**
     * @see org.jetel.data.Decimal#getBigDecimalOutput()
     */
    public BigDecimal getBigDecimalOutput() {
        if(isNaN()) {
            return null;
        }
        if(!satisfyPrecision()) return null;
        return BigDecimal.valueOf(value, scale); 
    }
    
    /**
     * @see org.jetel.data.Numeric#getDecimal()
     */
    public Decimal getDecimal() {
        return createCopy();
    }
    
    /**
     * @see org.jetel.data.Numeric#getDecimal(int, int)
     */
    public Decimal getDecimal(int precision, int scale) {
        return DecimalFactory.getDecimal(this, precision, scale);
    }
    
    /**
     * @see org.jetel.data.Numeric#duplicateNumeric()
     */
    public Numeric duplicateNumeric() {
        return createCopy();
    }
    
    /**
     * @see org.jetel.data.Decimal#setNaN(boolean)
     */
    public void setNaN(boolean isNaN) {
        nan = isNaN;
    }

    /**
     * @see org.jetel.data.Decimal#isNaN()
     */
    public boolean isNaN() {
        return nan;
    }
    
    /**
     * @see org.jetel.data.Numeric#isNull()
     */
    public boolean isNull() {
        return nan;
    }

    /**
     * @see org.jetel.data.Decimal#add(org.jetel.data.Numeric)
     */
    public void add(Numeric a) {
        if(isNull()) return;
        if(a.isNull()) setNaN(true);
        if(a instanceof IntegerDataField || a instanceof CloverInteger) {
            value += a.getInt() * TENPOWERS[scale];
        } else  if(a instanceof LongDataField || a instanceof CloverLong) {
            value += a.getLong() * TENPOWERS[scale];
        } else if(a instanceof NumericDataField || a instanceof CloverDouble) {
            value += a.getDouble() * TENPOWERS[scale];
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

    /**
     * @see org.jetel.data.Decimal#sub(org.jetel.data.Numeric)
     */
    public void sub(Numeric a) {
        if(isNull()) return;
        if(a.isNull()) setNaN(true);
        if(a instanceof IntegerDataField || a instanceof CloverInteger) {
            value -= a.getInt() * TENPOWERS[scale];
        } else  if(a instanceof LongDataField || a instanceof CloverLong) {
            value -= a.getLong() * TENPOWERS[scale];
        } else if(a instanceof NumericDataField || a instanceof CloverDouble) {
            value -= a.getDouble() * TENPOWERS[scale];
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

    /**
     * @see org.jetel.data.Decimal#mul(org.jetel.data.Numeric)
     */
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
            if(d instanceof IntegerDecimal && ((IntegerDecimal) d).scale == scale) {
                value *= ((double) ((IntegerDecimal) d).value) / TENPOWERS[scale];
            } else {
                setValue(getBigDecimal().multiply(d.getBigDecimal()));
            }
        } else {
            throw new RuntimeException("Unsupported class of parameter 'mul' operation (" + a.getClass().getName() + ").");
        }
    }

    /**
     * @see org.jetel.data.Decimal#div(org.jetel.data.Numeric)
     */
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
            if(d instanceof IntegerDecimal && ((IntegerDecimal) d).scale == scale) {
                value /= ((double) ((IntegerDecimal) d).value) / TENPOWERS[scale];
            } else {
                setValue(getBigDecimal().divide(d.getBigDecimal()));
            }
        } else {
            throw new RuntimeException("Unsupported class of parameter 'div' operation (" + a.getClass().getName() + ").");
        }
    }

    /**
     * @see org.jetel.data.Decimal#abs()
     */
    public void abs() {
        if(isNull()) return;
        value = Math.abs(value);
    }

    /**
     * @see org.jetel.data.Decimal#mod(org.jetel.data.Numeric)
     */
    public void mod(Numeric a) {
        if(isNull()) return;
        if(a.isNull()) setNaN(true);
        if(a instanceof IntegerDataField || a instanceof CloverInteger) {
            value %= a.getInt() * TENPOWERS[scale];
        } else  if(a instanceof LongDataField || a instanceof CloverLong) {
            value %= a.getLong() * TENPOWERS[scale];
        } else if(a instanceof NumericDataField || a instanceof CloverDouble) {
            value %= a.getDouble() * TENPOWERS[scale];
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

    /**
     * @see org.jetel.data.Decimal#neg()
     */
    public void neg() {
        if(isNull()) return;
        value = -value;
    }

    /**
     * @see org.jetel.data.Decimal#serialize(java.nio.ByteBuffer)
     */
    public void serialize(ByteBuffer byteBuffer) {
        if(isNaN())
            byteBuffer.putLong(Long.MIN_VALUE);
        else
            byteBuffer.putLong(value);
    }

    /**
     * @see org.jetel.data.Decimal#deserialize(java.nio.ByteBuffer)
     */
    public void deserialize(ByteBuffer byteBuffer) {
        value = byteBuffer.getLong();
        setNaN(value == Long.MIN_VALUE);
    }

    /**
     * @see org.jetel.data.Decimal#getSizeSerialized()
     */
    public int getSizeSerialized() {
        return Long.SIZE / 8;
    }

    /**
     * @see org.jetel.data.Decimal#toString(java.text.NumberFormat)
     */
    public String toString(NumberFormat numberFormat) {
        if(isNaN() || !satisfyPrecision()) {
            return "";
        }
        String s = Long.toString(value);
        StringBuffer sb = new StringBuffer();
        if(s.length() - scale > 0) {
            sb.append(s.substring(0, s.length() - scale));
        } else {
            sb.append('0');
        }
        if(scale > 0) {
            sb.append('.');
        }
        for(int i = 0; i > s.length() - scale; i--) {
            sb.append('0');
        }
        sb.append(s.substring(Math.max(0, s.length() - scale), s.length()));
        return sb.toString();
    }

    /**
     * @see java.lang.Object#toString()
     */
    public String toString() {
        return toString(null);
    }
    
    /**
     * @see org.jetel.data.Decimal#toCharBuffer(java.text.NumberFormat)
     */
    public CharBuffer toCharBuffer(NumberFormat numberFormat) {
        if(isNaN()) {
            return CharBuffer.allocate(0);
        }
        return CharBuffer.wrap(toString(numberFormat));
    }

    /**
     * @see org.jetel.data.Decimal#fromString(java.lang.String, java.text.NumberFormat)
     */
    public void fromString(String string, NumberFormat numberFormat) {
        if(string == null || string.length() == 0) {
            setNaN(true);
            return;
        }
        NumericFormat nf = new NumericFormat();
        BigDecimal bd = (BigDecimal) nf.parse(string, new ParsePosition(0));
        setValue(bd);
//        value = Long.parseLong(string);
//        if(!satisfyPrecision()) throw new NumberFormatException(); 
//        setNaN(false);
    }

    /**
     * @see org.jetel.data.Decimal#fromCharBuffer(java.nio.CharBuffer, java.text.NumberFormat)
     */
    public void fromCharBuffer(CharBuffer buffer, NumberFormat numberFormat) {
        fromString(buffer.toString(), numberFormat);
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
    
    /**
     * @see org.jetel.data.primitive.Decimal#compareTo(java.lang.Object)
     */
    public int compareTo(Object obj) {
        if (obj==null) return 1;
        if (isNaN()) return -1;
        
        if (obj instanceof Decimal) {
            if(obj instanceof IntegerDecimal && ((IntegerDecimal) obj).scale == scale) {
                return compareTo(((IntegerDecimal) obj).value);
            }
            return getBigDecimal().compareTo(((Decimal) obj).getBigDecimal());
        } else if (obj instanceof Integer) {
            return compareTo(((Integer) obj).intValue() * TENPOWERS[scale]);
        } else if(obj instanceof Long) {
            return compareTo(((Long) obj).longValue() * TENPOWERS[scale]);
        } else if (obj instanceof Double) { 
            return getBigDecimal().compareTo(BigDecimal.valueOf((Double) obj));
        } else if (obj instanceof IntegerDataField) {
            return compareTo(((IntegerDataField) obj).getInt() * TENPOWERS[scale]);
        } else if(obj instanceof LongDataField) {
            return compareTo(((LongDataField) obj).getLong() * TENPOWERS[scale]);
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
    
    public boolean equals(Object obj) {
        if(obj instanceof Numeric)
            return compareTo((Numeric) obj) == 0;
        else return false;
    }

    public int hashCode() {
        if(isNaN()) return Integer.MIN_VALUE;
        return (int)(value^value>>32);
    }
}
