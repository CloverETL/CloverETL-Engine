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

import org.jetel.data.DecimalDataField;
import org.jetel.data.IntegerDataField;
import org.jetel.data.LongDataField;
import org.jetel.data.Numeric;
import org.jetel.data.NumericDataField;

/**
 * Clover internal decimal value representation.
 * Implements Decimal interface and use "int type" for store value.
 *
 *@author     Martin Zatopek
 *@since      April 12, 2006
 *@see        org.jetel.data.primitive.Decimal
 */
public class IntegerDecimal implements Decimal {

    private final static int FIELD_SIZE_BYTES = 4;// standard size of field

    private int value;
    private int precision;
    private int scale;
    private boolean nan;

    private static int TENPOWERS[] = {0, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000};

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
    private IntegerDecimal(int value, int precision, int scale, boolean nan) {
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
            if(!satisfyPrecision()) {
                setNaN(true);
                throw new NumberFormatException("Number is out of available precision. (" + decimal + ")");
            }
            setNaN(false);
        } else {
            BigInteger bi = decimal.getBigDecimal().setScale(scale, BigDecimal.ROUND_DOWN).unscaledValue();
            if(HugeDecimal.precision(bi) > precision) {
                throw new NumberFormatException("Number is out of available precision. (" + decimal + ")");
            }
            value = bi.intValue();
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
        value = (int) (_value * TENPOWERS[scale]);
        if(!satisfyPrecision()) {
            setNaN(true);
            throw new NumberFormatException("Number is out of available precision. (" + _value + ")");
        }
        setNaN(false);
    }

    /**
     * @see org.jetel.data.Decimal#setValue(int)
     */
    public void setValue(int _value) {
        if(_value == Integer.MIN_VALUE) {
            setNaN(true);
            return;
        }
        value = _value * TENPOWERS[scale];
        if(!satisfyPrecision()) {
            setNaN(true);
            throw new NumberFormatException("Number is out of available precision. (" + _value + ")");
        }
        setNaN(false);
    }

    /**
     * @see org.jetel.data.Decimal#setValue(long)
     */
    public void setValue(long _value) {
        if(_value == Long.MIN_VALUE) {
            setNaN(true);
            return;
        }
        value = (int) _value * TENPOWERS[scale];
        if(!satisfyPrecision()) {
            setNaN(true);
            throw new NumberFormatException("Number is out of available precision. (" + _value + ")");
        }
        setNaN(false);
    }

    /**
     * @see org.jetel.data.Numeric#setValue(org.jetel.data.Numeric)
     */
    public void setValue(Numeric _value) {
        if(_value.isNull()) {
            setNaN(true);
            return;
        }
        value = (int) _value.getInt() * TENPOWERS[scale];
        if(!satisfyPrecision()) {
            setNaN(true);
            throw new NumberFormatException("Number is out of available precision. (" + _value + ")");
        }
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
        return value / TENPOWERS[scale];
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
        if(a instanceof IntegerDataField) {
            value += a.getInt() * TENPOWERS[scale];
        } else  if(a instanceof LongDataField) {
            value += (int) a.getLong() * TENPOWERS[scale];
        } else if(a instanceof NumericDataField) {
            value += (int) a.getDouble() * TENPOWERS[scale];
        } else if(a instanceof DecimalDataField) {
            Decimal d = a.getDecimal();
            if(d instanceof IntegerDecimal && ((IntegerDecimal) d).scale == scale) {
                value += ((IntegerDecimal) d).value;
            } else {
                value += d.getBigDecimal().intValue();
            }
        } else {
            throw new RuntimeException("Unsupported class of parameter 'add' operation (" + a.getClass().getName() + ").");
        }
    }

    /**
     * @see org.jetel.data.Decimal#sub(org.jetel.data.Numeric)
     */
    public void sub(Numeric a) {
        if(a instanceof IntegerDataField) {
            value -= a.getInt() * TENPOWERS[scale];
        } else  if(a instanceof LongDataField) {
            value -= (int) a.getLong() * TENPOWERS[scale];
        } else if(a instanceof NumericDataField) {
            value -= (int) a.getDouble() * TENPOWERS[scale];
        } else if(a instanceof DecimalDataField) {
            Decimal d = a.getDecimal();
            if(d instanceof IntegerDecimal && ((IntegerDecimal) d).scale == scale) {
                value -= ((IntegerDecimal) d).value;
            } else {
                value -= d.getBigDecimal().intValue();
            }
        } else {
            throw new RuntimeException("Unsupported class of parameter 'sub' operation (" + a.getClass().getName() + ").");
        }
    }

    /**
     * @see org.jetel.data.Decimal#mul(org.jetel.data.Numeric)
     */
    public void mul(Numeric a) {
        if(a instanceof IntegerDataField) {
            value *= a.getInt() * TENPOWERS[scale];
        } else  if(a instanceof LongDataField) {
            value *= (int) a.getLong() * TENPOWERS[scale];
        } else if(a instanceof NumericDataField) {
            value *= (int) a.getDouble() * TENPOWERS[scale];
        } else if(a instanceof DecimalDataField) {
            Decimal d = a.getDecimal();
            if(d instanceof IntegerDecimal && ((IntegerDecimal) d).scale == scale) {
                value *= ((IntegerDecimal) d).value;
            } else {
                value *= d.getBigDecimal().intValue();
            }
        } else {
            throw new RuntimeException("Unsupported class of parameter 'mul' operation (" + a.getClass().getName() + ").");
        }
    }

    /**
     * @see org.jetel.data.Decimal#div(org.jetel.data.Numeric)
     */
    public void div(Numeric a) {
        if(a instanceof IntegerDataField) {
            value /= a.getInt() * TENPOWERS[scale];
        } else  if(a instanceof LongDataField) {
            value /= (int) a.getLong() * TENPOWERS[scale];
        } else if(a instanceof NumericDataField) {
            value /= (int) a.getDouble() * TENPOWERS[scale];
        } else if(a instanceof DecimalDataField) {
            Decimal d = a.getDecimal();
            if(d instanceof IntegerDecimal && ((IntegerDecimal) d).scale == scale) {
                value /= ((IntegerDecimal) d).value;
            } else {
                value /= d.getBigDecimal().intValue();
            }
        } else {
            throw new RuntimeException("Unsupported class of parameter 'div' operation (" + a.getClass().getName() + ").");
        }
    }

    /**
     * @see org.jetel.data.Decimal#abs()
     */
    public void abs() {
        value = Math.abs(value);
    }

    /**
     * @see org.jetel.data.Decimal#mod(org.jetel.data.Numeric)
     */
    public void mod(Numeric a) {
        if(a instanceof IntegerDataField) {
            value %= a.getInt() * TENPOWERS[scale];
        } else  if(a instanceof LongDataField) {
            value %= (int) a.getLong() * TENPOWERS[scale];
        } else if(a instanceof NumericDataField) {
            value %= (int) a.getDouble() * TENPOWERS[scale];
        } else if(a instanceof DecimalDataField) {
            Decimal d = a.getDecimal();
            if(d instanceof IntegerDecimal && ((IntegerDecimal) d).scale == scale) {
                value %= ((IntegerDecimal) d).value;
            } else {
                value %= d.getBigDecimal().intValue();
            }
        } else {
            throw new RuntimeException("Unsupported class of parameter 'mod' operation (" + a.getClass().getName() + ").");
        }
    }

    /**
     * @see org.jetel.data.Decimal#neg()
     */
    public void neg() {
        value = -value;
    }

    /**
     * @see org.jetel.data.Decimal#serialize(java.nio.ByteBuffer)
     */
    public void serialize(ByteBuffer byteBuffer) {
        if(isNaN())
            byteBuffer.putInt(Integer.MIN_VALUE);
        else
            byteBuffer.putInt(value);
    }

    /**
     * @see org.jetel.data.Decimal#deserialize(java.nio.ByteBuffer)
     */
    public void deserialize(ByteBuffer byteBuffer) {
        value = byteBuffer.getInt();
        setNaN(value == Integer.MIN_VALUE);
    }

    /**
     * @see org.jetel.data.Decimal#getSizeSerialized()
     */
    public int getSizeSerialized() {
        return FIELD_SIZE_BYTES;
        //FIXME in 1.5 java we can use next line
        //return Integer.SIZE / 8;
    }

    /**
     * @see org.jetel.data.Decimal#toString(java.text.NumberFormat)
     */
    public String toString(NumberFormat numberFormat) {
        if(isNaN() || !satisfyPrecision()) {
            return "";
        }
        String s = Integer.toString(value);
        StringBuffer sb = new StringBuffer(s.substring(0, s.length() - scale));
        if(scale > 0) {
            sb.append('.').append(s.substring(s.length() - scale, s.length()));
        }
        return sb.toString();
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
        value = Integer.valueOf(string).intValue();
        if(!satisfyPrecision()) throw new NumberFormatException(); 
        setNaN(false);
    }

    /**
     * @see org.jetel.data.Decimal#fromCharBuffer(java.nio.CharBuffer, java.text.NumberFormat)
     */
    public void fromCharBuffer(CharBuffer buffer, NumberFormat numberFormat) {
        if(buffer == null || buffer.length() == 0) {
            setNaN(true);
            return;
        }
        value = Integer.valueOf(buffer.toString()).intValue();
        setNaN(false);
    }

    public int compareTo(Numeric value) {
        return 0; //TODO kokon
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
            BigDecimal bd = ((Decimal) obj).getBigDecimal().setScale(scale, BigDecimal.ROUND_DOWN);
            //FIXME in 1.5 java we will call next line plus catch exception caused by rounding
            //BigDecimal bd = ((Decimal) obj).getBigDecimal().setScale(scale);
            if(HugeDecimal.precision(bd.unscaledValue()) > precision) return -1;
            //FIXME in 1.5 java we will use next line
            //if(bd.precision() > precision) return -1;
            return compareTo(bd.unscaledValue().intValue());
        } else if (obj instanceof Integer) {
            return compareTo(((Integer) obj).intValue() * TENPOWERS[scale]);
        } else if(obj instanceof Long) {
            return compareTo(((Long) obj).longValue() * TENPOWERS[scale]);
        } else if (obj instanceof Double) {
            BigDecimal bd = new BigDecimal(((Double) obj).doubleValue()).setScale(scale, BigDecimal.ROUND_DOWN); //FIXME in java 1.5 call BigDecimal.valueof(a.getDouble()) - in actual way may be in result some inaccuracies
            //FIXME in 1.5 java we will call next line plus catch exception caused by rounding; if we are rounding "equal" is impossible
            //BigDecimal bd = BigDecimal.valueOf(((Double) obj).doubleValue()).setScale(scale);
            if(HugeDecimal.precision(bd.unscaledValue()) > precision) return -1;
            //FIXME in 1.5 java we will use next line
            //if(bd.precision() > precision) return -1;
            return compareTo(bd.unscaledValue().intValue());
        } else if (obj instanceof IntegerDataField) {
            return compareTo(((IntegerDataField) obj).getInt() * TENPOWERS[scale]);
        } else if(obj instanceof LongDataField) {
            return compareTo(((LongDataField) obj).getLong() * TENPOWERS[scale]);
        } else if (obj instanceof NumericDataField) {
            BigDecimal bd = new BigDecimal(((NumericDataField) obj).getDouble()).setScale(scale, BigDecimal.ROUND_DOWN); //FIXME in java 1.5 call BigDecimal.valueof(a.getDouble()) - in actual way may be in result some inaccuracies
            //FIXME in 1.5 java we will call next line plus catch exception caused by rounding; if we are rounding "equal" is impossible
            //BigDecimal bd = BigDecimal.valueOf(((NumericDataField) obj).getDouble()).setScale(scale);
            if(HugeDecimal.precision(bd.unscaledValue()) > precision) return -1;
            //FIXME in 1.5 java we will use next line
            //if(bd.precision() > precision) return -1;
            return compareTo(bd.unscaledValue().intValue());
        } else if (obj instanceof DecimalDataField) {
            return compareTo(((DecimalDataField) obj).getValue());
        } else throw new ClassCastException("Can't compare this DecimalDataField and " + obj.getClass().getName());
    }

    private int compareTo(int compInt) {
        if (value > compInt) {
            return 1;
        } else if (value < compInt) {
            return -1;
        } else {
            return 0;
        }
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
        return !(precision < HugeDecimal.intLength(value)); 
    }
}
