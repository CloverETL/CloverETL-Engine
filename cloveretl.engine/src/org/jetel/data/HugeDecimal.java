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
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.text.NumberFormat;


/**
 *  Clover internal decimal value representation.
 * Implements Decimal interface and use BigDecimal type for store value.
 *
 *@author     Martin Zatopek
 *@since      December 1, 2005
 *@see        org.jetel.data.Decimal
 */
public class HugeDecimal implements Decimal {
    private static int roundingMode = BigDecimal.ROUND_DOWN;
	public BigDecimal value;
	private int precision;
	private int scale;
	private boolean nan;
	private long scalePow; // = 10^scale


    private static BigInteger TENPOWERS[] = {BigInteger.ONE,
            BigInteger.valueOf(10),       BigInteger.valueOf(100),
            BigInteger.valueOf(1000),     BigInteger.valueOf(10000),
            BigInteger.valueOf(100000),   BigInteger.valueOf(1000000),
            BigInteger.valueOf(10000000), BigInteger.valueOf(100000000),
            BigInteger.valueOf(1000000000)};

	public HugeDecimal(BigDecimal value, int precision, int scale, boolean nan) {
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
		return new HugeDecimal(value, precision, scale, nan);
	}

	/**
	 * @see org.jetel.data.Decimal#setValue(org.jetel.data.Decimal)
	 */
	public void setValue(Decimal decimal) {
		if(decimal == null || decimal.isNaN()) {
			setNaN(true);
			return;
		}
		if(decimal instanceof HugeDecimal) {
			HugeDecimal dec = (HugeDecimal) decimal;
			if(precision >= dec.precision && scale >= dec.scale) {
				value = dec.value;
				setNaN(false);
			} else {
				throw new RuntimeException("Incompatible type of " + this.getClass().getName() + "(" + precision + "," + scale + ") and " + dec.getClass().getName() + "(" + dec.getPrecision() + "," + dec.getScale() + ").");
			}
		} else {
			throw new RuntimeException("Can't set value " + this.getClass().getName() + " from this Decimal implementation - " + decimal.getClass().getName());
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
		value = new BigDecimal(_value);
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
		value = new BigDecimal(_value);
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
		value = new BigDecimal(_value);
		setNaN(false);
	}

	/**
	 * @see org.jetel.data.Decimal#getDouble()
	 */
	public double getDouble() {
		return value.doubleValue();
	}

	/**
	 * @see org.jetel.data.Decimal#getInt()
	 */
	public int getInt() {
		return value.intValue();
	}

	/**
	 * @see org.jetel.data.Decimal#getLong()
	 */
	public long getLong() {
		return value.longValue();
	}

	/**
	 * @see org.jetel.data.Decimal#getBigDecimal()
	 */
	public BigDecimal getBigDecimal() {
		if(isNaN()) {
			return null;
		}
		if(precision() > precision) return null;
		
		if(value.scale() == scale)
			return value;
		else if(value.scale() < scale)
			return value.setScale(scale); 
		else 
			return value.setScale(scale, BigDecimal.ROUND_DOWN);
	}

	/**
	 * Number of digit in value (BigDecimal).
	 * @return
	 */
	private int precision() {
	    BigInteger intVal = value.unscaledValue();
        if (intVal.signum() == 0)       // 0 is one decimal digit
            return 1;
        // we have a nonzero magnitude
        BigInteger work = intVal;
        int digits = 0;                 // counter
        for (;work.bitLength()>32;) {
            // here when more than one integer in the magnitude; divide
            // by a billion (reduce by 9 digits) and try again
            work = work.divide(TENPOWERS[9]);
            digits += 9;
            if (work.signum() == 0)     // the division was exact
                return digits;          // (a power of a billion)
        }
        // down to a simple nonzero integer
        digits += intLength(work.intValue());
        // System.out.println("digitLength... "+this+"  ->  "+digits);
        return digits;
	}
	
    /**
     * Returns the length of an unsigned <tt>int</tt>, in decimal digits.
     * @param i the <tt>int</tt> (treated as unsigned)
     * @return the length of the unscaled value, in decimal digits
     */
    private int intLength(int i) {
        int digits;
        if (i < 0) {            // 'negative' is 10 digits unsigned
            i *= -1;
        } 
        // positive integer
        // binary search, weighted low (maximum 4 tests)
        if (i < 10000) {
            if (i < 100) {
                if (i < 10) digits = 1;
                else digits = 2;
            } else {
                if (i < 1000) digits = 3;
                else digits = 4;
            }
        } else {
            if (i < 1000000) {
                if (i < 100000) digits = 5;
                else digits = 6;
            } else {
                if (i < 100000000) {
                    if (i < 10000000) digits = 7;
                    else digits = 8;
                } else {
                    if (i < 1000000000) digits = 9;
                    else digits = 10;
                }
            }
        }
        return digits;
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
	 * @see org.jetel.data.Decimal#add(org.jetel.data.Numeric)
	 */
	public void add(Numeric a) {
		if(a instanceof IntegerDataField || a instanceof LongDataField) {
			value = value.add(BigDecimal.valueOf(a.getLong()));
		} else if(a instanceof NumericDataField) {
			value = value.add(new BigDecimal(a.getDouble()));
		} else if(a instanceof DecimalDataField && ((DecimalDataField) a).getValue() instanceof HugeDecimal) {
			value = value.add(((HugeDecimal) ((DecimalDataField) a).getValue()).value);
		} else {
			throw new RuntimeException("Unsupported class of parameter 'add' operation (" + a.getClass().getName() + ").");
		}
	}

	/**
	 * @see org.jetel.data.Decimal#sub(org.jetel.data.Numeric)
	 */
	public void sub(Numeric a) {
		if(a instanceof IntegerDataField || a instanceof LongDataField) {
			value = value.subtract(BigDecimal.valueOf(a.getLong()));
		} else if(a instanceof NumericDataField) {
			value = value.subtract(new BigDecimal(a.getDouble()));
		} else if(a instanceof DecimalDataField && ((DecimalDataField) a).getValue() instanceof HugeDecimal) {
			value = value.subtract(((HugeDecimal) ((DecimalDataField) a).getValue()).value);
		} else {
			throw new RuntimeException("Unsupported class of parameter 'sub' operation (" + a.getClass().getName() + ").");
		}
	}

	/**
	 * @see org.jetel.data.Decimal#mul(org.jetel.data.Numeric)
	 */
	public void mul(Numeric a) {
		if(a instanceof IntegerDataField || a instanceof LongDataField) {
			value = value.multiply(BigDecimal.valueOf(a.getLong()));
		} else if(a instanceof NumericDataField) {
			value = value.multiply(new BigDecimal(a.getDouble()));
		} else if(a instanceof DecimalDataField && ((DecimalDataField) a).getValue() instanceof HugeDecimal) {
			value = value.multiply(((HugeDecimal) ((DecimalDataField) a).getValue()).value);
		} else {
			throw new RuntimeException("Unsupported class of parameter 'mul' operation (" + a.getClass().getName() + ").");
		}
	}

	/**
	 * @see org.jetel.data.Decimal#div(org.jetel.data.Numeric)
	 */
	public void div(Numeric a) {
		if(a instanceof IntegerDataField || a instanceof LongDataField) {
			value = value.divide(BigDecimal.valueOf(a.getLong()), roundingMode);
		} else if(a instanceof NumericDataField) {
			value = value.divide(new BigDecimal(a.getDouble()), roundingMode);
		} else if(a instanceof DecimalDataField && ((DecimalDataField) a).getValue() instanceof HugeDecimal) {
			value = value.divide(((HugeDecimal) ((DecimalDataField) a).getValue()).value, roundingMode);
		} else {
			throw new RuntimeException("Unsupported class of parameter 'div' operation (" + a.getClass().getName() + ").");
		}
	}

	/**
	 * @see org.jetel.data.Decimal#abs()
	 */
	public void abs() {
		value = value.abs();
	}

	/**
	 * @see org.jetel.data.Decimal#mod(org.jetel.data.Numeric)
	 */
	public void mod(Numeric a) {
		if(a instanceof IntegerDataField || a instanceof LongDataField) {
			value = remainder(BigDecimal.valueOf(a.getLong()));
		} else if(a instanceof NumericDataField) {
			value = remainder(new BigDecimal(a.getDouble()));
		} else if(a instanceof DecimalDataField && ((DecimalDataField) a).getValue() instanceof HugeDecimal) {
			value = remainder(((HugeDecimal) ((DecimalDataField) a).getValue()).value);
		} else {
			throw new RuntimeException("Unsupported class of parameter 'mod' operation (" + a.getClass().getName() + ").");
		}
	}

	/**
	 * Returns a BigDecimal whose value is (this % divisor).
	 * @param divisor value by which this BigDecimal is to be divided
	 * @return this % divisor
	 */
	private BigDecimal remainder(BigDecimal divisor) {
	    return value.subtract(value.divide(divisor, 0, roundingMode).multiply(divisor));
	}
	
	/**
	 * @see org.jetel.data.Decimal#neg()
	 */
	public void neg() {
		value = value.negate();
	}

	/**
	 * @see org.jetel.data.Decimal#serialize(java.nio.ByteBuffer)
	 */
	public void serialize(ByteBuffer byteBuffer) {
		if(isNaN()) {
			byteBuffer.put((byte) 0);
			return;
		}
		byteBuffer.put((byte) (value.unscaledValue().bitLength()/8 + 1));
		byteBuffer.put(value.unscaledValue().toByteArray());
		byteBuffer.putInt(value.scale());
	}

	/**
	 * @see org.jetel.data.Decimal#deserialize(java.nio.ByteBuffer)
	 */
	public void deserialize(ByteBuffer byteBuffer) {
		byte size = byteBuffer.get();
		if(size == 0) {
			setNaN(true);
			return;
		}
		byte[] unscaledVal = new byte[size];
		byteBuffer.get(unscaledVal);
		value = new BigDecimal(new BigInteger(unscaledVal), byteBuffer.getInt());
		setNaN(false);
	}

	/**
	 * @see org.jetel.data.Decimal#getSizeSerialized()
	 */
	public int getSizeSerialized() {
		if(isNaN()) {
			return 1;
		}
		return (value.unscaledValue().bitLength()/8 + 1) + 4 + 1; //BigInteger size + scale(4) + size of serialized form(1)
	}

	/**
	 * @see org.jetel.data.Decimal#toString(java.text.NumberFormat)
	 */
	public String toString(NumberFormat numberFormat) {
		if(isNaN()) {
			return "";
		}
		BigDecimal bd = getBigDecimal();
		if(bd == null)
			return "";
		else return bd.toString();
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
		}
		value = new BigDecimal(string);
		setNaN(false);
	}

	/**
	 * @see org.jetel.data.Decimal#fromCharBuffer(java.nio.CharBuffer, java.text.NumberFormat)
	 */
	public void fromCharBuffer(CharBuffer buffer, NumberFormat numberFormat) {
		if(buffer == null || buffer.length() == 0) {
			setNaN(true);
		}
		value = new BigDecimal(buffer.toString());
		setNaN(false);
	}

	/**
	 * @see org.jetel.data.Decimal#compareTo(java.lang.Object)
	 */
	public int compareTo(Object obj) {
		if (obj==null) return 1;
		if (isNaN()) return -1;
	    
		if (obj instanceof HugeDecimal) {
			return value.compareTo(((HugeDecimal) obj).value);
		} else if (obj instanceof Integer || obj instanceof Long) {
			return value.compareTo(BigDecimal.valueOf(((Number) obj).longValue()));
		} else if (obj instanceof Double) {
			return value.compareTo(new BigDecimal(((Number) obj).doubleValue()));
		} else if (obj instanceof IntegerDataField || obj instanceof LongDataField) {
			return value.compareTo(BigDecimal.valueOf(((Numeric) obj).getLong()));
		} else if (obj instanceof NumericDataField) {
			return value.compareTo(new BigDecimal(((Numeric) obj).getDouble()));
		} else throw new ClassCastException("Can't compare this DecimalDataField and " + obj.getClass().getName());
	}

}
