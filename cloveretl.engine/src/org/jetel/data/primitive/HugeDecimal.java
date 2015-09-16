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
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetEncoder;
import java.text.ParseException;

import org.jetel.data.DecimalDataField;
import org.jetel.data.IntegerDataField;
import org.jetel.data.LongDataField;
import org.jetel.data.NumericDataField;
import org.jetel.exception.BadDataFormatException;
import org.jetel.util.bytes.ByteBufferUtils;
import org.jetel.util.bytes.CloverBuffer;
import org.jetel.util.formatter.NumericFormatter;
import org.jetel.util.formatter.NumericFormatterFactory;


/**
 *  Clover internal decimal value representation.
 * Implements Decimal interface and use BigDecimal type for store value.
 *
 *@author     Martin Zatopek
 *@since      December 1, 2005
 *@see        org.jetel.data.primitive.Decimal
 */
public final class HugeDecimal implements Decimal {
    private static RoundingMode ROUNDING_MODE = RoundingMode.HALF_UP;
	private BigDecimal value;
	private final int precision;
	private final int scale;
	private boolean nan;

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
		return new HugeDecimal(value, precision, scale, nan);
	}

	@Override
	public void setValue(double _value) {
		if(Double.isNaN(_value)) {
			setNaN(true);
			return;
		}
		value = BigDecimal.valueOf(_value);
        setNaN(false);
        if(!satisfyPrecision()) {
            setNaN(true);
            throw new OutOfPrecisionException("Number is out of available precision ["+precision+","+scale+"], value: " + _value);
        }
	}

	@Override
	public void setValue(int _value) {
		if(_value == Integer.MIN_VALUE) {
			setNaN(true);
			return;
		}
		value = BigDecimal.valueOf(_value);
        setNaN(false);
        if(!satisfyPrecision()) {
            setNaN(true);
            throw new OutOfPrecisionException("Number is out of available precision ["+precision+","+scale+"], value: " + _value);
        }
	}

	@Override
	public void setValue(long _value) {
		if(_value == Long.MIN_VALUE) {
			setNaN(true);
			return;
		}
		value = BigDecimal.valueOf(_value);
        setNaN(false);
        if(!satisfyPrecision()) {
            setNaN(true);
            throw new OutOfPrecisionException("Number is out of available precision ["+precision+","+scale+"], value: " + _value);
        }
		setNaN(false);
	}

	@Override
    public void setValue(Numeric _value) {
        if(_value == null || _value.isNull()) {
            setNaN(true);
            return;
        }
        value = _value.getBigDecimal();
        setNaN(false);
        if(!satisfyPrecision()) {
            setNaN(true);
            throw new OutOfPrecisionException("Number is out of available precision ["+precision+","+scale+"], value: " + (_value.getBigDecimal() != null ? _value.getBigDecimal() : _value));
        }
    }

	@Override
    public void setValue(BigDecimal _value) {
        if(_value == null) {
            setNaN(true);
            return;
        }
        value = _value;
        setNaN(false);
        if(!satisfyPrecision()) {
            setNaN(true);
            throw new OutOfPrecisionException("Number is out of available precision ["+precision+","+scale+"], value: " + _value);
        }
    }
    
	@Override
    public void setValue(Number value) {
       if (value instanceof Long){
           setValue(value.longValue());
       }else if (value instanceof Integer){
           setValue(value.intValue());
       }else if (value instanceof BigDecimal){
    	   setValue((BigDecimal) value);
       }else{
           setValue(value.doubleValue());
       }
    }
    
	@Override
	public double getDouble() {
        if(isNaN()) {
            return Double.NaN;
        }
		return value.doubleValue();
	}

	@Override
	public int getInt() {
        if(isNaN()) {
            return Integer.MIN_VALUE;
        }
		return value.intValue();
	}

	@Override
	public long getLong() {
        if(isNaN()) {
            return Long.MIN_VALUE;
        }
		return value.longValue();
	}

	@Override
	public BigDecimal getBigDecimal() {
		if(isNaN()) {
			return null;
		}
		return value;
	}

	@Override
    public BigDecimal getBigDecimalOutput() {
        if(isNaN()) {
            return null;
        }
        if(!satisfyPrecision()) return null;
        
        if(value.scale() == scale)
            return value;
        else
            return value.setScale(scale, BigDecimal.ROUND_DOWN);
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

    /**
	 * Number of digit in value (BigDecimal).
	 * @return
	 */
	public static int precision(BigInteger intVal) { //TODO move this static method in some utils class
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
    public static int intLength(int i) { //TODO move this static method in some utils class
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
     * Returns the length of an unsigned <tt>long</tt>, in decimal digits.
     * @param i the <tt>long</tt> (treated as unsigned)
     * @return the length of the unscaled value, in decimal digits
     */
    public static int longLength(long l) { //TODO move this static method in some utils class
        int digits;
        if (l < 0) {            // 'negative' is 10 digits unsigned
            l *= -1;
        } 
        // positive long
        // binary search, weighted low (maximum 5 tests)
        if (l < 1000000000) {
            if (l < 10000) {
                if (l < 100) {
                    if (l < 10) digits = 1;
                    else digits = 2;
                }
                else {
                    if (l < 1000) digits = 3;
                    else digits = 4;
                }
            } else {
                if (l < 1000000) {
                    if (l < 100000) digits = 5;
                    else digits = 6;
                } else {
                    if (l < 10000000) digits = 7;
                    else {
                        if (l < 100000000) digits = 8;
                        else digits = 9;
                    }
                }
            }
        } else {
            if (l < 100000000000000L) {
                if (l < 100000000000L) {
                    if (l < 10000000000L) digits = 10;
                    else digits = 11;
                } else {
                    if (l < 1000000000000L) digits = 12;
                    else {
                        if (l < 10000000000000L) digits = 13;
                        else digits = 14;
                    }
                }
            } else {
                if (l < 10000000000000000L) {
                    if (l < 1000000000000000L) digits = 15;
                    else digits = 16;
                } else {
                    if (l < 100000000000000000L) {
                        digits = 17;
                    } else {
                        if (l < 1000000000000000000L) digits = 18;
                        else digits = 19;
                    }
                }
            }
        }
        return digits;
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

	@Override
    public void setNull(){
        setNaN(true);
    }
    
	@Override
	public void add(Numeric a) {
        if(isNull()) return;
        if(a.isNull()) setNaN(true);
		if(a instanceof IntegerDataField || a instanceof CloverInteger) {
            value = value.add(BigDecimal.valueOf(a.getInt()));
        } else  if(a instanceof LongDataField || a instanceof CloverLong) {
			value = value.add(BigDecimal.valueOf(a.getLong()));
		} else if(a instanceof NumericDataField || a instanceof CloverDouble) {
			value = value.add(BigDecimal.valueOf(a.getDouble()));
		} else if(a instanceof DecimalDataField) {
            value = value.add(a.getDecimal().getBigDecimal());
        } else if(a instanceof Decimal) {
            value = value.add(a.getBigDecimal());
		} else {
			throw new RuntimeException("Unsupported class of parameter 'add' operation (" + a.getClass().getName() + ").");
		}
	}

	@Override
	public void sub(Numeric a) {
        if(isNull()) return;
        if(a.isNull()) setNaN(true);
		if(a instanceof IntegerDataField || a instanceof CloverInteger) {
            value = value.subtract(BigDecimal.valueOf(a.getInt()));
        } else if(a instanceof LongDataField || a instanceof CloverLong) {
			value = value.subtract(BigDecimal.valueOf(a.getLong()));
		} else if(a instanceof NumericDataField || a instanceof CloverDouble) {
			value = value.subtract(BigDecimal.valueOf(a.getDouble()));
		} else if(a instanceof DecimalDataField) {
			value = value.subtract(a.getDecimal().getBigDecimal());
        } else if(a instanceof Decimal) {
            value = value.subtract(a.getBigDecimal());
		} else {
			throw new RuntimeException("Unsupported class of parameter 'sub' operation (" + a.getClass().getName() + ").");
		}
	}

	@Override
	public void mul(Numeric a) {
        if(isNull()) return;
        if(a.isNull()) setNaN(true);
		if(a instanceof IntegerDataField || a instanceof CloverInteger) {
            value = value.multiply(BigDecimal.valueOf(a.getInt()));
        } else if(a instanceof LongDataField || a instanceof CloverLong) {
			value = value.multiply(BigDecimal.valueOf(a.getLong()));
		} else if(a instanceof NumericDataField || a instanceof CloverDouble) {
			value = value.multiply(BigDecimal.valueOf(a.getDouble()));
		} else if(a instanceof DecimalDataField) {
			value = value.multiply(a.getDecimal().getBigDecimal());
        } else if(a instanceof Decimal) {
            value = value.multiply(a.getBigDecimal());
		} else {
			throw new RuntimeException("Unsupported class of parameter 'mul' operation (" + a.getClass().getName() + ").");
		}
	}

	@Override
	public void div(Numeric a) {
        if(isNull()) return;
        if(a.isNull()) setNaN(true);
		if(a instanceof IntegerDataField || a instanceof CloverInteger) {
            value = value.divide(BigDecimal.valueOf(a.getInt()), scale, ROUNDING_MODE);
        } else if(a instanceof LongDataField || a instanceof CloverLong) {
			value = value.divide(BigDecimal.valueOf(a.getLong()), scale, ROUNDING_MODE);
		} else if(a instanceof NumericDataField || a instanceof CloverDouble) {
			value = value.divide(BigDecimal.valueOf(a.getDouble()), scale, ROUNDING_MODE);
		} else if(a instanceof DecimalDataField) {
			value = value.divide(a.getDecimal().getBigDecimal(), scale, ROUNDING_MODE);
        } else if(a instanceof Decimal) {
            value = value.divide(a.getBigDecimal(), scale, ROUNDING_MODE);
		} else {
			throw new RuntimeException("Unsupported class of parameter 'div' operation (" + a.getClass().getName() + ").");
		}
	}

	@Override
	public void abs() {
        if(isNull()) return;
		value = value.abs();
	}

	@Override
	public void mod(Numeric a) {
        if(isNull()) return;
        if(a.isNull()) setNaN(true);
		if(a instanceof IntegerDataField || a instanceof CloverInteger) {
            value = value.remainder(BigDecimal.valueOf(a.getInt()));
        } else if(a instanceof LongDataField || a instanceof CloverLong) {
			value = value.remainder(BigDecimal.valueOf(a.getLong()));
		} else if(a instanceof NumericDataField || a instanceof CloverDouble) {
			value = value.remainder(BigDecimal.valueOf(a.getDouble()));
		} else if(a instanceof DecimalDataField) {
			value = value.remainder(a.getDecimal().getBigDecimal());
        } else if(a instanceof Decimal) {
            value = value.remainder(a.getBigDecimal());
		} else {
			throw new RuntimeException("Unsupported class of parameter 'mod' operation (" + a.getClass().getName() + ").");
		}
	}

	@Override
	public void neg() {
        if(isNull()) return;
		value = value.negate();
	}

	@Override
	public void serialize(CloverBuffer byteBuffer) {
		try {
			if(isNaN()) {
				byteBuffer.put((byte) 0);
				return;
			}
	        byte[] bytes = value.unscaledValue().toByteArray();
	        ByteBufferUtils.encodeLength(byteBuffer, bytes.length);
			byteBuffer.put(bytes);
			byteBuffer.putInt(value.scale());
    	} catch (BufferOverflowException e) {
    		throw new RuntimeException("The size of data buffer is only " + byteBuffer.maximumCapacity() + ". Set appropriate parameter in defaultProperties file.", e);
    	}
	}

	@Override
	public void deserialize(CloverBuffer byteBuffer) {
		int size = ByteBufferUtils.decodeLength(byteBuffer);
		if (size == 0) {
			setNaN(true);
			return;
		}
		byte[] unscaledVal = new byte[size];
		byteBuffer.get(unscaledVal);
		value = new BigDecimal(new BigInteger(unscaledVal), byteBuffer.getInt());
		setNaN(false);
	}

	@Override
	public int getSizeSerialized() {
		if(isNaN()) {
			return 1;
		}
		return ((value.unscaledValue().bitLength() / 8) + 1) + 4 + 1; //BigInteger size + scale(4) + size of serialized form(1)
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
    public void toByteBuffer(CloverBuffer dataBuffer, CharsetEncoder encoder, NumericFormatter numericFormatter) throws CharacterCodingException {
    	try {
    		dataBuffer.put(encoder.encode(CharBuffer.wrap(toString(numericFormatter))));
    	} catch (BufferOverflowException e) {
			throw new RuntimeException("The size of data buffer is only " + dataBuffer.maximumCapacity() + ". Set appropriate parameter in defaultProperties file.", e);
    	}
	}

	@Override
    public void toByteBuffer(CloverBuffer dataBuffer) {
        if(!isNaN()) {
        	try {
        		dataBuffer.put(value.unscaledValue().toByteArray());
        		dataBuffer.putInt(value.scale());
        	} catch (BufferOverflowException e) {
    			throw new RuntimeException("The size of data buffer is only " + dataBuffer.maximumCapacity() + ". Set appropriate parameter in defaultProperties file.", e);
        	}
        }
    }
    
	@Override
	public void fromString(CharSequence source, NumericFormatter numericFormatter) {
		if(source == null || source.length() == 0) {
			setNaN(true);
            return;
		}
		try {
			setValue(numericFormatter.parseBigDecimal(source));
		} catch (ParseException e) {
			throw new BadDataFormatException("HugeDecimal cannot represent '" + source + "' value.", e);
		}
	}

	@Override
    public int compareTo(Numeric value) {
        if (isNull()) {
            return -1;
        }else if (value == null || value.isNull()) {
            return 1;
        }else {
            return compareTo((Object) value.getDecimal());
        }
    }

	@Override
	public int compareTo(Object obj) {
		if (obj==null) return 1;
		if (isNaN()) return -1;
	    
		if (obj instanceof BigDecimal){
			return value.compareTo((BigDecimal)obj);
		}else if (obj instanceof Decimal) {
			return value.compareTo(((Decimal) obj).getBigDecimal());
		} else if (obj instanceof Integer) {
            return value.compareTo(BigDecimal.valueOf(((Integer) obj).intValue()));
        } else if(obj instanceof Long) {
			return value.compareTo(BigDecimal.valueOf(((Long) obj).longValue()));
		} else if (obj instanceof Double) {
			return value.compareTo(BigDecimal.valueOf(((Double) obj).doubleValue()));
		} else if (obj instanceof IntegerDataField) {
            return value.compareTo(BigDecimal.valueOf(((IntegerDataField) obj).getInt()));
        } else if(obj instanceof LongDataField) {
			return value.compareTo(BigDecimal.valueOf(((LongDataField) obj).getLong()));
		} else if (obj instanceof NumericDataField) {
			return value.compareTo(BigDecimal.valueOf(((NumericDataField) obj).getDouble()));
        } else if (obj instanceof DecimalDataField) {
            return compareTo(((DecimalDataField) obj).getValue());
		} else throw new ClassCastException("Can't compare this DecimalDataField and " + obj.getClass().getName());
	}

    /**
     * Check if stored value is in dimension defined by precision.
     * @return true if value is shorter than precision; false else
     */
    public boolean satisfyPrecision() {
        if(isNaN()) return true;
        return !(HugeDecimal.precision(value.setScale(scale, BigDecimal.ROUND_DOWN).unscaledValue()) > precision);
    }

	@Override
    public boolean equals(Object obj) {
        if(obj instanceof Numeric)
            return compareTo((Numeric) obj) == 0;
        else return false;
    }
    
	@Override
    public int hashCode() {
        if(isNaN()) return Integer.MIN_VALUE; //FIXME ???
        return value.hashCode();
    }
}
