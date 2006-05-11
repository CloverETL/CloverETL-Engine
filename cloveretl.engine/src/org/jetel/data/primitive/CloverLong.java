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

import org.jetel.data.Numeric;

/**
 *  A class that represents a long integer value.<br>
 *  It is anologous java.lang.Long class but is mutable.
 *  You can change value during all live cycle of this object.
 * 
 *@author     Martin Zatopek (OpenTech s.r.o)
 *@since      December 2, 2005
 *@see        org.jetel.data.primitive.CloverInteger
 *@see        org.jetel.data.primitive.CloverDouble
 */
public class CloverLong extends Number implements Numeric {
	private long value;
	
	/**
	 * Constructor.
	 * @param value
	 */
	public CloverLong(long value) {
		this.value = value;
	}

    /**
     * Constructor.
     * @param value
     */
    public CloverLong(Numeric value) {
        setValue(value);
    }
    
	/**
	 * @see java.lang.Number#intValue()
	 */
	public int intValue() {
		return getInt();
	}

	/**
	 * @see java.lang.Number#longValue()
	 */
	public long longValue() {
		return value;
	}

	/**
	 * @see java.lang.Number#floatValue()
	 */
	public float floatValue() {
        if(value == Long.MIN_VALUE)
            return Float.MIN_VALUE;
        else
            return value;
	}

	/**
	 * @see java.lang.Number#doubleValue()
	 */
	public double doubleValue() {
		return getDouble();
	}

	/**
	 * @see org.jetel.data.Numeric#setValue(int)
	 */
	public void setValue(int value) {
        if(value == Integer.MIN_VALUE)
            this.value = Long.MIN_VALUE;
        else
            this.value = value;
	}

	/**
	 * @see org.jetel.data.Numeric#setValue(long)
	 */
	public void setValue(long value) {
		this.value = value;
	}

	/**
	 * @see org.jetel.data.Numeric#setValue(double)
	 */
	public void setValue(double value) {
        if(value == Double.NaN)
            this.value = Long.MIN_VALUE;
        else
            this.value = (long) value;
	}

    /**
     * @see org.jetel.data.Numeric#setValue(org.jetel.data.Numeric)
     */
    public void setValue(Numeric value) {
        if(value.isNull()) {
            this.value = Long.MIN_VALUE;
        } else {
            this.value = value.getLong();
        }
    }
    
	/**
	 * @see org.jetel.data.Numeric#setValue(org.jetel.data.primitive.Decimal)
	 */
	public void setValue(Decimal value) {
		this.value = value.getLong();
	}

	/**
	 * @see org.jetel.data.Numeric#getInt()
	 */
	public int getInt() {
        if(value == Long.MIN_VALUE)
            return Integer.MIN_VALUE;
        else
            return (int) value;
	}

	/**
	 * @see org.jetel.data.Numeric#getLong()
	 */
	public long getLong() {
		return value;
	}

	/**
	 * @see org.jetel.data.Numeric#getDouble()
	 */
	public double getDouble() {
        if(value == Long.MIN_VALUE)
            return Double.NaN;
        else
            return value;
	}

    /**
     * @see org.jetel.data.Numeric#duplicateNumeric()
     */
    public Numeric duplicateNumeric() {
        return new CloverLong(value);
    }
    
	/**
	 * @see org.jetel.data.Numeric#isNull()
	 */
	public boolean isNull() {
		return value == Long.MIN_VALUE;
	}

	/**
	 * @see org.jetel.data.Numeric#getDecimal()
	 */
	public Decimal getDecimal() {
		return DecimalFactory.getDecimal(value);
	}

	/**
	 * @see org.jetel.data.Numeric#getDecimal(int, int)
	 */
	public Decimal getDecimal(int precision, int scale) {
		return DecimalFactory.getDecimal(value, precision, scale);
	}

    /**
     * @see org.jetel.data.Numeric#getBigDecimal()
     */
    public BigDecimal getBigDecimal() {
        if(isNull()) 
            return null;
        else 
            return BigDecimal.valueOf(value);
    }

	/**
	 * @see org.jetel.data.Numeric#compareTo(org.jetel.data.Numeric)
	 */
	public int compareTo(Numeric value) {
	    if (isNull()) {
	        return -1;
	    }else if (value.isNull()) {
	        return 1;
	    }else {
	        return compareTo(value.getLong());
	    }
	}

	/**
	 *  Compares value to passed-in long value
	 *
	 * @param  compInt  Description of the Parameter
	 * @return          -1,0,1 if internal value(less-then,equals, greather then) passed-in value
	 */
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
	 * @see org.jetel.data.Numeric#add(org.jetel.data.Numeric)
	 */
	public void add(Numeric a) {
        if(value == Long.MIN_VALUE) return;
		value += a.getLong();
	}

	/**
	 * @see org.jetel.data.Numeric#sub(org.jetel.data.Numeric)
	 */
	public void sub(Numeric a) {
        if(value == Long.MIN_VALUE) return;
		value -= a.getLong();
	}

	/**
	 * @see org.jetel.data.Numeric#mul(org.jetel.data.Numeric)
	 */
	public void mul(Numeric a) {
        if(value == Long.MIN_VALUE) return;
		value *= a.getLong();
	}

	/**
	 * @see org.jetel.data.Numeric#div(org.jetel.data.Numeric)
	 */
	public void div(Numeric a) {
        if(value == Long.MIN_VALUE) return;
		value /= a.getLong();
	}

	/**
	 * @see org.jetel.data.Numeric#abs()
	 */
	public void abs() {
        if(value == Long.MIN_VALUE) return;
		value = Math.abs(value);
	}

	/**
	 * @see org.jetel.data.Numeric#mod(org.jetel.data.Numeric)
	 */
	public void mod(Numeric a) {
        if(value == Long.MIN_VALUE) return;
		value %= a.getLong();
	}

	/**
	 * @see org.jetel.data.Numeric#neg()
	 */
	public void neg() {
        if(value == Long.MIN_VALUE) return;
		value *= -1;
	}

    public boolean equals(Object obj) {
        if(obj instanceof Numeric)
            return compareTo((Numeric) obj) == 0;
        else return false;
    }

    public int hashCode() {
        return (int)(value^value>>32);
    }

    public String toString(){
        return String.valueOf(value);
    }
    
}
