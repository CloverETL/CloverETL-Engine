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


/**
 *  A class that represents a double value.<br>
 *  It is anologous java.lang.Double class but is mutable.
 *  You can change value during all live cycle of this object.
 * 
 *@author     Martin Zatopek, Javlin Consulting. (www.javlinconsulting.cz)
 *@since      December 2, 2005
 *@see        org.jetel.data.primitive.CloverInteger
 *@see        org.jetel.data.primitive.CloverLong
 */
public final class CloverDouble extends Number implements Numeric {
	private static final long serialVersionUID = -8855166719123357319L;
	
	private double value;
	
	/**
	 * Constructor.
	 * @param value
	 */
	public CloverDouble(double value) {
		this.value = value;
	}
	
    /**
     * Constructor.
     * @param value
     */
    public CloverDouble(Numeric value) {
        setValue(value);
    }
    
	/**
	 * @see java.lang.Number#intValue()
	 */
	@Override
	public int intValue() {
        return getInt();
	}

	/**
	 * @see java.lang.Number#longValue()
	 */
	@Override
	public long longValue() {
        return getLong();
	}

	/**
	 * @see java.lang.Number#floatValue()
	 */
	@Override
	public float floatValue() {
        if(Double.isNaN(value))
            return Float.MIN_VALUE;
        else
            return (float) value;
	}

	/**
	 * @see java.lang.Number#doubleValue()
	 */
	@Override
	public double doubleValue() {
		return value;
	}

	/**
	 * @see org.jetel.data.Numeric#setValue(int)
	 */
	@Override
	public void setValue(int value) {
        if(value == Integer.MIN_VALUE)
            this.value = Double.NaN;
        else
            this.value = value; 
	}

	/**
	 * @see org.jetel.data.Numeric#setValue(long)
	 */
	@Override
	public void setValue(long value) {
        if(value == Long.MIN_VALUE)
            this.value = Double.NaN;
        else
            this.value = value;
	}

    /**
     * @see org.jetel.data.Numeric#setValue(org.jetel.data.Numeric)
     */
    @Override
	public void setValue(Numeric value) {
        if(value.isNull()) {
            this.value = Double.NaN;
        } else {
            this.value = value.getDouble();
        }
    }
    
	/**
	 * @see org.jetel.data.Numeric#setValue(double)
	 */
	@Override
	public void setValue(double value) {
		this.value = value;
	}

    /**
     * @see org.jetel.data.Numeric#setValue(org.jetel.data.Numeric)
     */
    @Override
	public void setValue(Number value) {
        setValue(value.doubleValue());
    }
    
    
	/**
	 * @see org.jetel.data.Numeric#getInt()
	 */
	@Override
	public int getInt() {
        if(Double.isNaN(value))
            return Integer.MIN_VALUE;
        else
            return (int) value;
	}

	/**
	 * @see org.jetel.data.Numeric#getLong()
	 */
	@Override
	public long getLong() {
        if(Double.isNaN(value))
            return Long.MIN_VALUE;
        else
            return (long) value;
	}

	/**
	 * @see org.jetel.data.Numeric#getDouble()
	 */
	@Override
	public double getDouble() {
        return value;
	}

	/**
	 * @see org.jetel.data.Numeric#isNull()
	 */
	@Override
	public boolean isNull() {
		return Double.isNaN(value);
	}

    @Override
	public void setNull(){
        value=Double.NaN;
    }
    
    
	/**
	 * @see org.jetel.data.Numeric#getDecimal()
	 */
	@Override
	public Decimal getDecimal() {
		return DecimalFactory.getDecimal(value);
	}

	/**
	 * @see org.jetel.data.Numeric#getDecimal(int, int)
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
        if(isNull()) 
            return null;
        else 
            return BigDecimal.valueOf(value);
    }
    
    /**
     * @see org.jetel.data.Numeric#duplicateNumeric()
     */
    @Override
	public Numeric duplicateNumeric() {
        return new CloverDouble(value);
    }
    
	/**
	 * @see org.jetel.data.Numeric#compareTo(org.jetel.data.Numeric)
	 */
	@Override
	public int compareTo(Numeric value) {
	    if (isNull()) {
	        return -1;
	    }else if (value.isNull()) {
	        return 1;
	    }else {
	        return compareTo(value.getDouble());
	    }
	}

	/**
	 *  Compares value to passed-in double value
	 *
	 * @param  compInt  Description of the Parameter
	 * @return          -1,0,1 if internal value(less-then,equals, greather then) passed-in value
	 */
	private int compareTo(double compDouble) {
		return Double.compare(value, compDouble);
	}

	/**
	 * @see org.jetel.data.Numeric#add(org.jetel.data.Numeric)
	 */
	@Override
	public void add(Numeric a) {
	    value += a.getDouble();
	}

	/**
	 * @see org.jetel.data.Numeric#sub(org.jetel.data.Numeric)
	 */
	@Override
	public void sub(Numeric a) {
	    value -= a.getDouble();
	}

	/**
	 * @see org.jetel.data.Numeric#mul(org.jetel.data.Numeric)
	 */
	@Override
	public void mul(Numeric a) {
	    value *= a.getDouble();
	}

	/**
	 * @see org.jetel.data.Numeric#div(org.jetel.data.Numeric)
	 */
	@Override
	public void div(Numeric a) {
	    value /= a.getDouble();
	}

	/**
	 * @see org.jetel.data.Numeric#abs()
	 */
	@Override
	public void abs() {
	    value = Math.abs(value);
	}

	/**
	 * @see org.jetel.data.Numeric#mod(org.jetel.data.Numeric)
	 */
	@Override
	public void mod(Numeric a) {
	    value %= a.getDouble();
	}

	/**
	 * @see org.jetel.data.Numeric#neg()
	 */
	@Override
	public void neg() {
		value *= -1;
	}
    
    @Override
	public boolean equals(Object obj) {
        if(obj instanceof Numeric)
            return compareTo((Numeric) obj) == 0;
        else return false;
    }

    @Override
	public int hashCode() {
        long v = Double.doubleToLongBits(value);
        return (int)(v^(v >> 32));
    }
    
	@Override
	public String toString(){
	    return Double.toString(value);
    }
}
