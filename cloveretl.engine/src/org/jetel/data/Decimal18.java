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
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.text.NumberFormat;

/**
 *  Clover internal decimal value representation.
 * Implements Decimal interface and use long type for store value.
 *
 *@author     Martin Zatopek
 *@since      November 30, 2005
 *@see        org.jetel.data.Decimal
 */
public class Decimal18 implements Decimal {
	private long value;
	private int precision;
	private int scale;
	private boolean nan;
	private long scalePow; // = 10^scale
	
	/**
	 * Constructor.
	 * @param value
	 * @param precision 
	 * @param scale
	 */
	public Decimal18(long value, int precision, int scale, boolean nan) {
		this.value = value;
		this.precision = precision;
		this.scale = scale;
		this.nan = nan;

		scalePow = 1;
		for(int i = 0; i < scale; i++) {
			scalePow *= 10;
		}
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
		return new Decimal18(value, precision, scale, nan);
	}

	/**
	 * @see org.jetel.data.Decimal#setValue(org.jetel.data.Decimal)
	 */
	public void setValue(Decimal decimal) {
		if(decimal.isNaN()) {
			setNaN(true);
			return;
		}
		if(decimal instanceof Decimal18) {
			Decimal18 dec = (Decimal18) decimal;
			if(precision == dec.precision && scale == dec.scale) {
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
		_value *= scalePow;
		value = (long) _value;
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
		_value *= scalePow;
		value = (long) _value;
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
		value = _value * scalePow;
		setNaN(false);
	}

	/**
	 * @see org.jetel.data.Decimal#getDouble()
	 */
	public double getDouble() {
		return ((double) value) / scalePow;
	}

	/**
	 * @see org.jetel.data.Decimal#getInt()
	 */
	public int getInt() {
		return (int) (value / scalePow);
	}

	/**
	 * @see org.jetel.data.Decimal#getLong()
	 */
	public long getLong() {
		return value / scalePow;
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.Decimal#getBigDecimal()
	 */
	public BigDecimal getBigDecimal() {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * @see org.jetel.data.Decimal#setNaN(boolean)
	 */
	public void setNaN(boolean isNaN) {
		this.nan = isNaN; 
	}

	/**
	 * @see org.jetel.data.Decimal#isNaN()
	 */
	public boolean isNaN() {
		return nan;
	}

	/*
	 * @see org.jetel.data.Decimal#add(org.jetel.data.Numeric)
	 */
	public void add(Numeric a) {
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.Decimal#sub(org.jetel.data.Numeric)
	 */
	public void sub(Numeric a) {
		// TODO Auto-generated method stub
		
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.Decimal#mul(org.jetel.data.Numeric)
	 */
	public void mul(Numeric a) {
		// TODO Auto-generated method stub
		
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.Decimal#div(org.jetel.data.Numeric)
	 */
	public void div(Numeric a) {
		// TODO Auto-generated method stub
		
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.Decimal#abs()
	 */
	public void abs() {
		// TODO Auto-generated method stub
		
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.Decimal#mod(org.jetel.data.Numeric)
	 */
	public void mod(Numeric a) {
		// TODO Auto-generated method stub
		
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.Decimal#neg()
	 */
	public void neg() {
		// TODO Auto-generated method stub
		
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.Decimal#serialize(java.nio.ByteBuffer)
	 */
	public void serialize(ByteBuffer byteBuffer) {
		// TODO Auto-generated method stub
		
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.Decimal#deserialize(java.nio.ByteBuffer)
	 */
	public void deserialize(ByteBuffer byteBuffer) {
		// TODO Auto-generated method stub
		
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.Decimal#getSizeSerialized()
	 */
	public int getSizeSerialized() {
		// TODO Auto-generated method stub
		return 0;
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.Decimal#toString(java.text.NumberFormat)
	 */
	public String toString(NumberFormat numberFormat) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.Decimal#toCharBuffer(java.text.NumberFormat)
	 */
	public CharBuffer toCharBuffer(NumberFormat numberFormat) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.Decimal#fromString(java.lang.String, java.text.NumberFormat)
	 */
	public void fromString(String value, NumberFormat numberFormat) {
		// TODO Auto-generated method stub
		
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.Decimal#fromCharBuffer(java.nio.CharBuffer, java.text.NumberFormat)
	 */
	public void fromCharBuffer(CharBuffer value, NumberFormat numberFormat) {
		// TODO Auto-generated method stub
		
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.Decimal#compareTo(java.lang.Object)
	 */
	public int compareTo(Object value) {
		// TODO Auto-generated method stub
		return 0;
	}


}
