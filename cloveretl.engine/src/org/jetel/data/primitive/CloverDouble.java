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

import org.jetel.data.Numeric;

/**
 *  A class that represents a double value.<br>
 *  It is anologous java.lang.Double class but is mutable.
 *  You can change value during all live cycle of this object.
 * 
 *@author     Martin Zatopek (OpenTech s.r.o)
 *@since      December 2, 2005
 *@see        org.jetel.data.primitive.CloverInteger
 *@see        org.jetel.data.primitive.CloverLong
 */
public class CloverDouble extends Number implements Numeric {
	private double value;
	
	/**
	 * Constructor.
	 * @param value
	 */
	public CloverDouble(double value) {
		this.value = value;
	}
	
	/**
	 * @see java.lang.Number#intValue()
	 */
	public int intValue() {
		return (int) value;
	}

	/**
	 * @see java.lang.Number#longValue()
	 */
	public long longValue() {
		return (long) value;
	}

	/**
	 * @see java.lang.Number#floatValue()
	 */
	public float floatValue() {
		return (float) value;
	}

	/**
	 * @see java.lang.Number#doubleValue()
	 */
	public double doubleValue() {
		return value;
	}

	/**
	 * @see org.jetel.data.Numeric#setValue(int)
	 */
	public void setValue(int value) {
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
		this.value = value;
	}

	/**
	 * @see org.jetel.data.Numeric#setValue(org.jetel.data.primitive.Decimal)
	 */
	public void setValue(Decimal value) {
		this.value = value.getDouble();
	}

	/**
	 * @see org.jetel.data.Numeric#getInt()
	 */
	public int getInt() {
		return (int) value;
	}

	/**
	 * @see org.jetel.data.Numeric#getLong()
	 */
	public long getLong() {
		return (long) value;
	}

	/**
	 * @see org.jetel.data.Numeric#getDouble()
	 */
	public double getDouble() {
		return value;
	}

	/**
	 * @see org.jetel.data.Numeric#isNull()
	 */
	public boolean isNull() {
		return value == Double.NaN;
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
	 * @see org.jetel.data.Numeric#compareTo(org.jetel.data.Numeric)
	 */
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
	public int compareTo(double compDouble) {
		return Double.compare(value, compDouble);
	}

	/**
	 * @see org.jetel.data.Numeric#add(org.jetel.data.Numeric)
	 */
	public void add(Numeric a) {
		value += a.getDouble();
	}

	/**
	 * @see org.jetel.data.Numeric#sub(org.jetel.data.Numeric)
	 */
	public void sub(Numeric a) {
		value -= a.getDouble();
	}

	/**
	 * @see org.jetel.data.Numeric#mul(org.jetel.data.Numeric)
	 */
	public void mul(Numeric a) {
		value *= a.getDouble();
	}

	/**
	 * @see org.jetel.data.Numeric#div(org.jetel.data.Numeric)
	 */
	public void div(Numeric a) {
		value /= a.getDouble();
	}

	/**
	 * @see org.jetel.data.Numeric#abs()
	 */
	public void abs() {
		value = Math.abs(value);
	}

	/**
	 * @see org.jetel.data.Numeric#mod(org.jetel.data.Numeric)
	 */
	public void mod(Numeric a) {
		value %= a.getDouble();
	}

	/**
	 * @see org.jetel.data.Numeric#neg()
	 */
	public void neg() {
		value *= -1;
	}

}
