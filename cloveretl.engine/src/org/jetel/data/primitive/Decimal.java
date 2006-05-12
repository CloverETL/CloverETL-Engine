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
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.text.NumberFormat;

import org.jetel.data.Numeric;

/**
 *  Clover internal decimal value representation interface.
 *
 *@author     Martin Zatopek
 *@since      November 30, 2005
 *@see        org.jetel.data.DecimalDataField
 */
public interface Decimal extends Numeric {
	
	public int getPrecision();
	
	public int getScale();

	public Decimal createCopy();
	
	public void setValue(Decimal value);
	
	public void setValue(double value);

	public void setValue(int value);

	public void setValue(long value);
	
    public void setValue(BigDecimal _value);

	public double getDouble();

	public int getInt();
	
	public long getLong();
	
	/**
	 * @return value of decimal in BigDecimal form
	 */
	public BigDecimal getBigDecimal();

    /**
     * It is output method for all decimal implementations.
     * @return modified value according to precision and scale
     */
    public BigDecimal getBigDecimalOutput();

    public void setNaN(boolean isNaN);
	
	public boolean isNaN();
	
	public void add(Numeric a);
	
	public void sub(Numeric a);

	public void mul(Numeric a);
	
	public void div(Numeric a);

	public void abs();
	
	public void mod(Numeric a);

	public void neg();

	public void serialize(ByteBuffer byteBuffer);
	
	public void deserialize(ByteBuffer byteBuffer);
	
	public int getSizeSerialized();
	
	public String toString(NumberFormat numberFormat);

	public CharBuffer toCharBuffer(NumberFormat numberFormat);
	
	public void fromString(String value, NumberFormat numberFormat);

	public void fromCharBuffer(CharBuffer value, NumberFormat numberFormat);
	
	public int compareTo(Object value); //nas numeric interface, java.lang.number a tento decimal
}
