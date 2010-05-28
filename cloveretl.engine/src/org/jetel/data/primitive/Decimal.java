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
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetEncoder;


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
	
	
    public void setValue(BigDecimal _value);
	
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
	
	public void serialize(ByteBuffer byteBuffer);
	
	public void deserialize(ByteBuffer byteBuffer);
	
	public int getSizeSerialized();
	
	public String toString(NumericFormat numericFormat);

    public void toByteBuffer(ByteBuffer dataBuffer, CharsetEncoder encoder, NumericFormat numericFormat) throws CharacterCodingException;

    public void toByteBuffer(ByteBuffer dataBuffer);
	
	public void fromString(CharSequence seq, NumericFormat numericFormat);

	public int compareTo(Object value); //nas numeric interface, java.lang.number a tento decimal
}
