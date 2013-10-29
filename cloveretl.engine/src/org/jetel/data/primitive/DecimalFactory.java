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

import org.jetel.data.Defaults;




/**
 *  Factory class produce implementations of Decimal interface.
 *
 *@author     Martin Zatopek
 *@since      November 30, 2005
 *@see        org.jetel.data.primitive.Decimal
 */
public class DecimalFactory {
    
    /**
     * How many decimal digits correspond to binary digits
     */
    private static final int BOUNDS_FOR_DECIMAL_IMPLEMENTATION = 18;
	
    public static Decimal getDecimal(int value) {
    	//length must be big enough to fit even the longest integer
		Decimal d = getDecimal(10 + Defaults.DataFieldMetadata.DECIMAL_SCALE, Defaults.DataFieldMetadata.DECIMAL_SCALE);
		d.setValue(value);
		return d;
	}

	public static Decimal getDecimal(int value, int precision, int scale) {
		Decimal d = getDecimal(precision, scale);
		d.setValue(value);
		return d;
	}

	public static Decimal getDecimal(double value) {
        BigDecimal bd = BigDecimal.valueOf(value);
        Decimal d = getDecimal(bd.precision(), bd.scale());
        d.setValue(bd);
		return d;
	}

	public static Decimal getDecimal(double value, int precision, int scale) {
        BigDecimal bd = BigDecimal.valueOf(value);
		Decimal d = getDecimal(precision, scale);
		d.setValue(bd);
		return d;
	}

	public static Decimal getDecimal(long value) {
		//length must be big enough to fit even the longest long
		Decimal d = getDecimal(19 + Defaults.DataFieldMetadata.DECIMAL_SCALE, Defaults.DataFieldMetadata.DECIMAL_SCALE);
		d.setValue(value);
		return d;
	}

    public static Decimal getDecimal(BigDecimal value) {
        Decimal d = getDecimal(value.precision(), value.scale());
        d.setValue(value);
        return d;
    }
    
    public static Decimal getDecimal(String value) {
        BigDecimal bd= new BigDecimal(value);
        return getDecimal(bd);
    }
    
	public static Decimal getDecimal(long value, int precision, int scale) {
		Decimal d = getDecimal(precision, scale);
		d.setValue(value);
		return d;
	}

    public static Decimal getDecimal(Decimal value, int precision, int scale) {
        Decimal d = getDecimal(precision, scale);
        d.setValue(value);
        return d;
    }

	public static Decimal getDecimal() {
		return getDecimal(Defaults.DataFieldMetadata.DECIMAL_LENGTH, Defaults.DataFieldMetadata.DECIMAL_SCALE);
	}

	public static Decimal getDecimal(int precision, int scale) {
        if(precision <= BOUNDS_FOR_DECIMAL_IMPLEMENTATION && Math.abs(scale) <= BOUNDS_FOR_DECIMAL_IMPLEMENTATION) {
            return new IntegerDecimal(precision, scale);
        }
		return new HugeDecimal(null, precision, scale, true);
	}
}
