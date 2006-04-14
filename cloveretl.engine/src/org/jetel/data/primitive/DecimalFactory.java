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

import org.jetel.data.Defaults;
import org.jetel.data.Defaults.DataFieldMetadata;




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
    private static final double MAGIC_CONST = Math.log(10)/Math.log(2);
    private static final int BOUNDS_FOR_DECIMAL_IMPLEMENTATION = (int) Math.floor((Integer.SIZE - 1) / MAGIC_CONST);
	
    public static Decimal getDecimal(int value) {
		Decimal d = getDecimal();
		d.setValue(value);
		return d;
	}

	public static Decimal getDecimal(int value, int precision, int scale) {
		Decimal d = getDecimal(precision, scale);
		d.setValue(value);
		return d;
	}

	public static Decimal getDecimal(double value) {
		Decimal d = getDecimal();
		d.setValue(value);
		return d;
	}

	public static Decimal getDecimal(double value, int precision, int scale) {
		Decimal d = getDecimal(precision, scale);
		d.setValue(value);
		return d;
	}

	public static Decimal getDecimal(long value) {
		Decimal d = getDecimal();
		d.setValue(value);
		return d;
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
        if(precision <= BOUNDS_FOR_DECIMAL_IMPLEMENTATION) {
            return new IntegerDecimal(precision, scale);
        }
		return new HugeDecimal(null, precision, scale, true);
	}
}
