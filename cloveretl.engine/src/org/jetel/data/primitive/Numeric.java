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
 * @author david
 * @since  22.6.2005
 *
 * TODO To change the template for this generated type comment go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
public interface Numeric {
    
    public void setValue(int value);
    
    public void setValue(long value);
    
    public void setValue(double value);
    
    public void setValue(Numeric value);
    
    public void setValue(Number value);
    
    public int getInt();
    
    public long getLong();
    
    public double getDouble();
    
    public BigDecimal getBigDecimal();
    
    public Numeric duplicateNumeric();
    
    /**
     * @return True if Number is considered to have NULL value
     */
    public boolean isNull();
    
    public void setNull();
    
    public Decimal getDecimal();

    public Decimal getDecimal(int precision, int scale);
    
    /**
     * Compares Number internal value to passed-in value
     * 
     * @param value
     * @return 	-1,0,1 if internal value(less-then,equals, greather then) passed-in value
     */
    public int compareTo(Numeric value);
    
    /**
     * Sum of two Number, this and added parameter.
     * @param a second operand of sum
     */
    public void add(Numeric a);
    
    /**
     * Difference of two Number, this and added parameter.
     * @param a second operand of difference 
     */
    public void sub(Numeric a);
    
    /**
     * Multiplication of two Number, this and added parameter.
     * @param a secondt operand of multiplikation 
     */
    public void mul(Numeric a);
    
    /**
     * Division of two Number, this and added parameter.
     * @param a second operand of division 
     */
    public void div(Numeric a);
    
    /**
     * Absolute value of this Number.
     */
    public void abs();
    
    /**
     * Rest of a divided this by <code>a</code>.
     * @param a second operand of division 
     */
    public void mod(Numeric a);
    
    /**
     * Negation/Opposite value.
     */
    public void neg();
    
}
