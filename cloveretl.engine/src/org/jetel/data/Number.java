/*
 *    jETeL/Clover - Java based ETL application framework.
 *    Copyright (C) 2002-05  David Pavlis <david_pavlis@hotmail.com> and others.
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
 * Created on 22.6.2005
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package org.jetel.data;

/**
 * @author david
 * @since  22.6.2005
 *
 * TODO To change the template for this generated type comment go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
public interface Number {
    
    public void setValue(int value);
    
    public void setValue(long value);
    
    public void setValue(double value);
    
    // public void setValue(Decimal value);
    
    public int getInt();
    
    public long getLong();
    
    public double getDouble();
    
    // public Decimal getDecimal();
    
    /**
     * Compares Number internal value to passed-in value
     * 
     * @param value
     * @return 	-1,0,1 if internal value(less-then,equals, greather then) passed-in value
     */
    public int compareTo(Number value);
}
