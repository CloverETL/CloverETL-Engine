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
package org.jetel.util;

/**
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 4.7.2012
 */
public class CompareUtils {

    /**
     * Compares two given objects, where <code>null</code> equals <code>null</code>.
     * @param a first compared object
     * @param b second compared object
     * @return true if and only if two given objects are equals ({@link Object#equals(Object)} or both objects are null
     */
    public static boolean equals(Object a, Object b) {
        if (a != null) {
            return (b != null) && a.equals(b);
        } else {
            return (b == null);
        }
    }
    
    /**
     * Null safe comparison of Comparables. null is assumed to be less than a non-null value.
     */
    public static <T extends Comparable<? super T>> int compare(T o1, T o2) {
    	if (o1 == null) {
    		if (o2 == null) {
    			return 0;
    		} else {
    			return -1;
    		}
    	} else {
    		if (o2 == null) {
    			return 1;
    		} else {
    			return o1.compareTo(o2);
    		}
    	}
    }
    
}
