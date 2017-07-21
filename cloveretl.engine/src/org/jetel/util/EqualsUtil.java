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
 * Collected methods which allow easy implementation of <code>equals</code>.
 * 
 * Example use case in a class called Car:
 * 
 * <pre>
 * public boolean equals(Object aThat) {
 * 	if (this == aThat)
 * 		return true;
 * 	if (!(aThat instanceof Car))
 * 		return false;
 * 	Car that = (Car) aThat;
 * 	return EqualsUtil.areEqual(this.fName, that.fName) &amp;&amp; EqualsUtil.areEqual(this.fNumDoors, that.fNumDoors) &amp;&amp; EqualsUtil.areEqual(this.fGasMileage, that.fGasMileage) &amp;&amp; EqualsUtil.areEqual(this.fColor, that.fColor) &amp;&amp; Arrays.equals(this.fMaintenanceChecks, that.fMaintenanceChecks); // array!
 * }
 * </pre>
 * 
 * <em>Arrays are not handled by this class</em>. This is because the <code>Arrays.equals</code> methods should be used
 * for array fields.
 * 
 * @see http://www.javapractices.com/topic/TopicAction.do?Id=17
 */
public final class EqualsUtil {

	static public boolean areEqual(boolean aThis, boolean aThat) {
		return aThis == aThat;
	}

	static public boolean areEqual(char aThis, char aThat) {
		return aThis == aThat;
	}

	static public boolean areEqual(long aThis, long aThat) {
		return aThis == aThat;
	}

	static public boolean areEqual(float aThis, float aThat) {
		return Float.floatToIntBits(aThis) == Float.floatToIntBits(aThat);
	}

	static public boolean areEqual(double aThis, double aThat) {
		return Double.doubleToLongBits(aThis) == Double.doubleToLongBits(aThat);
	}

	/**
	 * Possibly-null object field.
	 * 
	 * Includes type-safe enumerations and collections, but does not include arrays. See class comment.
	 */
	static public boolean areEqual(Object aThis, Object aThat) {
		return aThis == null ? aThat == null : aThis.equals(aThat);
	}
	
}