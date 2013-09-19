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

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

/**
 * Collected methods which allow easy implementation of <code>hashCode</code>.
 * All Clover's data field types - implementations, use getHash() implementations
 * from this class; 
 * 
 * Example use case:
 * 
 * <pre>
 * public int hashCode() {
 * 	int result = HashCodeUtil.SEED;
 * 	// collect the contributions of various fields
 * 	result = HashCodeUtil.hash(result, fPrimitive);
 * 	result = HashCodeUtil.hash(result, fObject);
 * 	result = HashCodeUtil.hash(result, fArray);
 * 	return result;
 * }
 * </pre>
 * 
 * @see http://www.javapractices.com/topic/TopicAction.do?Id=28
 */
public final class HashCodeUtil {

	/**
	 * An initial value for a <code>hashCode</code>, to which is added contributions from fields. Using a non-zero value
	 * decreases collisons of <code>hashCode</code> values.
	 */
	public static final int SEED = 23;

	/**
	 * booleans.
	 */
	public static int hash(int aSeed, boolean aBoolean) {
		return firstTerm(aSeed) + (aBoolean ? 1 : 0);
	}

	/**
	 * chars.
	 */
	public static int hash(int aSeed, char aChar) {
		return firstTerm(aSeed) + (int) aChar;
	}

	/**
	 * ints.
	 */
	public static int hash(int aSeed, int aInt) {
		return firstTerm(aSeed) + aInt;
	}

	/**
	 * longs.
	 */
	public static int hash(int aSeed, long aLong) {
		return firstTerm(aSeed) + (int) (aLong ^ (aLong >>> 32));
	}

	/**
	 * floats.
	 */
	public static int hash(int aSeed, float aFloat) {
		return hash(aSeed, Float.floatToIntBits(aFloat));
	}

	/**
	 * doubles.
	 */
	public static int hash(int aSeed, double aDouble) {
		return hash(aSeed, Double.doubleToLongBits(aDouble));
	}

	/**
	 * <code>aObject</code> is a possibly-null object field, and possibly an array.
	 * 
	 * If <code>aObject</code> is an array, then each element may be a primitive or a possibly-null object.
	 */
	public static int hash(int aSeed, Object aObject) {
		int result = aSeed;
		if (aObject == null) {
			result = hash(result, 0);
		} else if (!isArray(aObject)) {
			result = hash(result, aObject.hashCode());
		} else {
			int length = Array.getLength(aObject);
			for (int idx = 0; idx < length; ++idx) {
				Object item = Array.get(aObject, idx);
				// recursive call!
				result = hash(result, item);
			}
		}
		return result;
	}

	// / PRIVATE ///
	private static final int fODD_PRIME_NUMBER = 37;

	private static int firstTerm(int aSeed) {
		return fODD_PRIME_NUMBER * aSeed;
	}

	private static boolean isArray(Object aObject) {
		return aObject.getClass().isArray();
	}
	
	//---------------------------------------------------------------------------------------
	// ---- implementations of hash functions for individual data types recognized by Clover
	
	public static final int getHash(double value){
		long v=Double.doubleToLongBits(value);
		return (int)(v^(v>>32));
	}
	
	public static final int getHash(int value){
		return value;
	}
	
	public static final int getHash(CharSequence value){
		int hash=5381;
		for (int i=0;i<value.length();i++){
			hash = ((hash << 5) + hash) + value.charAt(i); 
		}
		return (hash & 0x7FFFFFFF);
	}

	public static final int getHash(java.util.Date value){
		return value.hashCode();
	}
	
	public static final int getHash(byte[] value){
		return  Arrays.hashCode(value);
	}
	
	public static final int getHash(long value){
		return (int)(value^value>>32);
	}
	
	public static final int getHash(org.jetel.data.primitive.Decimal value){
		return value.hashCode();
	}
	
	public static final int getHash(java.math.BigDecimal value){
		return value.hashCode();
	}
	
	
	
	public static final int getHash(boolean value){
		return Boolean.valueOf(value).hashCode();
	}
	
	public static final <E> int getHash(java.util.List<E> list) {
		if (list == null)
			return 0;

		int result = 1;

		for (Object element : list)
			result = 31 * result + (element == null ? 0 : element.hashCode());

		return result;
	}
	
	public static final <K,V> int getHash(java.util.Map <K,V> map){
		if (map == null)
			return 0;

		int result = 0;
		Iterator<Map.Entry<K, V>> it = map.entrySet().iterator();
		while (it.hasNext()) {
			result += it.next().hashCode();
		}
		return result;
	}
	
	public static final int getHash(Object[] objects){
		if (objects == null)
			return 0;
		
		int result= 1;
	    for (Object element: objects){
	        result =31 *result + element.hashCode();
	    }
	    return result;
	}
}