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
	public static int hash(boolean aBoolean) {
		return hash(SEED, aBoolean);
	}

	/**
	 * booleans.
	 */
	public static int hash(int aSeed, boolean aBoolean) {
		return firstTerm(aSeed) + (aBoolean ? 1231 : 1237);
	}

	/**
	 * chars.
	 */
	public static int hash(char aChar) {
		return hash(SEED, aChar);
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
	public static int hash(int aInt) {
		return hash(SEED, aInt);
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
	public static int hash(long aLong) {
		return hash(SEED, aLong);
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
	public static int hash(float aFloat) {
		return hash(SEED, aFloat);
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
	public static int hash(double aDouble) {
		return hash(SEED, aDouble);
	}

	/**
	 * doubles.
	 */
	public static int hash(int aSeed, double aDouble) {
		return hash(aSeed, Double.doubleToLongBits(aDouble));
	}

	public static int hash(boolean[] aArray) {
		return hash(SEED, aArray);
	}

	public static int hash(int aSeed, boolean[] aArray) {
		int result = aSeed;
		if (aArray == null) {
			result = hash(result, 0);
		} else {
			for (boolean item : aArray) {
				result = hash(result, item);
			}
		}
		return result;
	}

	public static int hash(char[] aArray) {
		return hash(SEED, aArray);
	}

	public static int hash(int aSeed, char[] aArray) {
		int result = aSeed;
		if (aArray == null) {
			result = hash(result, 0);
		} else {
			for (char item : aArray) {
				result = hash(result, item);
			}
		}
		return result;
	}

	public static int hash(byte[] aArray) {
		return hash(SEED, aArray);
	}

	public static int hash(int aSeed, byte[] aArray) {
		int result = aSeed;
		if (aArray == null) {
			result = hash(result, 0);
		} else {
			for (byte item : aArray) {
				result = hash(result, item);
			}
		}
		return result;
	}

	public static int hash(short[] aArray) {
		return hash(SEED, aArray);
	}

	public static int hash(int aSeed, short[] aArray) {
		int result = aSeed;
		if (aArray == null) {
			result = hash(result, 0);
		} else {
			for (short item : aArray) {
				result = hash(result, item);
			}
		}
		return result;
	}

	public static int hash(int[] aArray) {
		return hash(SEED, aArray);
	}

	public static int hash(int aSeed, int[] aArray) {
		int result = aSeed;
		if (aArray == null) {
			result = hash(result, 0);
		} else {
			for (int item : aArray) {
				result = hash(result, item);
			}
		}
		return result;
	}

	public static int hash(long[] aArray) {
		return hash(SEED, aArray);
	}

	public static int hash(int aSeed, long[] aArray) {
		int result = aSeed;
		if (aArray == null) {
			result = hash(result, 0);
		} else {
			for (long item : aArray) {
				result = hash(result, item);
			}
		}
		return result;
	}

	public static int hash(float[] aArray) {
		return hash(SEED, aArray);
	}

	public static int hash(int aSeed, float[] aArray) {
		int result = aSeed;
		if (aArray == null) {
			result = hash(result, 0);
		} else {
			for (float item : aArray) {
				result = hash(result, item);
			}
		}
		return result;
	}

	public static int hash(double[] aArray) {
		return hash(SEED, aArray);
	}

	public static int hash(int aSeed, double[] aArray) {
		int result = aSeed;
		if (aArray == null) {
			result = hash(result, 0);
		} else {
			for (double item : aArray) {
				result = hash(result, item);
			}
		}
		return result;
	}

	public static int hash(Object[] aObjectArray) {
		return hash(SEED, aObjectArray);
	}

	public static int hash(int aSeed, Object[] aObjectArray) {
		int result = aSeed;
		if (aObjectArray == null) {
			result = hash(result, 0);
		} else {
			for (Object item : aObjectArray) {
				result = hash(result, item);
			}
		}
		return result;
	}

	public static int hash(Object aObject) {
		return hash(SEED, aObject);
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
	
}