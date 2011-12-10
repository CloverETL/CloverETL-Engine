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

import java.util.Random;

/**
 * <h3>Data Generator</h3>
 * 
 * The class should support all random data in future.
 * 
 * @author Jan Ausperger (jan.ausperger@javlin.eu)
 *         (c) Javlin, a.s. (www.javlin.eu)
 */
public class DataGenerator extends Random {

	private static final long serialVersionUID = 1L;

	public DataGenerator() {
		super();
	}
	
	public DataGenerator(long arg0) {
		super(arg0);
		nextInt();
	}
	
	/**
	 * Set seed.
	 * @param paramLong
	 */
	@Override
	public void setSeed(long paramLong) {
		super.setSeed(paramLong);
		nextInt();
	}
	
	/**
	 * This method creates random string from chars 'a' till 'z'
	 * 
	 * @param minLenght of string
	 * @param maxLenght of string
	 * @return string created from random characters. Length of this string is between minLenght and maxLenght inclusive
	 */
	public String nextString(int minLenght, int maxLenght) {
		return new String(nextChars(minLenght,maxLenght));
	}

	public char[] nextChars(int minLenght, int maxLenght) {
		if (minLenght < 0) {
            throw new IllegalArgumentException("min length must be positive");
		}
		if (maxLenght < 0) {
            throw new IllegalArgumentException("max length must be positive");
		}
        if (minLenght > maxLenght) {
            throw new IllegalArgumentException("min length cannot be bigger than max length");
        }
        
		char[] result;
		if (maxLenght != minLenght) {
			result = new char[(nextInt(maxLenght - minLenght + 1) + minLenght)];
		} else {// minLenght == maxLenght
			result = new char[minLenght];
		}
		for (int i = 0; i < result.length; i++) {
			result[i]=(char)(nextInt('z' - 'a' + 1) + 'a');
		}
		return result;
	}
	
	/**
	 * This method creates random long
	 * @param min
	 * @param max
	 * @return random long
	 */
	public long nextLong(long min, long max) {
		if (min > max) {
			throw new IllegalArgumentException("Min parameter cannot be bigger than max parameter.");
		}
		return nextLong(max - min + 1) + min;
	}

	/**
	 * Modification of nextInt(int n) method for long.
	 */
	public long nextLong(long n) {
        if (n <= 0)
            throw new IllegalArgumentException("n must be positive");

        if (n <= Integer.MAX_VALUE) {
        	return nextInt((int) n);
        }

// error checking and 2^x checking removed for simplicity.
//        if ((n & -n) == n)  // i.e., n is a power of 2
//            return (int)((n * (long)next(31)) >> 31);
        
		long bits, val;
		do {
			bits = (nextLong() << 1) >>> 1;
			val = bits % n;
		} while (bits - val + (n - 1) < 0L);
		return val;
	}
	
	/**
	 * This method creates random integer
	 * @param min
	 * @param max
	 * @return random integer
	 */
	public int nextInt(int min, int max) {
		if (min > max) {
			throw new IllegalArgumentException("Min parameter cannot be bigger than max parameter.");
		}
		return nextInt(max - min + 1) + min;
	}

}
