/*
 *    jETeL/Clover - Java based ETL application framework.
 *    Copyright (C) 2005-06  Javlin Consulting <info@javlinconsulting.cz>
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

package org.jetel.util;

import java.util.Random;

/**
 * <h3>Data Generator</h3>
 * 
 * The class should support all random data in future.
 * 
 * @author Jan Ausperger (jan.ausperger@javlin.eu)
 *         (c) OpenSys (www.opensys.eu)
 */
public class DataGenerator extends Random {

	private static final long serialVersionUID = 1L;
	
	/**
	 * This method creates random string from chars 'a' till 'z'
	 * 
	 * @param minLenght
	 *            minumum length of string
	 * @param maxLenght
	 *            maximum length of string
	 * @return string created from random characters. Length of this string is between minLenght and maxLenght inclusive
	 */
	public String nextString(int minLenght, int maxLenght) {
		StringBuilder result;
		if (maxLenght != minLenght) {
			result = new StringBuilder(nextInt(maxLenght - minLenght + 1) + minLenght);
		} else {// minLenght == maxLenght
			result = new StringBuilder(minLenght);
		}
		for (int i = 0; i < result.capacity(); i++) {
			result.append((char) (nextInt('z' - 'a' + 1) + 'a'));
		}
		return result.toString();
	}

	/**
	 * This method creates random long
	 * @param lFrom
	 * @param lTo
	 * @return random date
	 */
	public long nextLong(long lFrom, long lTo) {
		// raw random number
		long r = Math.abs(nextLong());
		
		// calculate an interval
		long interval = lTo-lFrom;
		
		// adjust the random number to the interval
		return r-(r/interval*interval)+lFrom;
	}

}
