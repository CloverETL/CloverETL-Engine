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
/**
 * 
 */
package org.jetel.util;

import java.io.File;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @author Jan Hadrava, Javlin Consulting (www.javlinconsulting.cz)
 * This implements list of filenames based on a specially formed mask. The mask
 * may contain a sequence of wildcard characters '$'. They will be replaced by sequential
 * number of the file. Count of wildcards specifies minimal length of the sequential number.
 * For a mask containing wildcards the list is infinite, for mask without wildcards it has only
 * one element - the mask itself. The mask must not be followed by any directory separators.
 */
public class MultiOutFile implements Iterator<String> {
	protected static final char NUM_CHAR='$';
	
	private int digitCnt;
	private int counter;
	private String beforeNum;
	private String afterNum;
	private String mask; 
	
	/**
	 * Sole ctor.
	 * @param mask
	 */
	public MultiOutFile(String mask) {
		StringBuilder before = new StringBuilder();
		StringBuilder after = new StringBuilder();
		digitCnt = splitMask(mask, before, after);
		beforeNum = before.toString();
		afterNum = after.toString();
		counter = 0;
		this.mask = mask;
	}

	public void reset() {
		counter = 0;
	}
	/**
	 * 
	 * @param mask
	 * @param beforeNum
	 * @param afterNum
	 * @return
	 */
	private static int splitMask(String mask, StringBuilder beforeNum, StringBuilder afterNum) {
		beforeNum.setLength(0);
		afterNum.setLength(0);
		int idx = mask.lastIndexOf(NUM_CHAR);
		if (idx == -1 || mask.indexOf(File.separatorChar, idx + 1) != -1) { // no wildcard or wildcard followed by dir separator
			beforeNum.append(mask);
			return 0;
		}
		int cnt = 1;
		for (idx--; idx > 0 && mask.charAt(idx) == NUM_CHAR; idx--) {
			cnt++;
		}
		beforeNum.append(mask.substring(0, idx + 1));
		afterNum.append(mask.substring(idx + 1 + cnt));
		return cnt;
	}

	@Override
	public boolean hasNext() {
		return digitCnt > 0 || counter == 0;
	}

	@Override
	public String next() {
		if (!hasNext()) {
			throw new NoSuchElementException("no next file name; " + digitCnt + " digits specified in the file name mask:\""+mask+"\"");
		}
		if (digitCnt == 0) {
			counter++;
			return beforeNum;
		}
		String retval = String.format("%s%0" + digitCnt + "d%s", beforeNum, counter, afterNum);
		counter++;
		return retval;
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();		
	}
	
	public int getDigitCnt() {
		return digitCnt;
	}
}
