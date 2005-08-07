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
package org.jetel.util;
import java.lang.IndexOutOfBoundsException;
/**
 * Class for representing strings of bits.<br>
 * Uses arrays of bytes to group bits by 8 together.
 * Uses bit manipulation operations for fast access to individual bits.
 * Once created, the bit array can't be extended (shorten or prolonged).
 *
 *@author     dpavlis
 *@created    8. April 2003
 */

public class BitArray {

	private final byte bits[];
	private final int length;


	/**
	 *  Constructor for the BitArray object
	 *
	 *@param  size  how many bits should the array keep
	 */
	public BitArray(int size) {
		length = size;
		if (length<1) throw new RuntimeException("Can't create ZERO length array !");
		bits = new byte[(size-1) / 8 + 1];
		// init array
		for (int i = 0; i < bits.length; i++) {
			bits[i]=0;
		}
	}


	/**
	 *  Sets the specified bit to 1/True
	 *
	 *@param  index  which bit to set
	 */
	public final void set(int index) {
		if (index >= length) {
			throw new IndexOutOfBoundsException();
		}
		bits[index / 8] |= (byte) (1 << (index % 8));
	}


	/**
	 *  Re-sets the specified bit to 0/false
	 *
	 *@param  index  which bit to reset
	 */
	public final void reset(int index) {
		if (index >= length) {
			throw new IndexOutOfBoundsException();
		}
		bits[index / 8] &= (~((byte) (1 << (index % 8))));
	}


	/**
	 *  Gets state of specified bit
	 *
	 *@param  index  which bit to check
	 *@return        true/false according to bit state
	 */
	public final boolean get(int index) {
		if (index >= length) {
			throw new IndexOutOfBoundsException();
		}
		byte pattern = (byte) (1 << (index % 8));
		return (bits[index / 8] & pattern) == pattern ? true : false;
	}


	/**
	 *  Gets state of specified bit<br>
	 *  Synonym for get()
	 *
	 *@param  index  which bit to check
	 *@return        true/false according to bit state
	 */
	public final boolean isSet(int index) {
		return get(index);
	}


	/**
	 *  Returns length of BitArray (in bits)
	 *
	 *@return    length
	 */
	public final int length() {
		return length;
	}


	/**
	 *  Formats BitArray into String where set bits
	 *  are displayed as 1 and reset as 0
	 *
	 *@return    String representation of BitArray
	 */
	public String toString() {
		StringBuffer str = new StringBuffer(length * 2 + 1);
		str.append("|");
		for (int i = 0; i < length; i++) {
			str.append(get(i) ? "1" : "0");
			str.append("|");
		}
		return str.toString();
	}


	/**
	 *  Returns backing array of bytes<br>
	 *  Can be used for bulk-load operations.
 	 *
	 *@return    The byteArray value
	 */
	public final byte[] getByteArray() {
		return bits;
	}

	   //this is for debugging only
	   /*
	   public static void main(String args[]){
		   BitArray array=new BitArray(16);
		   array.set(8);
		   array.set(0);
		   array.set(10);
		   array.set(7);
		   System.out.println(array.get(1));
		   System.out.println(array.get(0));
		   System.out.println(array.get(8));
		   System.out.println(array.get(9));
		   System.out.println(array.get(10));
		   System.out.println(array);
		   array.reset(10);
		   array.set(1);
		   array.reset(7);
		   System.out.println(array.get(10));
		   System.out.println(array);
	   }
	   */
}
/*
 *  End class BitArray
 */

