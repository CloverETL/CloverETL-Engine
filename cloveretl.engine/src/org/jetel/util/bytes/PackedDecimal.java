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
package org.jetel.util.bytes;

import java.util.Arrays;

public class PackedDecimal {

	final static int PlusSign = 0x0C; // Plus sign
	final static int MinusSign = 0x0D; // Minus
	final static int NoSign = 0x0F; // Unsigned

	/**
	 * Convert packed decimal encoded value into long
	 * 
	 * @param input
	 * @return
	 */
	public static long parse(byte[] input) {
		// Convert packed decimal to long
		final int DropHO = 0xFF; // AND mask to drop HO sign bits
		final int GetLO = 0x0F; // Get only LO digit
		long val = 0; // Value to return
		boolean signOK=false;

		loop:
		for (int i = 0; i < input.length; i++) {
			int aByte = input[i] & DropHO; // Get next 2 digits & drop sign bits
			int digit = aByte >> 4; // First get high digit
			val = val * 10 + digit;
			digit = aByte & GetLO; // now LOW digit or sign
			
			switch(digit){
			case PlusSign:
			case NoSign:
						signOK=true;
						break loop;
			case MinusSign:
					val=-val;
						signOK=true;
						break loop;
			default: // no sign
				val = val * 10 + digit;
			}
		}
		if (!signOK) { // last digit and no sign ?
			throw new IllegalArgumentException("Invalid (missing) Sign in packed decimal representation: "+Arrays.toString(input));
		}
		return val;
	}

	public static byte[] format(long value){
		byte[] result = new byte[16];
		int length=format(value,result);
		if (length<16) Arrays.fill(result, length, 16, (byte)0x0);
		return result;
	}
	
	/**
	 * Convert long value into packed decimal representation
	 * 
	 * @param value value to convert into packed decimal representation
	 * @param output array into which the result will be saved (bytes)
	 * @return number of bytes in output containing value (length of the result)
	 */
	public static int format(long value, byte[] output) {
		int i,j;
		i = output.length - 1;
		j=1;

		if (value < 0) {
			output[i] = MinusSign;
			value *= -1;
		} else
			output[i] = PlusSign;

		output[i] = (byte) (output[i] | ((value % 10) << 4));
		value /= 10;
		--i;
		j++;

		while (0 != value) {
			output[i] = (byte) (value % 10);
			value /= 10;
			output[i] = (byte) (output[i] | ((value % 10) << 4));
			value /= 10;
			--i;
			j++;

		}
		System.arraycopy(output, i+1, output, 0, j-1);
		return j-1;

	}
/*
	public static void main(String[] args) throws Exception {
		 byte[] pd = new byte[] {0x19, 0x2C}; // 192
		 System.out.println(PackedDecimal.parse(pd));
		 byte[] result=PackedDecimal.format(192);
		 System.out.println(Arrays .toString(result));
		 System.out.println(Arrays.toString(pd));
		 System.out.println(PackedDecimal.parse(result));
		 pd = new byte[] {(byte)0x98, 0x44, 0x32, 0x3D}; //-9844323
		 System.out.println(PackedDecimal.parse(pd));
		 System.out.println(Arrays .toString(result=PackedDecimal.format(-9844323)));
		 System.out.println(PackedDecimal.parse(result));
		 System.out.println(Arrays .toString(PackedDecimal.format(1234567890123456l)));
		 System.out.println(Arrays.toString(pd));
		 pd = new byte[] {(byte)0x98, 0x44, 0x32, (byte)0x89}; //invalid sign
		 System.out.println(PackedDecimal.parse(pd)); 
	}*/
	
}
