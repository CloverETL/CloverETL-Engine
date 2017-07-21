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

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PackedDecimal {

	final static int HexA = 0x0A; // Plus A sign
	final static int HexB = 0x0B; // Minus B sign
	final static int PlusSign = 0x0C; // Plus sign
	final static int MinusSign = 0x0D; // Minus
	final static int HexE = 0x0E; // Plus E sign
	final static int NoSign = 0x0F; // Unsigned

	/**
	 * Such value n that ((n * 100) + 99)
	 * will certainly fit into Long range.
	 */
	private static final long SAFE_LONG = (Long.MAX_VALUE / 100) - 1;
	
	private static final BigInteger[] digits = {
		BigInteger.ZERO,
		BigInteger.ONE,
		BigInteger.valueOf(2),
		BigInteger.valueOf(3),
		BigInteger.valueOf(4),
		BigInteger.valueOf(5),
		BigInteger.valueOf(6),
		BigInteger.valueOf(7),
		BigInteger.valueOf(8),
		BigInteger.valueOf(9)
	};
	
	private static final BigInteger LONG_MIN_VALUE = BigInteger.valueOf(Long.MIN_VALUE);
	private static final BigInteger LONG_MAX_VALUE = BigInteger.valueOf(Long.MAX_VALUE);
	
	private static final BigInteger ONE_HUNDRED = BigInteger.valueOf(100);
	
	public static BigInteger parseBigInteger(CloverBuffer dataBuffer) {
		byte[] bytes = new byte[dataBuffer.remaining()];
		dataBuffer.get(bytes);
		return parseBigInteger(bytes);
	}

	/**
	 * Convert packed decimal encoded value into long
	 * 
	 * @param input
	 * @return
	 */
	public static long parse(byte[] input) {
		return parseBigInteger(input).longValue();
	}
	
	/**
	 * Reads bytes from a provided byte array,
	 * interprets them as a packed decimal and converts
	 * it to a number.
	 * 
	 * @see http://www.simotime.com/datapk01.htm
	 * 
	 * @param input byte array containing a packed decimal
	 * @return value of the packed decimal
	 */
	public static BigInteger parseBigInteger(byte[] input) {
		final int DropHO = 0xFF; // AND mask to drop HO sign bits
		final int GetLO = 0x0F; // Get only LO digit

		int digit = 0;
		long resultLong = 0;
		int aByte = 0;
		int idx = 0;
		
		for ( ; (idx < input.length) && (resultLong <= SAFE_LONG); idx++) {
			aByte = input[idx] & DropHO; // Get next 2 digits & drop HO sign bits
			digit = aByte >> 4; // First get high digit
			resultLong = resultLong * 10 + digit;
			digit = aByte & GetLO; // now LOW digit or sign
			
			switch(digit){
			case PlusSign:
			case NoSign:
			case HexA:
			case HexE:
				return BigInteger.valueOf(resultLong);
			case MinusSign:
			case HexB:
				resultLong = -resultLong;
				return BigInteger.valueOf(resultLong);
			default: // no sign
				resultLong = resultLong * 10 + digit;
			}
		}

		// convert current result value to a BigInteger 
		BigInteger resultBI = BigInteger.valueOf(resultLong);

		// continue parsing if there are remaining bytes in the buffer
		for ( ; idx < input.length; idx++) {
			aByte = input[idx] & DropHO; // Get next 2 digits & drop HO sign bits
			digit = aByte >> 4; // First get high digit
			resultBI = resultBI.multiply(BigInteger.TEN).add(digits[digit]);
			digit = aByte & GetLO; // now LOW digit or sign
			
			switch(digit){
			case PlusSign:
			case NoSign:
			case HexA:
			case HexE:
				return resultBI;
			case MinusSign:
			case HexB:
				resultBI = resultBI.negate();
				return resultBI;
			default: // no sign
				resultBI = resultBI.multiply(BigInteger.TEN).add(digits[digit]);
			}
		}
		
		// TODO maybe check only last position for a sign, but then right alignment is required
		
		// last digit and no sign (otherwise we would have reached one of the return statements)
		throw new IllegalArgumentException("Invalid (missing) Sign in packed decimal representation: "
				+ new BigInteger(input).toString(16));
	}

	/*
	 * Milan Krivanek: uses left alignment with right padding,
	 * which may not yield standard-compliant packed decimals.
	 */
	public static byte[] format(long value){
		byte[] result = new byte[16];
		int length=format(value,result);
		if (length<16) Arrays.fill(result, length, 16, (byte)0x0);
		return result;
	}
	
	/*
	 * Redundant method optimized for "long" input values
	 * and at most 16 bytes output,
	 * retained for performance reasons.
	 */
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

	/**
	 * Convenience method, see {@link #putPackedDecimal(CloverBuffer, BigInteger)}.
	 */
	public static int putPackedDecimal(CloverBuffer dataBuffer, long value, int minLength) {
		return putPackedDecimal(dataBuffer, BigInteger.valueOf(value), minLength);
	}
		
	/**
	 * Puts the value into the CloverBuffer as a packed decimal.
	 * Set <code>minLength</code> to 0 to disable padding.
	 * 
	 * @see http://www.simotime.com/datapk01.htm
	 * 
	 * @param dataBuffer
	 * @param value
	 * @param minLength minimum length (used for padding)
	 * 
	 * @return number of produced bytes
	 */
	public static int putPackedDecimal(CloverBuffer dataBuffer, BigInteger value, int minLength) {
		byte aByte = 0;
		// 16 is commonly used as maximum length, hence usually no reallocation
		List<Byte> bytes = new ArrayList<Byte>(16);
		
		// the value fits into a long
		if ((LONG_MIN_VALUE.compareTo(value) <= 0)
				&& (value.compareTo(LONG_MAX_VALUE) <= 0)) {
			long remainingDigits = 0;
			
			if(value.signum() < 0) {
				aByte = MinusSign;
				remainingDigits = -value.longValue();  
			} else {
				aByte = PlusSign;
				remainingDigits = value.longValue();
			}
			
			aByte = (byte) (aByte | ((remainingDigits % 10) << 4));
			remainingDigits /= 10;
			bytes.add(aByte);
			
			while (remainingDigits != 0) {
				aByte = (byte) (remainingDigits % 10);
				remainingDigits /= 10;
				aByte = (byte) (aByte | ((remainingDigits % 10) << 4));
				remainingDigits /= 10;
				bytes.add(aByte);
			}
		} else {
			BigInteger[] quotientAndRemainder = null;
			BigInteger remainingDigits = null;
			
			if(value.signum() < 0) {
				aByte = MinusSign;
				remainingDigits = value.negate();  
			} else {
				aByte = PlusSign;
				remainingDigits = value;
			}

			quotientAndRemainder = remainingDigits.divideAndRemainder(BigInteger.TEN);
			aByte = (byte) (aByte | ((quotientAndRemainder[1].intValue()) << 4)); // remainder
			remainingDigits = quotientAndRemainder[0]; // quotient
			bytes.add(aByte);

			int lastTwoDigits = 0;
			while (remainingDigits.signum() != 0) {
				quotientAndRemainder = remainingDigits.divideAndRemainder(ONE_HUNDRED);
				lastTwoDigits = quotientAndRemainder[1].intValue(); // remainder
				remainingDigits = quotientAndRemainder[0]; // quotient
				
				aByte = (byte) (lastTwoDigits % 10); // second
				aByte = (byte) (aByte | ((lastTwoDigits / 10) << 4)); // first
				bytes.add(aByte);
			}
		}
		
		// left padding by zeros
		for (int i = bytes.size(); i < minLength; i++) {
			dataBuffer.put((byte) 0x00);
		}
		
		// write the bytes in reverse order
		for (int i = bytes.size() - 1; i >= 0; i--) {
			dataBuffer.put(bytes.get(i));
		}
		
		return Math.max(bytes.size(), minLength);
	}
	
}
