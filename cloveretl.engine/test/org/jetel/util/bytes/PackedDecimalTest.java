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
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.jetel.test.CloverTestCase;

/**
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 10.5.2011
 */
public class PackedDecimalTest extends CloverTestCase {
	
	private void checkParse(byte[] bytes, BigInteger expected) {
		BigInteger value = PackedDecimal.parseBigInteger(bytes);
		assertEquals(expected, value);
	}

	private void checkParse(byte[] bytes, long expected) {
		checkParse(bytes, BigInteger.valueOf(expected));
	}

	/**
	 * Checks if the value stores as the expected byte array in packed decimal format
	 * 
	 * @param value
	 * @param expected
	 */
	private void checkPutPackedDecimal(BigInteger value, byte[] expected) {
		CloverBuffer dataBuffer = CloverBuffer.allocate(expected.length);
		PackedDecimal.putPackedDecimal(dataBuffer, value, expected.length);
		assertTrue(Arrays.equals(expected, dataBuffer.array()));
	}
	
	private void checkPutPackedDecimal(long value, byte[] expected) {
		checkPutPackedDecimal(BigInteger.valueOf(value), expected);
	}
	
	/**
	 * Test for {@link PackedDecimal#parseBigInteger(ByteBuffer)}
	 */
	public void test_parseBigInteger() {
		// test a Long
		byte[] negativeLong  = {(byte) 0x92, 0x23, 0x37, 0x20, 0x36, (byte) 0x85, 0x47, 0x75, (byte) 0x80, 0x7D};
		checkParse(negativeLong, -9223372036854775807L);

		byte[] positiveLong = {(byte) 0x92, 0x23, 0x37, 0x20, 0x36, (byte) 0x85, 0x47, 0x75, (byte) 0x80, 0x7C};
		checkParse(positiveLong, 9223372036854775807L);
		
		byte[] zeros = {0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0C};
		checkParse(zeros, 1000000000000000000L);
	}
	
	/**
	 * Test for {@link PackedDecimal#putPackedDecimal(ByteBuffer, BigInteger)}
	 */
	public void test_putPackedDecimal() {
		// test a Long
		byte[] negativeLong  = {(byte) 0x92, 0x23, 0x37, 0x20, 0x36, (byte) 0x85, 0x47, 0x75, (byte) 0x80, 0x7D};
		checkPutPackedDecimal(-9223372036854775807L, negativeLong);

		byte[] positiveLong = {(byte) 0x92, 0x23, 0x37, 0x20, 0x36, (byte) 0x85, 0x47, 0x75, (byte) 0x80, 0x7C};
		checkPutPackedDecimal(9223372036854775807L, positiveLong);
		
		byte[] zeros = {0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0C};
		checkPutPackedDecimal(1000000000000000000L, zeros);
	}
	
}
