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
package org.jetel.util.crypto;

import junit.framework.TestCase;

/**
 * @author csochor (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created Jul 22, 2010
 */
public class Base64Test extends TestCase {

	public void testAllShortArrays() {
		for (int arrayLength = 1; arrayLength < 3; arrayLength++) {
			final byte[] bytes = new byte[arrayLength];
			testaAll(bytes, 0);
		}
	}

	public void testAllLongArrays() {
		for (int arrayLength = 1; arrayLength < 3; arrayLength++) {
			final byte[] bytes = new byte[arrayLength + 5];
			for (byte i = 0; i < 5; i++) {
				bytes[i] = i;
			}
			testaAll(bytes, 5);
		}
	}

	public void testInvalidStrings() {
		assertEquals(0, Base64.decode("l").length);
		assertEquals(0, Base64.decode("lo").length);
		assertEquals(0, Base64.decode("loc").length);
		final byte[] loca = Base64.decode("loca");
		assertEquals(3, loca.length);
		assertEquals(-106, loca[0]);
		assertEquals(-121, loca[1]);
		assertEquals(26, loca[2]);
	}

	private void testaAll(byte[] bytes, int i) {
		for (byte v = -126; v <= 125; v++) {
			bytes[i] = v;
			if (i == bytes.length - 1) {
				final String encoded = Base64.encodeBytes(bytes);
//				System.out.println("testing base64(" + byteArrayToHexString(bytes) + ") = \"" + encoded + "\"");
				final byte[] transformed = Base64.decode(encoded);
				compare(bytes, transformed);
			} else {
				testaAll(bytes, i + 1);
			}
		}

	}

	private void compare(byte[] expected, byte[] actual) {
		assertEquals(expected.length, actual.length);

		for (int i = 0; i < expected.length; i++) {
			assertEquals(expected[i], actual[i]);
		}

	}

	private static final String HEX_CHARS[] = { "0", "1", "2",
			"3", "4", "5", "6", "7", "8",
			"9", "A", "B", "C", "D", "E",
			"F" };
	
	static String byteArrayToHexString(byte in[]) {

		if (in == null || in.length <= 0){
			return "";
		}
		
		final StringBuilder ret = new StringBuilder(in.length * 2);

		byte ch = 0x00;
		int i = 0;
		while (i < in.length) {
			ch = (byte) (in[i] & 0xF0);
			ch = (byte) (ch >>> 4);
			ch = (byte) (ch & 0x0F);
			ret.append(HEX_CHARS[(int) ch]);
			ch = (byte) (in[i] & 0x0F);
			ret.append(HEX_CHARS[(int) ch]);
			i++;
		}

		return ret.toString();

	}

}
