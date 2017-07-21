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
package org.jetel.util.string;

import junit.framework.TestCase;

import org.junit.Test;

public class UnicodeBlanksTest extends TestCase {
	private static char[] special_space_chars =  {
		0x0020 // SPACE
		,0x007F // DELETE CHAR
		,0x00A0 // NO-BREAK SPACE
		,0x1680 // OGHAM SPACE MARK
		,0x180E // MONGOLIAN VOWEL SEPARATOR
		,0x2028 // LINE SEPARATOR
		,0x2029 // PARAGRAPH SEPARATOR
		,0x202F // NARROW NO-BREAK SPACE
		,0x205F // MEDIUM MATHEMATICAL SPACE
		,0x3000 // IDEOGRAPHIC SPACE
	};

	@Test
	public void testNonprintables() {
		for (int c = Character.MIN_VALUE; c <= Character.MAX_VALUE; c++) {
			char x = (char) c;
			boolean isOurWhitespace = UnicodeBlanks.isBlank(x);
			boolean isUnicodeWhitespace = false;
			for (char p : special_space_chars) {
				if (x == p) {
					isUnicodeWhitespace = true;
				}
			}
			if (x < 0x0020) { // ASCII control = Unicode C0 control
				isUnicodeWhitespace = true; 
			}
			if (x >= 0x2000 && x <= 0x200A) { // Unicode space
				isUnicodeWhitespace = true;
			}
			if (x >= 0x0080 && x <= 0x009F) { // Unicode C1
				isUnicodeWhitespace = true;
			}
			
			if (isUnicodeWhitespace && !isOurWhitespace) {
				throw new IllegalStateException(c + " isOurWhitespace:" + isOurWhitespace + " but isUnicodeWhitespace:" + isUnicodeWhitespace);
			}
			if (isUnicodeWhitespace != isOurWhitespace) {
				throw new IllegalStateException(c + " isOurWhitespace:" + isOurWhitespace + " but isUnicodeWhitespace:" + isUnicodeWhitespace);
			}
		}
	}
	
	/**
	 * Test the following invariant:
	 * 
	 * A "char" will never be both blank AND surrogate.
	 * 
	 * UnicodeBlanks implementation relies on this invariant, as it does only work with single chars and not code points.
	 */
	@Test
	public void testBlankOrSurrogate() {
		for (int i = Character.MIN_VALUE; i <= Character.MAX_VALUE; i++) {
			char c = (char) i;
			boolean isSurrogate = Character.isSurrogate(c);
			boolean isBlank = UnicodeBlanks.isBlank(c);
			
			if (isSurrogate && isBlank) {
				assertFalse("Character " + i + " is both surrogate and blank, therefore UnicodeBlank algorithm will not work properly for characters from Unicode supplementary plane", isSurrogate && isBlank);
			}
		}
	}
	
	/**
	 * Test the following invariant:
	 * 
	 * A supplementary char is never a blank char.
	 * 
	 * UnicodeBlanks implementation relies on this invariant, as it does only work with single chars and not code points.
	 */
	@Test
	public void testSupplementaryNeverBlank() {
		assertFalse(Character.isSupplementaryCodePoint(Character.MIN_SUPPLEMENTARY_CODE_POINT - 1));
		assertFalse(Character.isSupplementaryCodePoint(Character.MAX_CODE_POINT + 1));
		
		for (int i = Character.MIN_SUPPLEMENTARY_CODE_POINT; i <= Character.MAX_CODE_POINT; i++) {
			assertTrue(Character.isSupplementaryCodePoint(i));
			assertFalse(UnicodeBlanks.isBlank(i));
		}
	}
	
	private static void test(String expected, String input) {
		String trimmed = UnicodeBlanks.trim(input);
		if (!expected.equals(trimmed)) {
			System.err.println(input + " wasn't trimmed to " + expected);
		}
	}
	
	@Test
	public static void testTrimming() {
		test("", "");
		test("abc", "abc");
		test("abc", " abc");
		test("abc", "abc ");
		test("", "     ");
		test("", "\t\u00A0\n");
		test("x", "x            ");
		test("x", "x ");
		test("x", "\t\tx\t\t");
		test("x", "\n\r\tx\r \n\u00A0");
		test("x", "x\u2000");
		String x = "aa";
		String b = UnicodeBlanks.trim(x);
		if (x != b) {
			throw new IllegalStateException("Not equal references");
		}
		for (char wc : special_space_chars) {
			test("x", wc + "x" + wc);
		}
	}

}
