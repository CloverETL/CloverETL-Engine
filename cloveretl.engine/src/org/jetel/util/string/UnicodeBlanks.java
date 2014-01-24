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

/**
 * Provides utility methods for working with Unicode blank (non-printable, whitespace) characters.
 * 
 * <p>Intended as a replacement for {@link String#trim()}, {@link Character#isWhitespace(int)}, {@link Character#isSpaceChar(int)} and other outdated
 * and imperfect methods that do not follow the Unicode standard or do not do what the typical user would expect.
 * 
 * <p>Characters detected by this class are characters that represent horizontal or vertical spacing, or represent control commands. Such
 * characters cannot be "printed" in any way - they either represent a (white-) space in the document, or other formatting instructions that cannot
 * be rendered as a letter or any other visible symbol. Simply said, these characters cannot be "seen".
 * 
 * <p>As blank characters this class considers characters defined by the Unicode standard as either C0 Control, C1 Control or Space characters.
 * These include all ASCII control characters, (<= 32), including tabulation (09), line feed (10), or carriage return (13) and Unicode specific
 * space characters, such as non-breaking space, and others, and finally various control and format characters defined in the Unicode standard.
 * 
 * <p><b>Why is this class a better replacement of String.trim()?</b><br />
 * {@link String#trim()} only trims ASCII control characters. It does not trim other space characters defined by Unicode
 * such as no-break space (0x00A0) or line separator (0x2028).
 * 
 * <p><b>Why is this class a better replacement of Character.isWhitespace(int)?</b><br />
 * {@link Character#isWhitespace(int)} does not recognize Unicode space characters such as non-breaking space ('\u00A0', '\u2007', '\u202F') and others as whitespace.
 * 
 * <p><b>Why is this class a better replacement of Character.isSpaceChar(int)?</b><br />
 * {@link Character#isSpaceChar(int)} does not recognize tab (= 09), newline (LF = 10), carriage-return (CR = 13) characters to be blank.
 * Lots of the characters detected by {@link Character#isWhitespace(int)} are not detected by this method.
 * 
 * <p><b>Why not just combine Character.isWhitespace(int) || Character.isSpaceChar(int)?</b><br />
 * Close, but that ignores Unicode C1 control characters, such as 0x0085 - "Next line", and most of ASCII (Unicode C0) control characters, such as bell (07) or null (00). Also, such function would have worse performance.
 * 
 * @see
 * <ul>
 * <li>Unicode: <a href="http://www.unicode.org/charts/PDF/U0000.pdf">C0 Controls and Basic Latin</a>
 * <li>Unicode: <a href="http://www.unicode.org/charts/PDF/U0080.pdf">C1 Controls and Latin-1 Supplement</a>
 * <li>Unicode: <a href="http://www.unicode.org/charts/PDF/U2000.pdf">General Punctuation</a>
 * </ul>
 * 
 * @author Raszyk (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 9.9.2013
 */
public class UnicodeBlanks {

	private static final int BLANK_CHARACTERS_TYPE_MASK =
		(1 << Character.SPACE_SEPARATOR) |
		(1 << Character.LINE_SEPARATOR) |
		(1 << Character.PARAGRAPH_SEPARATOR) |
		(1 << Character.CONTROL);
	
	public static boolean isBlank(int c) {
		return (((BLANK_CHARACTERS_TYPE_MASK) >> Character.getType(c)) & 1) != 0;
	}
	
	public static boolean isBlank(char c) {
		return isBlank((int) c);
	}
	
	public static boolean isBlank(CharSequence data) {
		if (data == null) {
			return true;
		}
		for (int i = 0; i < data.length(); i++) {
			if (!isBlank(data.charAt(i))) {
				return false;
			}
		}
		return true;
	}
	
	public static String trim(String string) {
		if (string == null) {
			return null;
		}
		
		int start = 0;
		int end = string.length();
		
		while (start < end && isBlank(string.charAt(start))) {
			start++;
		}
		
		while (start < end && isBlank(string.charAt(end - 1))) {
			end--;
		}
		
		return (start != 0 || end != string.length()) ? string.substring(start, end) : string;
	}
	
	public static CharSequence trim(CharSequence sequence) {
		if (sequence == null) {
			return null;
		}
		
		int start = 0;
		int end = sequence.length();
		
		while (start < end && isBlank(sequence.charAt(start))) {
			start++;
		}
		
		while (start < end && isBlank(sequence.charAt(end - 1))) {
			end--;
		}
		
		return (start != 0 || end != sequence.length()) ? sequence.subSequence(start, end) : sequence;
	}
	
}
