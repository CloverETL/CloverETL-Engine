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
 * Utility to generate valid XML tag name from arbitrary string. Even that XML allows non-ASCII characters
 * as a tag name, this utility encodes them as well as any special ASCII characters. These characters are encoded as
 * "_xhhhh" where hhhh is the Unicode of the character as a hexa value. 
 * 
 * Public static methods of this class can be used in XPath of TreeReader mapping. See XmlXPathEvaluator.
 * 
 * @author jan.michalica
 */
public class TagName {

	private static final char ENC_SEQ_CHAR = '_';
	private static final String ENC_SEQ_START = ENC_SEQ_CHAR + "x";
	
	/**
	 * Encodes given input string using unicodes.
	 * <pre>
	 * Example:
	 * input: "@Funny*Tag-Name()"
	 * output: "_x0040Funny_x002aTag-Name_x0028_x0029"
	 * </pre>
	 * @param s
	 * @param encodeSeqChar if SEQ_CHAR ('_') should be encoded
	 * @return
	 */
	public static String encode(final String s, boolean encodeSeqChar) {
		
		if (s == null) {
			return null;
		}
		
		final StringBuilder sb = new StringBuilder(s.length());
		for (int i = 0; i < s.length(); ++i) {
			char c = s.charAt(i);
			if (isInvalidCharacter(i, c, encodeSeqChar)) {
				sb.append(ENC_SEQ_START);
				String scode = Integer.toHexString(c);
				for (int j = 0; j < 4 - scode.length(); ++j) {
					sb.append('0');
				}
				sb.append(scode);
			} else {
				sb.append(c);
			}
		}
		return sb.toString();
	}

	/**
	 * Encodes given input string using unicodes.
	 * <pre>
	 * Example:
	 * input: "@Funny*Tag-Name()"
	 * output: "_x0040Funny_x002aTag-Name_x0028_x0029"
	 * </pre>
	 * @param s
	 * @return
	 */
	public static String encode(final String s) {
		return encode(s, true);
	}

	/**
	 * @param i index of the character
	 * @param c the character
	 * @param encodeSeqChar treat SEQ_CHAR as invalid character
	 * @return true iff the character would be escaped by the {@link #encode(String)} method 
	 */
	private static boolean isInvalidCharacter(int i, char c, boolean encodeSeqChar) {
		if (c == ENC_SEQ_CHAR) {
			return encodeSeqChar;
		} else {
            return !('0' <= c && c <= '9' || 'a' <= c && c <= 'z' || 'A' <= c && c <= 'Z' || c == '-' || c == '.') ||
				(i == 0 && ('0' <= c && c <= '9' || c == '-' || c == '.'));
		}
	}
	
	/**
	 * @param s
	 * @return true iff at least one character of the string <code>s</code> would be escaped by the {@link #encode(String)} method
	 */
	public static boolean hasInvalidCharacters(String s) {
		return hasInvalidCharacters(s, true);
	}

	/**
	 * @param s
	 * @return true iff at least one character of the string <code>s</code> would be escaped by the {@link #encode(String)} method
	 */
	public static boolean hasInvalidCharacters(String s, boolean encodeSeqChar) {
		for (int i = 0; i < s.length(); ++i) {
			char c = s.charAt(i);
			if (isInvalidCharacter(i, c, encodeSeqChar)) {
				return true;
			}
		}
		return false;
	}
	
	/**
	 * Decodes given input string previously encoded by this class.
	 * e.g.:
	 * input: "tag_x0020with_x0020spaces"
	 * output: "tag with spaces"
	 * @param s
	 * @return
	 */
	public static String decode(final String s) {
		
		if  (s == null) {
			return null;
		}
		
		final StringBuilder sb = new StringBuilder(s.length());
		int pos = 0;
		while (pos < s.length()) {
			char c = s.charAt(pos++);
			if (c == ENC_SEQ_CHAR) {
				try {
					c = (char)Integer.parseInt(s.substring(pos + 1, pos + 5), 16);
					pos += 5;
				} catch (RuntimeException e) {
					// malformed sequence, ignore
				}
			}
			sb.append(c);
		}
		return sb.toString();
	}
}
