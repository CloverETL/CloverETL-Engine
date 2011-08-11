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
 * Class for conversion of string containing escape sequences
 * and/or quotes to standard string where each char is represented by itself.
 * This class supports only string quoting and doesn't implement any escape sequences.  
 * It is supposed to have some more refined subclasses.
 *  
 * @author Jan Hadrava (jan.hadrava@javlinconsulting.cz), Javlin Consulting (www.javlinconsulting.cz)
 * @since 10/11/06  
*/
public class QuotingDecoder {
	
	public static final String AUTO_MODE_STRING = "both";
	
	private static final char DOUBLE_QUOTE = '\"';
	private static final char SINGLE_QUOTE = '\'';
	
	private boolean autoMode = true;
	private char quoteChar;
	private char startQuote;
	

	/**
	 * Removes quotes and replaces escape sequences.
	 * @param quoted A sequence to be converted.
	 * @return Converted sequence.
	 */
	public CharSequence decode(CharSequence quoted) {
		int len = quoted.length();
		if (len < 2) {
			return quoted;
		}
		char first = quoted.charAt(0);
		char last = quoted.charAt(len - 1);
		
		if (autoMode) {
			if (first != SINGLE_QUOTE && first != DOUBLE_QUOTE) return quoted;
		} else {
			if (first != quoteChar) return quoted;
		}
		
		if (first != last) return quoted;

		// remove outer quote chars and remove doubled quote chars
		char quoteChar = first;
		StringBuilder decoded = new StringBuilder(quoted.length() - 2);
		for (int i = 1; i < quoted.length() - 1; i++) {
			decoded.append(quoted.charAt(i));
			// skip repeated quote char
			if (quoted.charAt(i) == quoteChar && quoted.charAt(i + 1) == quoteChar) i++;
		}
		return decoded;
	}
	
	/**
	 * Quote given string. All quoting characters will be doubled.
	 * For instance the string:
	 * a "b" c
	 * will be transformed:
	 * "a ""b"" c"
	 * 
	 * @param unquoted
	 * @return
	 */
	public CharSequence encode(CharSequence unquoted) {
		char quoteChar = autoMode ? DOUBLE_QUOTE : this.quoteChar;
		int unquotedLen = unquoted.length();
		StringBuilder result = new StringBuilder(unquotedLen + 2);

		result.append(quoteChar);
		for (int i = 0; i < unquotedLen; i++) {
			final char ch = unquoted.charAt(i);
			if (ch == quoteChar) {
				result.append(quoteChar);
				result.append(quoteChar);
			} else {
				result.append(ch);
			}
		}
		result.append(quoteChar);
		
		return result;
	}
	
	/**
	 * Checks whether a character is an opening quote.
	 * If character <code>c</code> is recognized as an opening quote, all next calls
	 * to {@link #isEndQuote(char)} method will return true only for the same 
	 * character <code>c</code>.
	 */
	public boolean isStartQuote(char c) {
		boolean isQuote = autoMode ? c == DOUBLE_QUOTE || c == SINGLE_QUOTE : c == quoteChar; 
		if (isQuote) {
			startQuote = c;
		}
		return isQuote;
	}

	/**
	 * Checks whether a character is a closing quote.
	 */
	public boolean isEndQuote(char c) {
		return c == startQuote; 
	}

	/**
	 * Sets quote character.
	 * @param quoteChar quote character. Can be <code>null</code>, which means
	 * both ' and " characters will be considered a quote characters, and " will be used for encoding.
	 */
	public void setQuoteChar(Character quoteChar) {
		if (quoteChar == null) {
			autoMode = true;
		} else {
			this.quoteChar = quoteChar;
			autoMode = false;
		}
	}
	
	/**
	 * Method for retrieving quote character from attribute of a component.
	 * @return quote character or null indicating default value of "auto mode".
	 */
	public static Character quoteCharFromString(String str) {
		if (StringUtils.isEmpty(str) || str.equals(AUTO_MODE_STRING)) return null;
		if (str.length() > 1) throw new IllegalArgumentException("String has more than 1 character");
		return str.charAt(0);
	}
	
}
