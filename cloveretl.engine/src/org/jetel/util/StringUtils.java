/*
 *  jETeL/Clover - Java based ETL application framework.
 *  Copyright (C) 2002-04  David Pavlis <david_pavlis@hotmail.com>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 *  This library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Lesser General Public License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public
 *  License along with this library; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 */
package org.jetel.util;

import sun.text.Normalizer;

/**
 *  Helper class with some useful methods regarding string/text manipulation
 *
 * @author      dpavlis
 * @since       July 25, 2002
 * @revision    $Revision$
 */

public class StringUtils {

    //  quoting characters
	public final static char QUOTE_CHAR='\'';
	public final static char DOUBLE_QUOTE_CHAR='"';
    
	/**
	 *  Converts control characters into textual representation<br>
	 *  Note: This code handles only \n, \r and \t special chars
	 *
	 * @param  controlString  string containing control characters
	 * @return                string where control characters are replaced by their
	 *      text representation (i.e.\n -> "\n" )
	 * @since                 July 25, 2002
	 */
	public static String specCharToString(String controlString) {
		StringBuffer copy = new StringBuffer();
		char character;
		for (int i = 0; i < controlString.length(); i++) {
			character = controlString.charAt(i);
			switch (character) {
				case '\n':
					copy.append("\\n");
					break;
				case '\t':
					copy.append("\\t");
					break;
				case '\r':
					copy.append("\\r");
					break;
				default:
					copy.append(character);
			}
		}
		return copy.toString();
	}
	
	/**
	 *  Converts textual representation of control characters into control
	 *  characters<br>
	 *  Note: This code handles only \n, \r and \t special chars
	 *
	 * @param  controlString  Description of the Parameter
	 * @return                String with control characters
	 * @since                 July 25, 2002
	 */
	public static String stringToSpecChar(String controlString) {
		if(controlString == null) {
			return null;
		}
		StringBuffer copy = new StringBuffer();
		char character;
		boolean isBackslash = false;
		for (int i = 0; i < controlString.length(); i++) {
			character = controlString.charAt(i);
			if (isBackslash) {
				switch (character) {
					case '\\':
						copy.append('\\');
						break;
					case 'n':
						copy.append('\n');
						break;
					case 't':
						copy.append('\t');
						break;
					case 'r':
						copy.append('\r');
						break;
					default:
						copy.append('\\');
						copy.append(character);
						break;
				}
				isBackslash = false;
			} else {
				if (character == '\\') {
					isBackslash = true;
				} else {
					copy.append(character);
				}
			}
		}
		return copy.toString();
	}


	/**
	 *  Formats string from specified messages and their lengths.<br>
	 *  Negative (&lt;0) length means justify to the left, positive (&gt;0) to the right<br>
	 *  If message is longer than specified size, it is trimmed; if shorter, it is padded with blanks.
	 *
	 * @param  messages  array of objects with toString() implemented methods
	 * @param  sizes     array of desired lengths (+-) for every message specified
	 * @return           Formatted string
	 */
	public static String formatString(Object[] messages, int[] sizes) {
		int formatSize;
		int stringStart;
		int counter;
		String message;
		StringBuffer strBuf = new StringBuffer(100);
		for (int i = 0; i < messages.length; i++) {
			message = messages[i].toString();
			// left or right justified ?
			if (sizes[i] < 0) {
				formatSize = sizes[i] * (-1);
				fillString(strBuf, message, 0, formatSize);
			} else {
				formatSize = sizes[i];
				if (message.length() < formatSize) {
					fillBlank(strBuf, formatSize - message.length());
					fillString(strBuf, message, 0, message.length());
				} else {
					fillString(strBuf, message, 0, formatSize);
				}
			}
		}
		return strBuf.toString();
	}


	/**
	 *  Description of the Method
	 *
	 * @param  strBuf  Description of the Parameter
	 * @param  source  Description of the Parameter
	 * @param  start   Description of the Parameter
	 * @param  length  Description of the Parameter
	 */
	private static void fillString(StringBuffer strBuf, String source, int start, int length) {
		int srcLength = source.length();
		for (int i = start; i < start + length; i++) {
			if (i < srcLength) {
				strBuf.append(source.charAt(i));
			} else {
				strBuf.append(' ');
			}
		}
	}


	/**
	 *  Description of the Method
	 *
	 * @param  strBuf  Description of the Parameter
	 * @param  length  Description of the Parameter
	 */
	private static void fillBlank(StringBuffer strBuf, int length) {
		for (int i = 0; i < length; i++) {
			strBuf.append(' ');
		}
	}


	/**
	 *  Returns True, if the character sequence contains only valid identifier chars.<br>
	 * [A-Za-z0-9_]
	 * @param  seq  Description of the Parameter
	 * @return      The validObjectName value
	 */
	public static boolean isValidObjectName(CharSequence seq) {
		for (int i = 0; i < seq.length(); i++) {
			if (!Character.isUnicodeIdentifierPart(seq.charAt(i))) {
				return false;
			}
		}
		return true;
	}

	/**
	 * Returns true if the passed-in character is quote ['] or double
	 * quote ["].
	 * 
	 * @param character
	 * @return true if character equals to ['] or ["]
	 */
	public static final boolean isQuoteChar(char character){
	    return (character==QUOTE_CHAR || character==DOUBLE_QUOTE_CHAR ) ? true : false;
	}
	
	
	/**
	 * Returns true of passed-in string is quoted - i.e.
	 * the first character is quote character and the
	 * last character is equal to the first one.
	 * 
	 * @param str
	 * @return true if the string is quoted
	 */
	public static final boolean isQuoted(String str){
	    return (isQuoteChar(str.charAt(0)) && str.charAt(0)==str.charAt(str.length()-1))
	            ? true : false;
	}
	
	
	/**
	 * This method removes from the string characters which are not letters nor digits
	 * 
	 * @param str - input String
	 * @param alpha - if true method leaves letters
	 * @param numeric - if true ethod leaves digits
	 * @return String where are only letters and (or) digits from input String
	 */
	public static String getOnlyAlpfaNumericChars(String str,boolean alpha,boolean numeric){
		if (!alpha && !numeric){
			return str;
		}
		int charRemoved=0;
		int lenght=str.length();
		char[]	pomChars=new char[lenght];
		str.getChars(0,lenght,pomChars,0);
		StringBuffer toRemove = new StringBuffer(str);
		boolean isLetter, isDigit;
		for (int j=0;j<lenght;j++){
			isLetter = Character.isLetter(pomChars[j]);
			isDigit = Character.isDigit(pomChars[j]);
			if (!(isLetter || isDigit)){
				toRemove.deleteCharAt(j-charRemoved++);
			}else{
				if (isLetter && !alpha){
					toRemove.deleteCharAt(j-charRemoved++);
				}
				if (isDigit && !numeric){
					toRemove.deleteCharAt(j-charRemoved++);
				}
			}
		}
		return toRemove.toString();
	}
	
	/**
	 * This method removed from string blank space
	 * 
	 * @param str - input String
	 * @return input string without blank space
	 */
	public static String removeBlankSpace(String str){
		int charRemoved=0;
		int lenght=str.length();
		char[]	pomChars=new char[lenght];
		str.getChars(0,lenght,pomChars,0);
		StringBuffer toRemove = new StringBuffer(str);
		for (int j=0;j<lenght;j++){
			if (Character.isWhitespace(pomChars[j])) {
				toRemove.deleteCharAt(j-charRemoved++);
			}
		}
		return toRemove.toString();
	}
	
	/**
	 * This method replaces diacritic chars by theirs equivalence without diacritic.
	 * It works only for chars for which decomposition is defined
	 * 
	 * @param str
	 * @return string in which diacritc chars are replaced by theirs equivalences without diacritic
	 */
	public static String removeDiacritic(String str){
		return Normalizer.decompose(str, false, 0).replaceAll("\\p{InCombiningDiacriticalMarks}+", "");
	}

	/**
	 * This method concates string array to one string. Parts are delimited by given char
	 * 
	 * @param strings - input array of strings
	 * @param delimiter
	 * @return 
	 */
	public static String stringArraytoString(String[] strings,char delimiter){
		int length = strings.length;
		if (length==0) {
			return null;
		}
		StringBuffer result = new StringBuffer();
		for (int i=0;i<length;i++){
			result.append(strings[i]);
			if (i<length-1) {
				result.append(delimiter);
			}
		}
		return result.toString();
	}
	
	public static String stringArraytoString(String[] strings){
		return stringArraytoString(strings,' ');
	}

    public static boolean isEmpty(String s) {
        return s == null || s.length() == 0;
    }
}



/*
 *  End class StringUtils
 */

