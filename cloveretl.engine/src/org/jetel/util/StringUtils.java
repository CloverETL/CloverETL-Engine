/*
 *  jETeL/Clover - Java based ETL application framework.
 *  Copyright (C) 2002-04  Javlin Consulting <info@javlinconsulting.cz>
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

import java.nio.CharBuffer;

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
	 *  Note: This code handles only \n, \r ,\t ,\f, \b special chars
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
                case '\b':
                    copy.append("\\b");
                    break;
                case '\f':
                    copy.append("\\f");
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
	 *  Note: This code handles only \n, \r , \t , \f, \" ,\', \\ special chars
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
                    case '"':
                        copy.append('"');
                        break;
                    case '\'':
                        copy.append('\'');
                        break;
                    case 'f':
                        copy.append('\f');
                        break;
                    case 'b':
                        copy.append('\b');
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
		String message;
		StringBuffer strBuff = new StringBuffer(100);
		for (int i = 0; i < messages.length; i++) {
			message = messages[i].toString();
			// left or right justified ?
			if (sizes[i] < 0) {
				formatSize = sizes[i] * (-1);
				fillString(strBuff, message, 0, formatSize);
			} else {
				formatSize = sizes[i];
				if (message.length() < formatSize) {
					fillBlank(strBuff, formatSize - message.length());
					fillString(strBuff, message, 0, message.length());
				} else {
					fillString(strBuff, message, 0, formatSize);
				}
			}
		}
		return strBuff.toString();
	}


	/**
	 *  Description of the Method
	 *
	 * @param  strBuf  Description of the Parameter
	 * @param  source  Description of the Parameter
	 * @param  start   Description of the Parameter
	 * @param  length  Description of the Parameter
	 */
	private static void fillString(StringBuffer strBuff, String source, int start, int length) {
		int srcLength = source.length();
		for (int i = start; i < start + length; i++) {
			if (i < srcLength) {
				strBuff.append(source.charAt(i));
			} else {
				strBuff.append(' ');
			}
		}
	}


	/**
	 *  Description of the Method
	 *
	 * @param  strBuf  Description of the Parameter
	 * @param  length  Description of the Parameter
	 */
	private static void fillBlank(StringBuffer strBuff, int length) {
		for (int i = 0; i < length; i++) {
			strBuff.append(' ');
		}
	}


	/**
	 *  Returns True, if the character sequence contains only valid identifier chars.<br>
	 * [A-Za-z0-9_]
	 * @param  seq  Description of the Parameter
	 * @return      The validObjectName value
	 */
	public static boolean isValidObjectName(CharSequence seq) {
        if(seq == null) return false;
        
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
	    return character==QUOTE_CHAR || character==DOUBLE_QUOTE_CHAR;
	}
	
	
	/**
	 * Returns true of passed-in string is quoted - i.e.
	 * the first character is quote character and the
	 * last character is equal to the first one.
	 * 
	 * @param str
	 * @return true if the string is quoted
	 */
	public static final boolean isQuoted(CharSequence str){
	    return isQuoteChar(str.charAt(0)) && str.charAt(0)==str.charAt(str.length()-1);
	}
	
    
	/**
	 * Modifies buffer scope so that the string quotes are ignored,
	 * in case quotes are not present doesn't do anything.
	 * @param buf
	 * @return
	 */
	public static CharBuffer unquote(CharBuffer buf) {
		if (StringUtils.isQuoted(buf)) {
			buf.position(buf.position() + 1);
			buf.limit(buf.limit() - 1);
		}
		return buf;
	}
	

	/**
	 * Modifies string so that the string quotes are ignored,
	 * in case quotes are not present doesn't do anything.
	 * @param str
	 * @return
	 */
	public static String unquote(String str) {
		if (StringUtils.isQuoted(str)) {
			str = str.substring(1,str.length()-1);
		}
		return str;
	}
	
	/**
	 * Modifies buffer scope so that the leading whitespace is ignored.
	 * @param buf
	 * @return
	 */
	public static CharBuffer trimLeading(CharBuffer buf) {
		int pos = buf.position();
		int lim = buf.limit();
		while (pos < lim && Character.isWhitespace(buf.get(pos))) {
			pos++;
		}
		buf.position(pos);
		return buf;
	}
	
	/**
	 * Modifies buffer scope so that the trailing whitespace is ignored.
	 * @param buf
	 * @return
	 */
	public static CharBuffer trimTrailing(CharBuffer buf) {
		int pos = buf.position();
		int lim = buf.limit();
		while (pos < lim && Character.isWhitespace(buf.get(lim - 1))) {
			lim--;
		}
		buf.limit(lim);
		return buf;
	}
	
	/**
	 * Modifies buffer scope so that the leading and trailing whitespace is ignored.
	 * @param buf
	 * @return
	 */
	public static CharBuffer trim(CharBuffer buf) {
		trimLeading(buf);
		trimTrailing(buf);
		return buf;
	}
	
    /**
     * This method removes from the string characters which are not letters nor digits
     * 
     * @param str - input String
     * @param takeAlpha - if true method leaves letters
     * @param takeNumeric - if true method leaves digits
     * @return String where are only letters and (or) digits from input String
     */
    public static String getOnlyAlphaNumericChars(String str,boolean takeAlpha,boolean takeNumeric){
        if (!takeAlpha && !takeNumeric){
            return str;
        }
        int counter=0;
        int length=str.length();
        char[] chars=str.toCharArray();
        char[] result=new char[length];
        char character;
        for (int j=0;j<length;j++){
            character=chars[j];
            if ( (Character.isLetter(character) && takeAlpha) || 
                    (Character.isDigit(character) && takeNumeric) ){
                result[counter++]=chars[j];
            }
        }
        return new String(result,0,counter);
    }
	
	/**
	 * This method removed from string blank space
	 * 
	 * @param str - input String
	 * @return input string without blank space
	 */
	public static String removeBlankSpace(String str){
		int length=str.length();
		int counter = 0;
		char[]	chars=str.toCharArray();
		char[] result = new char[length];
		for (int j=0;j<length;j++){
			if (!Character.isWhitespace(chars[j])) {
				result[counter++] = chars[j];
			}
		}
        return new String(result,0,counter);
	}
    
     /**
      * Test whether parameter consists of space characters
      * only
     * @param data
     * @return  true if parameter contains space characters only
     */
    public static boolean isBlank(CharBuffer data){
            data.mark();
            for(int i=0;i<data.length();i++){
                if (!Character.isSpaceChar(data.get())){
                    data.reset();
                    return false;
                }
            }
            data.reset();
            return true;
        }
	
    /**
     * Test whether parameter consists of space characters
     * only
    * @param data
    * @return  true if parameter contains space characters only
    */
    public static boolean isBlank(CharSequence data){
         for(int i=0;i<data.length();i++){
             if (!Character.isSpaceChar(data.charAt(i))){
                 return false;
             }
         }
         return true;
     }
     
	/**
	 * This method replaces diacritic chars by theirs equivalence without diacritic.
	 * It works only for chars for which decomposition is defined
	 * 
	 * @param str
	 * @return string in which diacritic chars are replaced by theirs equivalences without diacritic
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

	/**
     * This method appends passed-in CharSequence to the end of
     * passed-in StringBuffer;<br>
     * It returns reference to buffer object to allow cascading
     * of these operations.
     * @param buff buffer to which append sequence of characters
     * @param seq characters sequence to append
     * @return  reference to passed-in buffer
     */
    public static final StringBuffer strBuffAppend(StringBuffer buff,CharSequence seq){
        int seqLen=seq.length();
        buff.ensureCapacity(buff.length()+seqLen);
        buff.append(seq);
        return buff;
    }

    /**
     * This method appends passed-in CharSequence to the end of
     * passed-in StringBuffer;<br>
     * It returns reference to buffer object to allow cascading
     * of these operations.
     * @param buff buffer to which append sequence of characters
     * @param seq characters sequence to append
     * @return  reference to passed-in buffer
     */
    public static final StringBuilder strBuffAppend(StringBuilder buff,CharSequence seq){
        int seqLen=seq.length();
        buff.ensureCapacity(buff.length()+seqLen);
        buff.append(seq);
        return buff;
    }
    

}



/*
 *  End class StringUtils
 */

