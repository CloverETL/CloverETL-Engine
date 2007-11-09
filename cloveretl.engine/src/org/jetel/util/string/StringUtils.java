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
package org.jetel.util.string;

import java.nio.CharBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Random;
import java.util.regex.Pattern;

import org.jetel.data.Defaults;
import org.jetel.data.parser.AhoCorasick;
import org.jetel.metadata.DataFieldMetadata;

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
	
	public final static char DECIMAL_POINT='.';
	public final static char EXPONENT_SYMBOL = 'e';
	public final static char EXPONENT_SYMBOL_C = 'E';
	
	public final static int ASCII_LAST_CHAR = 127;
	
	private final static int SEQUENTIAL_TRANLATE_LENGTH = 16;
	    
	/**
	 *  Converts control characters into textual representation<br>
	 *  Note: This code handles only \n, \r ,\t ,\f, \b, \\ special chars
	 *
	 * @param  controlString  string containing control characters
	 * @return                string where control characters are replaced by their
	 *      text representation (i.e.\n -> "\n" )
	 * @since                 July 25, 2002
	 */
	public static String specCharToString(CharSequence controlString) {
        if(controlString == null) return null;

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
                case '\\':
                	copy.append("\\\\");
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
	public static String stringToSpecChar(CharSequence controlString) {
		if(controlString == null) return null;

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
	    return isEmpty(str) ? false :
	    	isQuoteChar(str.charAt(0)) && str.charAt(0)==str.charAt(str.length()-1);
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
	
	public static String quote(CharSequence str){
		return "\"".concat(str.toString()).concat("\"");
	}
	
	public static StringBuilder trimLeading(StringBuilder str){
		int pos = 0;
		int length = str.length();
		while (pos < length && Character.isWhitespace(str.charAt(pos))) {
			pos++;
		}
		str.delete(0, pos);
		return str;
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

	public static StringBuilder trimTrailing(StringBuilder str){
		int pos = str.length() - 1;
		while (pos > 0 && Character.isWhitespace(str.charAt(pos))) {
			pos--;
		}
		str.setLength(pos+1);
		return str;
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
	
    public static StringBuilder trim(StringBuilder buf) {
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
	 * This method removes from string blank space
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
    
    public static StringBuilder removeBlankSpace(StringBuilder target,CharSequence str){
        int length=str.length();
        char character = 0;
        for (int j=0;j<length;j++){
            character=str.charAt(j);
            if (!Character.isWhitespace(character)) {
                target.append(character);
            }
        }
        return target;
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
    	if (data == null) return true;
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
  
	/**
	 * Splits the given string into mapping items. It's compatible with double quoted
	 * strings, so a delimiter in a double quoted string doesn't cause a split.
	 *  
	 * @param str
	 * @return mapping items.
	 */
	public static String[] split(String str) {
		Pattern delimiterPattern = Pattern.compile(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);
		ArrayList<String> result = new ArrayList<String>();
		
		StringBuilder item = new StringBuilder();
		boolean insideQuotes = false;
		char prevChar = '\0';
		for (int i = 0; i < str.length(); i++) {
			char c = str.charAt(i);
			if (insideQuotes) {
				if ((c == '"') && (prevChar != '\\')) {
					insideQuotes = false;
				} 
				item.append(c);
			} else {
				if (c == '"') {
					insideQuotes = true;
					item.append(c);
				} else if (delimiterPattern.matcher(Character.toString(c)).matches()) {
					result.add(item.toString());
					item = new StringBuilder();
				} else {
					item.append(c);
				}
			}

			prevChar = c;
		}
		String lastString = item.toString();
		if (!lastString.equals("")) {	// if the ";" is the last char of the mapping, 
										// then an empty last item is created
			result.add(item.toString());
		}
		
		return (String[]) result.toArray(new String[result.size()]);
	}

	public static boolean isEmpty(CharSequence s) {
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

    /**
     * This method finds index of string from string array
     * 
     * @param str String to find
     * @param array String array for searching
     * @return index or found String or -1 if String was not found
     */
    public static int findString(String str,String[] array){
		for (int i=0;i<array.length;i++){
			if (str.equals(array[i])) return i;
		}
		return -1;
	}

    /**
     * This method finds index of string from string array
     * 
     * @param str String to find
     * @param array String array for searching
     * @return index or found String or -1 if String was not found
     */
    public static int findStringIgnoreCase(String str,String[] array){
		for (int i=0;i<array.length;i++){
			if (str.equalsIgnoreCase(array[i])) return i;
		}
		return -1;
	}

    
    /**
     * This method finds index of first charater, which can't be part of identifier
     * 
     * @param str String for searching
     * @param fromIndex 
     * @return index of first character after identifier name
     */
    public static int findIdentifierEnd(CharSequence str, int fromIndex){
    	int index = fromIndex;
    	while (index < str.length() && Character.isUnicodeIdentifierPart(str.charAt(index))){
    		index++;
    	}
    	return index;
    }
    
    /**
     * This method finds index of first charater, which can be part of identifier
     * 
     * @param str String for searching
     * @param fromIndex
     * @return index of first character, which can be in identifier name
     */
    public static int findIdentifierBegining(CharSequence str, int fromIndex){
    	int index = fromIndex;
    	while (index < str.length() && !Character.isUnicodeIdentifierPart(str.charAt(index))){
    		index++;
    	}
    	return index;
    }

    /**
     * This method finds maximal lenth from set of Strings
     * 
     * @param strings 
     * @return length of longest string
     */
    public static int getMaxLength(String...strings){
    	int length = 0;
    	for (int i=0;i<strings.length;i++){
    		if (strings[i] != null && strings[i].length() > length){
    			length = strings[i].length(); 
    		}
    	}
    	return length;
    }

    /**
     * This method checks if given string can be parse to integer number
     * 
     * @param str string to check
     * @return -1 if str is not integer<br>
     * 	0 if str can be parsed to short<br>
     * 	1 if str can be parsed to int<br>
     * 	2 if str can be parsed to long<br>
     * 	3 if str is integer but has more than 18 digits
     */
    public static short isInteger(CharSequence str){
    	int start = 0;
    	if (str.charAt(0) == '-') {
    		start = 1;
    	}
    	int length = 0;
      	for (int index =  start; index<str.length();index++) {
    		if (!Character.isDigit(str.charAt(index))) {
    			return -1;
     		}
    		length++;
    	}
    	if (length <= 4) return 0;
    	if (length <= DataFieldMetadata.INTEGER_LENGTH) return 1;
    	if (length <= DataFieldMetadata.LONG_LENGTH) return 2;
    	return 3;
    }
    
    /**
     * This method checks if given string can be parse to double number with 10 radix
     * 
     * @param str
     * @return
     */
    public static boolean isNumber(CharSequence str){
    	int start = 0;
    	if (str.charAt(0) == '-') {
    		start = 1;
    	}
    	boolean decimalPiontIndex = false;
    	boolean wasE = false;
    	char c;
      	for (int index =  start; index<str.length();index++) {
      		c = str.charAt(index);
    		if (!Character.isDigit(c)) {
    			switch (c) {
				case DECIMAL_POINT:
					if (decimalPiontIndex) return false; //second decimal point
					decimalPiontIndex = true;
					break;
				case EXPONENT_SYMBOL:
				case EXPONENT_SYMBOL_C:
					if (wasE) return false; //second E
					if (++index == str.length()) {
						return false;//last char is 'e'
					}else{
						c = str.charAt(index);
					}
					if (!(Character.isDigit(c)))
							if (!(c == '+' || c == '-')) return false;//char after 'e' has to be digit or '+' or '-'
							else if (index + 1 == str.length()) return false;//last char is '+' or '-'
					decimalPiontIndex = true;
					wasE = true;
					break;
				default:
					return false; //not digit, '.', 'e' nor 'E'
				}
     		}
    	}
      	return true;
    }
    
    public static boolean isAscii(CharSequence str) {
    	for (int i=0; i < str.length(); i++){
    		if (str.charAt(i) > ASCII_LAST_CHAR) return false;
    	}
    	return true;
    }
    
    /**
     * This method copies substring of source to target.
     * 
     * @param target    target to which save the substring
     * @param src       source string
     * @param from      positing at which start (zero based)
     * @param length    number of characters to take
     * @return          target containing substring of original or empty string if specified from/length values
     *                  are out of ranges.
     * @since 23.5.2007
     */
    public static StringBuilder subString(StringBuilder target,CharSequence src,int from, int length) {
        final int end=from+length;
        final int maxLength=src.length();
        for(int i= ( from<0 ? 0 : from );i<end;i++) {
            if (i>=maxLength) break;
            target.append(src.charAt(i));
        }
        return target;
    }
    
    /**
     * This method copies substring of source to target.
     * 
     * @param target    target to which save the substring
     * @param src       source string
     * @param from      positing at which start (zero based)
     * @param length    number of characters to take
     * @return          target containing substring of original or empty string if specified from/length values
     *                  are out of ranges.
     * @since 23.5.2007
     */
    
    public static StringBuffer subString(StringBuffer target,CharSequence src,int from, int length) {
        final int end=from+length;
        final int maxLength=src.length();
        for(int i= ( from<0 ? 0 : from );i<end;i++) {
            if (i>=maxLength) break;
            target.append(src.charAt(i));
        }
        return target;
    }
    
    /**
     * Returns the index within input string of the first occurrence of the specified 
     * 	substring, starting at the specified index.
     * 
     * @param input string for searching 
     * @param pattern the substring for which to search
     * @param fromIndex the index from which to start the search
     * @return the index within this string of the first occurrence of the specified 
     * 	substring, starting at the specified index, or -1  if the sequence 
     * 	does not occur
     */
    public static int indexOf(CharSequence input, CharSequence pattern, int fromIndex){
    	AhoCorasick ac = new AhoCorasick(new String[]{pattern.toString()});
    	for (int i = fromIndex; i < input.length(); i++) {
    		ac.update(input.charAt(i));
    		if (ac.isPattern(0)) {
    			return i-pattern.length() + 1;
    		}
    	}
    	return -1;
    }
    
    /**
     * Returns the index within this string of the first occurrence of the specified 
     * 	character, starting the search at the specified index
     * 
     * @param input input string for searching 
     * @param cha character 
     * @param fromIndex the index from which to start the search
     * @return the index of the first occurrence of the character in the character 
     * 	sequence that is greater than or equal to fromIndex, or -1  if the character 
     * 	does not occur
     */
    public static int indexOf(CharSequence input, char ch, int fromIndex){
    	for(int i=fromIndex; i<input.length(); i++){
    		if (input.charAt(i) == ch) return i;
    	}
    	return -1;
    }
    
    /**
     * Returns number of occurrences of given char in the char sequence
     * 
     * @param input input string
     * @param ch char to find
     * @return number of occurrences
     */
    public static int count(CharSequence input, char ch) {
    	int result = 0;
    	int index = -1;
    	while ((index = indexOf(input, ch, index + 1)) > -1) {
    		result++;
    	}
    	return result;
    }
    
    /**
     * Translates single characters in a string to different characters:
     * replaces single characters at a time, translating the <i>n</i>th character in the match 
     * set with the <i>n</i>th character in the replacement set
     * 
     * @param in input string
     * @param searchSet character to replace
     * @param replaceSet replacing characters
     * @return original string with replaced requested characters
     */
    private static CharSequence translate(CharSequence in, 
    		CharSequence searchSet,	CharSequence replaceSet){
    	if (searchSet.length() < SEQUENTIAL_TRANLATE_LENGTH) {
    		return translateSequentialSearch(in, searchSet, replaceSet);
    	}else{
    		return translateBinarySearch(in, searchSet, replaceSet);
    	}
    }
    
    private static CharSequence translateOneByOne(CharSequence in, 
    		CharSequence searchSet,	CharSequence replaceSet){
    	Character[] result = new Character[in.length()];
    	for (int i=0; i< result.length; i++){
    		result[i] = in.charAt(i);
    	}
    	char search, replace;
    	for (int i=0; i<replaceSet.length(); i++){
    		search = searchSet.charAt(i);
    		replace = replaceSet.charAt(i);
    		for (int j=0;j<in.length(); j++) {
    			if (in.charAt(j) == search) {
    				result[j] = replace;
    			}
    		}
    	}
    	for (int i = replaceSet.length(); i < searchSet.length(); i++){
       		search = searchSet.charAt(i);
    		for (int j=0;j<in.length(); j++) {
    			if (in.charAt(j) == search) {
    				result[i] = null;
    			}
    		}
    	}
    	StringBuilder out = new StringBuilder(result.length);
    	for (Character character : result) {
			if (character != null) {
				out.append(character);
			}
		}
    	return out;
    }
    
    private static CharSequence translateMapSearch(CharSequence in, 
    		CharSequence searchSet,	CharSequence replaceSet){
    	HashMap<Character, Character> replacement = new HashMap<Character, Character>(searchSet.length());
    	int replaceSetLength = replaceSet.length();
    	for (int i=0; i<searchSet.length(); i++) {
    		replacement.put(searchSet.charAt(i), i < replaceSetLength ? replaceSet.charAt(i) : null);
    	}
    	StringBuilder result = new StringBuilder();
    	Character r;
    	char ch;
    	for (int i=0; i < in.length(); i++) {
    		ch = in.charAt(i);
    		if (replacement.containsKey(ch)) {
    			if ( (r = replacement.get(ch)) != null) {
    				result.append(r);
    			}
    		}else {
    			result.append(ch);
    		}
    	}
    	return result;
    }
    
    private static CharSequence translateBinarySearch(CharSequence in, 
    		CharSequence searchSet,	CharSequence replaceSet){
    	CharPair[] replacement = new CharPair[searchSet.length()];
    	int replaceSetLength = replaceSet.length();
    	for (int i=0; i<searchSet.length(); i++) {
    		replacement[i] = new CharPair(searchSet.charAt(i), 
    				i < replaceSetLength ? replaceSet.charAt(i) : null);
    	}
    	StringBuilder result = new StringBuilder();
    	Arrays.sort(replacement);
    	int index;
    	char ch;
    	for (int i=0; i < in.length(); i++) {
    		ch = in.charAt(i);
    		if ((index = Arrays.binarySearch(replacement, ch)) > -1) {
    			if ( replacement[index].value != null) {
    				result.append(replacement[index].value);
    			}
    		}else {
    			result.append(ch);
    		}
    	}
    	return result;
    }
    
    public static CharSequence translateSequentialSearch(CharSequence in, 
    		CharSequence searchSet,	CharSequence replaceSet){
    	StringBuilder result = new StringBuilder();
    	char[] search =  searchSet.toString().toCharArray();
    	char[] replace = replaceSet.toString().toCharArray();
    	int length = replace.length;
    	int index;
    	char ch;
    	for (int i=0; i < in.length(); i++) {
    		ch = in.charAt(i);
    		if ((index = indexOf(search, ch)) > -1) {
    			if ( index < length) {
    				result.append(replace[index]);
    			}
    		}else {
    			result.append(ch);
    		}
    	}
    	return result;
    }
   
    private static int indexOf(char[] array, char ch){
    	for (int i=0;i<array.length; i++) {
			if (array[i] == ch) return i;
		}
    	return -1;
    }
/*
 *  End class StringUtils
 */
	public static void main(String[] args) {
		
		StringBuilder in = new StringBuilder();
		StringBuilder searchSet = new StringBuilder();
		StringBuilder replaceSet = new StringBuilder();
		
		Random r = new Random();
		for (int i=0; i<3000000; i++){
			in.append((char)(r.nextInt('z' - 'a' + 1) + 'a'));
			if (i<10) {
				searchSet.append((char)('a'+ i));
				replaceSet.append((char)(r.nextInt('z' - 'a' + 1) + 'a'));
			}
		}
		
		CharSequence t1, t2, t3, t4;
		System.out.println("Search set:" + searchSet);
		System.out.println("Replace set:" + replaceSet);
		long start = System.currentTimeMillis();
		t1 = StringUtils.translateBinarySearch(in, searchSet, replaceSet);
		long end = System.currentTimeMillis();
		System.out.println("Binary search time:" + (end - start));
		start = System.currentTimeMillis();
		t2 = StringUtils.translateSequentialSearch(in, searchSet, replaceSet);
		end = System.currentTimeMillis();
		System.out.println("Sequential search time:" + (end - start));
		start = System.currentTimeMillis();
		t3 = StringUtils.translateMapSearch(in, searchSet, replaceSet);
		end = System.currentTimeMillis();
		System.out.println("Map search time:" + (end - start));
		start = System.currentTimeMillis();
		t4 = StringUtils.translateOneByOne(in, searchSet, replaceSet);
		end = System.currentTimeMillis();
		System.out.println("One by one time:" + (end - start));
		if (!t1.toString().equals(t2.toString()) || !t1.toString().equals(t3.toString())
				|| !t1.toString().equals(t4.toString())) 
			throw new RuntimeException();

		for (int i=0; i < 10; i++){
			searchSet.append((char)('k' + i));
			replaceSet.append((char)(r.nextInt('z' - 'a' + 1) + 'a'));
		}
		System.out.println("Search set:" + searchSet);
		System.out.println("Replace set:" + replaceSet);
		start = System.currentTimeMillis();
		t1 = StringUtils.translateBinarySearch(in, searchSet, replaceSet);
		end = System.currentTimeMillis();
		System.out.println("Binary search time:" + (end - start));
		start = System.currentTimeMillis();
		t2 = StringUtils.translateSequentialSearch(in, searchSet, replaceSet);
		end = System.currentTimeMillis();
		System.out.println("Sequential search time:" + (end - start));
		start = System.currentTimeMillis();
		t3 = StringUtils.translateMapSearch(in, searchSet, replaceSet);
		end = System.currentTimeMillis();
		System.out.println("Map search time:" + (end - start));
		start = System.currentTimeMillis();
		t4 = StringUtils.translateOneByOne(in, searchSet, replaceSet);
		end = System.currentTimeMillis();
		System.out.println("One by one time:" + (end - start));
		if (!t1.toString().equals(t2.toString()) || !t1.toString().equals(t3.toString())
				|| !t1.toString().equals(t4.toString())) 
			throw new RuntimeException();

		for (int i=0; i < 7; i++){
			searchSet.append((char)('u' + i));
			replaceSet.append((char)(r.nextInt('z' - 'a' + 1) + 'a'));
		}
		System.out.println("Search set:" + searchSet);
		System.out.println("Replace set:" + replaceSet);
		start = System.currentTimeMillis();
		t1 = StringUtils.translateBinarySearch(in, searchSet, replaceSet);
		end = System.currentTimeMillis();
		System.out.println("Binary search time:" + (end - start));
		start = System.currentTimeMillis();
		t2 = StringUtils.translateSequentialSearch(in, searchSet, replaceSet);
		end = System.currentTimeMillis();
		System.out.println("Sequential search time:" + (end - start));
		start = System.currentTimeMillis();
		t3 = StringUtils.translateMapSearch(in, searchSet, replaceSet);
		end = System.currentTimeMillis();
		System.out.println("Map search time:" + (end - start));
		start = System.currentTimeMillis();
		t4 = StringUtils.translateOneByOne(in, searchSet, replaceSet);
		end = System.currentTimeMillis();
		System.out.println("One by one time:" + (end - start));
		if (!t1.toString().equals(t2.toString()) || !t1.toString().equals(t3.toString())
				|| !t1.toString().equals(t4.toString())) 
			throw new RuntimeException();
}

}

class CharPair implements Comparable{
	
	Character key, value;
	
	CharPair(Character key, Character value){
		this.key = key;
		this.value = value;
	}

	public int compareTo(Object o) {
		if (o == null) {
			return 1;
		}		
		if (key == null) {
			return -1;
		}		
		if (o instanceof Character) {
			return key.compareTo((Character)o);
		}		
		if (o instanceof CharPair) {
			return key.compareTo(((CharPair)o).key);
		}		
		throw new ClassCastException();
	}
	
}