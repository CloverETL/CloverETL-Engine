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

import java.awt.event.KeyEvent;
import java.io.IOException;
import java.io.StreamTokenizer;
import java.io.UnsupportedEncodingException;
import java.nio.CharBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.codec.language.DoubleMetaphone;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.Defaults;
import org.jetel.metadata.DataFieldMetadata;

import com.ibm.icu.text.Normalizer;
import com.ibm.icu.text.Transliterator;

/**
 * Helper class with some useful methods regarding string/text manipulation
 * 
 * @author dpavlis
 * @since July 25, 2002
 */

public class StringUtils {

	// quoting characters
	public final static char QUOTE_CHAR = '\'';
	public final static char DOUBLE_QUOTE_CHAR = '"';
	public final static String ILLICIT_CHAR_REPLACEMENT = "_"; //$NON-NLS-1$

	public final static char DECIMAL_POINT = '.';
	public final static char EXPONENT_SYMBOL = 'e';
	public final static char EXPONENT_SYMBOL_C = 'E';

	public static final char XML_NAMESPACE_DELIMITER = ':';

	private static final int DEFAULT_METAPHONE_LENGTH = 4;

	public final static int ASCII_LAST_CHAR = 127;

	private final static int SEQUENTIAL_TRANLATE_LENGTH = 16;

    private static final String HEX_STRING_ENCODING = "ISO-8859-1"; //$NON-NLS-1$
    private static final String HEX_STRING_LINE_SEPARATOR = System.getProperty("line.separator"); //$NON-NLS-1$
    private static final int HEX_STRING_BYTES_PER_LINE = 16;

    private static Pattern delimiterPattern;
	
	public final static String OBJECT_NAME_PATTERN = "[_A-Za-z]+[_A-Za-z0-9]*"; //$NON-NLS-1$
	public final static String GRAPH_NAME_PATTERN = "[^\\\\]+"; //$NON-NLS-1$
	
	private final static String INVALID_CHARACTER_CLASS = "[^_A-Za-z0-9]"; //$NON-NLS-1$
	private final static Pattern INVALID_CHAR = Pattern.compile(INVALID_CHARACTER_CLASS + "{1}"); //$NON-NLS-1$
	private final static Pattern LEADING_INVALID_SUBSTRING = Pattern.compile("^" + INVALID_CHARACTER_CLASS + "+");  //$NON-NLS-1$//$NON-NLS-2$
	private final static Pattern TRAILING_INVALID_SUBSTRING = Pattern.compile(INVALID_CHARACTER_CLASS + "+$"); //$NON-NLS-1$
	private final static Pattern INVALID_SUBSTRING = Pattern.compile(INVALID_CHARACTER_CLASS + "+"); //$NON-NLS-1$
	
	private static final int MAX_OBJECT_NAME_LENGTH = 250;
	
	private static Transliterator LATIN_TRANSLITERATOR = null;

	static {
		try {
			// try to be fail-safe to prevent errors caused by missing locales etc.
			LATIN_TRANSLITERATOR = Transliterator.getInstance("Latin");
		} catch (Throwable t) {
			Log logger = LogFactory.getLog(StringUtils.class);
			if (logger.isDebugEnabled()) {
				logger.debug("Failed to load transliterator", t);
			}
		}
	}
	
	private final static char[] vowels = {'A', 'E', 'I', 'O', 'U'};

	private static void initPattern() {
		delimiterPattern = Pattern.compile(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);
	}

	/**
	 * Removes all substrings matching the pattern from the given string.
	 *
	 * @param string a string to be processed
	 * @param pattern a regex pattern used to find the substrings
	 *
	 * @return a new string with all the substrings replaced by an empty string
	 *
	 * @version 7th October 2009
	 * @since 7th October 2009
	 */
	public static String removeAllSubstrings(String string, Pattern pattern) {
		if (string == null) {
			return null;
		}

		return pattern.matcher(string).replaceAll("");
	}

	/**
	 * Unescapes specified characters within the given string buffer.
	 *
	 * @param stringBuilder a string buffer containing escaped characters
	 * @param character the only character that should be unescaped
	 *
	 * @version 16th September 2009
	 * @since 15th September 2009
	 */
	public static void unescapeCharacters(StringBuilder stringBuilder, char character) {
		String characterString = String.valueOf(character);
		int index = stringBuilder.indexOf(characterString, 1);

		while (index >= 0) {
			if (stringBuilder.charAt(index - 1) == '\\') {
				stringBuilder.deleteCharAt(index - 1);
			}

			index = stringBuilder.indexOf(characterString, index + 1);
		}
	}

	/**
	 * Trims the XML namespace prefix and delimiter from the given elemenet name.
	 *
	 * @param elementName the element name with (or without) its namespace prefix
	 *
	 * @return the element name without its namespace prefix
	 *
	 * @since 30th November 2008
	 */
	public static String trimXmlNamespace(String elementName) {
		if (elementName == null) {
			return null;
		}

		return elementName.substring(elementName.indexOf(XML_NAMESPACE_DELIMITER) + 1);
	}

	/**
	 * Converts a string into its HEX representation (that can be used for display).
	 *
	 * @param string the string to be converted
	 *
	 * @return the string in HEX mode
	 *
	 * @throws NullPointerException if the string is <code>null</code>
	 *
	 * @since 6th January 2009
	 */
    public static String stringToHexString(String string) {
        if (string == null) {
            throw new NullPointerException("string");
        }

        StringBuilder stringBuilder = new StringBuilder();
        byte[] stringBytes = null;

        try {
            stringBytes = string.getBytes(HEX_STRING_ENCODING);
        } catch (UnsupportedEncodingException exception) {
            // this should NOT happen at all
        }

        for (int i = 0; i < stringBytes.length; i += HEX_STRING_BYTES_PER_LINE) {
            stringBuilder.append(String.format("%1$08X:  ", i));

            int lineLength = Math.min(HEX_STRING_BYTES_PER_LINE, stringBytes.length - i);

            for (int j = 0; j < lineLength; j++) {
                stringBuilder.append(String.format("%1$02X ", stringBytes[i + j]));
            }

            for (int j = lineLength; j < HEX_STRING_BYTES_PER_LINE; j++) {
                stringBuilder.append("   ");
            }

            stringBuilder.append(' ');

            for (int j = 0; j < lineLength; j++) {
                char currentChar = (char) stringBytes[i + j];
                stringBuilder.append(!Character.isWhitespace(currentChar) ? currentChar : ' ');
            }

            stringBuilder.append(HEX_STRING_LINE_SEPARATOR);
        }

        return stringBuilder.toString();
    }
    
    /**
	 * Converts a string into its HEX representation (that can be used for display).
	 *
	 * @param string the string to be converted
	 * @param charsetName 
	 * @return the string in HEX mode
	 *
	 * @throws NullPointerException if the string is <code>null</code>
	 * @throws IllegalArgumentException if charsetName is unsupported
	 *
	 * @since 17th October 2013
	 */
    public static String stringToHexString(String string, String charsetName) {
        if (string == null) {
            throw new NullPointerException("string");
        }

        StringBuilder stringBuilder = new StringBuilder();
        byte[] stringBytes = null;
        
        try {
            stringBytes = string.getBytes(charsetName);
        } catch (UnsupportedEncodingException exception) {
        	throw new IllegalArgumentException("Unsupported charsetName " + charsetName);
        }

        int lineSpacesForNextRow = 0;//count of spaces added to beginning of the row (if previous char is greater than one byte)
        int stringPosition = 0;//current absolute position within given string parameter
        for (int i = 0; i < stringBytes.length; i += HEX_STRING_BYTES_PER_LINE) {
            stringBuilder.append(String.format("%1$08X:  ", i));

            int lineLength = Math.min(HEX_STRING_BYTES_PER_LINE, stringBytes.length - i);

            for (int j = 0; j < lineLength; j++) {
                stringBuilder.append(String.format("%1$02X ", stringBytes[i + j]));
            }

            for (int j = lineLength; j < HEX_STRING_BYTES_PER_LINE; j++) {
                stringBuilder.append("   ");
            }

            stringBuilder.append(' ');
            
            int j = lineSpacesForNextRow;
            if(lineSpacesForNextRow>0) {//add spaces from previous row
            	for(int l=0; l<lineSpacesForNextRow;l++) {
            		stringBuilder.append(' ');
            	}
            	lineSpacesForNextRow = 0;
            }
            byte[] charBytes = null;
            while(j<lineLength) {
            	String currentCharacter = String.valueOf(string.charAt(stringPosition++));
            	stringBuilder.append(!Character.isWhitespace(currentCharacter.charAt(0)) ? currentCharacter : ' ');
            	try {
            		charBytes = currentCharacter.getBytes(charsetName);
            		j++;
            		if(charBytes.length>1) {
            			for(int k=0; k<charBytes.length-1; k++) {
            				if(j<lineLength) {
            					stringBuilder.append(' ');
            				} else {
            					lineSpacesForNextRow++;
            				}
            				j++;
            			}
            		}
	            } catch (UnsupportedEncodingException exception) {
	            	throw new IllegalArgumentException("Unsupported charsetName " + charsetName);
	            }
            }
            stringBuilder.append(HEX_STRING_LINE_SEPARATOR);
        }

        return stringBuilder.toString();
    }

	//FIXME Move to Utils ---------------------------
	private static final char[] HEX_CHARS = "0123456789abcdef".toCharArray();

    /**
	 * Converts the given byte array into its HEX representation (that can be used for display).
	 * For instance byte[] { 1, 10, 15, 16, 127, -127, 0, 63, 64, 65} will be converted to "010A0F107F81003F4041"
     * @param buf converted byte array
     * @return string hex representation of the given byte array
     */
    public static String bytesToHexString(byte[] buf) {
    	if (buf == null) {
    		return null;
    	}
    	
        char[] chars = new char[2 * buf.length];
        for (int i = 0; i < buf.length; ++i) {
            chars[2 * i] = HEX_CHARS[(buf[i] & 0xF0) >>> 4];
            chars[2 * i + 1] = HEX_CHARS[buf[i] & 0x0F];
        }
        return new String(chars);
    }

    /**
	 * Converts the given string with HEX representation of an array into the byte array.
	 * For instance "010A0F107F81003F4041" will be converted to byte[] { 1, 10, 15, 16, 127, -127, 0, 63, 64, 65}
     * @param str string to be converted
     * @return represented byte array
     */
    public static byte[] hexStringToBytes(String str) {
    	if (str == null) {
    		return null;
    	}
    	
		char[] charArray = str.toCharArray();
		byte[] byteArr = new byte[charArray.length / 2];
		int j = 0;
		for (int i = 0; i < charArray.length - 1; i = i + 2) {
			byteArr[j++] = (byte) (((byte) Character.digit(charArray[i], 16) << 4) | (byte) Character.digit(charArray[i + 1], 16));
		}
    	return byteArr;
    }
    
	public static boolean isVowel(char ch){
		return Arrays.binarySearch(vowels, ch) > -1;
	}
	
	/**
    * Finds the metaphone value of a String. This is similar to the
    * soundex algorithm, but better at finding similar sounding words.
    * All input is converted to upper case.
    * Limitations: Input format is expected to be a single ASCII word
    * with only characters in the A - Z range, no punctuation or numbers.
    *
    * @param input String to find the metaphone code for
    * @param maxLength Maximal length of output string
    * @return A metaphone code corresponding to the String supplied
    */
	public static String metaphone(String input, int maxLength){
		if(input == null){
			return null;
		}
		String tmp = getOnlyAlphaNumericChars(input.trim(), true, true).toUpperCase();
		char[] in = new char[tmp.length()]; 
		tmp.getChars(0, in.length, in, 0);
		StringBuilder metaphone = new StringBuilder();
		char previous = (char)-1;
		char current = in.length > 0 ?  in[0] : (char)-1;
		char next = in.length > 1 ? in[1] : (char)-1;
		char afternext = in.length > 2 ? in[2] : (char)-1;
		int length = 0;
		int index = 3;
		while (current != (char)-1 && length < maxLength) {
			if (isVowel(current)) {
				if (previous == (char)-1) {//vowels are only kept when they are first letter
					if (current == 'A' && next == 'E') {//if starts with ae - drop first letter
						previous = (char)-1; 
						current = next;
						next = afternext;
						afternext = in.length > index ? afternext = in[index++] : (char)-1;
						continue;
					}else{
						metaphone.append(current);
						length++;
					}
				}
			}else if (current == 'C' || current != previous){//consonant, double letters except "c" -> drop 2nd letter
				switch (current) {
				case 'B'://unless at the end of e word after "m" as in "dumb"
					if (previous != 'M' || next != (char)-1) {
						metaphone.append('B');
						length++;
					}
					break;
				case 'C':
					if ((next == 'I' && afternext != 'A') || next == 'E' || next == 'Y') {// -ci-, -ce-, -cy-, but not -cia-
						metaphone.append('S');
					}else if ((next == 'I' && afternext == 'A') || (next == 'H' && previous != 'S')) {//-cia- or -ch-, but not -sch-
						metaphone.append('X');
						if (next == 'H') {
							previous = next; 
							current = afternext;
							next = in.length > index ? afternext = in[index++] : (char)-1;
							afternext = in.length > index ? afternext = in[index++] : (char)-1;
							length++;
							continue;
						}
					}else{
						metaphone.append('K');
					}
					length++;
					break;
				case 'D':
					if (previous == 'D' && (next == 'E' || next == 'Y' || next == 'I')){
						metaphone.append('J');
					}else{
						metaphone.append('T');
					}
					length++;
					break;
				case 'F':
				case 'J':
				case 'L':
				case 'M':
				case 'N':
				case 'R':
					metaphone.append(current);
					length++;
					break;
				case 'G':
					if (next == 'I' || next == 'E' || next == 'Y'){
						metaphone.append('J');
						length++;
					}else if (next != 'N' && (next != 'H' || (afternext != (char)-1 && isVowel(afternext)))) {
						metaphone.append('K');
						length++;
					}//silent if in -gh- and not at the end or before a vowel in -gn- or -gned-
					break;
				case 'H'://silent if after vowel and no vowel follows
					if (!isVowel(previous) || isVowel(next)) {
						metaphone.append('H');
						length++;
					}
					break;
				case 'K'://silent if after 'c' or kn- on the beginning
					if (previous != 'C' && (previous != (char)-1 || next != 'N')) {
						metaphone.append('K');
						length++;
					}
					break;
				case 'P':
					if ((previous != (char)-1 || next != 'N')) {
						if (next == 'H') {
							metaphone.append('F');
						}else{
							metaphone.append('P');
						}
						length++;
					}
					break;
				case 'Q':
					metaphone.append('K');
					length++;
					break;
				case 'S':
					if (next == 'H' || (next == 'I' && (afternext == 'O' || afternext == 'A'))){
						metaphone.append('X');
						if (next == 'H') {
							previous = next; 
							current = afternext;
							next = in.length > index ? afternext = in[index++] : (char)-1;
							afternext = in.length > index ? afternext = in[index++] : (char)-1;
							length++;
							continue;
						}
					}else{
						metaphone.append('S');
					}
					length++;
					break;
				case 'T':
					if (next == 'I' && (afternext == 'A' || afternext == 'O')) {//if -tia- or -tio-
						metaphone.append('X');
						length++;
					}else if (next == 'H'){//if before 'h'
						metaphone.append('0');
						length++;
						previous = next; 
						current = afternext;
						next = in.length > index ? afternext = in[index++] : (char)-1;
						afternext = in.length > index ? afternext = in[index++] : (char)-1;
						length++;
						continue;
					}else if (next != 'C' || afternext != 'H'){//if not -tch-
						metaphone.append('T');
						length++;
					}
					break;
				case 'V': 
					metaphone.append('F');
					length++;
					break;
				case 'W':
					if (isVowel(next)) {
						metaphone.append('W');
						length++;
					}
					break;
				case 'X':
					if (previous != (char)-1) {
						metaphone.append("KS");
						length++;
					}else{
						metaphone.append('S');
					}
					length++;
					break;
				case 'Y':
					if (isVowel(next)) {
						metaphone.append('Y');
						length++;
					}
					break;
				case 'Z':
					metaphone.append('S');
					length++;
					break;
				default:
					throw new IllegalArgumentException("Not English word: " + input);
				}
			}
			previous = current; 
			current = next;
			next = afternext;
			afternext = in.length > index ? afternext = in[index++] : (char)-1;
		}
		return metaphone.toString();
	}
	
	public static String metaphone(String input){
		return metaphone(input, DEFAULT_METAPHONE_LENGTH);
	}

	/**
	 * Encodes a given word using the Double Metaphone algorithm.
	 *
	 * @param word the word to be encoded
	 *
	 * @return the result of encoding
	 */
	public static String doubleMetaphone(String word) {
		DoubleMetaphone doubleMetaphone = new DoubleMetaphone();
		doubleMetaphone.setMaxCodeLen(word.length());

		return doubleMetaphone.doubleMetaphone(word);
	}

	/**
	 * Finds The New York State Identification and Intelligence System Phonetic Code
	 * 
	 * @param input String to find the NYSIIS code for
	 * @return NYSIIS code
	 */
	public static String NYSIIS(String input){
		if (input == null){
			return null;
		}
		if (input.equals("")){ 
			return "";
		}
		StringBuilder nysiis = new StringBuilder();
		String tmp = input.trim().toUpperCase();
		char[] in = new char[tmp.length()]; 
		int index = 0;
		if (tmp.startsWith("MAC")) {
			in[index++] = 'M';
			in[index++] = 'C';
			in[index++] = 'C';
		}else if (tmp.startsWith("KN")){
			in[index++] = 'N';
			in[index++] = 'N';
		}else if (tmp.startsWith("K")){
			in[index++] = 'C';
		}else if (tmp.startsWith("PH")){
			in[index++] = 'F';
			in[index++] = 'F';
		}else if (tmp.startsWith("PF")){
			in[index++] = 'F';
			in[index++] = 'F';
		}else if (tmp.startsWith("SCH")) {
			in[index++] = 'S';
			in[index++] = 'S';
			in[index++] = 'S';
		}
		int endIndex = tmp.length();
		if (tmp.endsWith("EE")) {
			in[--endIndex] = (char)-1;
			in[--endIndex] = 'Y';
		}else if (tmp.endsWith("IE")){
			in[--endIndex] = (char)-1;
			in[--endIndex] = 'Y';
		}else if (tmp.endsWith("DT")){
			in[--endIndex] = (char)-1;
			in[--endIndex] = 'D';
		}else if (tmp.endsWith("RT")){
			in[--endIndex] = (char)-1;
			in[--endIndex] = 'D';
		}else if (tmp.endsWith("RD")){
			in[--endIndex] = (char)-1;
			in[--endIndex] = 'D';
		}else if (tmp.endsWith("NT")){
			in[--endIndex] = (char)-1;
			in[--endIndex] = 'D';
		}else if (tmp.endsWith("ND")){
			in[--endIndex] = (char)-1;
			in[--endIndex] = 'D';
		}
		tmp.getChars(index, endIndex, in, index);
		index = 0;
		char previous = in[index++];
		char current = in.length > index ?  in[index++] : (char)-1;
		char next = in.length > index ? in[index++] : (char)-1;
		char afternext = in.length > index ? in[index++] : (char)-1;		
		char lastAppended = previous;
		nysiis.append(previous);
		
		while (current != (char)-1) {
			if (isVowel(current)) {
				if (lastAppended != 'A') {
					lastAppended = 'A';
					nysiis.append(lastAppended);
				}
				if (current == 'E' && next == 'V') {
					previous = 'A';
					current = 'F';
					next = afternext;
					afternext = in.length > index ?  in[index++] : (char)-1;
				}else{
					previous = 'A';
					current = next;
					next = afternext;
					afternext = in.length > index ?  in[index++] : (char)-1;
				}
			}else{
				switch (current) {
				case 'Q':
					previous = 'G';
					current = next;
					next = afternext;
					afternext = in.length > index ?  in[index++] : (char)-1;
					break;
				case 'Z':
					previous = 'S';
					current = next;
					next = afternext;
					afternext = in.length > index ?  in[index++] : (char)-1;
					break;
				case 'M':
					previous = 'N';
					current = next;
					next = afternext;
					afternext = in.length > index ?  in[index++] : (char)-1;
					break;
				case 'K':
					if (next == 'N'){
						previous = 'N';
						current = afternext;
						next = in.length > index ?  in[index++] : (char)-1;
						afternext = in.length > index ?  in[index++] : (char)-1;
					}else{
						previous = 'C';
						current = next;
						next = afternext;
						afternext = in.length > index ?  in[index++] : (char)-1;
					}
					break;
				case 'S':
					if (next == 'C' && afternext == 'H') {
						previous = current;
						current = 'S';
						next = 'S';
						afternext = in.length > index ?  in[index++] : (char)-1;
					}else{
						previous = current;
						current = next;
						next = afternext;
						afternext = in.length > index ?  in[index++] : (char)-1;
					}
					break;
					case 'P':
						if (next == 'H') {
							previous = 'F';
							current = 'F';
							next = afternext;
							afternext = in.length > index ?  in[index++] : (char)-1;
						}else{
							previous = current;
							current = next;
							next = afternext;
							afternext = in.length > index ?  in[index++] : (char)-1;
							
						}
						break;
					case 'H':
						if (!isVowel(previous) || !isVowel(next)) {
							current = next;
							next = afternext;
							afternext = in.length > index ?  in[index++] : (char)-1;
						}else{
							previous = current;
							current = next;
							next = afternext;
							afternext = in.length > index ?  in[index++] : (char)-1;
						}
						break;
					case 'W':
						if (isVowel(previous)){
							current = next;
							next = afternext;
							afternext = in.length > index ?  in[index++] : (char)-1;
						}else{
							previous = current;
							current = next;
							next = afternext;
							afternext = in.length > index ?  in[index++] : (char)-1;
						}
						break;
				default:
					previous = current;
					current = next;
					next = afternext;
					afternext = in.length > index ?  in[index++] : (char)-1;
					break;
				}
				if (lastAppended != previous) {
					lastAppended = previous;
					nysiis.append(lastAppended);
				}
			}
		}
		if (lastAppended == 'S' || lastAppended == 'A') {
			nysiis.setLength(nysiis.length() - 1);
		}else if (nysiis.toString().endsWith("AY")) {//repleace "AY" by "Y"
			nysiis.deleteCharAt(nysiis.length() - 2);
		}
		
		return nysiis.toString();
	}
	
	/**
	 * Converts control characters into textual representation<br>
	 * Note: This code handles only \n, \r ,\t ,\f, \b, \\ special chars
	 * 
	 * @param controlString
	 *            string containing control characters
	 * @return string where control characters are replaced by their text representation (i.e.\n -> "\n" )
	 * @since July 25, 2002
	 */
	public static String specCharToString(CharSequence controlString) {
		if (controlString == null) {
			return null;
		}

		StringBuilder copy = new StringBuilder();
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
				break;
			default:
				if (!StringUtils.isPrintableChar(character)) {
					// if the character is not printable - for instance 'Ctrl+B' - string representation '\u0000' will be used
					copy.append("\\u");
					String hex = Integer.toHexString(character & 0xFFFF);	// Get hex value of the char. 
					for (int j = 0; j < 4 - hex.length(); j++) {	// Prepend zeros because unicode requires 4 digits
						copy.append("0");
					}
					copy.append(hex.toLowerCase());		// standard unicode format.
				} else {
					copy.append(character);
				}
			}
		}
		return copy.toString();
	}

	/**
	 * Converts a given character to its Unicode escape representation.
	 * 
	 * @param character
	 * @return Unicode escape string for <code>character</code>
	 */
	public static String toUnicode(char character) {
		StringBuilder outBuffer = new StringBuilder();
		outBuffer.append("\\u");
		String hex = Integer.toHexString(character & 0xFFFF);	// Get hex value of the char. 
		for (int j = 0; j < 4 - hex.length(); j++) {	// Prepend zeros because unicode requires 4 digits
			outBuffer.append("0");
		}
		outBuffer.append(hex.toLowerCase());		// standard unicode format.
        return outBuffer.toString();
	}
	
	/**
	 * In <code>inputString</code>, 
	 * replaces all characters from <code>escapeChars</code>
	 * by their Unicode escape strings.
	 * 
	 * To convert back, {@link #stringToSpecChar(CharSequence)}
	 * can be used.
	 * 
	 * @param inputString the input string
	 * @param escapeChars the characters to be replaced
	 * @return <code>inputString</code> with <code>escapeChars</code> replaced with their Unicode escape codes
	 */
	public static String unicodeEscapeChars(String inputString, String escapeChars) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < inputString.length(); i++) {
			char c = inputString.charAt(i);
			if (escapeChars.indexOf(c) >= 0) {
				sb.append(toUnicode(c));
			} else {
				sb.append(c);
			}
		}
		return sb.toString();
	}
	
	
	private static String stringToSpecChar(CharSequence controlString, boolean lenient) {
		if (controlString == null) {
			return null;
		}

		StringBuilder copy = new StringBuilder();
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
				case '`':
					copy.append('`');
					break;
				case 'f':
					copy.append('\f');
					break;
				case 'b':
					copy.append('\b');
					break;
				case 'u':	// '\u003B' will be converted to ';'
					if (i + 4 < controlString.length()) {
						String hex = controlString.subSequence(i + 1, i + 5).toString();
						char[] chars;
						try {
							chars = Character.toChars(Integer.parseInt(hex, 16));
							copy.append(chars);
							i += 4;
						} catch (NumberFormatException e) {
							if (lenient) {
								copy.append('\\');
								copy.append('u');
							} else {
								throw new IllegalArgumentException("Invalid unicode character \\u" + hex);
							}
						}
					} else {
						if (lenient) {
							copy.append('\\');
							copy.append('u');
						} else {
							throw new IllegalArgumentException("Invalid unicode character");
						}
					}
					break;
				default:
					if (lenient) {
						copy.append('\\');
						copy.append(character);
					} else {
						throw new IllegalArgumentException("Invalid escape sequence: \\" + character);
					}
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
		if (isBackslash) {
			if (lenient) {
				copy.append('\\');
			} else {
				throw new IllegalArgumentException("Invalid escape sequence: \\");
			}
		}
		return copy.toString();
	}

	/**
	 * Converts textual representation of control characters into control characters<br>
	 * Note: This code handles only \n, \r , \t , \f, \" ,\', \`, \\ special chars
	 * 
	 * @param controlString
	 *            Description of the Parameter
	 * @return String with control characters
	 * @since July 25, 2002
	 */
	public static String stringToSpecChar(CharSequence controlString) {
		return stringToSpecChar(controlString, true);
	}

	/**
	 * Converts textual representation of control characters into control characters<br>
	 * Note: This code handles only \n, \r , \t , \f, \" ,\', \`, \\ special chars
	 * 
	 * @param controlString
	 *            Description of the Parameter
	 * @return String with control characters
	 * 
	 * @throws IllegalArgumentException if an invalid escape sequence is encountered
	 * 
	 * @since July 25, 2002
	 */
	public static String stringToSpecCharStrict(CharSequence controlString) {
		return stringToSpecChar(controlString, false);
	}

	/**
	 * Formats string from specified messages and their lengths.<br>
	 * Negative (&lt;0) length means justify to the left, positive (&gt;0) to the right<br>
	 * If message is longer than specified size, it is trimmed; if shorter, it is padded with specified fill char.
	 * 
	 * @param messages
	 *            array of objects with toString() implemented methods
	 * @param sizes
	 *            array of desired lengths (+-) for every message specified
	 * @return Formatted string
	 */
	public static String formatString(Object[] messages, int[] sizes, char fillchar) {
		int formatSize;
		String message;
		StringBuilder strBuff = new StringBuilder(100);
		for (int i = 0; i < messages.length; i++) {
			message = messages[i] != null ? messages[i].toString() : "";
			// left or right justified ?
			if (sizes[i] < 0) {
				formatSize = sizes[i] * (-1);
				fillString(strBuff, message, 0, formatSize,fillchar);
			} else {
				formatSize = sizes[i];
				if (message.length() < formatSize) {
					fillBlank(strBuff, formatSize - message.length());
					fillString(strBuff, message, 0, message.length(),fillchar);
				} else {
					fillString(strBuff, message, 0, formatSize,fillchar);
				}
			}
		}
		return strBuff.toString();
	}
	
	/**
	 * Like formatString() but pads with blanks
	 * 
	 * @param messages
	 * @param sizes
	 * @return
	 */
	
	public static String formatString(Object[] messages, int[] sizes){
		return formatString(messages,sizes,' ');
	}
	
	

	/**
	 * Fills the provided buffer with source string and pads with specified fill character up to
	 * specified length
	 * 
	 * @param strBuf
	 *            Description of the Parameter
	 * @param source
	 *            Description of the Parameter
	 * @param start
	 *            Description of the Parameter
	 * @param length
	 *            Description of the Parameter
	 */
	private static void fillString(StringBuilder strBuff, String source, int start, int length,char fillchar) {
		int srcLength = source.length();
		for (int i = start; i < start + length; i++) {
			if (i < srcLength) {
				strBuff.append(source.charAt(i));
			} else {
				strBuff.append(fillchar);
			}
		}
	}
	
	private static void fillString(StringBuilder strBuff, String source, int start, int length) {
		fillString(strBuff,source,start,length,' ');
	}
	

	/**
	 * Description of the Method
	 * 
	 * @param strBuf
	 *            Description of the Parameter
	 * @param length
	 *            Description of the Parameter
	 */
	private static void fillBlank(StringBuilder strBuff, int length) {
		for (int i = 0; i < length; i++) {
			strBuff.append(' ');
		}
	}

	/**
	 * Returns True, if the character sequence contains only valid identifier chars.<br>
	 * "[_A-Za-z]+[_A-Za-z0-9]*"
	 * 
	 * @param seq
	 *            Description of the Parameter
	 * @return The validObjectName value
	 */
	public static boolean isValidObjectName(CharSequence seq) {
		if (seq == null) {
			return false;
		}
		if (seq.length() > MAX_OBJECT_NAME_LENGTH) {
			return false;
		}

		return seq.toString().matches(OBJECT_NAME_PATTERN);

	}
	
	/**
	 * Validates name of graph
	 * 
	 * @param name name of graph
	 * @return <code>true</code> if name is a valid graph name, <code>false</code> otherwise.
	 */
	public static boolean isValidGraphName(CharSequence name) {
		if (name == null) {
			return false;
		}
		return name.toString().matches(GRAPH_NAME_PATTERN);
	}
	
	/**
	 * This function should be used to convert arbitrary field
	 * or record name into a valid identifier.
	 * 
	 * Valid identifiers (containing) should be left untouched.
	 * 
	 * Empty strings should be replaced with a string containing
	 * one underscore so that metadata extraction works even for
	 * databases which do not return table name in the ResultSetMetadata.
	 * 
	 * Even valid identifiers should be truncated to DEFAULT_TRUNC_LENGTH.
	 * 
	 * @param originalName
	 * @return
	 */
	public static String normalizeName(String originalName) {
		if (originalName == null) {
			return null;
		}
		String result;
		
		if (isValidObjectName(originalName)) {
			// do not modify valid names
			result = originalName;
		} else {
			// trim whitespace first
			originalName = originalName.trim();
			
			// first transliterate to Latin characters
			result = StringUtils.transliterateToLatin(originalName);
			
			// must be done before removing invalid, can be swapped with removing non-printable
			result = StringUtils.removeDiacritic(result);
			// must be done before removing invalid, can be swapped with removing diacritic
			result = StringUtils.removeNonPrintable(result);
			
			// must be done before prepending an underscore
			result = LEADING_INVALID_SUBSTRING.matcher(result).replaceFirst(""); // beginning
			result = TRAILING_INVALID_SUBSTRING.matcher(result).replaceFirst(""); // end
			result = INVALID_SUBSTRING.matcher(result).replaceAll(ILLICIT_CHAR_REPLACEMENT);
			
			// isDigit accepts other than arabic digits, but those should be removed above
			// also replace empty string with a single underscore
			if (result.isEmpty() || Character.isDigit(result.charAt(0))) {
				result = "_" + result;
			}
		}
		
		// trim also valid identifiers
		// must be done last (or when the length never changes again)
		if (result.length() > MAX_OBJECT_NAME_LENGTH) {
			result = result.substring(0, MAX_OBJECT_NAME_LENGTH);
		}
		
		return result;
	}
	
	/**
	 * Normalizes the name, inserts an underscore after each 
	 * lower-case letter followed by an upper-case letter
	 * and converts the result to upper-case. 
	 * 
	 * The proposed ID may not be unique!
	 * 
	 * @param name name of the component
	 * @return proposal of the new ID of the component (not unique)
	 */
	public static String convertNameToId(String name) {
		if (isBlank(name)) {
			return null;
		}
		String id = normalizeName(name).replaceAll("([a-z])([A-Z])", "$1_$2").toUpperCase(); 
		return id;
	}


	/**
	 * Returns an array containing strings from originalNames
	 * with numbers appended if necessary.
	 * 
	 * @param originalNames
	 * @return unique names
	 */
	public static String[] getUniqueNames(String... originalNames) {
		return getUniqueNames(Arrays.asList(originalNames));
	}
	
	/**
	 * Returns an array containing strings from originalNames with numbers appended
	 * so that resulting strings are unique with respect to both originalNames and otherNames.
	 * 
	 * @param originalNames names to make unique
	 * @param reservedNamesNames other already unique reserved names to take into account
	 * @return unique names made from originalNames
	 */
	public static String[] getUniqueNames(String[] originalNames, List<String> reservedNamesNames) {
		return getUniqueNames(Arrays.asList(originalNames), reservedNamesNames);
	}
	
	/**
	 * Returns an array containing strings from originalNames
	 * with numbers appended if necessary.
	 * 
	 * @param originalNames
	 * @return unique names
	 */
	public static String[] getUniqueNames(List<String> originalNames) {
		return getUniqueNames(originalNames, null);
	}
	
	/**
	 * Returns an array containing strings from originalNames with numbers appended
	 * so that resulting strings are unique with respect to both originalNames and otherNames.
	 * 
	 * @param originalNames names to make unique
	 * @param reservedNames other already unique reserved names to take into account
	 * @return unique names made from originalNames
	 */
	public static String[] getUniqueNames(List<String> originalNames, List<String> reservedNames) {
		int length = originalNames.size();
		String[] result = new String[length];
		Set<String> uniqueNames = reservedNames == null ? new HashSet<String>() : new HashSet<String>(reservedNames);
		for (int n = 0; n < length; n++) {
			String newName = originalNames.get(n);
			if (!uniqueNames.contains(newName)) {
				result[n] = newName;
				uniqueNames.add(newName);
			} else {
				int i = 1;
				String extendedName = newName + "_" + i;
				while (uniqueNames.contains(extendedName)) {
					extendedName = newName + "_" + (++i);
				}
				result[n] = extendedName;
				uniqueNames.add(extendedName);
			}
		}
		return result;
	}
	
	/**
	 * Normalizes all strings in originalNames and ensures that
	 * the results are unique.
	 * 
	 * @param originalNames
	 * @return unique normalized names
	 */
	public static String[] normalizeNames(String... originalNames) {
		return normalizeNames(Arrays.asList(originalNames), null);
	}
	
	/**
	 * Normalizes all strings in originalNames and ensures that
	 * the results are unique.
	 * 
	 * @param originalNames
	 * @return unique normalized names
	 */
	public static String[] normalizeNames(List<String> originalNames) {
		return normalizeNames(originalNames, null);
	}
	
	/**
	 * Normalizes all strings in originalNames and ensures that
	 * the results are unique also with respect to reservedNames.
	 * 
	 * @param originalNames names to normalize and to make unique
	 * @param reservedNames already normalized and unique names which cannot appear in result
	 * @return unique normalized names
	 */
	public static String[] normalizeNames(List<String> originalNames, List<String> reservedNames) {
		String[] normalizedNames = new String[originalNames.size()];
		int i = 0;
		for (String s: originalNames) {
			normalizedNames[i++] = normalizeName(s);
		}
		return getUniqueNames(normalizedNames, reservedNames);
	}

	public static boolean isValidObjectId(CharSequence seq) {
		if (seq == null) {
			return false;
		}

		for (int i = 0; i < seq.length(); i++) {
			if (!Character.isUnicodeIdentifierPart(seq.charAt(i))) {
				return false;
			}
		}
		return true;
	}

	/**
	 * This method changes all characters, which can be part of identifier, in given string to "_"
	 * 
	 * @param seq
	 * @return
	 */
	public static String normalizeString(CharSequence seq) {
		if (seq == null) {
			return null;
		}

		StringBuilder result = new StringBuilder(removeDiacritic(seq.toString()));
		if (result.length() == 0 || Character.isDigit(result.charAt(0))) {
			result.insert(0, ILLICIT_CHAR_REPLACEMENT);
		}
		Matcher invalidNameMatcher = INVALID_CHAR.matcher(result);
		return invalidNameMatcher.replaceAll(ILLICIT_CHAR_REPLACEMENT);
	}

	/**
	 * Returns true if the passed-in character is quote ['] or double quote ["].
	 * 
	 * @param character
	 * @return true if character equals to ['] or ["]
	 */
	public static final boolean isQuoteChar(char character) {
		return character == QUOTE_CHAR || character == DOUBLE_QUOTE_CHAR;
	}

	/**
	 * Returns true of passed-in string is quoted - i.e. the first character is quote character and the last character
	 * is equal to the first one.
	 * 
	 * @param str
	 * @return true if the string is quoted
	 */
	public static final boolean isQuoted(CharSequence str) {
		return str != null && str.length() > 1 && isQuoteChar(str.charAt(0)) && str.charAt(0) == str.charAt(str.length() - 1);
	}

	/**
	 * Modifies buffer scope so that the string quotes are ignored, in case quotes are not present doesn't do anything.
	 * 
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
	 * Modifies string so that the string quotes are ignored, in case quotes are not present doesn't do anything.
	 * 
	 * @param str
	 * @return
	 */
	public static String unquote(String str) {
		if (StringUtils.isQuoted(str)) {
			str = str.substring(1, str.length() - 1);
		}
		return str;
	}

	/**
	 * Modifies string so that the string quotes are ignored, in case quotes are not present doesn't do anything.
	 * Escaped quote character is left without the backslash.
	 * For example:
	 * dummy -> dummy
	 * "dummy" -> dummy
	 * a"dummy" -> a"dummy"
	 * "dummy"a -> "dummy"a
	 * "I said: \"hello\"" -> I said: "hello"
	 * 
	 * @param str
	 * @return
	 */
	public static String unquote2(String s) {
        if (s == null) {
            return null;
        }
        if (s.length() < 2) {
            return s;
        }

        char first = s.charAt(0);
        char last = s.charAt(s.length()-1);
        if (first != last || (first != '"' && first != '\'')) {
            return s;
        }
        
        StringBuilder b = new StringBuilder(s.length() - 2);
    	boolean quote = false;
        for (int i = 1; i < s.length() - 1; i++) {
            char c = s.charAt(i);

            if (c == '\\' && !quote) {
                quote = true;
                continue;
            }
            quote = false;
            b.append(c);
        }
        
        return b.toString();
	}
	
	/**
	 * The given string is simply surrounded by quoting character. 
	 * Quote characters already present in the given string are left unchanged. 
	 * @param str
	 * @return
	 */
	public static String quote(CharSequence str) {
		return "\"".concat(str.toString()).concat("\"");
	}

	public static StringBuilder trimLeading(StringBuilder str) {
		int pos = 0;
		int length = str.length();
		while (pos < length && Character.isWhitespace(str.charAt(pos))) {
			pos++;
		}
		str.delete(0, pos);
		return str;
	}

	public static CloverString trimLeading(CloverString str) {
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
	 * 
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
	 * 
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

	public static StringBuilder trimTrailing(StringBuilder str) {
		int pos = str.length() - 1;
		while (pos >= 0 && Character.isWhitespace(str.charAt(pos))) {
			pos--;
		}
		str.setLength(pos + 1);
		return str;
	}

	public static CloverString trimTrailing(CloverString str) {
		int pos = str.length() - 1;
		while (pos >= 0 && Character.isWhitespace(str.charAt(pos))) {
			pos--;
		}
		str.setLength(pos + 1);
		return str;
	}

	/**
	 * Modifies buffer scope so that the leading and trailing whitespace is ignored.
	 * 
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

	public static CloverString trim(CloverString buf) {
		trimLeading(buf);
		trimTrailing(buf);
		return buf;
	}

	/**
	 * This method removes from the string characters which are not letters nor digits
	 * 
	 * @param str -
	 *            input String
	 * @param takeAlpha -
	 *            if true method leaves letters
	 * @param takeNumeric -
	 *            if true method leaves digits
	 * @return String where are only letters and (or) digits from input String
	 */
	public static String getOnlyAlphaNumericChars(String str, boolean takeAlpha, boolean takeNumeric) {
		if (str == null){
			return null;
		}
		if (!takeAlpha && !takeNumeric) {
			return str;
		}
		int counter = 0;
		int length = str.length();
		char[] chars = str.toCharArray();
		char[] result = new char[length];
		char character;
		for (int j = 0; j < length; j++) {
			character = chars[j];
			if ((Character.isLetter(character) && takeAlpha) || (Character.isDigit(character) && takeNumeric)) {
				result[counter++] = chars[j];
			}
		}
		return new String(result, 0, counter);
	}

	/**
	 * This method removes from string blank space
	 * 
	 * @param str -
	 *            input String
	 * @return input string without blank space
	 */
	public static String removeBlankSpace(String str) {
		if (str == null){
			return null;
		}
		int length = str.length();
		int counter = 0;
		char[] chars = str.toCharArray();
		char[] result = new char[length];
		for (int j = 0; j < length; j++) {
			if (!Character.isWhitespace(chars[j])) {
				result[counter++] = chars[j];
			}
		}
		return new String(result, 0, counter);
	}

	public static StringBuilder removeBlankSpace(StringBuilder target, CharSequence str) {
		int length = str.length();
		char character = 0;
		for (int j = 0; j < length; j++) {
			character = str.charAt(j);
			if (!Character.isWhitespace(character)) {
				target.append(character);
			}
		}
		return target;
	}

	/**
	 * Test whether parameter consists of space characters only
	 * 
	 * @param data
	 * @return true if parameter contains space characters only
	 */
	public static boolean isBlank(CharBuffer data) {
		data.mark();
		for (int i = 0; i < data.length(); i++) {
			if (!Character.isSpaceChar(data.get())) {
				data.reset();
				return false;
			}
		}
		data.reset();
		return true;
	}

	/**
	 * Test whether parameter consists of space characters only
	 * 
	 * @param data
	 * @return true if parameter contains space characters only
	 */
	public static boolean isBlank(CharSequence data) {
		if (data == null) {
			return true;
		}
		for (int i = 0; i < data.length(); i++) {
			if (!Character.isSpaceChar(data.charAt(i))) {
				return false;
			}
		}
		return true;
	}
	
	/**
	 * Replaces the characters from non-latin alphabets
	 * with their Latin counterparts.
	 * 
	 * @param str input string
	 * @return transliterated string
	 */
	public static String transliterateToLatin(String str) {
		if ((LATIN_TRANSLITERATOR == null) || (str == null)) {
			return str;
		}
		try {
			return LATIN_TRANSLITERATOR.transliterate(str);
		} catch (Exception ex) {
			Log logger = LogFactory.getLog(StringUtils.class);
			if (logger.isDebugEnabled()) {
				logger.debug("Failed to transliterate " + str, ex);
			}
			return str;
		}
	}

	/**
	 * This method replaces diacritic chars by theirs equivalence without diacritic. It works only for chars for which
	 * decomposition is defined
	 * 
	 * @param str
	 * @return string in which diacritic chars are replaced by theirs equivalences without diacritic
	 */
	public static String removeDiacritic(String str) {
		if (str == null){
			return null;
		}
		return Normalizer.decompose(str, false, 0).replaceAll("\\p{InCombiningDiacriticalMarks}+", "");
	}

	public static String removeNonPrintable(String str){
		if (str == null){
			return null;
		}
		return str.replaceAll("[\\p{C}]+", ""); // CLO-1814 - previously "[^\\p{Print}]+"
	}
	
	/**
	 * This method concates string array to one string. Parts are delimited by given char
	 * 
	 * @param strings -
	 *            input array of strings
	 * @param delimiter
	 * @return
	 */
	public static String stringArraytoString(String[] strings, char delimiter) {
		return stringArraytoString(strings, String.valueOf(delimiter));
	}

	/**
	 * This method concates string array to one string. Parts are delimited by given string
	 * 
	 * @param strings -
	 *            input array of strings
	 * @param delimiter
	 * @return
	 */
	public static String stringArraytoString(String[] strings, String delimiter) {
		int length = strings.length;
		if (length == 0) {
			return null;
		}
		StringBuilder result = new StringBuilder();
		for (int i = 0; i < length; i++) {
			result.append(strings[i]);
			if (i < length - 1) {
				result.append(delimiter);
			}
		}
		return result.toString();
	}

	/**
	 * This method concates string array to one string. Parts are delimited by ' ' (one space)
	 * 
	 * @param strings -
	 *            input array of strings
	 * @return
	 */
	public static String stringArraytoString(String[] strings) {
		return stringArraytoString(strings, ' ');
	}

	/**
	 * This method concates string array to one string. Parts are delimited by given string
	 * 
	 * @param strings -
	 *            input array of strings
	 * @param delimiter
	 * @return
	 */
	public static String mapToString(Map<?, ?> map, String assignChar, String delimiter) {
		if (map.size() == 0) {
			return null;
		}
		StringBuilder result = new StringBuilder();
		for (Object entry : map.entrySet()) {
			result.append(((Entry<?, ?>) entry).getKey().toString());
			result.append(assignChar);
			result.append(((Entry<?, ?>) entry).getValue().toString());
			result.append(delimiter);
		}
		result.setLength(result.length() - delimiter.length());
		return result.toString();
	}

	/**
	 * Splits the given string into mapping items. It's compatible with double quoted strings, so a delimiter in a
	 * double quoted string doesn't cause a split.
	 * 
	 * @param str
	 * @return mapping items.
	 */
	public static String[] split(String str) {
		if (delimiterPattern == null) {
			initPattern();
		}
		Matcher delimiterMatcher = delimiterPattern.matcher(str);
		boolean isQuoted;
		ArrayList<String> result = new ArrayList<String>();
		int index = 0;
		String candidate;
		while (delimiterMatcher.find()) {
			candidate = str.substring(index, delimiterMatcher.start());
			isQuoted = count(candidate, '"') % 2 != 0;
			if (!isQuoted) {
				result.add(candidate);
				index = delimiterMatcher.end();
			}
		}
		if (index < str.length()) {// if string doesn't end with delimiter, add all after last delimiter found
			result.add(str.substring(index));
		}
		return result.toArray(new String[result.size()]);
	}

	public static boolean isEmpty(CharSequence s) {
		return s == null || s.length() == 0;
	}

	  /**
	   * Splits input string into parts delimited by specified delimiter. It's compatible with double quoted strings and quoted strings, so a delimiter in a
	   * double quoted string or quoted string doesn't cause a split.
	   * 
	   * @param input
	   * @param delimiter
	   * @return
	   */
	  public static String[] split(String input, String delimiter) {
	    if(input==null) {
	      return null;
	    }
	    if(delimiter==null || delimiter.length()==0) {
	      return new String[] {input};
	    }
	    boolean escaped = false;
	    char quote = 0;
	    
	    ArrayList<String> parts = new ArrayList<String>();
	    StringBuilder currentPart = new StringBuilder();
	    
	    for(int i=0; i<input.length(); i++) {
	      char c = input.charAt(i);
	      
	      //this character is escaped
	      if(escaped) {
	        //if we are not inside quotes, start string
	        if(quote==0) {
	          if(c=='"' || c=='\'') {
	            quote=c;
	          }
	        }
	        escaped = false;
	      }else{
	        //next character is escaped
	        if(c=='\\') { escaped = true; }
	        else if(c=='"' || c=='\'') {
	          //end of string
	          if(quote==c) { quote = 0; }
	          //start of string
	          else if(quote==0) { quote = c; }
	        }
	      }
	      //check for bounds of string
	      int endIndex = i+delimiter.length();
	      if(endIndex>input.length()) {
	        endIndex = input.length();
	      }
	      if(quote==0 && input.substring(i,endIndex).equals(delimiter)) {
	        //delimiter found .... let's split
	        i = i + delimiter.length()-1;
	        parts.add(currentPart.toString());
	        currentPart = new StringBuilder();
	      }else{
	        currentPart.append(c);
	      }
	    }
	    //add last part
	    if(currentPart.length()>0) {
	      parts.add(currentPart.toString());
	    }
	    return parts.toArray(new String[] {});
	  }
	  	
	
	/**
	 * This method finds index of string from string array
	 * 
	 * @param str
	 *            String to find
	 * @param array
	 *            String array for searching
	 * @return index or found String or -1 if String was not found
	 */
	public static int findString(String str, String[] array) {
		for (int i = 0; i < array.length; i++) {
			if (str.equals(array[i])) {
				return i;
			}
		}
		return -1;
	}

	public static int findString(String str, String[] array, int fromIndex) {
		for (int i = fromIndex; i < array.length; i++) {
			if (str.equals(array[i])) {
				return i;
			}
		}
		return -1;
	}

	/**
	 * This method finds index of string from string array
	 * 
	 * @param str
	 *            String to find
	 * @param array
	 *            String array for searching
	 * @return index or found String or -1 if String was not found
	 */
	public static int findStringIgnoreCase(String str, String[] array) {
		for (int i = 0; i < array.length; i++) {
			if (str.equalsIgnoreCase(array[i])) {
				return i;
			}
		}
		return -1;
	}

	/**
	 * This method finds index of first charater, which can't be part of identifier
	 * 
	 * @param str
	 *            String for searching
	 * @param fromIndex
	 * @return index of first character after identifier name
	 */
	public static int findIdentifierEnd(CharSequence str, int fromIndex) {
		int index = fromIndex;
		while (index < str.length() && Character.isUnicodeIdentifierPart(str.charAt(index))) {
			index++;
		}
		return index;
	}

	/**
	 * This method finds index of first charater, which can be part of identifier
	 * 
	 * @param str
	 *            String for searching
	 * @param fromIndex
	 * @return index of first character, which can be in identifier name
	 */
	public static int findIdentifierBegining(CharSequence str, int fromIndex) {
		int index = fromIndex;
		while (index < str.length() && !Character.isUnicodeIdentifierPart(str.charAt(index))) {
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
	public static int getMaxLength(String... strings) {
		int length = 0;
		for (int i = 0; i < strings.length; i++) {
			if (strings[i] != null && strings[i].length() > length) {
				length = strings[i].length();
			}
		}
		return length;
	}

	/**
	 * This method checks if given string can be parse to integer number
	 * 
	 * @param str
	 *            string to check
	 * @return -1 if str is not integer<br>
	 *         0 if str can be parsed to short<br>
	 *         1 if str can be parsed to int<br>
	 *         2 if str can be parsed to long<br>
	 *         3 if str is integer but has more than 18 digits
	 */
	public static short isInteger(CharSequence str) {
		if (str == null || str.length() == 0) {
			return -1;
		}
		int start = 0;
		if (str.charAt(0) == '-') {
			start = 1;
		}
		int length = 0;
		for (int index = start; index < str.length(); index++) {
			if (!Character.isDigit(str.charAt(index))) {
				return -1;
			}
			length++;
		}
		if (length <= 0) {
			return -1;
		}
		if (length <= 4) {
			return 0;
		}
		if (length <= DataFieldMetadata.INTEGER_LENGTH) {
			return 1;
		}
		if (length <= DataFieldMetadata.LONG_LENGTH) {
			return 2;
		}
		return 3;
	}

	/**
	 * This method checks if given string can be parse to double number with 10 radix
	 * 
	 * @param str
	 * @return
	 */
	public static boolean isNumber(CharSequence str) {
		if (str == null || str.length() == 0) {
			return false;
		}
		int start = 0;
		if (str.charAt(0) == '-') {
			start = 1;
		}
		boolean decimalPiontIndex = false;
		boolean wasE = false;
		char c;
		for (int index = start; index < str.length(); index++) {
			c = str.charAt(index);
			if (!Character.isDigit(c)) {
				switch (c) {
				case DECIMAL_POINT:
					if (decimalPiontIndex) {
						return false; // second decimal point
					}
					decimalPiontIndex = true;
					break;
				case EXPONENT_SYMBOL:
				case EXPONENT_SYMBOL_C:
					if (wasE) {
						return false; // second E
					}
					if (++index == str.length()) {
						return false;// last char is 'e'
					} else {
						c = str.charAt(index);
					}
					if (!(Character.isDigit(c))) {
						if (!(c == '+' || c == '-')) {
							return false;// char after 'e' has to be digit or '+' or '-'
						} else if (index + 1 == str.length()) {
							return false;// last char is '+' or '-'
						}
					}
					decimalPiontIndex = true;
					wasE = true;
					break;
				default:
					return false; // not digit, '.', 'e' nor 'E'
				}
			}
		}
		return true;
	}

	public static boolean isAscii(CharSequence str) {
		if (str != null) {
			return Pattern.matches("\\p{ASCII}*", str);
		} else {
			return true;
		}
	}

	public static String removeNonAscii(String str){
		if (str == null){
			return null;
		}
		return str.replaceAll("[^\\p{ASCII}]+", "");
	}

	/**
	 * This method copies substring of source to target.
	 * 
	 * @param target
	 *            target to which save the substring
	 * @param src
	 *            source string
	 * @param from
	 *            positing at which start (zero based)
	 * @param length
	 *            number of characters to take
	 * @return target containing substring of original or empty string if specified from/length values are out of
	 *         ranges. If from+length exceeds src.lenght, target.lenght is only src.lenght-from
	 * @throws IOException 
	 * @since 23.5.2007
	 */
	public static Appendable subString(Appendable target, CharSequence src, int from, int length) throws IOException {
		final int end = from + length;
		final int maxLength = src.length();
		for (int i = (from < 0 ? 0 : from); i < end; i++) {
			if (i >= maxLength) {
				break;
			}
			target.append(src.charAt(i));
		}
		return target;
	}
	
	/**
	 * Returns the index within input string of the first occurrence of the specified substring, starting at the
	 * specified index.
	 * 
	 * @param input
	 *            string for searching
	 * @param pattern
	 *            the substring for which to search
	 * @param fromIndex
	 *            the index from which to start the search
	 * @return the index within this string of the first occurrence of the specified substring, starting at the
	 *         specified index, or -1 if the sequence does not occur
	 */
	public static int indexOf(CharSequence input, CharSequence pattern, int fromIndex) {
		if (fromIndex >= input.length()) {
			return (pattern.length() == 0 ? input.length() : -1);
		}
		if (fromIndex < 0) {
			fromIndex = 0;
		}
		if (pattern.length() == 0) {
			return fromIndex;
		}

		char first = pattern.charAt(0);
		int max = (input.length() - pattern.length());

		for (int i = fromIndex; i <= max; i++) {
			/* Look for first character. */
			if (input.charAt(i) != first) {
				while (++i <= max && input.charAt(i) != first)
					;
			}

			/* Found first character, now look at the rest of v2 */
			if (i <= max) {
				int j = i + 1;
				int end = j + pattern.length() - 1;
				for (int k = 1; j < end && input.charAt(j) == pattern.charAt(k); j++, k++)
					;

				if (j == end) {
					/* Found whole string. */
					return i;
				}
			}
		}
		return -1;
	}

	/**
	 * Returns the index within this string of the first occurrence of the specified character, starting the search at
	 * the specified index
	 * 
	 * @param input
	 *            input string for searching
	 * @param cha
	 *            character
	 * @param fromIndex
	 *            the index from which to start the search
	 * @return the index of the first occurrence of the character in the character sequence that is greater than or
	 *         equal to fromIndex, or -1 if the character does not occur
	 */
	public static int indexOf(CharSequence input, char ch, int fromIndex) {
		for (int i = fromIndex; i < input.length(); i++) {
			if (input.charAt(i) == ch) {
				return i;
			}
		}
		return -1;
	}

	
	/**
	 * Tests if this string starts with the specified prefix. 
	 * 
	 * @param input	input string to check
	 * @param offset index at which start (zero based)
	 * @param prefix the prefix 
	 * @return true if the character sequence represented by the prefix argument is a prefix of the character sequence represented by the input string; false otherwise.
	 * Note also that true will be returned if the argument is an empty string or is equal to this String object as determined by the equals(Object) method.
	 */
	public static boolean startsWith(CharSequence input, int offset, CharSequence prefix){
		final int sLength=prefix.length();
		final int iLength=input.length();
		if (offset>iLength) throw new StringIndexOutOfBoundsException(offset);
		if (iLength-offset < sLength) return false;
		for(int i=0;i< sLength;i++){
			if (input.charAt(offset++)!=prefix.charAt(i)) 
				return false;
		}
		return true;
	}
	
	/**
	 * Tests if this string starts with the specified prefix. 
	 * 
	 * @param input	input string to check
	 * @param prefix the prefix 
	 * @return true if the character sequence represented by the prefix argument is a prefix of the character sequence represented by the input string; false otherwise.
	 * Note also that true will be returned if the argument is an empty string or is equal to this String object as determined by the equals(Object) method.
	 */
	public static boolean startsWith(CharSequence input, CharSequence subString){
		return startsWith(input,0,subString);
	}
	
	/**
	 * Tests if this string starts with the specified prefix - case insensitive comparison
	 * 
	 * @param input input string to check
	 * @param offset  index at which start (zero based)
	 * @param subString the prefix 
	 * @return true if the character sequence represented by the prefix argument is a prefix of the character sequence represented by the input string; false otherwise
	 */
	public static boolean startsWithIgnoreCase(CharSequence input, int offset, CharSequence subString){
		final int sLength=subString.length();
		final int iLength=input.length();
		if (offset>iLength) throw new StringIndexOutOfBoundsException(offset);
		if (iLength-offset < sLength) return false;
		for(int i=0;i< sLength;i++){
			if ( Character.toUpperCase(input.charAt(offset++)) != Character.toUpperCase(subString.charAt(i))) 
				return false;
		}
		return true;
	}
	
	/**
	 * Tests if this string starts with the specified prefix - case insensitive comparison
	 * 
	 * @param input input string to check
	 * @param subString the prefix 
	 * @return true if the character sequence represented by the prefix argument is a prefix of the character sequence represented by the input string; false otherwise
	 */
	public static boolean startsWithIgnoreCase(CharSequence input, CharSequence subString){
		return startsWith(input,0,subString);
	}
	
	/**
	 * Returns number of occurrences of given char in the char sequence
	 * 
	 * @param input
	 *            input string
	 * @param ch
	 *            char to find
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
	 * Parses a memory string in following formats and returns size in bytes Examples: "32m", "32mb", "2 g", "2gb",
	 * "128k", "128kb" "8192"
	 * 
	 * @param s
	 * @return value in bytes
	 */
	public static long parseMemory(String s) {
		if (s == null) {
			return -1;
		}
		s = s.trim().toUpperCase();
		try {
			if (s.endsWith("K")) {
				return Long.valueOf(s.substring(0, s.length() - 1).trim()).longValue() * 1024;
			} else if (s.endsWith("KB")) {
				return Long.valueOf(s.substring(0, s.length() - 2).trim()).longValue() * 1024;
			} else if (s.endsWith("M")) {
				return Long.valueOf(s.substring(0, s.length() - 1).trim()).longValue() * 1024 * 1024;
			} else if (s.endsWith("MB")) {
				return Long.valueOf(s.substring(0, s.length() - 2).trim()).longValue() * 1024 * 1024;
			} else if (s.endsWith("G")) {
				return Long.valueOf(s.substring(0, s.length() - 1).trim()).longValue() * 1024 * 1024 * 1024;
			} else if (s.endsWith("GB")) {
				return Long.valueOf(s.substring(0, s.length() - 2).trim()).longValue() * 1024 * 1024 * 1024;
			}
		} catch (NumberFormatException e) {
		}
		return -1;
	}

	/**
	 * Translates single characters in a string to different characters: replaces single characters at a time,
	 * translating the <i>n</i>th character in the match set with the <i>n</i>th character in the replacement set
	 * 
	 * @param in
	 *            input string
	 * @param searchSet
	 *            character to replace
	 * @param replaceSet
	 *            replacing characters
	 * @return original string with replaced requested characters
	 */
	public static CharSequence translate(CharSequence in, CharSequence searchSet, CharSequence replaceSet) {
		if (searchSet.length() < SEQUENTIAL_TRANLATE_LENGTH) {
			return translateSequentialSearch(in, searchSet, replaceSet);
		} else {
			return translateBinarySearch(in, searchSet, replaceSet);
		}
	}


	public static CharSequence translateMapSearch(CharSequence in, CharSequence searchSet, CharSequence replaceSet) {
		HashMap<Character, Character> replacement = new HashMap<Character, Character>(searchSet.length());
		int replaceSetLength = replaceSet.length();
		for (int i = 0; i < searchSet.length(); i++) {
			replacement.put(searchSet.charAt(i), i < replaceSetLength ? replaceSet.charAt(i) : null);
		}
		StringBuilder result = new StringBuilder();
		Character r;
		char ch;
		for (int i = 0; i < in.length(); i++) {
			ch = in.charAt(i);
			if (replacement.containsKey(ch)) {
				if ((r = replacement.get(ch)) != null) {
					result.append(r);
				}
			} else {
				result.append(ch);
			}
		}
		return result.toString();
	}

	public static CharSequence translateBinarySearch(CharSequence in, CharSequence searchSet, CharSequence replaceSet) {
		CharPair[] replacement = new CharPair[searchSet.length()];
		int replaceSetLength = replaceSet.length();
		for (int i = 0; i < searchSet.length(); i++) {
			replacement[i] = new CharPair(searchSet.charAt(i), i < replaceSetLength ? replaceSet.charAt(i) : null);
		}
		
		
		final Comparator<Object> comparator = new Comparator<Object>(){

			@Override
			public int compare(Object o1, Object o2) {
					if (o1 == null && o2 ==null) {
						return 0;
					}
					if( o1 == null && o2 != null){
						return -1;
					}
					if( o1 != null && o2 == null){
						return 1;
					}
			
					final char ch1 = getChar(o1);
					final char ch2 = getChar(o2);
					
					return ch1-ch2;
					
					
			}

			private char getChar(Object o) {
				if (o instanceof Character) {
					return ((Character) o).charValue();
				}
				if (o instanceof CharPair) {
					return ((CharPair) o).key;
				}
				throw new ClassCastException();
			}
			
		};
		
		Arrays.sort(replacement, comparator);
		
		final StringBuilder result = new StringBuilder();
		
		for (int i = 0; i < in.length(); i++) {
			final char ch = in.charAt(i);
			final int index = Arrays.binarySearch(replacement, ch, comparator);
			if (index > -1) {
				if (replacement[index].value != null) {
					result.append(replacement[index].value);
				}
			} else {
				result.append(ch);
			}
		}
		return result.toString();
	}
	
	private static class CharPair {

		public Character key, value;

		CharPair(Character key, Character value) {
			this.key = key;
			this.value = value;
		}
	}		


	public static CharSequence translateSequentialSearch(CharSequence in, CharSequence searchSet,
			CharSequence replaceSet) {
		if(searchSet == null || replaceSet == null){
			throw new NullPointerException("searchSet or replaceSet is null");
		}
		if(in == null){
			return null;
		}
		StringBuilder result = new StringBuilder();
		char[] search = charSequence2char(searchSet);
		char[] replace = charSequence2char(replaceSet);
		int length = replace.length;
		int index;
		char ch;
		for (int i = 0; i < in.length(); i++) {
			ch = in.charAt(i);
			if ((index = indexOf(search, ch)) > -1) {
				if (index < length) {
					result.append(replace[index]);
				}
			} else {
				result.append(ch);
			}
		}
		return result.toString();
	}

	public static char[] charSequence2char(final CharSequence in) {
		char[] array = new char[in.length()];
		for (int i = 0; i < in.length(); i++) {
			array[i] = in.charAt(i);
		}
		return array;
	}

	private static int indexOf(char[] array, char ch) {
		for (int i = 0; i < array.length; i++) {
			if (array[i] == ch) {
				return i;
			}
		}
		return -1;
	}

	public static String[] toLowerCase(String... str) {
		String[] result = new String[str.length];
		for (int i = 0; i < str.length; i++) {
			result[i] = str[i].toLowerCase();
		}
		return result;
	}

	public static String[] toUpperCase(String... str) {
		String[] result = new String[str.length];
		for (int i = 0; i < str.length; i++) {
			result[i] = str[i].toUpperCase();
		}
		return result;
	}
	
	/**
	 * Counts the number of lines in the input CharSequence.
	 * Uses universal matching pattern that prefers (\n\r or \r\n) over (\r or \n).
	 * 
	 * @return the number of lines
	 */
	public static int countLines(CharSequence input) {
		if (input.length() == 0) {
			return 0;
		}
		return countMatches(input, "\r\n|\n\r|\r|\n") + 1;
	}
	
	/**
	 * Counts the number of occurrences of the pattern in the input sequence.
	 */
	public static int countMatches(CharSequence input, String regex) {
		int result = 0;
		Matcher m = Pattern.compile(regex).matcher(input);
		while (m.find()) {
			result++;
		}
		return result;
	}

	/**
	 * Convert each backslash in string to slash.
	 * 
	 * @param string
	 *            string with backslashes
	 * @return string with slashes
	 */
	public static String backslashToSlash(CharSequence controlString) {
		if (controlString == null) {
			return null;
		}

		StringBuilder copy = new StringBuilder();
		char character;
		for (int i = 0; i < controlString.length(); i++) {
			character = controlString.charAt(i);
			switch (character) {
			case '\n':
				copy.append("/n");
				break;
			case '\t':
				copy.append("/t");
				break;
			case '\r':
				copy.append("/r");
				break;
			case '\b':
				copy.append("/b");
				break;
			case '\f':
				copy.append("/f");
				break;
			case '\\':
				copy.append("/");
				break;
			default:
				copy.append(character);
			}
		}
		return copy.toString();
	}
	
	/**
	 * Calculates hashCode/value for specified CharSequence
	 * 
	 * @param seq
	 * @return
	 */
	public static int hashCode(CharSequence seq) {
		int h = 5381;
		final int length = seq.length();
		for (int i = 0; i < length; i++) {
			h = ((h << 5) + h) + seq.charAt(i);
		}
		return (h & 0x7FFFFFFF);
	}
	
	/**
	 * @param c tested character
	 * @return is the given character printable?
	 */
	public static boolean isPrintableChar(char c) {
        Character.UnicodeBlock block = Character.UnicodeBlock.of( c );
        return (!Character.isISOControl(c)) &&
                c != KeyEvent.CHAR_UNDEFINED &&
                block != null &&
                block != Character.UnicodeBlock.SPECIALS;
    }
	
	
	/**
	 * Abbreviates input char sequence by using first letter of each word composing the sequence.
	 * Skips any spaces, punctuation and other special chars.
	 * 
	 * @param input	character sequence to abbreviate
	 * @param elementLength how many characters from each word to use (if more than 1, then each element is divided by "_" in output
	 * @param capitalize capitalize letters - i.e. convert to uppercase
	 * @param useNumbers also include numbers in sequence
	 * @return abbreviation of input sequence
	 */
	public static CharSequence abbreviateString(CharSequence input, int elementLength, boolean capitalize,boolean useNumbers){
		StringBuilder out=new StringBuilder();      
		StreamTokenizer st = new StreamTokenizer(new CharSequenceReader(input));
		st.ordinaryChar('.');
		st.ordinaryChar('-');
		try{
		while(st.nextToken() !=
	        StreamTokenizer.TT_EOF) {
	        switch(st.ttype) {
	          case StreamTokenizer.TT_WORD:
	        	  if (elementLength>1 && out.length()>0) out.append('_'); // append underscore to divide abbr.pieces
	        	  if (capitalize){
	        		  String s = st.sval.subSequence(0, elementLength).toString();
	        		  out.append(s.toUpperCase());
	        	  }else{
	        		  out.append(st.sval.subSequence(0, elementLength));
	        	  }
	            break;
	          case StreamTokenizer.TT_NUMBER:
	        	  if (useNumbers) out.append((int)st.nval);
	        	  break;
	          default: 
	        	  // do nothing
	        }
		}
		}catch(IOException ex){
		}
		return out;
	}
	
	/**
	 * Abbreviate input char sequence.
	 * 
	 * @param input
	 * @return abbreviation of input
	 */
	public static CharSequence abbreviateString(CharSequence input){
		return abbreviateString(input,1,false,false);
	}
	
	/*
	 * End class StringUtils
	 */
	/*
	 * public static void main(String[] args) {
	 * 
	 * StringBuilder in = new StringBuilder(); StringBuilder searchSet = new StringBuilder(); StringBuilder replaceSet =
	 * new StringBuilder();
	 * 
	 * Random r = new Random(); for (int i=0; i<3000000; i++){ in.append((char)(r.nextInt('z' - 'a' + 1) + 'a')); if (i<10) {
	 * searchSet.append((char)('a'+ i)); replaceSet.append((char)(r.nextInt('z' - 'a' + 1) + 'a')); } }
	 * 
	 * CharSequence t1, t2, t3, t4; System.out.println("Search set:" + searchSet); System.out.println("Replace set:" +
	 * replaceSet); long start = System.currentTimeMillis(); t1 = StringUtils.translateBinarySearch(in, searchSet,
	 * replaceSet); long end = System.currentTimeMillis(); System.out.println("Binary search time:" + (end - start));
	 * start = System.currentTimeMillis(); t2 = StringUtils.translateSequentialSearch(in, searchSet, replaceSet); end =
	 * System.currentTimeMillis(); System.out.println("Sequential search time:" + (end - start)); start =
	 * System.currentTimeMillis(); t3 = StringUtils.translateMapSearch(in, searchSet, replaceSet); end =
	 * System.currentTimeMillis(); System.out.println("Map search time:" + (end - start)); start =
	 * System.currentTimeMillis(); t4 = StringUtils.translateOneByOne(in, searchSet, replaceSet); end =
	 * System.currentTimeMillis(); System.out.println("One by one time:" + (end - start)); if
	 * (!t1.toString().equals(t2.toString()) || !t1.toString().equals(t3.toString()) ||
	 * !t1.toString().equals(t4.toString())) throw new RuntimeException();
	 * 
	 * for (int i=0; i < 10; i++){ searchSet.append((char)('k' + i)); replaceSet.append((char)(r.nextInt('z' - 'a' + 1) +
	 * 'a')); } System.out.println("Search set:" + searchSet); System.out.println("Replace set:" + replaceSet); start =
	 * System.currentTimeMillis(); t1 = StringUtils.translateBinarySearch(in, searchSet, replaceSet); end =
	 * System.currentTimeMillis(); System.out.println("Binary search time:" + (end - start)); start =
	 * System.currentTimeMillis(); t2 = StringUtils.translateSequentialSearch(in, searchSet, replaceSet); end =
	 * System.currentTimeMillis(); System.out.println("Sequential search time:" + (end - start)); start =
	 * System.currentTimeMillis(); t3 = StringUtils.translateMapSearch(in, searchSet, replaceSet); end =
	 * System.currentTimeMillis(); System.out.println("Map search time:" + (end - start)); start =
	 * System.currentTimeMillis(); t4 = StringUtils.translateOneByOne(in, searchSet, replaceSet); end =
	 * System.currentTimeMillis(); System.out.println("One by one time:" + (end - start)); if
	 * (!t1.toString().equals(t2.toString()) || !t1.toString().equals(t3.toString()) ||
	 * !t1.toString().equals(t4.toString())) throw new RuntimeException();
	 * 
	 * for (int i=0; i < 7; i++){ searchSet.append((char)('u' + i)); replaceSet.append((char)(r.nextInt('z' - 'a' + 1) +
	 * 'a')); } System.out.println("Search set:" + searchSet); System.out.println("Replace set:" + replaceSet); start =
	 * System.currentTimeMillis(); t1 = StringUtils.translateBinarySearch(in, searchSet, replaceSet); end =
	 * System.currentTimeMillis(); System.out.println("Binary search time:" + (end - start)); start =
	 * System.currentTimeMillis(); t2 = StringUtils.translateSequentialSearch(in, searchSet, replaceSet); end =
	 * System.currentTimeMillis(); System.out.println("Sequential search time:" + (end - start)); start =
	 * System.currentTimeMillis(); t3 = StringUtils.translateMapSearch(in, searchSet, replaceSet); end =
	 * System.currentTimeMillis(); System.out.println("Map search time:" + (end - start)); start =
	 * System.currentTimeMillis(); t4 = StringUtils.translateOneByOne(in, searchSet, replaceSet); end =
	 * System.currentTimeMillis(); System.out.println("One by one time:" + (end - start)); if
	 * (!t1.toString().equals(t2.toString()) || !t1.toString().equals(t3.toString()) ||
	 * !t1.toString().equals(t4.toString())) throw new RuntimeException(); }
	 */
	

	/*
	public static String replaceVariables(String temlate, Properties properties, String startString, String endString) {
		return replaceVariables(properties, temlate);
	}
	*/
	
	public static String replaceVariables(String template, Map<String, String> variables) {
		return replaceVariables(template, new MapVariableResolver(variables), "${", "}");
	}
	
	public static String replaceVariables(String template, Properties variables) {
		return replaceVariables(template, new PropertiesVariableResolver(variables), "${", "}");
	}
	
	public static String replaceVariables(String template, Map<String, String> variables, String variableStart, String variableEnd) {
		return replaceVariables(template, new MapVariableResolver(variables), variableStart, variableEnd);
	}
	
	public static String replaceVariables(String template, Properties variables, String variableStart, String variableEnd) {
		return replaceVariables(template, new PropertiesVariableResolver(variables), variableStart, variableEnd);
	}
	
	public static String replaceVariables(String template, VariableResolver resolver, String variableStart, String variableEnd) {
		final StringBuilder ret = new StringBuilder();
		ret.append(template);
		int index = 0;
		while (index < ret.length()) {
			index = ret.indexOf(variableStart, index);
			if (index == -1) {
				break;
			}
			int end = ret.indexOf(variableEnd, index + 1);
			if (end == -1) {
				throw new IllegalArgumentException("Closing "+variableEnd+" not found " + ret.substring(index, index + 10 < ret.length() ? index + 10 : ret.length()));
			}
			final String foundKey = ret.substring(index + variableStart.length(), end);
			final String value = resolver.get(foundKey);
			if (value == null) {
				throw new IllegalArgumentException("Unknown variable " + foundKey);
			}
			ret.replace(index, end + 1, value);
			index += value.length();
		}
		return ret.toString();
	}

    /**
     * Unlike {@link String#replace(CharSequence, CharSequence)},
     * this method replaces only the last occurrence.
     * 
     * @param input
     * @param substring
     * @param replacement
     * @return input with the last occurrence of substring replaced with replacement
     */
    public static String replaceLast(CharSequence input, CharSequence substring, CharSequence replacement) {
    	StringBuilder sb = new StringBuilder(input);
    	int index = sb.lastIndexOf(substring.toString());
    	if (index >= 0) {
    		sb.delete(index, input.length());
    		sb.append(replacement);
    	}
    	return sb.toString();
    }
    
	/**
	 * Compares two string values. Similar to call s1.equals(s2), but s1 and s2 can be null.
	 * Null values are considered equal. Null value does not equal non-null value.
	 */
	public static boolean equalsWithNulls(String s1, String s2) {
		if (s1 == null) {
			return s2 == null;
		}
		return s1.equals(s2);
	}
	
	/**
	 * Given string elements are concatenated and the given delimiter is used to separate them. 
	 * @param elements string elements to be concatenated
	 * @param delimiter string delimiter
	 * @return concatenation of elements separated by delimiter
	 */
	public static String join(List<String> elements, String delimiter) {
		StringBuilder result = new StringBuilder();
		if (elements == null) {
			return null;
		}
		boolean first = true;
		for (String element : elements) {
			if (!first) {
				result.append(delimiter);
			} else {
				first = false;
			}
			result.append(element);
		}
		return result.toString();
	}
	
	private static String toStringInternalBytes(byte[] bytes) {
		try {
			return new String(bytes, Defaults.DataFormatter.DEFAULT_CHARSET_ENCODER);
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(String.format("Unknown charset: %s", Defaults.DataFormatter.DEFAULT_CHARSET_ENCODER));
		}
	}

	private static final <E> String toStringInternalList(List<E> list) {
        Iterator<E> i = list.iterator();
		if (! i.hasNext())
		    return "[]";

		StringBuilder sb = new StringBuilder();
		sb.append('[');
		for (;;) {
		    E e = i.next();
		    if (e instanceof byte[]) {
		    	sb.append(toStringInternalBytes((byte[]) e));
		    } else {
		    	sb.append(e == list ? "(this Collection)" : e);
		    }
		    if (! i.hasNext()) {
		    	return sb.append(']').toString();
		    }
		    sb.append(", ");
		}
	}
	
	private static final <K, V> String toStringInternalMap(Map<K, V> map) {
		Iterator<Entry<K,V>> i = map.entrySet().iterator();
		if (! i.hasNext())
		    return "{}";

		StringBuilder sb = new StringBuilder();
		sb.append('{');
		for (;;) {
		    Entry<K,V> e = i.next();
		    K key = e.getKey();
		    V value = e.getValue();
		    if (key instanceof byte[]) {
		    	sb.append(toStringInternalBytes((byte[]) key));
		    } else {
			    sb.append(key   == map ? "(this Map)" : key);
		    }
		    sb.append('=');
		    if (value instanceof byte[]) {
		    	sb.append(toStringInternalBytes((byte[]) value));
		    } else {
			    sb.append(value == map ? "(this Map)" : value);
		    }
		    if (! i.hasNext())
			return sb.append('}').toString();
		    sb.append(", ");
		}
	}

	/**
	 * Converts an object into a human readable string.
	 * Used in CTL.
	 * 
	 * Converts byte arrays into
	 * Strings using the default charset
	 * instead of the default Java byte[].toString(). 
	 * 
	 * @param o an object
	 * @return string represention of o
	 */
	public static String toOutputStringCTL(Object o) {
		if (o == null) {
			return "null";
		}
		if (o instanceof byte[]) {
			return toStringInternalBytes((byte[]) o);
		}
		if (o instanceof List) {
			return toStringInternalList((List<?>) o);
		}
		if (o instanceof Map) {
			return toStringInternalMap((Map<?, ?>) o);
		}
		return o.toString();
	}
	
	/**
	 * Converts given object to string. For non-null object <code>o.toString()</code> is returned, <code>nullValue</code> otherwise.
	 * @param o converted object
	 * @param nullValue returned value in case null object <code>o</code>
	 * @return string representation of the given object (<code>o.toString()</code>) or <code>nullValue</code> if <code>o==null</code>  
	 */
	public static String toString(Object o, String nullValue) {
		if (o == null) {
			return nullValue;
		} else {
			return String.valueOf(o);
		}
	}
	
	private static interface VariableResolver{
		String get(String key);
	}
	
	private static class MapVariableResolver implements VariableResolver {
		private final Map<String,String> variables;
		
		public MapVariableResolver(Map<String, String> variables) {
			this.variables = variables;
		}

		@Override
		public String get(String key) {
			return variables.get(key);
		}
	}
	
	private static class PropertiesVariableResolver implements VariableResolver {
		private final Properties variables;
		
		public PropertiesVariableResolver(Properties variables) {
			this.variables = variables;
		}

		@Override
		public String get(String key) {
			return variables.getProperty(key);
		}
	}
}

