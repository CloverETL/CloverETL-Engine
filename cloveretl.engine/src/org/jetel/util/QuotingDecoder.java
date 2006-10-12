/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2006 Javlin Consulting <info@javlinconsulting>
*    
*    This library is free software; you can redistribute it and/or
*    modify it under the terms of the GNU Lesser General Public
*    License as published by the Free Software Foundation; either
*    version 2.1 of the License, or (at your option) any later version.
*    
*    This library is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU    
*    Lesser General Public License for more details.
*    
*    You should have received a copy of the GNU Lesser General Public
*    License along with this library; if not, write to the Free Software
*    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*
*/
package org.jetel.util;

/*
 * Class for conversion of string containing escape sequences
 * and/or quotes to standard string where each char is represented by itself.
 * This class supports only string quoting and doesn't implement any escape sequences.  
 * It is supposed to have some more refined subclasses.
 *  
 * @author Jan Hadrava (jan.hadrava@javlinconsulting.cz), Javlin Consulting (www.javlinconsulting.cz)
 * @since 10/11/06  
*/
public class QuotingDecoder {
	public QuotingDecoder() {
	}

	/*
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
		if ((first == '\"') || (first == '\"') && first == last) {
			return quoted.subSequence(1, len - 1);
		}

		return quoted;
	}
	
	/*
	 * Checks whether a character is an opening quote.
	 */
	public boolean isStartQuote(char c) {
		return c == '\"' || c == '\''; 
	}

	/*
	 * Checks whether a character is a closing quote.
	 */
	public boolean isEndQuote(char c) {
		return c == '\"' || c == '\''; 
	}

	/*
	 * Checks whether a character is an escape character.
	 */
	public boolean isEscape(char c) {
		return false;
	}

}
