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
package org.jetel.component.validator.rules;

import org.jetel.component.validator.ValidatorMessages;

/**
 * Simple phone number pattern representation.
 * 
 * Converts the pattern into regexp which can further be used for text matching.
 * 
 * In the pattern, there can be following meta-symbols used:<ul>
 * <li> "D" - represents arbitrary number (chars 0-9)
 * <li> {N,M} - repetition specification; the char preceding this range specification must
 * repeated at least N times and at most M times</ul>
 * 
 * Examples:<ul>
 * <li>+420 DDD DDD DDD - a phone number that starts with "+420" prefix and than contains exactly
 * nine digits separated into groups by three with spaces
 * <li>+1D{7,10} - a phone number starting with "+1" and then has between 7 and 10 digits
 * </ul>
 * 
 * Produced a regexp is always valid, if the pattern was valid.
 * 
 * @author Raszyk (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 31.5.2013
 */
public class PhoneNumberPattern {
	private final String pattern;
	private final String regexp;
	
	private PhoneNumberPattern(String pattern, String regexp) {
		this.pattern = pattern;
		this.regexp = regexp;
	}

	public static PhoneNumberPattern create(String pattern) throws PhoneNumberPatternFormatException {
		String regexp = patternToRegexp(pattern);
		return new PhoneNumberPattern(pattern, regexp);
	}
	
	public final static class PhoneNumberPatternFormatException extends Exception {
		private static final long serialVersionUID = 1617794836746911931L;
		
		private final String message;
		private final String pattern;
		private final int position;
		
		private static final String nl = System.getProperty("line.separator"); //$NON-NLS-1$
		
		public PhoneNumberPatternFormatException(String message, String pattern, int position) {
			this.message = message;
			this.pattern = pattern;
			this.position = position;
		}
		
		@Override
		public String getMessage() {
			StringBuffer sb = new StringBuffer();
			sb.append(message);
			if (position >= 0) {
				sb.append(" near index "); //$NON-NLS-1$
				sb.append(position);
			}
			sb.append(nl);
			sb.append(pattern);
			if (position >= 0) {
				sb.append(nl);
				for (int i = 0; i < position; i++) sb.append(' ');
			sb.append('^');
			}
			return sb.toString();
		}
	}
	
	private static String patternToRegexp(String patternString) throws PhoneNumberPatternFormatException {
		char[] format = patternString.toCharArray();
		StringBuilder output = new StringBuilder();
		
		boolean inRange = false;
		
		for (int i = 0; i < format.length; i++) {
			char c = format[i];
			if (inRange) {
				if (Character.isDigit(c) || c == ',') {
					output.append(c);
				}
				else if (c == '}') {
					inRange = false;
					output.append(c);
				}
				else {
					throw new PhoneNumberPatternFormatException(ValidatorMessages.getString("PhoneNumberPattern.InvalidRangeException") + c, patternString, i);  //$NON-NLS-1$
				}
			}
			else if (c == 'D') {
				output.append('\\').append('d');
			}
			else if (Character.isDigit(c) || Character.isLetter(c) || Character.isWhitespace(c)) {
				output.append(c);
			}
			else if (c == '{') {
				inRange = true;
				output.append(c);
			}
			else if (c == '}' && !inRange) {
				throw new PhoneNumberPatternFormatException(ValidatorMessages.getString("PhoneNumberPattern.UnexpectedEndOfRangeChar") + c, patternString, i); //$NON-NLS-1$
			}
			else {
				output.append('\\').append(c);
			}
		}
		
		if (inRange) {
			throw new PhoneNumberPatternFormatException(ValidatorMessages.getString("PhoneNumberPattern.RangeNotClosed"), patternString, format.length - 1); //$NON-NLS-1$
		}
		
		return output.toString();
	}
	
	public String getPattern() {
		return pattern;
	}
	
	public String getRegexp() {
		return regexp;
	}
	
	public boolean matches(String text) {
		return text.matches(regexp);
	}

}
