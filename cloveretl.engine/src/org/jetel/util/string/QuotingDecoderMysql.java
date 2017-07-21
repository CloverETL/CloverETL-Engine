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
 * Quoting decoder for mysql client output 
 * @author Jan Hadrava (jan.hadrava@javlinconsulting.cz), Javlin Consulting (www.javlinconsulting.cz)
 * @since 10/11/06
 * @see  org.jetel.util.DelimitedDataParser
 */
public class QuotingDecoderMysql extends QuotingDecoder {
	public QuotingDecoderMysql() {
		super();
	}

	@Override
	public CharSequence decode(CharSequence quoted) {
		quoted = super.decode(quoted);	// remove eventual quotes

		int inlen = quoted.length();
		StringBuilder unquoted = new StringBuilder(inlen);
		
		int subOffset = 0;
		int outlen = 0;
		for (int pos = 0; pos < inlen; pos++) {
			char c = quoted.charAt(pos);
			if (c == '\\') {
				unquoted.insert(outlen, quoted.subSequence(subOffset, pos));
				outlen += pos - subOffset;
				pos++;
				if (pos == inlen) {	// escape char is the last one
					unquoted.setLength(outlen);
					return unquoted;
				}
				c = quoted.charAt(pos);
				switch (c) {
				case '0':
					c = 0;
					break;
				case 'b':
					c = '\b';
					break;
				case 't':
					c = '\t';
					break;
				case 'n':
					c = '\n';
					break;
				case 'r':
					c = '\r';
					break;
				case 'Z':
					c = 26;
					break;
				default:
					subOffset = pos;
					continue;
				}
				unquoted.insert(outlen, c);
				outlen++;
				subOffset = pos + 1;
			}
		}
		if (outlen == 0) {	// no special characters encountered
			// this is just optimization
			return quoted;
		}
		unquoted.insert(outlen, quoted.subSequence(subOffset, inlen));
		outlen += inlen - subOffset;
		unquoted.setLength(outlen);
		return unquoted;
	}

	public boolean isEscape(char c) {
		return c == '\\';
	}
}
