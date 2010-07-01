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

import java.util.ArrayList;
import java.util.regex.Pattern;

/**
 * @author csochor (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created Jun 30, 2010
 */
public class MultiPattern {

	private final CharSequence[] parts;

	private MultiPattern(CharSequence[] parts) {
		this.parts = parts;
	}

	public static MultiPattern parse(String format) {
		if (StringUtils.isEmpty(format)) {
			throw new IllegalArgumentException("format is required");
		}

		if (format.length() == 1) {
			return new MultiPattern(new String[] { format });
		}
		final char separator = format.charAt(0);
		final char lastchar = format.charAt(format.length() - 1);

		if (separator != lastchar) {
			return new MultiPattern(new String[] { format });
		} else {
			final ArrayList<CharSequence> pars = new ArrayList<CharSequence>();
			for (int fromI = 0, toI = format.indexOf(separator, fromI + 1); toI != -1 ; fromI = toI, toI = format.indexOf(separator, toI + 1)) {
				pars.add(format.subSequence(fromI+1, toI));
			}
			return new MultiPattern(pars.toArray(new CharSequence[pars.size()]));
		}

	}
	
	public int size(){
		return parts.length;
	}
	
	public CharSequence getCharSequence(int index){
		return parts[index];
	}

	public Pattern getPattern(int index){
		return Pattern.compile(parts[index].toString());
	}

	public String getString(int index) {
		return getCharSequence(index).toString();
	}
	
}
