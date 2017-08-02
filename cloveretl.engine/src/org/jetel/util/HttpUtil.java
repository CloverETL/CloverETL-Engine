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
package org.jetel.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jetel.util.string.UnicodeBlanks;

/**
 * Collected methods which help working with HTTP.
 * 
 * @author kocik (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created Aug 2, 2017
 */
public class HttpUtil {
	private static String regex = "(\\(|\\)|<|>|@|,|;|:|\\\\|\"|/|\\[|\\]|\\?|=|\\{|}|[\\u0000-\\u0020])";
	private static Pattern pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);

	/**
	 * Returns true if headerName is valid. Header name is defined in https://www.ietf.org/rfc/rfc2616.txt.
	 */
	public static boolean isValidHttpHeader(String headerName) {

		if (UnicodeBlanks.isBlank(headerName)) {
			return false;
		}

		Matcher m = pattern.matcher(headerName);
		if (m.find()) {
			return false;
		}

		return true;
	}
}
