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
package org.jetel.component.fileoperation;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WildcardFilter {
	
	private static final Pattern NON_WILDCARD_PATTERN = Pattern.compile("([^?*]+).*"); //$NON-NLS-1$
	
	private final Pattern pattern;
	
	private final boolean onlyDirectories;
	
	public WildcardFilter(String pattern, boolean onlyDirectories) {
		this.onlyDirectories = onlyDirectories;
		StringBuilder sb = new StringBuilder();
		Matcher matcher = NON_WILDCARD_PATTERN.matcher(pattern);
		for (int i = 0; i < pattern.length(); ) {
			matcher.region(i, pattern.length());
			if (matcher.matches()) {
				String group = matcher.group(1);
				sb.append(Pattern.quote(group));
				i += group.length();
			} else {
				switch (pattern.charAt(i)) {
				case '?':
					sb.append('.');
					break;
				case '*':
					sb.append(".*"); //$NON-NLS-1$
					break;
				}
				i++;
			}
		}
		this.pattern = Pattern.compile(sb.toString());
	}
	
	public boolean accept(String fileName, boolean isDirectory) {
		if (onlyDirectories && !isDirectory) {
			return false;
		}
		return pattern.matcher(fileName).matches();
	}

}