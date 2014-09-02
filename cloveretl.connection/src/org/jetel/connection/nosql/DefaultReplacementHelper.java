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
package org.jetel.connection.nosql;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jetel.util.string.StringUtils;

/**
 * Default {@link ReplacementHelper} implementation.
 * <p>
 * Uses simple placeholder substitution, 
 * fails if the replacement value is <code>null</code>.
 * </p>
 * 
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 1. 9. 2014
 */
public class DefaultReplacementHelper extends AbstractReplacementHelper {
	
	public DefaultReplacementHelper(Matcher m) {
		super(getPattern(m));
	}
	
	private static Pattern getPattern(Matcher m) {
		StringBuilder realPattern = new StringBuilder();
		realPattern.append(Pattern.quote(m.group(1)));
		realPattern.append('(').append(StringUtils.OBJECT_NAME_PATTERN).append(')');
		realPattern.append(Pattern.quote(m.group(2)));
		
		return Pattern.compile(realPattern.toString());
	}

	public DefaultReplacementHelper(Pattern pattern) {
		super(pattern);
	}

}
