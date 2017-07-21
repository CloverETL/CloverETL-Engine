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
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created Apr 15, 2013
 */
public class StatementBuilderFactory {

	public static final String DEFAULT_FIELD_PATTERN = "${field}";
	
	private static final String PATTERN_VALIDATOR_STRING = "(.*)field(.*)";
	private static final Pattern PATTERN_VALIDATOR = Pattern.compile(PATTERN_VALIDATOR_STRING);
	
	private final Pattern fieldPattern;

	public StatementBuilderFactory() {
		this(DEFAULT_FIELD_PATTERN);
	}
	
	public StatementBuilderFactory(String fieldPattern) {
		Matcher m = PATTERN_VALIDATOR.matcher(fieldPattern);
		if (!m.matches()) {
			throw new IllegalArgumentException("Pattern must have the form '*field*'");
		}
		StringBuilder realPattern = new StringBuilder();
		realPattern.append(Pattern.quote(m.group(1)));
		realPattern.append('(').append(StringUtils.OBJECT_NAME_PATTERN).append(')');
		realPattern.append(Pattern.quote(m.group(2)));
		this.fieldPattern = Pattern.compile(realPattern.toString());
	}

	public StatementBuilder createStatementBuilder() {
		return new StatementBuilder(fieldPattern);
	}
}
