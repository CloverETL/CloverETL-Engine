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

import org.jetel.data.DataRecord;


/**
 * Base {@link ReplacementHelper} implementation.
 * 
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 1. 9. 2014
 */
public abstract class AbstractReplacementHelper implements ReplacementHelper {
	
	private final Pattern pattern;

	protected AbstractReplacementHelper(Pattern pattern) {
		this.pattern = pattern;
	}

	/**
	 * Returns the name of the field whose value should be used
	 * as a replacement for the current match.
	 * 
	 * @param matcher	the control {@link Matcher}
	 * @return name of the replacement field
	 */
	protected String getFieldName(Matcher matcher) {
		return matcher.group(1);
	}
	
	/**
	 * Utility method, returns the value of the specified field.
	 * 
	 * @param values	source {@link DataRecord}
	 * @param fieldName	name of the field
	 * @return the value of the field
	 */
	protected Object getFieldValue(DataRecord values, String fieldName) {
		return values.getField(fieldName).getValue();
	}

	@Override
	public void appendReplacement(StringBuffer sb, Matcher matcher, DataRecord values) {
		matcher.appendReplacement(sb, quoteReplacement(getReplacement(matcher, values)));
	}
	
	/**
	 * Quotes the replacement value, so that group references
	 * and other special sequences are inserted as string literals.
	 * 
	 * @param input replacement value
	 * @return quoted replacement value
	 */
	protected String quoteReplacement(String input) {
		return Matcher.quoteReplacement(input);
	}

	protected String getReplacement(Matcher matcher, DataRecord values) {
		String fieldName = getFieldName(matcher);
		Object value = getFieldValue(values, fieldName);
		if (value == null) {
			throw new NullPointerException("Unexpected null value: " + fieldName);
		}
		return value.toString();
	}

	@Override
	public Matcher getMatcher(String input) {
		return pattern.matcher(input);
	}

}
