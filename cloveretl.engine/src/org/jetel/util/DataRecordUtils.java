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

import org.jetel.data.ByteDataField;
import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.exception.FieldNotFoundException;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.property.PropertyRefResolver;
import org.jetel.util.string.StringUtils;

/**
 * @author Tomas Laurincik (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 5.3.2012
 */
public class DataRecordUtils {
	/** Returns true, if the metadata contains a group of delimiters for a record or field, where for some delimiter
	 *  x in the group, there is also a prefix of x in that group. This is typically a case for \r, \r\n group.
	 * 
	 * @param metadata
	 * @return true, if the metadata contains a group of delimiters for a record or field, where for some delimiter
	 *  x in the group, there is also a prefix of x in that group.
	 */
	public static boolean containsPrefixDelimiters(DataRecordMetadata metadata) {
		if (containsPrefix(metadata.getRecordDelimiters())) {
			return true;
		}
		
		for (DataFieldMetadata field : metadata.getFields()) {
			if (containsPrefix(field.getDelimiters())) {
				return true;
			}
		}
		return false;
	}
	
	/** Returns true, if given set of strings contains some string x and it's prefix. This is typically a case for \r, \r\n group.
	 * 
	 * @param metadata
	 * @return true, if given set of strings contains some string x and it's prefix.
	 */	
	public static boolean containsPrefix(String[] strings) {
		if (strings != null) {
			for (int i=0; i < strings.length; i++) {
				String str1 = strings[i];
				for (int j=i+1; j < strings.length; j++) {
					String str2 = strings[j];
					if ( (str1.length() < str2.length() && str2.startsWith(str1)) || str1.startsWith(str2)) {
						return true;
					}
				}
			}
		}
		
		return false;
	}
	
	static Pattern varPattern = Pattern.compile("(\\$)([\\w_\\d]+)");
	/**
	 * Expands $field markers in `value` with values from `record`
	 * 
	 * Returns a String or byte[] according to type of expanded fields
	 * 
	 * @param value
	 * @param record
	 * @return
	 */
	public static Object expandFieldName(String value, DataRecord record, PropertyRefResolver resolver) {
		if (StringUtils.isEmpty(value)) {
			return value;
		}
		Matcher m = varPattern.matcher(value);
		DataField field;
		
		StringBuilder sb = null;
		int last = 0;
		String ret;
		while (m.find()) {
			if (sb == null) {
				sb = new StringBuilder();
			}
			
			String fieldName = m.group(2);
			if (!record.hasField(fieldName)) {
				throw new FieldNotFoundException(record, fieldName);
			}
			field = record.getField(fieldName);
			
			if (field instanceof ByteDataField) {
				return field.getValue();
			} else {
				sb.append(value.substring(last, m.start()));
				if (field.getValue() != null) {
					sb.append(String.valueOf(field.getValue()));
				} else {
					sb.append("");
				}
				last = m.end();
			}
			
		}
		if (sb != null) {
			sb.append(value.substring(last));
			ret = sb.toString();
		} else {
			ret = value;
		}

		return (resolver != null) ? resolver.resolveRef(ret) : ret;
		
	}
	
	public static Object expandFieldName(String value, DataRecord record, Object defaultValue, PropertyRefResolver resolver) {
		Object ret = DataRecordUtils.expandFieldName(value, record, resolver);
		return ret == null ? defaultValue : ret;
	}
}
