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

import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;

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
}
