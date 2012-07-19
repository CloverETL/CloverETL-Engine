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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * Utils for conversion between String and List representations of a record key
 * 
 * @author Pavel Simecek (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 21.5.2012
 */
public class KeyFieldNamesUtils {
	public static List<String> getFieldNamesAsList(String fieldNamesAsString) {
		String [] fieldNamesArray = fieldNamesAsString.split(",");
		List<String> fieldNamesList = new ArrayList<String>();
		for (String fieldName : fieldNamesArray) {
			String trimmedFieldName = fieldName.trim();
			if (!trimmedFieldName.isEmpty()) {
				fieldNamesList.add(trimmedFieldName);
			}
		}
		
		return fieldNamesList;
	}
	
	public static String getFieldNamesAsString(Collection<String> fieldNames) {
		StringBuilder newKey = new StringBuilder();
		Iterator<String> fieldNamesIterator = fieldNames.iterator();
		while (fieldNamesIterator.hasNext()) {
			String newKeyFieldName = fieldNamesIterator.next();
			newKey.append(newKeyFieldName);
			if (fieldNamesIterator.hasNext()) {
				newKey.append(", ");
			}
		}
		return newKey.toString();
	}
}
