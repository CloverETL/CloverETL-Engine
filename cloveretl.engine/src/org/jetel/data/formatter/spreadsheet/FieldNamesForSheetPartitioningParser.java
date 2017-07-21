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
package org.jetel.data.formatter.spreadsheet;

import java.util.ArrayList;
import java.util.List;

import org.jetel.data.Defaults;
import org.jetel.util.string.StringUtils;

/**
 * A class used for parsing field names out of a list of them given in a "Sheet" property of SpreadsheetWriter component.
 * 
 * @author Pavel Simecek (pavel.simecek@javlin.eu)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 19.1.2012
 */
public class FieldNamesForSheetPartitioningParser {
	/** A prefix of a sheet name specifying that a reference to the column should be used */  
	private static final String CLOVER_FIELD_PREFIX = "$";

	public static boolean hasSomeFieldNames(String sheetNameString) {
    	if (!StringUtils.isEmpty(sheetNameString)) {
    		String[] sheetReferences = sheetNameString.split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);
    		for (String sheetReference : sheetReferences) {
    			if (sheetReference.startsWith(CLOVER_FIELD_PREFIX)) {
    				return true;
    			}
    		}
    	}
		return false;
	}
	
	public static String createSheetNameString(String [] fieldNames) {
		StringBuilder sheetNameStringBuilder = new StringBuilder();

		for (int i=0; i<fieldNames.length; ++i) {
			sheetNameStringBuilder.append(CLOVER_FIELD_PREFIX);
			sheetNameStringBuilder.append(fieldNames[i]);
			if (i+1<fieldNames.length) {
				sheetNameStringBuilder.append(Defaults.Component.KEY_FIELDS_DELIMITER);
			}
		}
		
		return sheetNameStringBuilder.toString();
	}
	
	public static String [] parseFieldNames(String sheetNameString) {
		if (!StringUtils.isEmpty(sheetNameString) && sheetNameString.startsWith(CLOVER_FIELD_PREFIX)) {
        	String[] sheetReferences = sheetNameString.split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);

        	List<String> fieldNames = new ArrayList<String>();
        	
			for (String sheetReference : sheetReferences) {
				if (sheetReference.startsWith(CLOVER_FIELD_PREFIX))
				fieldNames.add(sheetReference.substring(1));
			}
			if (fieldNames.isEmpty()) {
				return null;
			} else {
				return fieldNames.toArray(new String[0]);
			}

        }
		return null;

	}
}
