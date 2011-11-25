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
package org.jetel.ctl;

import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.string.StringUtils;

public final class TLUtils {

	public static DataFieldMetadata getFieldMetadata(DataRecordMetadata[] recordMetadata, int recordNo, int fieldNo) {
		if (recordNo >= recordMetadata.length) {
			throw new IllegalArgumentException("Record [" + recordNo + "] does not exist");
		}
		DataRecordMetadata record = recordMetadata[recordNo];
		if (record == null) {
			throw new IllegalArgumentException("Metadata for record [ " + recordNo + "] null (not assigned?)");
		}
		
		return record.getField(fieldNo);
	}

	public static String operatorToString(int operator) {
		switch (operator) {
			case TransformLangParserConstants.EQUAL:
				return "==";
			case TransformLangParserConstants.NON_EQUAL:
				return "!=";
			case TransformLangParserConstants.LESS_THAN:
				return "<";
			case TransformLangParserConstants.LESS_THAN_EQUAL:
				return "<=";
			case TransformLangParserConstants.GREATER_THAN:
				return ">";
			case TransformLangParserConstants.GREATER_THAN_EQUAL:
				return ">=";

			default:
				// the operator is unknown
				return null;
		}
	}

	/**
	 * Compares two given metadata objects.
	 * Metadata objects are considered as equal if have same number of fields and fields are equal
	 * (see {@link #equals(DataFieldMetadata, DataFieldMetadata)}).
	 * @param metadata1
	 * @param metadata2
	 * @return <code>true</code> if metadata objects are considered as equal, <code>false</code> otherwise
	 */
	public static boolean equals(DataRecordMetadata metadata1, DataRecordMetadata metadata2) {
		if (metadata1 == null || metadata2 == null) {
			return false;
		}

		if (metadata1 == metadata2) {
			return true;
		}

		if (metadata1.getNumFields() != metadata2.getNumFields()) {
			return false;
		}

		for (int i = 0; i < metadata1.getNumFields(); i++) {
			if (!equals(metadata1.getField(i), metadata2.getField(i))) {
				return false;
			}
		}

		return true;
	}
	
	/**
	 * Compares two given metadata fields.
	 * Metadata fields are considered as equal if have same name and type.
	 * @param field1
	 * @param field2
	 * @return <code>true</code> if metadata fields are considered as equal, <code>false</code> otherwise
	 */
	public static boolean equals(DataFieldMetadata field1, DataFieldMetadata field2) {
		if (field1 == null || field2 == null) {
			return false;
		}
		
		//field names have to be equal
		if (!field1.getName().equals(field2.getName())) {
			return false;
		}
		
		//field types have to be equal
		if (! (field1.getType() == field2.getType())) {
			return false;
		}
		
		if (field1.getType() == DataFieldMetadata.DECIMAL_FIELD) {
			
			if (!StringUtils.equalsWithNulls(field1.getProperty(DataFieldMetadata.LENGTH_ATTR),
					field2.getProperty(DataFieldMetadata.LENGTH_ATTR))
					|| !StringUtils.equalsWithNulls(field1.getProperty(DataFieldMetadata.SCALE_ATTR),
					field2.getProperty(DataFieldMetadata.SCALE_ATTR))) {
				return false;
			}
		}
		
		return true;
	}
	
	private TLUtils() {
		// not available
	}

}
