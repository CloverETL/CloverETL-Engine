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

	private TLUtils() {
		// not available
	}

}
