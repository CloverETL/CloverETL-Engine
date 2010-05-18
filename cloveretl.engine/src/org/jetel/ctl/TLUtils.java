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
