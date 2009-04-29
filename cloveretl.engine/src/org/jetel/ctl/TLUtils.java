package org.jetel.ctl;

import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;

public final class TLUtils {

	private TLUtils() {
		// not available
	}
	
	
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
	
	
}
