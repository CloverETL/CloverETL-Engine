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
package org.jetel.ctl.debug;

import java.io.Serializable;

import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.Defaults;
import org.jetel.data.NullRecord;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.bytes.CloverBuffer;

/**
 * {@link DataRecord} wrapper that can be serialized using standard Java serialization.
 * 
 * @author jan.michalica (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 12.5.2016
 */
public class SerializedDataRecord implements Serializable {

	private static final long serialVersionUID = 1L;

	private DataRecordMetadata metadata;
	private byte serializedRecord[];
	private long id;
	private transient DataRecord record;
	
	public static SerializedDataRecord fromDataRecord(DataRecord record) {
		SerializedDataRecord result = new SerializedDataRecord();
		result.metadata = record.getMetadata();
		if (!NullRecord.NULL_RECORD.getMetadata().equals(record.getMetadata())) {
			CloverBuffer buffer = CloverBuffer.allocate(Defaults.Record.RECORD_INITIAL_SIZE, false);
			record.serialize(buffer);
			byte content[] = new byte[buffer.position()];
			buffer.flip().get(content);
			result.serializedRecord = content;
		}
		return result;
	}
	
	public DataRecord getDataRecord() {
		if (record == null) {
			if (NullRecord.NULL_RECORD.getMetadata().equals(metadata)) {
				this.record = NullRecord.NULL_RECORD;
			} else {
				DataRecord record = DataRecordFactory.newRecord(metadata);
				CloverBuffer buffer = CloverBuffer.wrap(serializedRecord);
				record.deserialize(buffer);
				this.record = record;
			}
		}
		return record;
	}
	
	public long getId() {
		return id;
	}
	
	public void setId(long id) {
		this.id = id;
	}
}
