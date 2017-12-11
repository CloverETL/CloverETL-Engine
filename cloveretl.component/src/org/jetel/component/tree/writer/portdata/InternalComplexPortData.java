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
package org.jetel.component.tree.writer.portdata;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;
import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.InputPort;

/**
 * Internal (i.e. in memory) port data, that can be looked up by multiple
 * keys consisting of multiple data fields.
 * 
 * @author jan.michalica (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 13.8.2013
 */
class InternalComplexPortData extends InternalPortData {

	private MultiValuedMap<DataRecordKey, DataRecord> records;

	InternalComplexPortData(InputPort inPort, Set<List<String>> keys) {
		super(inPort, keys);
	}
	
	@Override
	public void init() throws ComponentNotReadyException {
		super.init();
		records = new ArrayListValuedHashMap<>();
	}
	
	@Override
	public void free() {
		super.free();
		records = null;
	}
	
	@Override
	public void postExecute() throws ComponentNotReadyException {
		super.postExecute();
		records.clear();
	}

	@Override
	public void put(DataRecord record) throws IOException {
		if (nullKey) {
			records.put(DataRecordKey.NULL_KEY, record);
		}
		for (int keys[] : primaryKey) {
			records.put(DataRecordKey.forDataRecord(record, keys), record);
		}
	}
	
	@Override
	protected Collection<DataRecord> fetchData(int key[], int parentKey[], DataRecord keyData) {
		
		if (key == null) {
			return records.get(DataRecordKey.NULL_KEY);
		}
		DataRecordKey recordKey = createRecordKey(key, parentKey, keyData, keyRecord);
		Collection<DataRecord> data = records.get(recordKey);
		keyRecord.reset();
		return data;
	}

	protected DataRecordKey createRecordKey(int key[], int parentKey[], DataRecord parentData, DataRecord childData) {
		
		for (int i = 0; i < key.length; ++i) {
			childData.getField(key[i]).setValue(parentData.getField(parentKey[i]));
		}
		return DataRecordKey.forDataRecord(childData, key); 
	}
	
	static class DataRecordKey {
		
		static final DataRecordKey NULL_KEY = new DataRecordKey();
		
		DataField fields[];
		
		static DataRecordKey forDataRecord(DataRecord record, int keyIndices[]) {
			
			if (keyIndices == null || keyIndices.length == 0) {
				throw new IllegalArgumentException("Null or empty key.");
			}
			
			DataRecordKey key = new DataRecordKey();
			key.fields = new DataField[keyIndices.length];
			for (int i = 0; i < keyIndices.length; ++i) {
				key.fields[i] = record.getField(keyIndices[i]).duplicate();
			}
			return key;
		}

		@Override
		public int hashCode() {
			return Arrays.hashCode(fields);
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			DataRecordKey other = (DataRecordKey) obj;
			if (!Arrays.equals(fields, other.fields))
				return false;
			return true;
		}
		
		@Override
		public String toString() {
			StringBuilder sb = new StringBuilder("KEY[");
			if (fields != null) {
				for (int i = 0; i < fields.length; ++i) {
					sb.append(fields[i]);
					if (i < fields.length - 1) {
						sb.append(", ");
					}
				}
			} else {
				sb.append(fields);
			}
			sb.append(']');
			return sb.toString();
		}
	}
}
