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
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.jetel.component.tree.writer.portdata.StreamedSimplePortData.SortCheckDataIterator;
import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.InputPort;

/**
 * Streamed port data with in-memory cache for current key data.
 * 
 * @author jan.michalica (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 19.3.2014
 */
class InternalStreamedPortData extends StreamedPortDataBase {

	private SortHint sortHint;
	private BufferedInputPortReader portReader;
	/*
	 * Last used key to fetch data
	 */
	private DataRecord prevKey;
	
	InternalStreamedPortData(InputPort inPort, Set<List<String>> keys, SortHint sortHint) {
		super(inPort, keys);
		this.portReader = new BufferedInputPortReader(inPort);
		this.sortHint = sortHint;
	}

	@Override
	public DataIterator iterator(int[] key, int[] parentKey, DataRecord keyData, DataRecord nextKeyData)
			throws IOException {

		if (key == null) {
			if (sortHint != null) {
				return new SortCheckDataIterator(inPort, sortHint);
			} else {
				return new SimpleDataIterator(inPort);
			}
		} else {
			if (prevKey != null) {
				if (areEqual(prevKey, keyData, parentKey)) {
					portReader.rewind();
				} else {
					portReader.evict();
				}
			}
			prevKey = keyData.duplicate();
			return new KeyDataIterator(key, parentKey, keyData, inPort);
		}
	}
	
	@Override
	public void postExecute() throws ComponentNotReadyException {
		super.postExecute();
		portReader.clear();
		prevKey = null;
	}
	
	
	
	class KeyDataIterator extends SortCheckDataIterator {
		
		private int key[];
		private int parentKey[];
		private DataRecord keyData;
		private DataRecord parentData;
		
		public KeyDataIterator(int key[], int parentKey[], DataRecord parentKeyData, InputPort inputPort) {
			super(inputPort, sortHint);
			this.key = key;
			this.parentKey = parentKey;
			this.parentData = parentKeyData;
		}
		
		@Override
		protected DataRecord doFetchNext(DataRecord target) throws IOException {
			
			DataRecord next = portReader.read();
			if (next == null) {
				return null;
			}
			if (areEqual(getKeyRecord(), next, key)) {
				target.copyFrom(next);
				return target;
			} else {
				// return record we are not interested in
				portReader.pushBack();
				return null;
			}
		}
		
		private DataRecord getKeyRecord() {
			if (keyData == null) {
				keyData = DataRecordFactory.newRecord(inPort.getMetadata());
				keyData.init();
				for (int i = 0; i < key.length; ++i) {
					keyData.getField(key[i]).setValue(parentData.getField(parentKey[i]).getValue());
				}
			}
			return keyData;
		}
	}
	
	private static boolean areEqual(DataRecord one, DataRecord another, int indices[]) {
		
		for (int i = 0; i < indices.length; ++i) {
			DataField keyField = one.getField(indices[i]);
			DataField recordField = another.getField(indices[i]);
			if (keyField.isNull() && recordField.isNull()) {
				continue;
			}
			if (!keyField.equals(recordField)) {
				return false;
			}
		}
		return true;
	}
	
	/**
	 * Buffered reader for data from {@link InputPort}. All of data
	 * are stored in buffer, so that they repeated retrieval is possible.
	 */
	static class BufferedInputPortReader {
		
		private List<DataRecord> buffer = new ArrayList<DataRecord>();
		private DataRecord record;
		private InputPort inputPort;
		private int index = -1;
		
		
		BufferedInputPortReader(InputPort inputPort) {
			this.record = DataRecordFactory.newRecord(inputPort.getMetadata());
			this.record.init();
			this.inputPort = inputPort;
		}
		
		/**
		 * Reads next record from port, stores it in the buffer and shifts
		 * buffer pointer one record forward.
		 * 
		 * @return
		 * @throws IOException
		 */
		public DataRecord read() throws IOException {
			if (index + 1 > buffer.size() - 1) {
				if (readNext()) {
					return buffer.get(++index);
				} else {
					return null;
				}
			} else {
				return buffer.get(++index);
			}
		}
		
		/**
		 * Shifts reader pointer one record back.
		 * @param record
		 */
		public void pushBack() {
			if (index >= 0) {
				--index;
			} else {
				throw new IllegalStateException("Cannot push back.");
			}
		}
		
		/**
		 * Discards buffered data up to current pointer.
		 */
		public void evict() {
			if (!buffer.isEmpty()) {
				buffer.subList(0, index + 1).clear();
			}
			index = -1;
		}
		
		/**
		 * Rewinds the buffer pointer to first record.
		 */
		public void rewind() {
			index = -1;
		}
		
		public void clear() {
			index = -1;
			buffer.clear();
		}
		
		private boolean readNext() throws IOException {
			try {
				if (inputPort.readRecord(record) != null) {
					buffer.add(record.duplicate());
					record.reset();
					return true;
				}
				return false;
			} catch (InterruptedException e) {
				throw new IOException(e);
			}
		}
	}
}
