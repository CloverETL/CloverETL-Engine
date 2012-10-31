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
import java.util.List;
import java.util.Set;

import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.InputPort;

/**
 * Implementation of data provider which does not cache all the records, but reads records directly from input port as
 * needed. If data are looked up under specific key, records must be sorted!
 * 
 * Partial caching is required as the data lookup with the same key can be performed multiple times.   
 * 
 * @author lkrejci (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created 20 Dec 2010
 */
class StreamedPortData extends PortData {
	
	private int[] sortKeys;
	private String[] sortKeysString;
	private boolean[] ascending;
	private boolean portRead = false;

	private DataRecord current;
	private DataRecord next;
	private DataRecord unused;

	private DirectDynamicRecordBuffer cacheData;
	private DataRecord cacheKey;

	private DataRecord temp = null;

	private boolean unusedData = false;

	public StreamedPortData(InputPort inPort, Set<List<String>> keys, SortHint sortHint) {
		super(inPort, keys);
		if (sortHint != null) {
			this.ascending = sortHint.getAscending();

			sortKeysString = sortHint.getKeyFields();
			this.sortKeys = new int[sortKeysString.length];
			for (int i = 0; i < sortKeysString.length; i++) {
				sortKeys[i] = inPort.getMetadata().getFieldPosition(sortKeysString[i]);
			}
		}
	}

	@Override
	public void init() throws ComponentNotReadyException {
		super.init();
		next = DataRecordFactory.newRecord(inPort.getMetadata());
		next.init();
		current = DataRecordFactory.newRecord(inPort.getMetadata());
		current.init();
		unused = DataRecordFactory.newRecord(inPort.getMetadata());
		unused.init();
	}

	@Override
	public void preExecute() throws ComponentNotReadyException {
		super.preExecute();
		cacheData = new DirectDynamicRecordBuffer();
		try {
			cacheData.init();
		} catch (IOException e) {
			throw new ComponentNotReadyException(e);
		}
	}

	@Override
	public void postExecute() throws ComponentNotReadyException {
		super.postExecute();
		cacheKey = null;
		try {
			cacheData.close();
		} catch (IOException e) {
			throw new ComponentNotReadyException(e);
		}
	}

	@Override
	public DataIterator iterator(int[] key, int[] parentKey, DataRecord keyData, DataRecord nextKeyData) throws IOException {
		if (key == null) {
			return new SimpleDataIterator();
		} else {
			return new KeyDataIterator(key, parentKey, keyData, nextKeyData);
		}
	}

	@Override
	public boolean readInputPort() {
		return false;
	}

	@Override
	public void put(DataRecord record) {
	}

	private void checkOrder() throws IOException {
		int result = 0;
		for (int i = 0; i < sortKeys.length; i++) {
			DataField currentField = current.getField(sortKeys[i]);
			DataField nextField = next.getField(sortKeys[i]);
			if (currentField.isNull() && nextField.isNull()) {
				continue;
			} else {
				result = currentField.compareTo(nextField);
				if (result != 0) {
					if (ascending[i]) {
						result *= -1;
					}
					
					if (result > 0) {
						break;
					} else {
						throw new IOException("Input data records are not sorted on input port "+this.getInPort().getInputPortNumber()
								+". In record #"+(this.getInPort().getInputRecordCounter()) 
								+", key field \""+this.sortKeysString[i]+"\""
								+", value \""+nextField.toString()+"\""
								+" is " + (ascending[i]?"less":"greater")
								+" than previous value \""+currentField.toString()+"\".");
					}
				}
			}
		}
	}

	private class KeyDataIterator implements DataIterator {

		private final int[] keyFields;
		private final DataField[] parentKey;

		private boolean dataAvailable = true;

		private final boolean readFromCache;
		private final boolean saveToCache;

		public KeyDataIterator(int[] key, int[] parentKey, DataRecord keyData, DataRecord nextKeyData) throws IOException {
			this.keyFields = key;
			this.parentKey = new DataField[parentKey.length];
			for (int i = 0; i < parentKey.length; i++) {
				this.parentKey[i] = keyData.getField(parentKey[i]);
			}

			if (cacheKey == null) { // Cache is empty
				readFromCache = false;
				if (nextKeyData != null && equals(keyData, nextKeyData, parentKey)) { // next record has the same key => needs to be cached
					cacheKey = keyData.duplicate();
					saveToCache = true;
				} else {
					saveToCache = false;
				}
			} else { // Cache is not empty
				// cache hit
				if (equals(cacheKey, keyData, parentKey)) { 
					readFromCache = true;
					saveToCache = false;
				} else { // cache miss
					readFromCache = false;
					if (nextKeyData != null && equals(keyData, nextKeyData, parentKey)) {
						saveToCache = true;
						cacheKey.copyFrom(keyData);
					} else {
						saveToCache = false;
					}
				}
			}
			if (saveToCache) {
				cacheData.clear();
			}
			if (readFromCache) {
				cacheData.reset();
				cacheData.loadData();
				dataAvailable = cacheData.next(next);
			} else {
				dataAvailable = readFromPort();
			}
		}

		@Override
		public DataRecord next() throws IOException {
			if (readFromCache) {
				temp = current;
				current = next;
				next = temp;
				
				dataAvailable = cacheData.next(next);
				return current;
			} else {
				dataAvailable = readFromPort();
				return current;
			}
		}

		@Override
		public boolean hasNext() {
			return dataAvailable;
		}

		@Override
		public DataRecord peek() {
			return next;
		}

		private boolean readFromPort() throws IOException {
			if (unusedData) {
				temp = unused;
				unused = next;
				next = temp;
				int result = compare();
				if (result == 0) {
					unusedData = false;
					if (saveToCache) {
						cacheData.write(next);
					}
					return true;
				} else if (result < 0) {
					temp = unused;
					unused = next;
					next = temp;
					unusedData = true;
					return false;
				} else {
					unusedData = false;
				}
			}
			temp = current;
			current = next;
			next = temp;
			try {
				while (inPort.readRecord(next) != null) {
					if (portRead) {
						checkOrder();
					} else {
						portRead = true;
					}
					
					int result = compare();
					if (result == 0) {
						if (saveToCache) {
							cacheData.write(next);
						}
						return true;
					} else if (result > 0) {
						continue;
					} else {
						unusedData = true;
						temp = unused;
						unused = next;
						next = temp;
						break;
					}
				}
			} catch (InterruptedException e) {
				throw new IOException(e);
			}
			if (saveToCache) {
				cacheData.flushBuffer();
			}
			return false;
		}

		private int compare() {
			int result = 0;
			for (int i = 0; i < keyFields.length; i++) {
				DataField field = next.getField(keyFields[i]);
				if (field.isNull() && parentKey[i].isNull()) {
					continue;
				} else {
					result = field.compareTo(parentKey[i]);
					if (result != 0) {
						if (ascending[i]) {
							result *= -1;
							break;
						}
					}
				}
			}
			return result;
		}

		private boolean equals(DataRecord keyData, DataRecord otherKeyData, int[] key) {
			for (int i = 0; i < parentKey.length; i++) {
				DataField field1 = keyData.getField(key[i]);
				DataField field2 = otherKeyData.getField(key[i]);
				if (!field1.equals(field2)) {
					if (!(field1.isNull() && field2.isNull())) {
						return false;
					}
				}
			}
			return true;
		}
	}

	private class SimpleDataIterator implements DataIterator {

		private boolean dataAvailable = true;

		public SimpleDataIterator() throws IOException {
			try {
				if (inPort.readRecord(next) == null) {
					dataAvailable = false;
				}
			} catch (InterruptedException e) {
				throw new IOException(e);
			}
		}

		@Override
		public DataRecord next() throws IOException {
			temp = current;
			current = next;
			next = temp;
			try {
				if (inPort.readRecord(next) == null) {
					dataAvailable = false;
				} else if (sortKeys != null) {
					checkOrder();
				}
				return current;
			} catch (InterruptedException e) {
				throw new IOException(e);
			}
		}

		@Override
		public DataRecord peek() {
			return next;
		}

		@Override
		public boolean hasNext() {
			return dataAvailable;
		}
	}
}
