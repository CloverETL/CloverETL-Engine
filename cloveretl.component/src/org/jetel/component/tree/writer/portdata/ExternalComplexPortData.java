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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import jdbm.helper.Tuple;
import jdbm.helper.TupleBrowser;

import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.InputPort;
import org.jetel.metadata.DataRecordMetadata;

/**
 * @author lkrejci (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 13 Sep 2011
 */
class ExternalComplexPortData extends ExternalPortData {
	
	private static final String NULL_INDEX_NAME = "$NULL_INDEX";
	
	private String[] stringKeys;
	private DirectDynamicRecordBuffer dataStorage;
	private Map<String, BTree<byte[], byte[]>> dataMap;

	public ExternalComplexPortData(InputPort inPort, Set<List<String>> keys) {
		super(inPort, keys);
	}
	
	@Override
	public void init() throws ComponentNotReadyException {
		super.init();
		dataMap = new HashMap<String, BTree<byte[], byte[]>>();
	}

	@Override
	public void preExecute() throws ComponentNotReadyException {
		super.preExecute();
		
		dataStorage = new DirectDynamicRecordBuffer();
		try {
			dataStorage.init();
		} catch (IOException e) {
			throw new ComponentNotReadyException("Could not initialize record buffer.", e);
		}
		
		DataRecordMetadata metadata = inPort.getMetadata();
		try {
			stringKeys = new String[primaryKey.length];
			for (int outer = 0; outer < primaryKey.length; outer++) {
				int[] key = primaryKey[outer];
				stringKeys[outer] = generateKey(metadata, key);
				BTree<byte[], byte[]> tree = BTree.createInstance(sharedCache, recordKeyComparator, serializer, serializer, PAGE_SIZE);
				dataMap.put(stringKeys[outer], tree);
			}
			if (nullKey) {
				BTree<byte[], byte[]> tree = BTree.createInstance(sharedCache, recordKeyComparator, serializer, serializer, PAGE_SIZE);
				dataMap.put(NULL_INDEX_NAME, tree);
			}
		} catch (IOException e) {
			throw new ComponentNotReadyException(e);
		}
	}
	
	@Override
	public void postExecute() throws ComponentNotReadyException {
		super.postExecute();
		dataMap.clear();
		try {
			dataStorage.close();
		} catch (IOException e) {
			throw new ComponentNotReadyException("Could not delete record buffer.", e);
		}
	}

	@Override
	public void put(DataRecord record) throws IOException {
		IndexKey value = dataStorage.writeRaw(record);

		recordBuffer.putInt(value.getPosition());
		recordBuffer.putInt(value.getLength());
		int length = recordBuffer.position();
		recordBuffer.flip();
		byte[] serializedValue = new byte[length];
		recordBuffer.get(serializedValue);
		recordBuffer.clear();

		for (int i = 0; i < primaryKey.length; i++) {
			int[] key = primaryKey[i];

			record.serializeUnitary(recordBuffer, key);
			recordBuffer.put(toByteArray(keyCounter));
			byte[] serializedKey = new byte[recordBuffer.position()];
			recordBuffer.flip();
			recordBuffer.get(serializedKey);
			recordBuffer.clear();

			BTree<byte[], byte[]> tree = dataMap.get(stringKeys[i]);
			tree.insert(serializedKey, serializedValue, false);
		}
		if (nullKey) {
			BTree<byte[], byte[]> tree = dataMap.get(NULL_INDEX_NAME);
			tree.insert(toByteArray(keyCounter), serializedValue, false);
		}
		
		keyCounter++;
	}

	@Override
	public DataIterator iterator(int[] key, int[] parentKey, DataRecord keyData, DataRecord nextKeyData)
			throws IOException {
		if (key == null) {
			return new SimpleCachedDataIterator();
		} else {
			return new KeyDataIterator(key, parentKey, keyData);
		}
	}
	
	private static String generateKey(DataRecordMetadata metadata, int[] key) {
		StringBuilder stringKey = new StringBuilder();
		for (int j = 0; j < key.length; j++) {
			stringKey.append(metadata.getField(key[j]).getName());
		}
		return stringKey.toString();
	}
	
	private void readData(Tuple<byte[], byte[]> tuple, DataRecord record) throws IOException {
		int length = 0;
		int position = 0;
		
		recordBuffer.put(tuple.getValue());
		recordBuffer.flip();
		position = recordBuffer.getInt();
		length = recordBuffer.getInt();
		recordBuffer.clear();
		
		recordBuffer.limit(length);
		dataStorage.read(recordBuffer, position);
		record.deserializeUnitary(recordBuffer);
		recordBuffer.clear();
	}
	
	private class KeyDataIterator implements DataIterator {

		private DataRecord current;
		private DataRecord next;
		private DataRecord temp;

		private String indexKey;
		private BTree<byte[], byte[]> index;

		private TupleBrowser<byte[], byte[]> browser;
		private Tuple<byte[], byte[]> tuple;
		private byte[] serializedKey;

		public KeyDataIterator(int[] key, int[] parentKey, DataRecord keyData) throws IOException {

			DataRecordMetadata metadata = inPort.getMetadata();
			indexKey = generateKey(metadata, key);
			index = dataMap.get(indexKey);

			current = DataRecordFactory.newRecord(metadata);
			current.init();
			next = DataRecordFactory.newRecord(metadata);
			next.init();
			tuple = new Tuple<byte[], byte[]>();

			serializedKey = getDatabaseKey(current, key, keyData, parentKey);
			browser = index.browse(serializedKey);

			if (browser.getNext(tuple)) {
				if (equalsKey(tuple, serializedKey)) {
					readData(tuple, next);
				} else {
					next = null;
				}
			} else {
				next = null;
			}
		}

		@Override
		public DataRecord next() throws IOException {
			temp = current;
			current = next;
			next = temp;

			if (browser.getNext(tuple)) {
				if (equalsKey(tuple, serializedKey)) {
					readData(tuple, next);
				} else {
					next = null;
				}
			} else {
				next = null;
			}
			return current;
		}

		@Override
		public boolean hasNext() {
			return next != null;
		}

		@Override
		public DataRecord peek() {
			return next;
		}
	}

	private class SimpleCachedDataIterator implements DataIterator {
		private TupleBrowser<byte[], byte[]> browser;
		private Tuple<byte[], byte[]> tuple;

		private DataRecord current;
		private DataRecord next;
		private DataRecord temp;

		public SimpleCachedDataIterator() throws IOException {
			DataRecordMetadata metadata = inPort.getMetadata();

			current = DataRecordFactory.newRecord(metadata);
			current.init();
			next = DataRecordFactory.newRecord(metadata);
			next.init();
			tuple = new Tuple<byte[], byte[]>();

			browser = dataMap.get(NULL_INDEX_NAME).browse();
			if (browser.getNext(tuple)) {
				readData(tuple, next);
			} else {
				next = null;
			}
		}

		@Override
		public DataRecord next() throws IOException {
			temp = current;
			current = next;
			next = temp;

			if (browser.getNext(tuple)) {
				readData(tuple, next);
			} else {
				next = null;
			}
			return current;
		}

		@Override
		public boolean hasNext() {
			return next != null;
		}

		@Override
		public DataRecord peek() {
			return next;
		}
	}

}
