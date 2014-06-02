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
import java.nio.BufferOverflowException;
import java.util.List;
import java.util.Set;

import jdbm.helper.Tuple;
import jdbm.helper.TupleBrowser;

import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.InputPort;
import org.jetel.metadata.DataRecordMetadata;

/**
 * @author lkrejci (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created 8 Sep 2011
 */
class ExternalSimplePortData extends ExternalPortData {

	private BTree<byte[], byte[]> tree;

	public ExternalSimplePortData(InputPort inPort, Set<List<String>> keys) {
		super(inPort, keys);
	}

	@Override
	public void preExecute() throws ComponentNotReadyException {
		super.preExecute();
		try {
			tree = BTree.createInstance(sharedCache, recordKeyComparator, serializer, serializer, PAGE_SIZE);
		} catch (IOException e) {
			throw new ComponentNotReadyException(e);
		}
	}
	
	@Override
	public void put(DataRecord dataRecord) throws IOException {
		tree.insert(serializeKey(dataRecord), serializeValue(dataRecord), false);
	}
	
	private byte[] serializeValue(DataRecord record) throws IOException {
		try {
            record.serializeUnitary(recordBuffer);
        } catch (BufferOverflowException ex) {
            throw new IOException("Internal buffer is not big enough to accomodate data record ! (See RECORD_LIMIT_SIZE parameter)");
        }
        byte[] serializedValue = new byte[recordBuffer.position()];
        recordBuffer.flip();
        recordBuffer.get(serializedValue);
        recordBuffer.clear();

		return serializedValue;
	}
	
	private byte[] serializeKey(DataRecord record) {
		if (!nullKey) {
			record.serializeUnitary(recordBuffer, primaryKey[0]);
		}
		recordBuffer.put(toByteArray(keyCounter++));
		byte[] serializedKey = new byte[recordBuffer.position()];
		recordBuffer.flip();
		recordBuffer.get(serializedKey);
		recordBuffer.clear();
		
		return serializedKey;
	}
	
	@Override
	public DataIterator iterator(int[] key, int[] parentKey, DataRecord keyData, DataRecord nextKeyData) throws IOException {
		if (key == null) {
			return new SimpleDataIterator();
		} else {
			return new KeyDataIterator(key, parentKey, keyData);
		}
	}
	
	private void readData(Tuple<byte[], byte[]> tuple, DataRecord record) throws IOException {
		recordBuffer.put(tuple.getValue());
		recordBuffer.flip();
		record.deserializeUnitary(recordBuffer);
		recordBuffer.clear();
	}
	
	private class KeyDataIterator implements DataIterator {

		private DataRecord current;
		private DataRecord next;
		private DataRecord temp;

		private TupleBrowser<byte[], byte[]> browser;
		private Tuple<byte[], byte[]> tuple;
		private byte[] serializedKey;

		public KeyDataIterator(int[] key, int[] parentKey, DataRecord keyData) throws IOException {
			DataRecordMetadata metadata = inPort.getMetadata();

			current = DataRecordFactory.newRecord(metadata);
			current.init();
			next = DataRecordFactory.newRecord(metadata);
			next.init();
			tuple = new Tuple<byte[], byte[]>();

			serializedKey = getDatabaseKey(current, key, keyData, parentKey);
			browser = tree.browse(serializedKey);

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
	
	private class SimpleDataIterator implements DataIterator {

		private TupleBrowser<byte[], byte[]> browser;
		private Tuple<byte[], byte[]> tuple;

		private DataRecord current;
		private DataRecord next;
		private DataRecord temp;

		public SimpleDataIterator() throws IOException {
			DataRecordMetadata metadata = inPort.getMetadata();

			current = DataRecordFactory.newRecord(metadata);
			current.init();
			next = DataRecordFactory.newRecord(metadata);
			next.init();
			tuple = new Tuple<byte[], byte[]>();

			browser = tree.browse();

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
