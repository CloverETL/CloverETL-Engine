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
package org.jetel.component.xml.writer;

import java.io.IOException;
import java.io.Serializable;
import java.nio.BufferOverflowException;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.InputPort;
import org.jetel.metadata.DataRecordMetadata;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.OperationStatus;

/**
 * Implementation of data provider which handles the situation when records need to be cached and need to be looked up
 * under exactly one key
 * 
 * @author lkrejci (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created 11 Mar 2011
 */
public class ExternalSimplePortData extends ExternalPortData {

	private Environment environment;
	private Database database;
	
	private long keyCounter = 0;
	private long valueCounter = 0;
	
	public ExternalSimplePortData(InputPort inPort, Set<List<String>> keys, String tempDirectory, long cacheSize) {
		super(inPort, keys, tempDirectory, cacheSize);
	}
	
	@Override
	public void preExecute() throws ComponentNotReadyException {
		super.preExecute();
		environment = getEnvironment();
		DatabaseConfig dbConfig = getDbConfig();
		dbConfig.setDuplicateComparator(new DuplicateComparator());
		
		database = environment.openDatabase(null, Long.toString(System.nanoTime()), dbConfig);
	}

	@Override
	public void put(DataRecord record) throws IOException {
		DatabaseEntry databaseValue = getDatabaseValue(record);
		
		if (nullKey) {
			database.put(null, new DatabaseEntry(toByteArray(keyCounter++)), databaseValue);
		} else {
			int[] key = primaryKey[0];
		
			record.serialize(recordBuffer, key);
			byte[] serializedKey = new byte[recordBuffer.position()];
			recordBuffer.flip();
			recordBuffer.get(serializedKey);
			recordBuffer.clear();

			database.put(null, new DatabaseEntry(serializedKey), databaseValue);
		}
	}

	@Override
	public DataIterator iterator(int[] key, int[] parentKey, DataRecord keyData, DataRecord nextKeyData) throws IOException {
		if (key == null) {
			return new SimpleCachedDataIterator();
		} else {
			return new KeyDataIterator(key, parentKey, keyData);
		}
	}

	@Override
	public void postExecute() throws ComponentNotReadyException {
		database.close();
		environment.close();
		
		super.postExecute();
	}
	
	private DatabaseEntry getDatabaseValue(DataRecord record) throws IOException {
		//prepend counter, so that record order can be preserved
		recordBuffer.put(toByteArray(valueCounter++));
		
		try {
            record.serialize(recordBuffer);
        } catch (BufferOverflowException ex) {
            throw new IOException("Internal buffer is not big enough to accomodate data record ! (See MAX_RECORD_SIZE parameter)");
        }
        byte[] serializedValue = new byte[recordBuffer.position()];
        recordBuffer.flip();
        recordBuffer.get(serializedValue);
        recordBuffer.clear();

		return new DatabaseEntry(serializedValue);
	}
	
	private void readData(DatabaseEntry foundValue, DataRecord record) throws IOException {
		recordBuffer.put(foundValue.getData());
		recordBuffer.flip();
		recordBuffer.position(SERIALIZED_COUNTER_LENGTH); // discard counter
		record.deserialize(recordBuffer);
		recordBuffer.clear();
	}

	private class KeyDataIterator implements DataIterator {
		
		private DataRecord current;
		private DataRecord next;
		private DataRecord temp;
		
		private Cursor cursor;
		
		private DatabaseEntry databaseKey;
		private DatabaseEntry foundValue = new DatabaseEntry();
		
		public KeyDataIterator(int[] key, int[] parentKey, DataRecord keyData) throws IOException {
			
			DataRecordMetadata metadata = inPort.getMetadata();
			
			cursor = database.openCursor(null, null);
			
			current = new DataRecord(metadata);
			current.init();
			
			next = new DataRecord(metadata);
			next.init();
		
			databaseKey = getDatabaseKey(current, key, keyData, parentKey);
			
			if (cursor.getSearchKey(databaseKey, foundValue, null) == OperationStatus.SUCCESS) {
				readData(foundValue, next);
			} else {
				cursor.close();
				next = null;
				
			}
		}

		@Override
		public DataRecord next() throws IOException {
			temp = current;
			current = next;
			next = temp;
			
			if (cursor.getNextDup(databaseKey, foundValue, null) == OperationStatus.SUCCESS) {
				readData(foundValue, next);
			} else {
				cursor.close();
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
		
		private Cursor cursor;
		
		private DatabaseEntry foundKey = new DatabaseEntry();
		private DatabaseEntry foundValue = new DatabaseEntry();
		
		private DataRecord current;
		private DataRecord next;
		private DataRecord temp;
		
		public SimpleCachedDataIterator() throws IOException {
			DataRecordMetadata metadata = inPort.getMetadata();
			
			current = new DataRecord(metadata);
			current.init();
			next = new DataRecord(metadata);
			next.init();
			
			cursor = database.openCursor(null, null);
			
			if (cursor.getNext(foundKey, foundValue, null) == OperationStatus.SUCCESS) {
				readData(foundValue, next);
			} else {
				cursor.close();
				next = null;
			}
		}

		@Override
		public DataRecord next() throws IOException {
			temp = current;
			current = next;
			next = temp;
			
			if (cursor.getNext(foundKey, foundValue, null) == OperationStatus.SUCCESS) {
				readData(foundValue, next);
			} else {
				cursor.close();
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
	
	public static class DuplicateComparator implements Comparator<byte[]>, Serializable {

		private static final long serialVersionUID = 1L;
		
		// We need to be able to store duplicate items under duplicate keys,
		// therefore comparator must never return 0.
		@Override
		public int compare(byte[] o1, byte[] o2) {
			int result;
			for (int i = 0; i < SERIALIZED_COUNTER_LENGTH; i++) {
				result = o1[i] - o2[i];
				if (result != 0) {
					return result;
				}
			}
			return 0;
		}
		
	}
}
