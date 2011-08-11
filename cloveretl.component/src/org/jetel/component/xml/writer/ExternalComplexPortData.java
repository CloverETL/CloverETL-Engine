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
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.InputPort;
import org.jetel.metadata.DataRecordMetadata;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.OperationStatus;

/**
 * Implementation of data provider which handles the most complex use case - data must be cached and lookup up under
 * multiple keys.
 * 
 * @author lkrejci (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created 11 Mar 2011
 */

public class ExternalComplexPortData extends ExternalPortData {
	
	public static final String NULL_INDEX_NAME = "$NULL_INDEX";
	
	private DirectDynamicRecordBuffer dataStorage;
	private Environment environment;
	private Map<String, Database> dataMap;

	private String[] stringKeys;
	private ByteBuffer indexBuffer;
	
	private long counter = 0;
		
	public ExternalComplexPortData(InputPort inPort, Set<List<String>> keys, String tempDirectory, long cacheSize) {
		super(inPort, keys, tempDirectory, cacheSize);
	}
	
	@Override
	public void init() throws ComponentNotReadyException {
		super.init();
		indexBuffer = ByteBuffer.allocateDirect(Defaults.Record.MAX_RECORD_SIZE);
		
		dataStorage = new DirectDynamicRecordBuffer(tempDirectory);
		try {
			dataStorage.init();
		} catch (IOException e) {
			throw new ComponentNotReadyException(e);
		}
		 
		dataMap = new HashMap<String, Database>();
		
	}
	
		@Override
	public void preExecute() throws ComponentNotReadyException {
		super.preExecute();
		DataRecordMetadata metadata = inPort.getMetadata();
		
		environment = getEnvironment();
		
		stringKeys = new String[primaryKey.length];
		for (int outer = 0; outer < primaryKey.length; outer++) {
			int[] key = primaryKey[outer];
			stringKeys[outer] = generateKey(metadata, key);
			Database database = environment.openDatabase(null, Long.toString(System.nanoTime()), getDbConfig());
			dataMap.put(stringKeys[outer], database);
		}
		if (nullKey) {
			Database database = environment.openDatabase(null, Long.toString(System.nanoTime()), getDbConfig());
			dataMap.put(NULL_INDEX_NAME, database);
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
		DatabaseEntry databaseValue = new DatabaseEntry(serializedValue);
		
		for (int i = 0; i < primaryKey.length; i++) {
			int[] key = primaryKey[i];
			
			DatabaseEntry databaseKey;
		
			record.serialize(recordBuffer, key);
			length = recordBuffer.position();
			recordBuffer.flip();
			byte[] serializedKey = new byte[length];
			recordBuffer.get(serializedKey);
			recordBuffer.clear();

			databaseKey = new DatabaseEntry(serializedKey);
			
			Database database = dataMap.get(stringKeys[i]);
			database.put(null, databaseKey, databaseValue);
		}
		if (nullKey) {
			DatabaseEntry databaseKey = new DatabaseEntry(toByteArray(counter++));
			Database database = dataMap.get(NULL_INDEX_NAME);
			database.put(null, databaseKey, databaseValue);
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
	public void postExecute() throws ComponentNotReadyException{
		try {
			dataStorage.close();
			for (Database database : dataMap.values()) {
				database.close();
			}
			dataStorage.clear();
			environment.close();
		} catch (IOException e) {
			throw new ComponentNotReadyException(e);
		}
		
		super.postExecute();
	}
	
	private String generateKey(DataRecordMetadata metadata, int[] key) {
		StringBuilder stringKey = new StringBuilder();
		for (int j = 0; j < key.length; j++) {
			stringKey.append(metadata.getField(key[j]).getName());
		}
		return stringKey.toString();
	}
	
	private void readData(DatabaseEntry foundValue, DataRecord record) throws IOException {
		int length = 0;
		int position = 0;
		
		indexBuffer.put(foundValue.getData());
		indexBuffer.flip();
		position = indexBuffer.getInt();
		length = indexBuffer.getInt();
		indexBuffer.clear();
		
		recordBuffer.limit(length);
		dataStorage.read(recordBuffer, position);
		record.deserialize(recordBuffer);
		recordBuffer.clear();
	}

	private class KeyDataIterator implements DataIterator {
		
		private DataRecord current;
		private DataRecord next;
		private DataRecord temp;
		
		private String indexKey;
		private Database index;
		
		private Cursor cursor;
		
		private DatabaseEntry databaseKey;
		private DatabaseEntry foundValue = new DatabaseEntry();
		
		public KeyDataIterator(int[] key, int[] parentKey, DataRecord keyData) throws IOException {
			
			DataRecordMetadata metadata = inPort.getMetadata();
			indexKey = generateKey(metadata, key);
			
			index = dataMap.get(indexKey);
			cursor = index.openCursor(null, null);
			
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
			
			cursor = dataMap.get(NULL_INDEX_NAME).openCursor(null, null);
			indexBuffer = ByteBuffer.allocateDirect(Defaults.Record.MAX_RECORD_SIZE);
			
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
}
