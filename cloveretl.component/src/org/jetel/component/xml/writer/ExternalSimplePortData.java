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

import java.io.File;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;

import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.InputPort;
import org.jetel.metadata.DataRecordMetadata;

import com.sleepycat.je.CacheMode;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.OperationStatus;

/**
 * Implementation of data provider which handles the situation when records need to be cached and need to be looked up
 * under exactly one key
 * 
 * @author lkrejci (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created 11 Mar 2011
 */
public class ExternalSimplePortData extends PortData {

	private Database database;

	private ByteBuffer recordBuffer;
	
	private long cacheSize;
	
	private long counter = 0;
	
	public ExternalSimplePortData(InputPort inPort, Set<List<String>> keys, String tempDirectory, long cacheSize) {
		super(inPort, keys, tempDirectory);
		this.cacheSize = cacheSize; 
	}
	
	@Override
	public void init() throws ComponentNotReadyException {
		super.init();
		recordBuffer = ByteBuffer.allocateDirect(Defaults.Record.MAX_RECORD_SIZE);
		
		EnvironmentConfig envConfig = new EnvironmentConfig();
		envConfig.setCacheSize(cacheSize);
		envConfig.setAllowCreate(true);
		envConfig.setLocking(false);
		envConfig.setTransactional(false);
		envConfig.setSharedCache(true);
		envConfig.setCacheMode(CacheMode.MAKE_COLD);
		Environment dbEnvironment;
		try {
			File f = File.createTempFile("berkdb", "", tempDirectory != null ? new File(tempDirectory) : null);
			f.delete();
			f.mkdir();
			f.deleteOnExit();
			
			dbEnvironment = new Environment(f, envConfig);
		} catch (Exception e) {
			throw new ComponentNotReadyException(e);
		}
		
		DatabaseConfig dbConfig = new DatabaseConfig();
		dbConfig.setAllowCreate(true);
		dbConfig.setTemporary(true);
		dbConfig.setSortedDuplicates(true);
		dbConfig.setTransactional(false);
		dbConfig.setExclusiveCreate(true);
		
		database = dbEnvironment.openDatabase(null, Long.toString(System.nanoTime()), dbConfig);
	}

	@Override
	public void put(DataRecord record) throws IOException {
		try {
            record.serialize(recordBuffer);
        } catch (BufferOverflowException ex) {
            throw new IOException("Internal buffer is not big enough to accomodate data record ! (See MAX_RECORD_SIZE parameter)");
        }
        byte[] serializedValue = new byte[recordBuffer.position()];
        recordBuffer.flip();
        recordBuffer.get(serializedValue);
        recordBuffer.clear();

		DatabaseEntry databaseValue = new DatabaseEntry(serializedValue);
		
		DatabaseEntry databaseKey;
		if (nullKey) {
			database.put(null, new DatabaseEntry(toByteArray(counter++)), databaseValue);
		} else {
			int[] key = primaryKey[0];
		
			record.serialize(recordBuffer, key);
			byte[] serializedKey = new byte[recordBuffer.position()];
			recordBuffer.flip();
			recordBuffer.get(serializedKey);
			recordBuffer.clear();

			databaseKey = new DatabaseEntry(serializedKey);
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
	public boolean readInputPort() {
		return true;
	}
	
	private void readData(DatabaseEntry foundValue, DataRecord record) throws IOException {
		recordBuffer.put(foundValue.getData());
		recordBuffer.flip();
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
			ByteBuffer keyBuffer = ByteBuffer.allocateDirect(Defaults.Record.MAX_RECORD_SIZE);
			keyData.serialize(keyBuffer, parentKey);
			int dataLength = keyBuffer.position();
			keyBuffer.flip();
			byte[] serializedKey = new byte[dataLength];
			keyBuffer.get(serializedKey);

			databaseKey = new DatabaseEntry(serializedKey);
			
			if (cursor.getSearchKey(databaseKey, foundValue, null) != OperationStatus.SUCCESS) {
				next = null;
			} else {
				readData(foundValue, next);
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
