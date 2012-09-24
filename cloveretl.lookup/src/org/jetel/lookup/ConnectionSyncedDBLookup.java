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
package org.jetel.lookup;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.log4j.Logger;
import org.jetel.connection.jdbc.CopySQLData;
import org.jetel.connection.jdbc.SQLCloverStatement;
import org.jetel.connection.jdbc.SQLUtil;
import org.jetel.connection.jdbc.specific.JdbcSpecific;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.Defaults;
import org.jetel.data.HashKey;
import org.jetel.data.NullRecord;
import org.jetel.data.RecordKey;
import org.jetel.data.tape.DataRecordTape;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.primitive.SimpleCache;

/**
 * DBLookup that performs data fetch at once in single connection-synchronized operation.
 * 
 * @author jan.michalica (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 21.9.2012
 */
public class ConnectionSyncedDBLookup extends DBLookup {

	private static final Logger log = Logger.getLogger(ConnectionSyncedDBLookup.class);
	
	private SimpleCache recordCache;
	private Iterator<DataRecord> currentIterator;
	private List<DataRecord> fallbackList = new LinkedList<DataRecord>(); // when cached and cache capacity is insufficient
	private DataRecordTape recordBuffer;
	private int recordCount = -1;
	private int readCount;
	
	public ConnectionSyncedDBLookup(SQLCloverStatement statement, RecordKey key,
			DataRecord record, JdbcSpecific jdbcSpecific) throws SQLException, ComponentNotReadyException {
		super(statement, key, record, jdbcSpecific);
	}
	
	@Override
	public boolean hasNext() {
		checkDataFetched();
		if (isCached()) {
			return currentIterator.hasNext();
		} else {
			return readCount < recordCount;
		}
	}

	@Override
	public DataRecord next() {
		checkDataFetched();
		DataRecord result = null;
		if (isCached()) {
			result = currentIterator.next();
			result = result.duplicate();
		} else {
			try {
				DataRecord record = DataRecordFactory.newRecord(dbMetadata);
				record.init();
				if (!recordBuffer.get(record)) {
					throw new NoSuchElementException();
				}
				result = record;
			} catch (InterruptedException e) {
				throw new JetelRuntimeException(e);
			} catch (IOException e) {
				throw new JetelRuntimeException(e);
			}
			
		}
		++readCount;
		return result;
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException("remove");
	}

	@Override
	public RecordKey getKey() {
		return key.getRecordKey();
	}

	@SuppressWarnings("unchecked")
	@Override
	public void seek() {
		
		reset();
		if (isCached()) {
			if (recordCache != null && recordCache.containsKey(key)) {
				List<DataRecord> records = recordCache.getAll(key);
				recordCount = records.size();
				currentIterator = records.iterator();
				return;
			}
		}
		synchronized (lookupTable.dbConnection) {
			recordCount = fetchData(isCached());
		}
		if (isCached()) {
			List<DataRecord> records = recordCache.getAll(key);
			if (records == null) {
				records = fallbackList;
			}
			currentIterator = records.iterator();
		} else {
			if (recordBuffer != null) {
				try {
					recordBuffer.rewind();
				} catch (Exception e) {
					throw new JetelRuntimeException(e);
				}
			}
		}
	}

	@Override
	public void seek(DataRecord keyRecord) {
		
		key.setDataRecord(keyRecord);
		try {
			statement.setInRecord(keyRecord);
		} catch (ComponentNotReadyException e) {
			throw new RuntimeException("Failed to update statement record.", e);
		}
		seek();
	}
	
	private int fetchData(boolean cacheRecords) {
		
		ResultSet resultSet = null;
		try {
			resultSet = statement.executeQuery();
			DataRecordMetadata dbMetadata = this.dbMetadata;
			if (dbMetadata == null) {
				if (statement.getCloverOutputFields() == null) {
					dbMetadata = SQLUtil.dbMetadata2jetel(resultSet.getMetaData(), lookupTable.dbConnection.getJdbcSpecific());
				} else {
					ResultSetMetaData dbMeta = resultSet.getMetaData();
					JdbcSpecific jdbcSpecific = lookupTable.dbConnection.getJdbcSpecific();
					String[] fieldName = statement.getCloverOutputFields();
					DataFieldMetadata fieldMetadata;
					String tableName = dbMeta.getTableName(1);
					dbMetadata = new DataRecordMetadata(DataRecordMetadata.EMPTY_NAME, DataRecordMetadata.DELIMITED_RECORD);
					dbMetadata.setLabel(tableName);
					dbMetadata.setFieldDelimiter(Defaults.Component.KEY_FIELDS_DELIMITER);
					dbMetadata.setRecordDelimiter("\n");
					for (int i = 1; i <= dbMeta.getColumnCount(); i++) {
						fieldMetadata = SQLUtil.dbMetadata2jetel(fieldName[i], dbMeta, i, jdbcSpecific);
						dbMetadata.addField(fieldMetadata);
					}
					dbMetadata.normalize();
				}
			}
			DataRecord record = DataRecordFactory.newRecord(dbMetadata);
			record.init();
			int count = 0;
			HashKey key = null;
			if (cacheRecords) {
				key = new HashKey(this.key.getRecordKey(), this.key.getDataRecord().duplicate());
			}
			boolean recordsCached = true;
			while (resultSet.next()) {
				CopySQLData transMap[] = CopySQLData.sql2JetelTransMap(
						SQLUtil.getFieldTypes(dbMetadata, lookupTable.dbConnection.getJdbcSpecific()), 
						dbMetadata, record, lookupTable.dbConnection.getJdbcSpecific());
				//get data from results
				for (int i = 0; i < transMap.length; i++) {
					transMap[i].sql2jetel(resultSet);
				}
				if (cacheRecords) {
					fallbackList.add(record);
					recordsCached &= addToCache(key, record);
				} else {
					addToBuffer(record);
				}
				++count;
			}
			if (cacheRecords) {
				if (recordsCached) {
					fallbackList.clear();
				} else {
					log.warn("Too many data records for a single key. Enlarge the cache " +
            			"size to accomodate more data records.");
					recordCache.clear();
				}
			}
			if (count == 0 && storeNulls) {
				if (cacheRecords) {
					addToCache(key, NullRecord.NULL_RECORD);
				} else {
					addToBuffer(NullRecord.NULL_RECORD);
				}
				count = 1;
			}
			return count;
		} catch (Exception e) {
			throw new JetelRuntimeException(e);
		} finally {
			if (resultSet != null) {
				try {
					resultSet.close();
				} catch (SQLException e) {
					// ignore
				}
			}
		}
	}
	
	private boolean addToCache(HashKey key, DataRecord record) {
		
		if (recordCache == null) {
			recordCache = new SimpleCache(1, cacheSize);
			recordCache.enableDuplicity();
		}
		return recordCache.put(key, record.duplicate());
	}
	
	private void addToBuffer(DataRecord record) throws InterruptedException, IOException {
		
		if (recordBuffer == null) {
			recordBuffer = new DataRecordTape();
			recordBuffer.open();
			recordBuffer.addDataChunk();
		}
		recordBuffer.put(record);
	}

	@Override
	public int getNumFound() {
		checkDataFetched();
		return recordCount;
	}

	private void checkDataFetched() {
		if (recordCount < 0) {
			throw new IllegalStateException("no data, call seek() first");
		}
	}
	
	@Override
	public void close() throws SQLException {
		super.close();
		if (recordBuffer != null) {
			try {
				recordBuffer.close();
			} catch (Exception e) {
				throw new SQLException(e);
			}
		}
	}
	
	private void reset() {
		
		readCount = 0;
		recordCount = -1;
		currentIterator = null;
		fallbackList.clear();
		if (recordBuffer != null) {
			try {
				recordBuffer.clear();
				recordBuffer.addDataChunk();
			} catch (Exception e) {
				throw new JetelRuntimeException(e);
			}
		}
	}
	
	private boolean isCached() {
		return cacheSize > 0;
	}
}
