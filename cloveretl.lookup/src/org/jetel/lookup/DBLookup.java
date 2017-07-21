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

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;
import org.jetel.connection.jdbc.CopySQLData;
import org.jetel.connection.jdbc.SQLCloverStatement;
import org.jetel.connection.jdbc.SQLUtil;
import org.jetel.connection.jdbc.specific.JdbcSpecific;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.HashKey;
import org.jetel.data.NullRecord;
import org.jetel.data.RecordKey;
import org.jetel.data.lookup.Lookup;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.primitive.SimpleCache;

/**
 * DBLookup that performs data fetch at once in single connection-synchronized operation.
 * All fetched data is kept in memory.
 * 
 * @author jan.michalica (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 21.9.2012
 */
public final class DBLookup implements Lookup {

	private static final Logger log = Logger.getLogger(DBLookup.class);
	
	private static final List<DataRecord> NEGATIVE_RESPONSE = Collections.singletonList(NullRecord.NULL_RECORD);
	
	private DBLookupTable lookupTable;
	private SimpleCache recordCache;
	private Iterator<DataRecord> currentIterator;
	private int recordCount = -1;
	private HashKey key;
	private SQLCloverStatement statement;
	private DataRecordMetadata dbMetadata;
	/*
	 * only for testing
	 */
	private int allHits;
	private int cacheHits;
	
	public DBLookup(SQLCloverStatement statement, RecordKey key,
			DataRecord record) throws SQLException, ComponentNotReadyException {
		this.statement = statement;
		this.statement.init();
		this.key = new HashKey(key, record);
	}
	
	@Override
	public DBLookupTable getLookupTable() {
		return this.lookupTable;
	}
	
	public void setLookupTable(DBLookupTable lookupTable) {
		this.lookupTable = lookupTable;
		this.dbMetadata = lookupTable.getMetadata();
	}
	
	@Override
	public boolean hasNext() {
		checkDataFetched();
		return currentIterator.hasNext();
	}

	@Override
	public DataRecord next() {
		checkDataFetched();
		DataRecord record = currentIterator.next();
		return record.duplicate();
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
		++allHits;
		reset();
		if (isCached()) {
			if (recordCache != null) {
				List<DataRecord> records = recordCache.getAll(key);
				if (records != null) {
					if (NEGATIVE_RESPONSE.equals(records)) {
						recordCount = 0;
						currentIterator = Collections.<DataRecord>emptyList().iterator();
					} else {
						recordCount = records.size();
						currentIterator = records.iterator();
					}
					++cacheHits;
					return;
				}
			} else {
				recordCache = new SimpleCache(1, lookupTable.maxCached);
				recordCache.enableDuplicity();
			}
		}
		List<DataRecord> records;
		synchronized (lookupTable.dbConnection.getSqlConnection()) {
			records = fetchData();
		}
		recordCount = records.size();
		currentIterator = records.iterator();
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
	
	private List<DataRecord> fetchData() {
		
		ResultSet resultSet = null;
		List<DataRecord> records = new LinkedList<DataRecord>();
		try {
			resultSet = statement.executeQuery();
			if (dbMetadata == null) {
				/*
				 * TODO discover cases where metadata need to be defined from incoming result set
				 * and move this logic in an appropriate unit
				 */
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
			DataRecord record = new DataRecord(dbMetadata);
			record.init();
			HashKey key = null;
			if (isCached()) {
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
				DataRecord storedRecord = record.duplicate();
				if (isCached()) {
					recordsCached &= recordCache.put(key, storedRecord);
				}
				records.add(storedRecord);
			}
			if (isCached() && !recordsCached) {
				log.warn("Too many data records for a single key: " + toString(key) +
    				" Enlarge the cache size to accomodate more data records.");
				recordCache.clear();
			}
			if (records.isEmpty()) {
				if (lookupTable.storeNulls) {
					recordCache.put(key, NullRecord.NULL_RECORD);
				}
				return Collections.emptyList();
			}
			return records;
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
	
	private String toString(HashKey key) {
		
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < key.getKeyFields().length; ++i) {
			sb.append(key.getRecordKey().getKeyFieldNames()[i]);
			sb.append('=');
			sb.append(key.getDataRecord().getField(i).getValue());
			sb.append(',');
		}
		if (sb.length() > 0) {
			sb.delete(sb.length() - 1, sb.length());
		}
		return sb.toString();
	}
	
	@Override
	public int getNumFound() {
		checkDataFetched();
		return recordCount;
	}
	
	public DataRecordMetadata getMetadata() {
		return this.dbMetadata;
	}

	private void checkDataFetched() {
		if (recordCount < 0) {
			throw new IllegalStateException("no data, call seek() first");
		}
	}
	
	public void close() throws SQLException {
	}
	
	private void reset() {
		
		recordCount = -1;
		currentIterator = null;
	}
	
	private boolean isCached() {
		return lookupTable.maxCached > 0;
	}
	
	int getTotalNumber() {
		return allHits;
	}
	
	int getCacheNumber() {
		return cacheHits;
	}
}
