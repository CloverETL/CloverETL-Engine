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
package org.jetel.data;

import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Specialized hash map implementation for storing DataRecords with the single RecordKey. It enables to store more
 * values with the same key. Duplicate values are stored as linked list. T also enables to change functionality of put
 * operation. (if map contains value with specified key, new value is not saved at all).
 * 
 * @author lkrejci
 * 
 * @created May 6, 2010
 */
public class DataRecordMap {

	protected final int putKeyFields[];
	private final boolean equalNULLs;
	private final boolean duplicate;
	private final boolean overwrite;
	private FullDataRecordEntry[] table;

	
	/**
	 * The number of duplicate key-value mappings contained in this map.
	 */
	private int duplicates;

	/**
	 * The number of key-value mappings contained in this map.
	 */
	private int size;

	/**
	 * The next size value at which to resize (capacity * load factor).
	 * 
	 * @serial
	 */
	int threshold;

	private static final float LOAD_FACTOR = 0.75f;
	private static final int DEFAULT_INITIAL_CAPACITY = 16;
	private static final int MAXIMUM_CAPACITY = 1 << 30;

	/**
	 * Constructs DataRecordMap object with initial capacity of DEFAULT_INITIAL_CAPACITY (16). 
	 * 
	 * @param key
	 * 			The general key for put operation.
	 * @param duplicate
	 * 			If set to true, more values can be stored under one key 
	 */
	public DataRecordMap(RecordKey key, boolean duplicate) {
		this(key, duplicate, true);
	}

	/**
	 * Constructs DataRecordMap object with the specified initial capacity
	 * 
	 * @param key
	 * 			The general key for put operation.
	 * @param duplicate
	 * 			If set to true, more values can be stored under one key 
	 * @param initialCapacity
	 * 			the initial capacity
	 */
	public DataRecordMap(RecordKey key, boolean duplicate, int initialCapacity) {
		this(key, duplicate, initialCapacity, true);
	}

	
	/**
	 * Constructs DataRecordMap object with initial capacity of DEFAULT_INITIAL_CAPACITY (16). 
	 * 
	 * @param key
	 * 			The general key for put operation.
	 * @param duplicate
	 * 			If set to true, more values can be stored under one key 
	 * @param overwrite
	 * 			Flag which changes behaviour of put operation
	 * 			True (default) - existing value is overwritten by the new one
	 * 			False - existing value remains, new is scrapped
	 */
	public DataRecordMap(RecordKey key, boolean duplicate, boolean overwrite) {
		this(key, duplicate, DEFAULT_INITIAL_CAPACITY, overwrite);
	}

	/**
	 * /**
	 * Constructs DataRecordMap object with the specified initial capacity 
	 * 
	 * @param key
	 * 			The general key for put operation.
	 * @param duplicate
	 * 			If set to true, more values can be stored under one key 
	 * @param initialCapacity
	 * 			the initial capacity
	 * @param overwrite
	 * 			Flag which changes behaviour of put operation
	 * 			True (default) - existing value is overwritten by the new one
	 * 			False - existing value remains, new is scrapped
	 */
	public DataRecordMap(RecordKey key, boolean duplicate, int initialCapacity, boolean overwrite) {
		if (key == null) {
			throw new NullPointerException("Put key can not be NULL");
		}
		if (initialCapacity < 0)
			throw new IllegalArgumentException("Illegal initial capacity: " + initialCapacity);
		if (initialCapacity > MAXIMUM_CAPACITY)
			initialCapacity = MAXIMUM_CAPACITY;

		int capacity = 1;
		while (capacity < initialCapacity)
			capacity <<= 1;

		threshold = (int) (capacity * LOAD_FACTOR);
		table = new FullDataRecordEntry[capacity];

		this.putKeyFields = key.getKeyFields();
		this.equalNULLs = key.isEqualNULLs();
		this.duplicate = duplicate;
		this.overwrite = overwrite;
	}

	/**
	 * Insert the record into hash map. Key is generated according to key fields indexes specified by RecordKey object
	 * Record is scrapped if overwrite flag is turned off.  
	 * 
	 * @param record
	 * 			record to be inserted
	 */
	public void put(DataRecord record) {
		if (record == null) {
			throw new NullPointerException("NULL can not be inserted");
		}
		DataField[] key = getKey(record);
		int hash = hash(Arrays.hashCode(key));
		int i = indexFor(hash, table.length);

		for (FullDataRecordEntry le = table[i]; le != null; le = le.next) {
			if (le.hash == hash && (equals(le.value, key))) {
				if (duplicate) {
					addDuplicateEntry(le, record);
					return;
				} else if (overwrite) {
					le.value = record;
					return;
				} else {
					return;
				}
			}
		}

		addEntry(hash, record, i);
		return;
	}
	
	/**
	 * Retrieves DataRecord which is stored under key specified by given RecordKey and DataRecord
	 * If duplicates flag is turned on, this method returns only the first data record associated with the key
	 * 
	 * IMPORTANT: Use DataRecordLookup object if record key and data record Object do not change
	 * 
	 * @param key
	 * 			object which sets which part of record is key
	 * @param record
	 * 			record from which is generated key
	 * 
	 * @return data record stored under specified key.
	 */

	public DataRecord get(RecordKey key, DataRecord record) {
		if (key == null) {
			throw new NullPointerException("Get key can not be NULL");
		} else if (record == null) {
			throw new NullPointerException("Get record can not be NULL");
		} else if (putKeyFields.length != key.getKeyFields().length) {
			return null;
		}
		int[] getKeyFieldsIndexes = key.getKeyFields();
		DataField[] getKeyFields = new DataField[getKeyFieldsIndexes.length];
		for (int i = 0; i < getKeyFields.length; i++) {
			getKeyFields[i] = record.getField(getKeyFieldsIndexes[i]);
		}
		int hash = DataRecordMap.hash(Arrays.hashCode(getKeyFields));
		for (FullDataRecordEntry e = table[indexFor(hash, table.length)]; e != null; e = e.next) {
			if (e.hash == hash && DataRecordMap.this.equals(e.value, getKeyFields)) {
				return e.value;
			}
		}
		return null;
	}
	
	/**
	 * Retrieves all data records which are stored under key specified by given RecordKey and DataRecord.
	 * Multiple values are stored as linked list so iterator is returned. 
	 * get() method should be called if duplicates flag is turned off
	 * 
	 * @param key
	 * 			object which sets which part of record is key
	 * @param record
	 * 			record from which is generated key
	 * 
	 * @return Data record iterator stored under specified key.
	 */

	public DataRecordIterator getAll(RecordKey key, DataRecord record) {
		if (key == null) {
			throw new NullPointerException("Get key can not be NULL");
		} else if (record == null) {
			throw new NullPointerException("Get record can not be NULL");
		} else if (putKeyFields.length != key.getKeyFields().length) {
			return null;
		}
		int[] getKeyFieldsIndexes = key.getKeyFields();
		DataField[] getKeyFields = new DataField[getKeyFieldsIndexes.length];
		for (int i = 0; i < getKeyFields.length; i++) {
			getKeyFields[i] = record.getField(getKeyFieldsIndexes[i]);
		}
		int hash = DataRecordMap.hash(Arrays.hashCode(getKeyFields));
		for (FullDataRecordEntry e = table[indexFor(hash, table.length)]; e != null; e = e.next) {
			if (e.hash == hash && DataRecordMap.this.equals(e.value, getKeyFields)) {
				return new DataRecordIterator(e);
			}
		}
		return null;
	}
	
	/**
	 * Removes all records associated with key generated with the use of put key.
	 * 
	 * @param record
	 * 			data record from which key will be generated
	 * @return
	 */

	public boolean remove(DataRecord record) {
		return removeForKey(getKey(record));
	}

	
	/**
	 * Removes all records associated with key generated with the use of specified key.
	 * 
	 * @param recordKey
	 * 			record key which specifies which columns of data record will be sued as key
	 * @param dataRecord
	 * 			data record from which key will be generated	
	 * @return
	 */
	public boolean remove(RecordKey recordKey, DataRecord dataRecord) {
		int[] removeKeys = recordKey.getKeyFields();
		if (removeKeys.length != putKeyFields.length) {
			return false;
		}
		DataField[] key = new DataField[putKeyFields.length];
		for (int i = 0; i < key.length; i++) {
			key[i] = dataRecord.getField(removeKeys[i]);
		}
		return removeForKey(key);
	}
	
	
	/**
	 * Creates lookup object which is optimized for repeated call of get() method under
	 * the same RecordKey and DataRecord object.  
	 * 
	 * @param key
	 * @param record
	 * @return
	 */
	public DataRecordLookup createDataRecordLookup(RecordKey key, DataRecord record) {
		return new DataRecordLookup(key, record);
	}

	/**
	 * return iterator for which iterates over all entries which were not retrieved.
	 * To mark entry as retrieved use getAndMark() method
	 * 
	 * @return
	 */
	public Iterator<DataRecord> getOrphanedIterator() {
		return new OrphanedDataRecordIterator();
	}
	
	
	/**
	 * Clears the table
	 * 
	 */
	public void clear() {
		DataRecordEntry[] tab = table;
		for (int i = 0; i < tab.length; i++)
			tab[i] = null;
		size = 0;
		duplicates = 0;
	}
	
	
	/**
	 * returns the number of stored entries in the table
	 * 
	 * @return
	 */
	public int size() {
		if (duplicate) {
			return size + duplicates;
		}
		return size;
	}

	/**
	 * returns the iterator which will iterate over all stored values
	 * 
	 * @return
	 */
	public Iterator<DataRecord> valueIterator() {
		return new ValueIterator<DataRecord>();
	}
	
	public DataRecordIterator getNULLIterator() {
		return new DataRecordIterator(new FullDataRecordEntry(0, NullRecord.NULL_RECORD, null));
	}

	private DataField[] getKey(DataRecord record) {
		DataField[] ret = new DataField[putKeyFields.length];
		for (int i = 0; i < putKeyFields.length; i++) {
			ret[i] = record.getField(putKeyFields[i]);
		}

		return ret;
	}
	
	/**
	 * Applies a supplemental hash function to a given hashCode, which defends against poor quality hash functions. This
	 * is critical because HashMap uses power-of-two length hash tables, that otherwise encounter collisions for
	 * hashCodes that do not differ in lower bits. Note: Null keys always map to hash 0, thus index 0.
	 */
	static int hash(int h) {
		// This function ensures that hashCodes that differ only by
		// constant multiples at each bit position have a bounded
		// number of collisions (approximately 8 at default load factor).
		h ^= (h >>> 20) ^ (h >>> 12);
		return h ^ (h >>> 7) ^ (h >>> 4);
	}

	static int indexFor(int h, int length) {
		return h & (length - 1);
	}
	
	private boolean removeForKey(DataField[] key) {
		int hash = hash(Arrays.hashCode(key));
		int i = indexFor(hash, table.length);
		FullDataRecordEntry prev = table[i];

		for (FullDataRecordEntry le = table[i]; le != null; le = le.next) {
			if (le.hash == hash && (equals(le.value, key))) {
				size--;
				for (DataRecordEntry duplicate = le.duplicate; duplicate != null; duplicate = duplicate.duplicate) {
					duplicates--;
				}
				if (prev == le) {
					table[i] = le.next;
				} else {
					prev.next = le.next;
				}
				return true;
			}
			prev = le;
		}

		return false;
	}

	private void addDuplicateEntry(FullDataRecordEntry entry, DataRecord record) {
		//add the new duplicate to the end of linked list to keep the order
		entry.lastDuplicate.duplicate = new DataRecordEntry(record, null);
		entry.lastDuplicate = entry.lastDuplicate.duplicate;
		duplicates++;
	}

	private boolean equals(DataRecord record1, DataField[] key2) {
		if (equalNULLs) {
			for (int i = 0; i < putKeyFields.length; i++) {
				DataField field1 = record1.getField(putKeyFields[i]);
				if (!field1.equals(key2[i])) {
					if (!(field1.isNull() && key2[i].isNull())) {
						return false;
					}
				}
			}
		} else {
			for (int i = 0; i < putKeyFields.length; i++) {
				DataField field1 = record1.getField(putKeyFields[i]);
				if (!field1.equals(key2[i])) {
					return false;
				}
			}
		}
		return true;
	}

	/**
	 * Adds a new entry with the specified key, value and hash code to the specified bucket. It is the responsibility of
	 * this method to resize the table if appropriate.
	 * 
	 * Subclass overrides this to alter the behavior of put method.
	 */
	private void addEntry(int hash, DataRecord value, int bucketIndex) {
		FullDataRecordEntry e = table[bucketIndex];
		table[bucketIndex] = new FullDataRecordEntry(hash, value, e);
		if (size++ >= threshold)
			resize(2 * table.length);
	}

	/**
	 * Rehashes the contents of this map into a new array with a larger capacity. This method is called automatically
	 * when the number of keys in this map reaches its threshold.
	 * 
	 * If current capacity is MAXIMUM_CAPACITY, this method does not resize the map, but sets threshold to
	 * Integer.MAX_VALUE. This has the effect of preventing future calls.
	 * 
	 * @param newCapacity
	 *            the new capacity, MUST be a power of two; must be greater than current capacity unless current
	 *            capacity is MAXIMUM_CAPACITY (in which case value is irrelevant).
	 */
	private void resize(int newCapacity) {
		DataRecordEntry[] oldTable = table;
		int oldCapacity = oldTable.length;
		if (oldCapacity == MAXIMUM_CAPACITY) {
			threshold = Integer.MAX_VALUE;
			return;
		}
		FullDataRecordEntry[] newTable = new FullDataRecordEntry[newCapacity];
		transfer(newTable);
		table = newTable;
		threshold = (int) (newCapacity * LOAD_FACTOR);
	}

	private void transfer(FullDataRecordEntry[] newTable) {
		FullDataRecordEntry[] src = table;
		int newCapacity = newTable.length;
		for (int j = 0; j < src.length; j++) {
			FullDataRecordEntry e = src[j];
			if (e != null) {
				src[j] = null;
				do {
					FullDataRecordEntry next = e.next;
					int i = indexFor(e.hash, newCapacity);
					e.next = newTable[i];
					newTable[i] = e;
					e = next;
				} while (e != null);
			}
		}
	}

	static class FullDataRecordEntry extends DataRecordEntry {

		FullDataRecordEntry next;
		final int hash;
		boolean retrieved = false;
		DataRecordEntry lastDuplicate;
		
		FullDataRecordEntry(int h, DataRecord v, FullDataRecordEntry n) {
			value = v;
			next = n;
			hash = h;
			lastDuplicate = this;
		}
	}

	static class DataRecordEntry {

		protected DataRecord value;
		DataRecordEntry duplicate;

		protected DataRecordEntry() {

		}

		public DataRecordEntry(DataRecord v, DataRecordEntry n) {
			value = v;
			duplicate = n;
		}
	}

	public class DataRecordLookup {

		protected int[] keyFieldsIndexes;
		private DataField[] keyFields;

		DataRecordLookup(RecordKey key, DataRecord record) {
			this.keyFieldsIndexes = key.getKeyFields();
			setDataRecord(record);
		}

		public DataRecord get() {
			if (keyFields == null)
				throw new IllegalStateException("No key data for performing lookup");
			int hash = DataRecordMap.hash(Arrays.hashCode(keyFields));
			for (FullDataRecordEntry e = table[indexFor(hash, table.length)]; e != null; e = e.next) {
				if (e.hash == hash && DataRecordMap.this.equals(e.value, keyFields)) {
					return e.value;
				}
			}
			return null;
		}

		/**
		 * Marks retrieved element.
		 * 
		 */
		public DataRecord getAndMark() {
			if (keyFields == null)
				throw new IllegalStateException("No key data for performing lookup");
			int hash = DataRecordMap.hash(Arrays.hashCode(keyFields));
			for (FullDataRecordEntry e = table[indexFor(hash, table.length)]; e != null; e = e.next) {
				if (e.hash == hash && DataRecordMap.this.equals(e.value, keyFields)) {
					e.retrieved = true;
					return e.value;
				}
			}
			return null;
		}

		public DataRecordIterator getAll() {
			if (keyFields == null)
				throw new IllegalStateException("No key data for performing lookup");
			int hash = DataRecordMap.hash(Arrays.hashCode(keyFields));
			for (FullDataRecordEntry e = table[indexFor(hash, table.length)]; e != null; e = e.next) {
				if (e.hash == hash && DataRecordMap.this.equals(e.value, keyFields)) {
					return new DataRecordIterator(e);
				}
			}
			return null;
		}

		public DataRecordIterator getAllAndMark() {
			if (keyFields == null)
				throw new IllegalStateException("No key data for performing lookup");
			int hash = DataRecordMap.hash(Arrays.hashCode(keyFields));
			for (FullDataRecordEntry e = table[indexFor(hash, table.length)]; e != null; e = e.next) {
				if (e.hash == hash && DataRecordMap.this.equals(e.value, keyFields)) {
					e.retrieved = true;
					return new DataRecordIterator(e);
				}
			}
			return null;
		}

		public void setDataRecord(DataRecord keyRecord) {
			if (keyRecord == null) {
				this.keyFields = null;
			} else {
				this.keyFields = new DataField[keyFieldsIndexes.length];
				for (int i = 0; i < keyFieldsIndexes.length; i++) {
					keyFields[i] = keyRecord.getField(keyFieldsIndexes[i]);
				}
			}
		}

		
	}

	public class DataRecordIterator {

		private DataRecordEntry origin;
		private DataRecordEntry current;

		public DataRecordIterator(FullDataRecordEntry entry) {
			this.origin = current = entry;
		}

		public boolean hasNext() {
			return current != null;
		}

		public DataRecord next() {
			if (current == null)
				throw new NoSuchElementException();
			DataRecord ret = current.value;
			current = current.duplicate;
			return ret;
		}

		public void reset() {
			this.current = origin;
		}
		
		public int size() {
			DataRecordEntry entry = origin;
			int result = 1;
			while ((entry = entry.duplicate) != null) {
				result++;
			}
			return result;
		}
	}

	private class OrphanedDataRecordIterator implements Iterator<DataRecord> {
		private int bucket = 0;
		private DataRecordEntry current;
		private FullDataRecordEntry currentFirst;

		public OrphanedDataRecordIterator() {
			seekInTable();
		}

		@Override
		public boolean hasNext() {
			return current != null;
		}

		@Override
		public DataRecord next() {
			if (current == null)
				throw new NoSuchElementException();
			DataRecord ret = current.value;
			seek();
			return ret;
		}

		private void seekInTable() {
			while (bucket < table.length) {
				if (table[bucket] != null) {
					currentFirst = table[bucket];
					if (currentFirst.retrieved) {
						while ((currentFirst = currentFirst.next) != null) {
							if (!currentFirst.retrieved) {
								current = currentFirst;
								return;
							}
						}
					} else {
						current = currentFirst;
						return;
					}
				}
				bucket++;
			}
			current = null;
		}

		private void seekInBucket() {
			while ((currentFirst = currentFirst.next) != null) {
				if (!currentFirst.retrieved) {
					current = currentFirst;
					return;
				}
			}
			bucket++;
			seekInTable();
		}

		private void seek() {
			if ((current = current.duplicate) != null) {
				return;
			}
			seekInBucket();
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
	}

	private class ValueIterator<V> implements Iterator<DataRecord> {

		private int bucket = 0;
		private DataRecordEntry current;
		private FullDataRecordEntry currentFirst;
		private DataRecordEntry next;

		public ValueIterator() {
			seekInTable();
			current = next;
		}

		@Override
		public boolean hasNext() {
			return next != null;
		}

		@Override
		public DataRecord next() {
			if (next != null) {
				current = next;
				seek();
				return current.value;
			} else {
				throw new NoSuchElementException();
			}
		}

		@Override
		public void remove() {
			DataRecordMap.this.remove(current.value);
		}

		private void seekInTable() {
			while (bucket < table.length) {
				if (table[bucket] != null) {
					next = currentFirst = table[bucket];
					return;
				}
				bucket++;
			}
			next = null;
		}

		private void seekInBucket() {
			while ((currentFirst = currentFirst.next) != null) {
				next = currentFirst;
				return;
			}
			bucket++;
			seekInTable();
		}

		private void seek() {
			if ((next = next.duplicate) != null) {
				return;
			}
			seekInBucket();
		}
	}
}
