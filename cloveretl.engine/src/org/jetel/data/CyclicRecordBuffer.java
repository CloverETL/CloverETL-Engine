/*
 * jETeL/Clover.ETL - Java based ETL application framework.
 * Copyright (C) 2002-2009  David Pavlis <david.pavlis@javlin.eu>
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
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
package org.jetel.data;

import java.nio.ByteBuffer;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.jetel.metadata.DataRecordMetadata;

/**
 * Represents a cyclic data record buffer which can store up to a given number of data records. When the buffer is
 * full, the first (oldest) data record is removed before a new data record is added. This way the buffer stores
 * the latest (newest) data records added to the buffer. Data records are internally stored in a fixed-length array.
 * <p>
 * This implementation is NOT thread-safe!
 *
 * @author Martin Janik, Javlin a.s. &lt;martin.janik@javlin.eu&gt;
 *
 * @version 5th March 2009
 * @since 5th March 2009
 */
public class CyclicRecordBuffer implements Iterable<DataRecord> {

	/**
	 * An iterator used to iterate over data records currently stored in the buffer from the first (oldest) one
	 * to the last (newest) one. The iterator throws an exception if it detects a concurrent modification
	 * of the backing buffer.
	 *
	 * @author Martin Janik, Javlin a.s. &lt;martin.janik@javlin.eu&gt;
	 *
	 * @version 27th February 2009
	 * @since 27th February 2009
	 */
	private class DataRecordIterator implements Iterator<DataRecord> {

		/** an index from which the next data record will be read */
		private int readIndex = (writeIndex + dataRecords.length - numberOfRecords) % dataRecords.length;
		/** the number of actually read data records */
		private int readRecordCounter = 0;

		/** the expected number of modifications used to detect the concurrent modification */
		private final int expectedNumberOfModifications = numberOfModifications;

		public boolean hasNext() {
			return (readRecordCounter < numberOfRecords);
		}

		public DataRecord next() {
			if (readRecordCounter >= numberOfRecords) {
				throw new NoSuchElementException();
			}

			if (numberOfModifications != expectedNumberOfModifications) {
				throw new ConcurrentModificationException();
			}

			DataRecord dataRecord = dataRecords[readIndex];

			readIndex = (readIndex + 1) % dataRecords.length;
			readRecordCounter++;

			return dataRecord;
		}

		public void remove() {
			throw new UnsupportedOperationException("Method not supported!");
		}

	}

	/** an array of data records that form the buffer */
	private final DataRecord[] dataRecords;

	/** an index where a new data record will be stored */
	private int writeIndex = 0;
	/** the number of data records actually stored in the buffer */
	private int numberOfRecords = 0;

	/** the number of modifications since the buffer was created */
	private int numberOfModifications = 0;

	/**
	 * Creates a <code>CyclicRecordBuffer</code> of a given size for data records of a given meta data.
	 *
	 * @param capacity the maximum number of data records that can be stored in the buffer
	 * @param metadata the meta data of data records that will be stored in the buffer
	 *
	 * @throws IllegalArgumentException if the capacity is <= 0
	 * @throws NullPointerException if the metadata is <code>null</code>
	 */
	public CyclicRecordBuffer(int capacity, DataRecordMetadata metadata) {
		if (capacity <= 0) {
			throw new IllegalArgumentException("capacity <= 0");
		}

		if (metadata == null) {
			throw new NullPointerException("metadata");
		}

		this.dataRecords = new DataRecord[capacity];

		for (int i = 0; i < dataRecords.length; i++) {
			dataRecords[i] = new DataRecord(metadata);
			dataRecords[i].init();
		}
	}

	/**
	 * Adds a data record to the buffer by copying its contents.
	 *
	 * @param dataRecord the data record to be added
	 *
	 * @throws NullPointerException if the dataRecord is <code>null</code>
	 */
	public void add(DataRecord dataRecord) {
		if (dataRecord == null) {
			throw new NullPointerException("dataRecord");
		}

		dataRecords[writeIndex].copyFrom(dataRecord);
		updateInternalState();
	}

	/**
	 * Adds a data record to the buffer by deserializing its contents from a byte buffer.
	 *
	 * @param byteBuffer the byte buffer to be used to deserialize the data record
	 *
	 * @throws NullPointerException if the byteBuffer is <code>null</code>
	 */
	public void add(ByteBuffer byteBuffer) {
		if (byteBuffer == null) {
			throw new NullPointerException("byteBuffer");
		}

		dataRecords[writeIndex].deserialize(byteBuffer);
		updateInternalState();
	}

	private void updateInternalState() {
		writeIndex = (writeIndex + 1) % dataRecords.length;

		if (numberOfRecords < dataRecords.length) {
			numberOfRecords++;
		}

		numberOfModifications++;
	}

	/**
	 * Clears the contents of the buffer.
	 */
	public void clear() {
		writeIndex = 0;
		numberOfRecords = 0;

		numberOfModifications++;
	}

	/**
	 * Returns an iterator over data records currently stored in the buffer from the first (oldest) one to the last
	 * (newest) one. The iterator throws an exception if it detects a concurrent modification of the backing buffer.
	 *
	 * @return an iterator over the data records currently stored in the buffer
	 */
	public Iterator<DataRecord> iterator() {
		return new DataRecordIterator();
	}

}
