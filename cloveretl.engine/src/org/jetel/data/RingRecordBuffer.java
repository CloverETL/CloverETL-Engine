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

import java.io.IOException;

import org.jetel.exception.ComponentNotReadyException;
import org.jetel.util.bytes.CloverBuffer;

/**
 * This record buffer implement FIFO record storage with limited number of records.
 * If buffer is full, pushing next data record is going to result in 
 * the oldest data record to be removed from the buffer.
 * None of the main record manipulating methods (push/pop) is blocking.
 * Instances of RingRecordBuffer are not safe for use by multiple threads!
 * Usage scenario:
 * 		contructor(record_buffer_size)
 * 		init()
 * 		push/pop(Record) - multiple times
 * 		reset
 * 		push/pop(Record) - multiple times
 * 		free()
 * 
 * @author Martin Zatopek (martin.zatopek@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created 5.3.2009
 */
public class RingRecordBuffer {

	private RecordBuffer recordBuffer;
	
	private final long recordBufferSize;
	
	private int numBufferedRecords;
	
	/**
	 * @param recordBufferSize maximum number of records stored in the buffer at once
	 */
	public RingRecordBuffer(long recordBufferSize) {
		this(recordBufferSize, Defaults.Record.RECORDS_BUFFER_SIZE);
	}
	
	/**
	 * @param recordBufferSize maximum number of records stored in the buffer at once
	 * @param internalBufferSize size of internal byte buffer
	 */
	public RingRecordBuffer(long recordBufferSize, int internalBufferSize) {
		this.recordBufferSize = recordBufferSize;
		this.recordBuffer = new RecordBuffer(internalBufferSize);
	}

	/**
	 * @return <code>true</code> if the buffer is full, i.e. pushing another data record is going to result in
	 * the oldest data record to be removed from the buffer, <code>false</code> otherwise
	 */
	public boolean isFull() {
		return (numBufferedRecords >= recordBufferSize);
	}

	/**
	 * Basic record buffer initialization. It is necessary to invoke this method right once before first usage.
	 * @throws ComponentNotReadyException
	 */
	public void init() throws ComponentNotReadyException {
		if (recordBufferSize < 1) {
			throw new ComponentNotReadyException("RingRecordBuffer cannot be initialized with non-positive record buffer size: " + recordBufferSize);
		}
		recordBuffer.init();
		numBufferedRecords = 0;
	}
	
	/**
	 * Push a record into the buffer. It is non-blocking operation. If the record buffer is already
	 * full, the oldest record is picked up from the buffer.
	 * @param record
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public void pushRecord(DataRecord record) throws IOException, InterruptedException {
		recordBuffer.writeRecord(record);
		numBufferedRecords++;

		ensureMaxBufferCapacity();
	}

	/**
	 * Push a record into the buffer. It is non-blocking operation. If the record buffer is already
	 * full, the oldest record is picked up from the buffer.
	 * @param record
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public void pushRecord(CloverBuffer record) throws IOException, InterruptedException {
		recordBuffer.writeRecord(record);
		numBufferedRecords++;

		ensureMaxBufferCapacity();
	}

	private void ensureMaxBufferCapacity() throws IOException, InterruptedException {
		if (numBufferedRecords > recordBufferSize) {
			recordBuffer.ensureSuccessfulReading();

			numBufferedRecords--;
			recordBuffer.readRecord();
		}
	}
	
	/**
	 * Pick up the oldest record from the buffer or return null if the buffer is empty.
	 * @param record
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public DataRecord popRecord(DataRecord record) throws IOException, InterruptedException {
		if (numBufferedRecords > 0) {
			recordBuffer.ensureSuccessfulReading();

			numBufferedRecords--;
			return recordBuffer.readRecord(record); 
		} else {
			return null;
		}
	}

	/**
	 * Pick up the oldest record from the buffer or return null if the buffer is empty.
	 * @param record
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public CloverBuffer popRecord(CloverBuffer record) throws IOException, InterruptedException {
		if (numBufferedRecords > 0) {
			recordBuffer.ensureSuccessfulReading();

			numBufferedRecords--;
			recordBuffer.readRecord(record);
			return record;
		} else {
			return null;
		}
	}

	/**
	 * Bring back the record buffer to initiate state.
	 */
	public void reset() {
		recordBuffer.reset();
		numBufferedRecords = 0;
	}
	
	/**
	 * Release all internal resources.
	 * @throws IOException
	 */
	public void free() throws IOException {
		recordBuffer.close();
	}
	
	private class RecordBuffer extends DynamicRecordBuffer {

		private RecordBuffer(int dataBufferSize) {
			super(dataBufferSize);
		}
		
		private void ensureSuccessfulReading() {
			if (!hasData() && numBufferedRecords > 0) {
				//only data records are in writeDataBuffer, lets prepare it for reading
				//potentially very awkward hack - class is not definitely thread safe
		    	writeDataBuffer.flip();
		        readDataBuffer.clear();
		        readDataBuffer.put(writeDataBuffer);
		        readDataBuffer.flip();
		        writeDataBuffer.clear();
			}
		}
		
	}
	
}
