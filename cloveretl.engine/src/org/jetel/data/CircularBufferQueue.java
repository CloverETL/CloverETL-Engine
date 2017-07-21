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

import org.jetel.util.bytes.ByteBufferUtils;
import org.jetel.util.bytes.CloverBuffer;

/**
 * CircularBufferQueue is in-memory queue for data records.
 * Only single-thread data records producer and single-thread data record consumer is supported.
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 17.5.2013
 */
public class CircularBufferQueue {
	
	//private static final Logger logger = Logger.getLogger(CircularBufferQueue.class);
	
	/** This instance of CloverBuffer is returned by {@link #poll(CloverBuffer)} or {@link #blockingPoll(CloverBuffer)}
	 * if EOF annotation is reached. */
	public static final CloverBuffer EOF_CLOVER_BUFFER = CloverBuffer.allocate(0);

	/** This instance of DataRecord is returned by {@link #poll(DataRecord)} or {@link #blockingPoll(DataRecord)}
	 * if EOF annotation is reached. */
	public static final DataRecord EOF_DATA_RECORD = NullRecord.NULL_RECORD;
	
	/** Internal data cache for written data records. This instance is used by writing thread. */
	private CloverBuffer writingDataBuffer;
	
	/** This is just a view to {@link #writingDataBuffer} for reading purpose. */
	private CloverBuffer readingDataBuffer;
	
	/**
	 * Synchronised position of writing thread in {@link #writingDataBuffer}.
	 * Real position of {@link #writingDataBuffer} can differ in short term only.
	 */
	private int writingPosition;

	/**
	 * Synchronised position of reading thread in {@link #readingDataBuffer}.
	 * Real position of {@link #readingDataBuffer} can differ in short term only.
	 */
	private int readingPosition;
	
	/**
	 * This limit is used by reading thread to see where is the last real data in buffer.
	 */
	private int readingLimit;
	
	/**
	 * Capacity of internal buffer, can be increased if incoming record does not fit even into empty buffer. 
	 */
	private int capacity;
	
	/**
	 * Should be blocking operation really blocking.
	 */
	private boolean isBlocking;
	
	public CircularBufferQueue(int initialCapacity, int maximumCapacity, boolean direct) {
		writingDataBuffer = CloverBuffer.allocate(initialCapacity, maximumCapacity, direct);
		writingDataBuffer.clear();
		readingDataBuffer = writingDataBuffer.asReadOnlyBuffer(); //create just a view to single data buffer
		readingDataBuffer.clear();
		writingPosition = 0;
		readingPosition = 0;
		readingLimit = initialCapacity;
		this.capacity = initialCapacity;
		isBlocking = true;
	}

	/**
	 * Tries to write given data record into this queue.
	 * @param dataRecord written data record
	 * @return true if data record fits to the queue, false otherwise
	 */
	public boolean offer(DataRecord dataRecord) {
		return offer(dataRecord, dataRecord.getSizeSerialized());
	}

	/**
	 * Tries to write given data record into this queue.
	 * NOTE: this method is useful in case the size of serialised form of data record is already calculated.
	 * @param dataRecord written data record
	 * @param recordSize size of serialised data record
	 * @return true if data record fits to the queue, false otherwise
	 */
	public boolean offer(DataRecord dataRecord, int recordSize) {
		//first ensure empty space in internal buffer
    	if (secureWriting(recordSize + ByteBufferUtils.SIZEOF_INT)) {
    		//encode data record into internal buffer
    		ByteBufferUtils.encodeLength(writingDataBuffer, recordSize);
    		dataRecord.serialize(writingDataBuffer);
    		//advance writing position
    		advanceWritingPosition();
    		return true;
    	} else {
    		return false;
    	}
	}
	
	/**
	 * Tries to write given clover buffer into this queue.
	 * @param cloverBuffer written data buffer
	 * @return true if buffer fits to the queue, false otherwise
	 */
    public boolean offer(CloverBuffer cloverBuffer) {
    	int dataSize = cloverBuffer.remaining();
    	
		//first ensure empty space in internal buffer
    	if (secureWriting(dataSize + ByteBufferUtils.SIZEOF_INT)) {
    		//encode given buffer into internal buffer
    		ByteBufferUtils.encodeLength(writingDataBuffer, dataSize);
    		writingDataBuffer.put(cloverBuffer);
    		//advance writing position
    		advanceWritingPosition();
    		return true;
    	} else {
    		return false;
    	}
    }

    /**
     * Ensures empty space in writing data buffer.
     * @param expectedRemaining requested size of empty space
     * @return true if requested space is empty, false otherwise
     */
    private synchronized boolean secureWriting(int expectedRemaining) {
    	if (readingPosition <= writingPosition) {
    		if (capacity - writingPosition >= expectedRemaining) {
    			return true;
    		} else if (readingPosition > expectedRemaining) {
    			readingLimit = writingPosition;
    			writingDataBuffer.position(0);
    			return true;
    		} else if (writingPosition == readingPosition) {
    			//expand internal buffer, since queue is empty, but the incoming record does not fit into internal buffer
    			writingDataBuffer.expand(expectedRemaining); // readingDataBuffer is automatically updated as well
    			clear();
    			return true;
    		} else {
    			return false;
    		}
    	} else {
    		if (readingPosition - writingPosition > expectedRemaining) {
    			return true;
    		} else {
    			return false;
    		}
    	}
    }

    private synchronized void advanceWritingPosition() {
    	if (readingPosition == writingPosition) {
    		notify(); //notify blockingPoll()
    	}
    	writingPosition = writingDataBuffer.position();
    }

    /**
     * Tries to read a data record from this queue.
     * @param record read data record
     * @return read data record or {@link #EOF_DATA_RECORD} or null if no data record is available in this queue
     */
    public DataRecord poll(DataRecord record) {
    	//first ensure reading
    	if (secureReading()) {
    		//decode a record from internal buffer
    		int length = ByteBufferUtils.decodeLength(readingDataBuffer);
    		if (length != Integer.MAX_VALUE) {
    			//seems to be regular data record
	    		record.deserialize(readingDataBuffer);
	    		advanceReadingPosition();
	    		return record;
    		} else {
    			//seems to be EOF annotation
    			readingDataBuffer.position(readingPosition); //step back for possible next call
    			return EOF_DATA_RECORD;
    		}
    	} else {
    		return null;
    	}
    }
    
    /**
     * Tries to read a CloverBuffer from this queue.
     * @param buffer read CloverBuffer
     * @return read CloverBuffer or {@link #EOF_CLOVER_BUFFER} or null if no CloverBuffer is available in this queue
     */
    public CloverBuffer poll(CloverBuffer buffer) {
    	//first ensure reading
    	if (secureReading()) {
    		//decode a buffer from internal buffer
    		int length = ByteBufferUtils.decodeLength(readingDataBuffer);
    		if (length != Integer.MAX_VALUE) {
    			//seems to be regular CloverBuffer content
	    		readingDataBuffer.limit(readingDataBuffer.position() + length);
	    		buffer.clear();
	    		buffer.put(readingDataBuffer);
	    		readingDataBuffer.limit(capacity);
	    		advanceReadingPosition();
	    		buffer.flip();
	    		return buffer;
    		} else {
    			//seems to be EOF annotation
    			readingDataBuffer.position(readingPosition); //step back for possible next call
    			return EOF_CLOVER_BUFFER;
    		}
    	} else {
    		return null;
    	}
    }

    /**
     * Reads a data record from this queue.
     * @param record read data record
     * @return read data record or {@link #EOF_DATA_RECORD}
     */
    public DataRecord blockingPoll(DataRecord record) throws InterruptedException {
    	DataRecord result = poll(record);
    	if (result == null) {
    		synchronized (this) {
    			while (isBlocking && (result = poll(record)) == null) {
    				wait();
    			}
    		}
    	}
    	return result;
    }

    /**
     * Reads a CloverBuffer from this queue.
     * @param buffer read CloverBuffer
     * @return read CloverBuffer or {@link #EOF_CLOVER_BUFFER}
     */
    public CloverBuffer blockingPoll(CloverBuffer buffer) throws InterruptedException {
    	CloverBuffer result = poll(buffer);
    	if (result == null) {
    		synchronized (this) {
    			while (isBlocking && (result = poll(buffer)) == null) {
    				wait();
    			}
    		}
    	}
    	return result;
    }

    private synchronized boolean secureReading() {
    	if (readingPosition == writingPosition) {
    		//no data available
    		return false;
    	} else {
    		//some data available
    		if (readingPosition == readingLimit) {
    			//end of buffer reached, let's read data from buffer start
    			readingDataBuffer.clear();
    			readingLimit = capacity;
    		}
    		return true;
    	}
    }
    
    private synchronized void advanceReadingPosition() {
    	readingPosition = readingDataBuffer.position();
    }

	/**
	 * @return read only view into internal data buffer 
	 */
	public synchronized CloverBuffer getReadOnlyInternalBuffer() {
		return writingDataBuffer.asReadOnlyBuffer().flip();
	}
	
	/**
	 * This method publics internal data buffer. While the time is manipulated with
	 * the writingDataBuffer, no other action should performed on this CircularBufferQueue.
	 * Afterwards the internal buffer is updated by caller, clear() of flip() method has to be invoked.
	 * The buffer could be even expanded, so all internal states has to be reseted, for example
	 * capacity variable has to be updated.
	 * @return 
	 */
	public synchronized CloverBuffer getInternalBuffer() {
		return writingDataBuffer;
	}
	
	/**
	 * Resets all internal states of this queue. Queue is considered empty.
	 */
	public synchronized void clear() {
		writingDataBuffer.clear();
		readingDataBuffer.clear();
		writingPosition = 0;
		readingPosition = 0;
		isBlocking = true;
		//capacity is updated, since the writingBuffer could be expanded meanwhile, see getInternalBuffer()
		capacity = writingDataBuffer.capacity();
		readingLimit = capacity;
	}

	/**
	 * This method should be invoked after internal buffer has been externally
	 * populated via getInternalBuffer(). Data written into writingDataBuffer
	 * are ready to read afterwards.
	 */
	public synchronized void flip() {
		//is expected that writingDataBuffer has position=0, limit=<last_real_data_byte>
		//this data are ready to be read
		int newReadingLimit = writingDataBuffer.limit();
		clear();
		readingLimit = newReadingLimit;
		writingDataBuffer.position(newReadingLimit);
		writingPosition = newReadingLimit;
	}
	
	/**
	 * This method turns on/off blocking behaviour of methods {@link #blockingPoll(CloverBuffer)}
	 * and {@link #blockingPoll(DataRecord)}.
	 * Already blocked reading thread is possibly woken up.
	 * @param isBlocking
	 */
	public synchronized void setBlocking(boolean isBlocking) {
		this.isBlocking = isBlocking;
		notify();
	}
	
	/**
	 * @return size of internal data buffer
	 */
	public synchronized int getCapacity() {
		return capacity;
	}

	/**
	 * @return true if some data are ready to read
	 */
	public synchronized boolean hasData() {
		return writingPosition != readingPosition;
	}
	
	/**
	 * Writes special annotation into internal data buffer, which is
	 * identified as end of stream.
	 * 
	 * Read method {@link #poll(CloverBuffer)} returns
	 * {@link #EOF_CLOVER_BUFFER} if this annotation is reached while reading.
	 * 
	 * Read method {@link #poll(DataRecord)} returns
	 * {@link #EOF_DATA_RECORD} if this annotation is reached while reading.
	 * 
	 * @return true if eof annotation has been successfully written, false if no space left in internal data buffer
	 */
	public boolean eof() {
    	if (secureWriting(ByteBufferUtils.SIZEOF_INT)) {
    		ByteBufferUtils.encodeLength(writingDataBuffer, Integer.MAX_VALUE);
    		advanceWritingPosition();
    		return true;
    	} else {
    		return false;
    	}
	}
	
}
