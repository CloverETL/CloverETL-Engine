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

import org.apache.log4j.Logger;
import org.jetel.util.bytes.ByteBufferUtils;
import org.jetel.util.bytes.CloverBuffer;

/**
 * Single-thread producer and single-thread consumer.
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 17.5.2013
 */
public class CircularBufferQueue {
	
	private static final Logger logger = Logger.getLogger(CircularBufferQueue.class);
	
	public static final CloverBuffer EOF_CLOVER_BUFFER = CloverBuffer.allocate(0);
	public static final DataRecord EOF_DATA_RECORD = NullRecord.NULL_RECORD;
	
	private CloverBuffer writingDataBuffer;
	private CloverBuffer readingDataBuffer;
	private int writingPosition;
	private int readingPosition;
	//private int limit;
	private int capacity;
	private boolean isBlocking;
	
	public CircularBufferQueue(int initialCapacity) {
		this(initialCapacity, false);
	}

	public CircularBufferQueue(int initialCapacity, boolean direct) {
		writingDataBuffer = CloverBuffer.allocate(initialCapacity, initialCapacity, direct);
		writingDataBuffer.clear();
		readingDataBuffer = writingDataBuffer.asReadOnlyBuffer();
		readingDataBuffer.clear();
		writingPosition = 0;
		readingPosition = 0;
		//limit = initialCapacity;
		this.capacity = initialCapacity;
		isBlocking = true;
	}

	public boolean offer(DataRecord dataRecord) {
		return offer(dataRecord, dataRecord.getSizeSerialized());
	}

	public boolean offer(DataRecord dataRecord, int recordSize) {
    	
    	if (secureWriting(recordSize + ByteBufferUtils.SIZEOF_INT)) {
    		ByteBufferUtils.encodeLength(writingDataBuffer, recordSize);
    		dataRecord.serialize(writingDataBuffer);
    		advanceWritingPosition();
    		return true;
    	} else {
    		return false;
    	}
	}
	
    public boolean offer(CloverBuffer cloverBuffer) {
    	int dataSize = cloverBuffer.remaining();
    	
    	if (secureWriting(dataSize + ByteBufferUtils.SIZEOF_INT)) {
    		ByteBufferUtils.encodeLength(writingDataBuffer, dataSize);
    		writingDataBuffer.put(cloverBuffer);
    		advanceWritingPosition();
    		return true;
    	} else {
    		return false;
    	}
    }

    private synchronized boolean secureWriting(int expectedRemaining) {
    	if (readingPosition <= writingPosition) {
    		if (capacity - writingPosition >= expectedRemaining) {
    			return true;
    		} else if (readingPosition > expectedRemaining) {
    			readingDataBuffer.limit(writingPosition);
    			writingDataBuffer.position(0);
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
    	writingPosition = writingDataBuffer.position();
		//logger.info("notifying blockingPoll() " + this);
    	notify(); //notify blockingPoll()
    }

    public DataRecord poll(DataRecord record) {
    	if (secureReading()) {
    		int length = ByteBufferUtils.decodeLength(readingDataBuffer);
    		if (length != Integer.MAX_VALUE) {
	    		record.deserialize(readingDataBuffer);
	    		advanceReadingPosition();
	    		return record;
    		} else {
    			readingDataBuffer.position(readingPosition); //step back for possible next call
    			return EOF_DATA_RECORD;
    		}
    	} else {
    		return null;
    	}
    }
    
    public CloverBuffer poll(CloverBuffer buffer) {
    	if (secureReading()) {
    		int length = ByteBufferUtils.decodeLength(readingDataBuffer);
    		if (length != Integer.MAX_VALUE) {
    			int formerLimit = readingDataBuffer.limit();
	    		readingDataBuffer.limit(readingDataBuffer.position() + length);
	    		buffer.put(readingDataBuffer);
	    		readingDataBuffer.limit(formerLimit);
	    		advanceReadingPosition();
	    		buffer.flip();
	    		return buffer;
    		} else {
    			readingDataBuffer.position(readingPosition); //step back for possible next call
    			return EOF_CLOVER_BUFFER;
    		}
    	} else {
    		return null;
    	}
    }

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

    public CloverBuffer blockingPoll(CloverBuffer buffer) throws InterruptedException {
    	CloverBuffer result = poll(buffer);
    	if (result == null) {
    		synchronized (this) {
    			while (isBlocking && (result = poll(buffer)) == null) {
    				//logger.info("going to sleep in blockingPoll() " + this);
    				wait();
    				//logger.info("waking up from blockingPoll() " + this);
    			}
				//logger.info("woke up from blockingPoll() " + this);
    		}
    	}
    	return result;
    }

    private synchronized boolean secureReading() {
    	if (readingPosition == writingPosition) {
    		return false;
    	} else {
    		if (readingPosition == readingDataBuffer.limit()) {
    			readingDataBuffer.clear();
    		}
    		return true;
    	}
    }
    
    private synchronized void advanceReadingPosition() {
    	readingPosition = readingDataBuffer.position();
    }

	/**
	 * @return
	 */
	public synchronized CloverBuffer getReadOnlyInternalBuffer() {
		return writingDataBuffer.asReadOnlyBuffer().flip();
	}
	
	public synchronized CloverBuffer getInternalBuffer() {
		return writingDataBuffer;
	}
	
	public synchronized void clear() {
		writingDataBuffer.clear();
		readingDataBuffer.clear();
		writingPosition = 0;
		readingPosition = 0;
		isBlocking = true;
	}

	public synchronized void flip() {
		//writingDataBuffer has position=0, limit=<last_real_data_byte>
		int limit = writingDataBuffer.limit();
		readingDataBuffer.clear();
		readingDataBuffer.limit(limit);
		readingPosition = 0;
		writingDataBuffer.clear();
		writingDataBuffer.position(limit);
		writingPosition = limit;
		isBlocking = true;
	}
	
	public synchronized void setBlocking(boolean isBlocking) {
		this.isBlocking = isBlocking;
		notify();
	}
	
	/**
	 * @return
	 */
	public synchronized int getCapacity() {
		return capacity;
	}

	/**
	 * @return
	 */
	public synchronized boolean hasData() {
		return writingPosition != readingPosition;
	}
	
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
