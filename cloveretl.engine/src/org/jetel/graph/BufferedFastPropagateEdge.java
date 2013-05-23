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
package org.jetel.graph;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.jetel.data.CircularBufferQueue;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.PersistentBufferQueue;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.util.bytes.CloverBuffer;

/**
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 15.5.2013
 */
public class BufferedFastPropagateEdge extends EdgeBase {

	private static final Logger logger = Logger.getLogger(BufferedFastPropagateEdge.class);
	
	private CircularBufferQueue[] circularBufferQueues;
	
	private PersistentBufferQueue persistentBufferQueue;

	private CircularBufferQueue writingQueue;

	private CircularBufferQueue readingQueue;
	
	private volatile boolean eofReached;
	
    private long byteCounter;
    
    private long inputRecordCounter;

    private long outputRecordCounter;

	/**
	 * @param proxy
	 */
	public BufferedFastPropagateEdge(Edge proxy) {
		super(proxy);
	}

	@Override
	public void init() throws IOException, InterruptedException {
		circularBufferQueues = new CircularBufferQueue[2];
		circularBufferQueues[0] = new CircularBufferQueue(Defaults.Record.RECORD_INITIAL_SIZE);
		circularBufferQueues[1] = new CircularBufferQueue(Defaults.Record.RECORD_INITIAL_SIZE);
		
		persistentBufferQueue = new PersistentBufferQueue();
	}

	@Override
	public void preExecute() {
		super.preExecute();
		
		circularBufferQueues[0].clear();
		circularBufferQueues[1].clear();
		readingQueue = writingQueue = circularBufferQueues[0];
		eofReached = false;
		byteCounter = 0;
		inputRecordCounter = 0;
		outputRecordCounter = 0;
	}
	
	@Override
	public DataRecord readRecord(DataRecord record) throws IOException, InterruptedException {
		DataRecord result;
		if ((result = readingQueue.poll(record)) == null) {
			secureReadingQueue();
			while ((result = readingQueue.blockingPoll(record)) == null) {
				secureReadingQueue();
			}
		}
		if (result == CircularBufferQueue.EOF_DATA_RECORD) {
			eofReached = true;
			return null;
		}
		inputRecordCounter++;
		
		return record;
	}

	private synchronized void secureReadingQueue() {
		if (!readingQueue.hasData()) {
//																							logger.info("readingQueue does not have data");
			if (readingQueue != writingQueue) {
				CloverBuffer internalBuffer = readingQueue.getInternalBuffer();
				if (persistentBufferQueue.poll(internalBuffer) == null) {
//																							logger.info("readingQueue back to writingQueue");
					readingQueue = writingQueue;
				} else {
					readingQueue.flip();
//																							logger.info("readingQueue loaded from disk");
				}
			} else {
//																							logger.info("readingQueue equals writingQueue");
			}
		}
	}
	
	@Override
	public long getOutputRecordCounter() {
		return outputRecordCounter;
	}

	@Override
	public long getInputRecordCounter() {
		return inputRecordCounter;
	}

	@Override
	public long getOutputByteCounter() {
		return byteCounter;
	}

	@Override
	public long getInputByteCounter() {
		return byteCounter;
	}

	@Override
	public int getBufferedRecords() {
		return (int) (outputRecordCounter - inputRecordCounter);
	}

	@Override
	public int getUsedMemory() {
		return circularBufferQueues[0].getCapacity() + circularBufferQueues[1].getCapacity();
	}

	@Override
	public boolean readRecordDirect(CloverBuffer record) throws IOException, InterruptedException {
		CloverBuffer result;
		if ((result = readingQueue.poll(record)) == null) {
			secureReadingQueue();
			
			while ((result = readingQueue.blockingPoll(record)) == null) {
				secureReadingQueue();
			}

		}
		if (result == CircularBufferQueue.EOF_CLOVER_BUFFER) {
			eofReached = true;
			return false;
		}
		inputRecordCounter++;
		return true;
	}

	@Override
	public void writeRecord(DataRecord record) throws IOException, InterruptedException {
		int recordSize = record.getSizeSerialized();
		
		if (!writingQueue.offer(record, recordSize)) {
			secureWritingQueue();
			if (!writingQueue.offer(record, recordSize)) {
				throw new JetelRuntimeException("internal error - unexpected internal state");
			}
		}
		byteCounter += recordSize;
		outputRecordCounter++;
	}

	@Override
	public void writeRecordDirect(CloverBuffer record) throws IOException, InterruptedException {
		int recordSize = record.remaining();
		
		if (!writingQueue.offer(record)) {
			secureWritingQueue();
			if (!writingQueue.offer(record)) {
				throw new JetelRuntimeException("internal error - unexpected internal state");
			}
		}
		byteCounter += recordSize;
		outputRecordCounter++;
	}

	private synchronized void secureWritingQueue() {
//		logger.info("no space where to write");
		if (writingQueue == readingQueue) {
			writingQueue.setBlocking(false);
			writingQueue = theOtherQueue(writingQueue);
		} else {
			persistentBufferQueue.offer(writingQueue.getReadOnlyInternalBuffer());
		}
		writingQueue.clear();
	}
	
	@Override
	public void eof() throws IOException, InterruptedException {
		if (!writingQueue.eof()) {
			secureWritingQueue();
			if (!writingQueue.eof()) {
				throw new JetelRuntimeException("internal error - unexpected internal state");
			}
		}
	}

	@Override
	public boolean isEOF() {
		return eofReached;
	}

	@Override
	public void free() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public synchronized boolean hasData() {
		return !eofReached && (readingQueue.hasData() || readingQueue != writingQueue);
	}

	private CircularBufferQueue theOtherQueue(CircularBufferQueue queue) {
		if (queue == circularBufferQueues[0]) {
			return circularBufferQueues[1];
		} else {
			return circularBufferQueues[0];
		}
	}
	
}
