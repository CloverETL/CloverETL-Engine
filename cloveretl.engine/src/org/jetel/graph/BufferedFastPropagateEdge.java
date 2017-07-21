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

import org.jetel.data.CircularBufferQueue;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.PersistentBufferQueue;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.util.bytes.CloverBuffer;

/**
 * This edge implementation ensures that written data records are immediately available for reading thread (fast-propagate)
 * and ensures that writing operation never blocks the writing thread - internal unlimited cache is used.
 * 
 * Two in-memory records queue (CircularBufferQueue) are used. If reading thread is faster than writing thread, only
 * one queue is used for reading and writing at once. But if writing thread is getting faster than reading thread,
 * the shared records queue gets full. The second records queue is used for writing from now. Now if even the second queue is
 * full and the reading thread is still processing data records from the first queue, content of the second queue is saved
 * to unlimited persistent buffer queue (PersistentBufferQueue). From now both threads use different in-memory queues. Reading
 * thread reads data from a reading queue and writing thread writes data to a writing queue. If reading queue is empty, queue is
 * refilled from persistent buffer queue (from temporary file). If no data are available in persistent buffer queue, reading thread
 * starts to share in-memory records queue with writing thread.
 *   
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 15.5.2013
 */
public class BufferedFastPropagateEdge extends EdgeBase {

	//private static final Logger logger = Logger.getLogger(BufferedFastPropagateEdge.class);
	
	/** Two circular buffer queues. Either one is share by both reader and writer thread or
	 * reading thread uses one queue and writing thread uses the other one. **/
	private CircularBufferQueue[] circularBufferQueues;
	
	/** If writing thread is faster then reading thread, data has to be persisted in a temporary file */
	private PersistentBufferQueue persistentBufferQueue;

	/** Queue used by writing thread. One of queues from {@link #circularBufferQueues} array. */
	private CircularBufferQueue writingQueue;

	/** Queue used by reading thread. One of queues from {@link #circularBufferQueues} array. */
	private CircularBufferQueue readingQueue;
	
	/** EOF annotation already read from this edge by reading thread. */
	private volatile boolean eofReached;
	
    /**
     * Monitor for {@link #waitForEOF()}
     */
	private final Object eofMonitor = new Object();
	
    /** Number of processed bytes. For tracking purpose. */
    private long byteCounter;
    
    /** Number of read records. */
    private long inputRecordCounter;

    /** Number of written records. */
    private long outputRecordCounter;

	/** How long has been reader blocked on the edge (in nanoseconds). */
	private long readerWaitingTime;

	/** How long has been writer blocked on the edge (in nanoseconds). */
	private long writerWaitingTime;

	/**
	 * @param proxy
	 */
	public BufferedFastPropagateEdge(Edge proxy) {
		super(proxy);
	}

	@Override
	public void init() throws IOException, InterruptedException {
		circularBufferQueues = new CircularBufferQueue[2];
		circularBufferQueues[0] = new CircularBufferQueue(Defaults.Record.RECORD_INITIAL_SIZE, Defaults.Record.RECORD_LIMIT_SIZE, true);
		circularBufferQueues[1] = new CircularBufferQueue(Defaults.Record.RECORD_INITIAL_SIZE, Defaults.Record.RECORD_LIMIT_SIZE, true);
		
		persistentBufferQueue = new PersistentBufferQueue();
	}

	@Override
	public void preExecute() {
		super.preExecute();
		
		circularBufferQueues[0].clear();
		circularBufferQueues[1].clear();
		readingQueue = writingQueue = circularBufferQueues[0]; //both reading and writing threads start to share same in-memory queue
		eofReached = false;
		byteCounter = 0;
		inputRecordCounter = 0;
		outputRecordCounter = 0;
		readerWaitingTime = 0;
		writerWaitingTime = 0;
	}
	
	@Override
	public void postExecute() {
		super.postExecute();
		
		persistentBufferQueue.close(); //let's remove the temporary file if any
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
	public DataRecord readRecord(DataRecord record) throws IOException, InterruptedException {
		DataRecord result;
		//let's try to read a record from reading queue
		if ((result = readingQueue.poll(record)) == null) {
			//no data available in reading queue - re-load reading queue from persistent layer or switch to the writing queue
			secureReadingQueue();
			if (verbose) {
            	//readerWaitingTime is advanced only in verbose mode
                long startTime = System.nanoTime();
				while ((result = readingQueue.blockingPoll(record)) == null) {
					secureReadingQueue();
				}
                readerWaitingTime += System.nanoTime() - startTime;
			} else {
				//now we have correct reading queue, lets use blocking poll from current reading queue 
				while ((result = readingQueue.blockingPoll(record)) == null) {
					secureReadingQueue();
				}
			}
		}
		//maybe EOF has been reached
		if (result == CircularBufferQueue.EOF_DATA_RECORD) {
			eofReached();
			return null;
		}
		inputRecordCounter++;
		return record;
	}

	@Override
	public boolean readRecordDirect(CloverBuffer record) throws IOException, InterruptedException {
		CloverBuffer result;
		//let's try to read a record from reading queue
		if ((result = readingQueue.poll(record)) == null) {
			//no data available in reading queue - re-load reading queue from persistent layer or switch to the writing queue
			secureReadingQueue();
			if (verbose) {
            	//readerWaitingTime is advanced only in verbose mode
                long startTime = System.nanoTime();
				while ((result = readingQueue.blockingPoll(record)) == null) {
					secureReadingQueue();
				}
                readerWaitingTime += System.nanoTime() - startTime;
			} else {
				//now we have correct reading queue, lets use blocking poll from current reading queue 
				while ((result = readingQueue.blockingPoll(record)) == null) {
					secureReadingQueue();
				}
			}

		}
		//maybe EOF has been reached
		if (result == CircularBufferQueue.EOF_CLOVER_BUFFER) {
			eofReached();
			return false;
		}
		inputRecordCounter++;
		return true;
	}

	private synchronized void secureReadingQueue() {
		if (!readingQueue.hasData()) {
			//no data available in reading queue
			if (readingQueue != writingQueue) {
				//but some other data are available elsewhere
				CloverBuffer internalBuffer = readingQueue.getInternalBuffer();
				//try to populate data from persistent buffer queue
				if (persistentBufferQueue.poll(internalBuffer) == null) {
					//no data in temporary file, just switch to writing queue
					readingQueue = writingQueue;
				} else {
					readingQueue.flip();
				}
			}
		}
	}

	@Override
	public void writeRecord(DataRecord record) throws IOException, InterruptedException {
		int recordSize = record.getSizeSerialized();
		//try to write data records to writing queue
		if (!writingQueue.offer(record, recordSize)) {
			//writing queue seems to be full, try to ensure an empty writing queue 
			secureWritingQueue(); //writing queue either has been switch to the other queue or content has been saved into temporary file and writing queue is empty now
			if (!writingQueue.offer(record, recordSize)) {
				//upsss - even the second attempt to hopefully empty writing queue failed!
				throw new JetelRuntimeException("internal error - unexpected internal state");
			}
		}
		byteCounter += recordSize;
		outputRecordCounter++;
	}

	@Override
	public void writeRecordDirect(CloverBuffer record) throws IOException, InterruptedException {
		int recordSize = record.remaining();
		//try to write data records to writing queue
		if (!writingQueue.offer(record)) {
			//writing queue seems to be full, try to ensure an empty writing queue 
			secureWritingQueue(); //writing queue either has been switch to the other queue or content has been saved into temporary file and writing queue is empty now
			if (!writingQueue.offer(record)) {
				//upsss - even the second attempt to hopefully empty writing queue failed!
				throw new JetelRuntimeException("internal error - unexpected internal state");
			}
		}
		byteCounter += recordSize;
		outputRecordCounter++;
	}

	private synchronized void secureWritingQueue() {
		if (writingQueue == readingQueue) {
			//if writing thread shares queue with reading thread, just switch to the other queue
			//readingQueue is no more blocking queue (blockingPoll() method does not block calling thread)
			//this is necessary to unblock the reading queue, since the reading thread can be now already stuck on empty queue
			readingQueue.setBlocking(false);
			writingQueue = theOtherQueue(writingQueue);
		} else {
			if (verbose) {
				//in case verbose mode is on, time of data writing is added to writer waiting time
				//this is the best approximation how the writerWaitingTime can be calculated
				long startTime = System.nanoTime();
				persistentBufferQueue.offer(writingQueue.getReadOnlyInternalBuffer());
				writerWaitingTime += System.nanoTime() - startTime;
			} else {
				//if writing queue is different from the reading queue - data are saved to temporary file
				persistentBufferQueue.offer(writingQueue.getReadOnlyInternalBuffer());
			}
		}
		writingQueue.clear();
 	}
	
	@Override
	public void eof() throws IOException, InterruptedException {
		//EOF annotation is written to writing queue
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
	
	@Override
	public long getReaderWaitingTime() {
		return readerWaitingTime / 1000000;
	}
	
	@Override
	public long getWriterWaitingTime() {
		return writerWaitingTime / 1000000;
	}

	private void eofReached() {
    	synchronized (eofMonitor) {
    		eofReached = true;
    		eofMonitor.notifyAll();
    	}
	}
	
	@Override
	public void waitForEOF() throws InterruptedException {
    	synchronized (eofMonitor) {
    		while (!eofReached) {
    			eofMonitor.wait();
    		}
    	}
	}
	
}
