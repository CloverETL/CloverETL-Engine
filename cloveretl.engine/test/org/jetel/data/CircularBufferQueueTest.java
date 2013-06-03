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

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataFieldType;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.test.CloverTestCase;
import org.jetel.util.DataGenerator;
import org.jetel.util.bytes.CloverBuffer;
import org.jetel.util.string.CloverString;

/**
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 20.5.2013
 */
public class CircularBufferQueueTest extends CloverTestCase {

	private static final int NUM_REC = 50000;
	
	private static DataRecord createRecord() {
		DataFieldMetadata fieldMetadata;
		DataRecordMetadata metadata = new DataRecordMetadata("name");

		fieldMetadata = new DataFieldMetadata("field1", DataFieldType.INTEGER, "");
		metadata.addField(fieldMetadata);

		fieldMetadata = new DataFieldMetadata("field2", DataFieldType.STRING, "");
		metadata.addField(fieldMetadata);

		fieldMetadata = new DataFieldMetadata("field3", DataFieldType.STRING, "");
		metadata.addField(fieldMetadata);
		
		fieldMetadata = new DataFieldMetadata("field4", DataFieldType.STRING, "");
		metadata.addField(fieldMetadata);
		
		DataRecord record = DataRecordFactory.newRecord(metadata);
		record.init();
		record.reset();
		return record;
	}

	private static void populateRecord(DataRecord record, DataGenerator randomGenerator, int index) {
		record.getField(0).setValue(index);
		record.getField(1).setValue(randomGenerator.nextString(randomGenerator.nextInt(0, 10), randomGenerator.nextInt(10, 100)));
		record.getField(2).setValue(randomGenerator.nextString(randomGenerator.nextInt(0, 10), randomGenerator.nextInt(10, 100)));
	}
	
	private static void checkRecord(DataRecord record, DataGenerator randomGenerator, int index) {
		assertTrue(((Integer) record.getField(0).getValue()) == index);
		assertTrue(((CloverString) record.getField(1).getValue()).toString().equals(randomGenerator.nextString(randomGenerator.nextInt(0, 10), randomGenerator.nextInt(10, 100))));
		assertTrue(((CloverString) record.getField(2).getValue()).toString().equals(randomGenerator.nextString(randomGenerator.nextInt(0, 10), randomGenerator.nextInt(10, 100))));
		assertTrue(record.getField(3).isNull());
	}

	
	public void testSingleThreadDirect() {
		CircularBufferQueue queue = new CircularBufferQueue(100, 100, false);
		CloverBuffer buffer = CloverBuffer.allocate(10);
		
		for (int j = 0; j < NUM_REC; j++) {
			for (int i = 0; i < 11; i++) {
				buffer.clear();
				buffer.putInt(i * j);
				buffer.flip();
				assertTrue(queue.offer(buffer));
			}
			
			for (int i = 0; i < 11; i++) {
				buffer.clear();
				assertTrue(queue.poll(buffer) == buffer);
				assertTrue(buffer.limit() == 4);
				assertTrue(buffer.getInt() == i * j);
			}
		}
		
		queue.eof();
		assertTrue(queue.poll(buffer) == CircularBufferQueue.EOF_CLOVER_BUFFER);
		assertTrue(queue.poll(buffer) == CircularBufferQueue.EOF_CLOVER_BUFFER);
	}

	public void testOfferFullQueue() {
		CircularBufferQueue queue = new CircularBufferQueue(100, 100, false);
		CloverBuffer buffer = CloverBuffer.allocate(10);
		
		for (int i = 0; i < 19; i++) {
			buffer.clear();
			buffer.putInt(i);
			buffer.flip();
			assertTrue(queue.offer(buffer));
		}

		buffer.clear();
		buffer.putInt(20);
		buffer.flip();
		assertFalse(queue.offer(buffer));

	}

	public void testMultiThreadDirect() throws InterruptedException, ExecutionException {
		final CircularBufferQueue queue = new CircularBufferQueue(100, 100, true);
		
		Callable<Void> producent = new ProducentDirect(queue);
		Callable<Void> consument = new ConsumentDirect(queue, false);
		
		ExecutorService pool = Executors.newFixedThreadPool(2);
		Future<Void> consumentFuture = pool.submit(consument);
		Future<Void> producentFuture = pool.submit(producent);
		
		producentFuture.get();
		consumentFuture.get();
	}

	public void testMultiThreadBlockingPollDirect() throws InterruptedException, ExecutionException {
		final CircularBufferQueue queue = new CircularBufferQueue(100, 100, true);
		
		Callable<Void> producent = new ProducentDirect(queue);
		Callable<Void> consument = new ConsumentDirect(queue, true);
		
		ExecutorService pool = Executors.newFixedThreadPool(2);
		Future<Void> consumentFuture = pool.submit(consument);
		Future<Void> producentFuture = pool.submit(producent);
		
		producentFuture.get();
		consumentFuture.get();
	}

	public void testSingleThreadNonDirect() {
		CircularBufferQueue queue = new CircularBufferQueue(Defaults.Record.RECORD_INITIAL_SIZE, Defaults.Record.RECORD_LIMIT_SIZE, false);
		DataRecord record = createRecord();
		DataGenerator seedGenerator = new DataGenerator();
		long seed = seedGenerator.nextLong();
		
		DataGenerator randomGenerator = new DataGenerator(seed);
		for (int j = 0; j < 100; j++) {
			populateRecord(record, randomGenerator, j);
			assertTrue(queue.offer(record));
		}			

		randomGenerator = new DataGenerator(seed);
		for (int j = 0; j < 100; j++) {
			assertTrue(queue.poll(record) == record);
			checkRecord(record, randomGenerator, j);
		}
		
		queue.eof();
		assertTrue(queue.poll(record) == CircularBufferQueue.EOF_DATA_RECORD);
		assertTrue(queue.poll(record) == CircularBufferQueue.EOF_DATA_RECORD);
		assertTrue(queue.poll(record) == CircularBufferQueue.EOF_DATA_RECORD);
	}

	public void testMultiThreadNonDirect() throws InterruptedException, ExecutionException {
		CircularBufferQueue queue = new CircularBufferQueue(Defaults.Record.RECORD_INITIAL_SIZE, Defaults.Record.RECORD_LIMIT_SIZE, true);
		
		long seed = new DataGenerator().nextLong();
		
		Callable<Void> producent = new ProducentNonDirect(queue, seed);
		Callable<Void> consument = new ConsumentNonDirect(queue, seed, false);
		
		ExecutorService pool = Executors.newFixedThreadPool(2);
		Future<Void> consumentFuture = pool.submit(consument);
		Future<Void> producentFuture = pool.submit(producent);
		
		producentFuture.get();
		consumentFuture.get();
	}

	public void testMultiThreadBlockingPollNonDirect() throws InterruptedException, ExecutionException {
		CircularBufferQueue queue = new CircularBufferQueue(Defaults.Record.RECORD_INITIAL_SIZE, Defaults.Record.RECORD_LIMIT_SIZE, true);
		
		long seed = new DataGenerator().nextLong();
		
		Callable<Void> producent = new ProducentNonDirect(queue, seed);
		Callable<Void> consument = new ConsumentNonDirect(queue, seed, true);
		
		ExecutorService pool = Executors.newFixedThreadPool(2);
		Future<Void> consumentFuture = pool.submit(consument);
		Future<Void> producentFuture = pool.submit(producent);
		
		producentFuture.get();
		consumentFuture.get();
	}

	public void testMultiThreadGrowingBuffers() throws InterruptedException, ExecutionException {
		final CircularBufferQueue queue = new CircularBufferQueue(100, 100000, true);
		
		Callable<Void> producent = new Callable<Void>() {

			@Override
			public Void call() throws Exception {
				int size = 1;
				boolean grow = true;
				CloverBuffer buffer = CloverBuffer.allocate(10);
				for (int i = 0; i< NUM_REC; i++) {
					if (size > 10000) {
						grow = false;
					}
					if (size < 1) {
						size = 1;
						grow = true;
					}
					if (grow) {
						size *= 2;
					} else {
						size /= 2;
					}
					buffer.clear();
					for (int j = 0; j < size; j++) {
						buffer.put((byte) (j % 127));
					}
					buffer.flip();
					while (!queue.offer(buffer));
				}
				queue.eof();
				
				return null;
			}
		};
		Callable<Void> consument = new Callable<Void>() {
			@Override
			public Void call() throws Exception {
				int size = 1;
				boolean grow = true;
				CloverBuffer buffer = CloverBuffer.allocate(10);
				for (int i = 0; i < NUM_REC; i++) {
					if (size > 10000) {
						grow = false;
					}
					if (size < 1) {
						size = 1;
						grow = true;
					}
					if (grow) {
						size *= 2;
					} else {
						size /= 2;
					}
					buffer.clear();
					CloverBuffer result = queue.blockingPoll(buffer);
					assertTrue(result == buffer);
					assertTrue(buffer.limit() == size);
					for (int j = 0; j < size; j++) {
						assertTrue(buffer.get() == (j % 127));
					}
				}
				assertTrue(queue.blockingPoll(buffer) == CircularBufferQueue.EOF_CLOVER_BUFFER);
				assertTrue(queue.poll(buffer) == CircularBufferQueue.EOF_CLOVER_BUFFER);
				assertTrue(queue.poll(buffer) == CircularBufferQueue.EOF_CLOVER_BUFFER);
				return null;
			}
		};
		
		ExecutorService pool = Executors.newFixedThreadPool(2);
		Future<Void> consumentFuture = pool.submit(consument);
		Future<Void> producentFuture = pool.submit(producent);
		
		producentFuture.get();
		consumentFuture.get();
	}

	private class ProducentDirect implements Callable<Void> {
		private CircularBufferQueue queue;
		
		/**
		 * 
		 */
		public ProducentDirect(CircularBufferQueue queue) {
			this.queue = queue;
		}
		
		@Override
		public Void call() {
			CloverBuffer buffer = CloverBuffer.allocate(10);
			for (int i = 0; i< NUM_REC; i++) {
				buffer.clear();
				buffer.putInt(i);
				buffer.flip();
				while (!queue.offer(buffer));
			}
			queue.eof();
			
			return null;
		}
	};
	
	private class ConsumentDirect implements Callable<Void> {
		private CircularBufferQueue queue;
		private boolean useBlockingPoll;
		
		/**
		 * 
		 */
		public ConsumentDirect(CircularBufferQueue queue, boolean useBlockingPoll) {
			this.queue = queue;
			this.useBlockingPoll = useBlockingPoll;
		}
		
		@Override
		public Void call() throws InterruptedException {
			CloverBuffer buffer = CloverBuffer.allocate(10);
			for (int i = 0; i < NUM_REC; i++) {
				buffer.clear();
				if (useBlockingPoll) {
					CloverBuffer result = queue.blockingPoll(buffer);
					assertTrue(result == buffer);
				} else {
					CloverBuffer result;
					while((result = queue.poll(buffer)) == null);
					assertTrue(result == buffer);
				}
				assertTrue(buffer.limit() == 4);
				assertTrue(buffer.getInt() == i);
			}
			assertTrue(queue.blockingPoll(buffer) == CircularBufferQueue.EOF_CLOVER_BUFFER);
			assertTrue(queue.poll(buffer) == CircularBufferQueue.EOF_CLOVER_BUFFER);
			assertTrue(queue.poll(buffer) == CircularBufferQueue.EOF_CLOVER_BUFFER);
			return null;
		}
	};

	private class ProducentNonDirect implements Callable<Void> {
		private CircularBufferQueue queue;
		private long seed;
		
		/**
		 * 
		 */
		public ProducentNonDirect(CircularBufferQueue queue, long seed) {
			this.queue = queue;
			this.seed = seed;
		}
		
		@Override
		public Void call() {
			DataRecord record = createRecord();
			DataGenerator randomGenerator = new DataGenerator(seed);
			for (int i = 0; i < NUM_REC; i++) {
				populateRecord(record, randomGenerator, i);
				while (!queue.offer(record));
			}
			queue.eof();
			
			return null;
		}
	};
	
	private class ConsumentNonDirect implements Callable<Void> {
		private CircularBufferQueue queue;
		private long seed;
		private boolean useBlockingPoll;
		
		/**
		 * 
		 */
		public ConsumentNonDirect(CircularBufferQueue queue, long seed, boolean useBlockingPoll) {
			this.queue = queue;
			this.seed = seed;
			this.useBlockingPoll = useBlockingPoll;
		}
		
		@Override
		public Void call() throws InterruptedException {
			DataRecord record = createRecord();
			DataGenerator randomGenerator = new DataGenerator(seed);
			for (int i = 0; i < NUM_REC; i++) {
				if (useBlockingPoll) {
					DataRecord result = queue.blockingPoll(record);
					assertTrue(result == record);
				} else {
					DataRecord result;
					while((result = queue.poll(record)) == null);
					assertTrue(result == record);
				}
				checkRecord(record, randomGenerator, i);
			}
			assertTrue(queue.blockingPoll(record) == CircularBufferQueue.EOF_DATA_RECORD);
			assertTrue(queue.poll(record) == CircularBufferQueue.EOF_DATA_RECORD);
			assertTrue(queue.poll(record) == CircularBufferQueue.EOF_DATA_RECORD);
			return null;
		}
	};

}
