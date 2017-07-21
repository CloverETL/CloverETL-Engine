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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.Defaults;
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
 * @created 24.5.2013
 */
public class BufferedFastPropagateEdgeTest extends CloverTestCase {

	private static final int NUM_REC = 50;
	
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
	
	public void testSync() throws IOException, InterruptedException {
		BufferedFastPropagateEdge edge = new BufferedFastPropagateEdge(null);
		edge.init();
		edge.preExecute();

		assertTrue(edge.hasData() == false);
		assertTrue(edge.isEOF() == false);

		DataGenerator seedGenerator = new DataGenerator();
		long seed = seedGenerator.nextLong();
		DataGenerator randomGenerator;
		
		DataRecord record = createRecord();
		CloverBuffer cloverBuffer = CloverBuffer.allocateDirect(Defaults.Record.RECORD_INITIAL_SIZE, Defaults.Record.RECORD_LIMIT_SIZE);
		
		/*************************************************************/
		//cached records writing
		randomGenerator = new DataGenerator(seed);
		for (int i = 0; i < NUM_REC; i++) {
			populateRecord(record, randomGenerator, i);
			edge.writeRecord(record);
		}
		
		assertTrue(edge.getBufferedRecords() == NUM_REC);
		assertTrue(edge.getInputByteCounter() > 0);
		assertTrue(edge.getInputRecordCounter() == 0);
		assertTrue(edge.getOutputByteCounter() > 0);
		assertTrue(edge.getOutputRecordCounter() == NUM_REC);
		assertTrue(edge.isEOF() == false);
		
		//cached records reading
		randomGenerator = new DataGenerator(seed);
		for (int i = 0; i < NUM_REC; i++) {
			edge.readRecord(record);
			checkRecord(record, randomGenerator, i);
		}

		assertTrue(edge.getBufferedRecords() == 0);
		assertTrue(edge.getInputByteCounter() > 0);
		assertTrue(edge.getInputRecordCounter() == NUM_REC);
		assertTrue(edge.getOutputByteCounter() > 0);
		assertTrue(edge.getOutputRecordCounter() == NUM_REC);
		assertTrue(edge.isEOF() == false);

		/*************************************************************/
		//cached direct writing
		randomGenerator = new DataGenerator(seed);
		for (int i = 0; i < NUM_REC; i++) {
			populateRecord(record, randomGenerator, i);
			cloverBuffer.clear();
			record.serialize(cloverBuffer);
			cloverBuffer.flip();
			edge.writeRecordDirect(cloverBuffer);
		}
		
		//cached direct reading
		randomGenerator = new DataGenerator(seed);
		for (int i = 0; i < NUM_REC; i++) {
			cloverBuffer.clear();
			edge.readRecordDirect(cloverBuffer);
			record.deserialize(cloverBuffer);
			checkRecord(record, randomGenerator, i);
		}

		/*************************************************************/
		//cached records writing
		randomGenerator = new DataGenerator(seed);
		for (int i = 0; i < NUM_REC; i++) {
			populateRecord(record, randomGenerator, i);
			edge.writeRecord(record);
		}

		//cached direct reading
		randomGenerator = new DataGenerator(seed);
		for (int i = 0; i < NUM_REC; i++) {
			cloverBuffer.clear();
			edge.readRecordDirect(cloverBuffer);
			record.deserialize(cloverBuffer);
			checkRecord(record, randomGenerator, i);
		}

		/*************************************************************/
		//cached direct writing
		randomGenerator = new DataGenerator(seed);
		for (int i = 0; i < NUM_REC; i++) {
			populateRecord(record, randomGenerator, i);
			cloverBuffer.clear();
			record.serialize(cloverBuffer);
			cloverBuffer.flip();
			edge.writeRecordDirect(cloverBuffer);
		}
		edge.eof();
		assertTrue(edge.isEOF() == false);

		//cached records reading
		randomGenerator = new DataGenerator(seed);
		for (int i = 0; i < NUM_REC; i++) {
			edge.readRecord(record);
			checkRecord(record, randomGenerator, i);
		}

		assertTrue(edge.isEOF() == false);
		assertTrue(edge.readRecord(record) == null);
		assertTrue(edge.isEOF() == true);
		
		edge.postExecute();
		edge.free();
	}

	public void testAsync() throws IOException, InterruptedException, ExecutionException {
		final BufferedFastPropagateEdge edge = new BufferedFastPropagateEdge(null);
		edge.init();
		edge.preExecute();

		assertTrue(edge.hasData() == false);
		assertTrue(edge.isEOF() == false);

		DataGenerator seedGenerator = new DataGenerator();
		final long seed = seedGenerator.nextLong();
		
		ExecutorService pool = Executors.newFixedThreadPool(2);

		/*************************************************************/
		Future<Void> producentFuture = pool.submit(new Producent(seed, edge));
		Future<Void> consumentFuture = pool.submit(new Consument(seed, edge, false));
		producentFuture.get();
		consumentFuture.get();

		producentFuture = pool.submit(new ProducentDirect(seed, edge, false));
		consumentFuture = pool.submit(new ConsumentDirect(seed, edge));
		producentFuture.get();
		consumentFuture.get();

		producentFuture = pool.submit(new Producent(seed, edge));
		consumentFuture = pool.submit(new ConsumentDirect(seed, edge));
		producentFuture.get();
		consumentFuture.get();

		producentFuture = pool.submit(new ProducentDirect(seed, edge, true));
		consumentFuture = pool.submit(new Consument(seed, edge, true));
		producentFuture.get();
		consumentFuture.get();

		edge.postExecute();
		edge.free();
	}

	public void testGrowingBuffers() throws IOException, InterruptedException, ExecutionException {
		final BufferedFastPropagateEdge edge = new BufferedFastPropagateEdge(null);
		edge.init();
		edge.preExecute();

		ExecutorService pool = Executors.newFixedThreadPool(2);

		/*************************************************************/
		Future<Void> producentFuture = pool.submit(new Callable<Void>() {
			@Override
			public Void call() throws Exception {
				CloverBuffer buffer = CloverBuffer.allocate(10);
				int size = 1;
				boolean grow = true;
				for (int i = 0; i < NUM_REC; i++) {
					if (size > 1000000) {
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
					edge.writeRecordDirect(buffer);
				}
				edge.eof();
				return null;
			}
		});
		Future<Void> consumentFuture = pool.submit(new Callable<Void>() {
			@Override
			public Void call() throws Exception {
				CloverBuffer buffer = CloverBuffer.allocate(10);
				int size = 1;
				boolean grow = true;
				for (int i = 0; i < NUM_REC; i++) {
					if (size > 1000000) {
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
					assertTrue(edge.readRecordDirect(buffer));
					assertTrue(buffer.limit() == size);
					for (int j = 0; j < size; j++) {
						assertTrue(buffer.get() == (j % 127));
					}
				}
				
				assertTrue(edge.readRecordDirect(buffer) == false);
				return null;
			}
		});

		producentFuture.get();
		consumentFuture.get();

		edge.postExecute();
		edge.free();
	}

	private static class Producent implements Callable<Void> {
		private long seed;
		private BufferedFastPropagateEdge edge;
		
		/**
		 * 
		 */
		public Producent(long seed, BufferedFastPropagateEdge edge) {
			this.seed = seed;
			this.edge = edge;
		}
		
		@Override
		public Void call() throws Exception {
			DataGenerator randomGenerator = new DataGenerator(seed);
			DataRecord record = createRecord();
			for (int i = 0; i < NUM_REC; i++) {
				populateRecord(record, randomGenerator, i);
				edge.writeRecord(record);
			}
			return null;
		}
	};

	private static class Consument implements Callable<Void> {
		private long seed;
		private BufferedFastPropagateEdge edge;
		private boolean eof;
		
		/**
		 * 
		 */
		public Consument(long seed, BufferedFastPropagateEdge edge, boolean eof) {
			this.seed = seed;
			this.edge = edge;
			this.eof = eof;
		}
		
		@Override
		public Void call() throws Exception {
			DataGenerator randomGenerator = new DataGenerator(seed);
			DataRecord record = createRecord();
			for (int i = 0; i < NUM_REC; i++) {
				edge.readRecord(record);
				checkRecord(record, randomGenerator, i);
			}
			assertTrue(edge.isEOF() == false);
			if (eof) {
				assertTrue(edge.readRecord(record) == null);
				assertTrue(edge.isEOF() == true);
			}
			return null;
		}
	};

	private static class ProducentDirect implements Callable<Void> {
		private long seed;
		private BufferedFastPropagateEdge edge;
		private boolean eof;
		
		/**
		 * 
		 */
		public ProducentDirect(long seed, BufferedFastPropagateEdge edge, boolean eof) {
			this.seed = seed;
			this.edge = edge;
			this.eof = eof;
		}
		
		@Override
		public Void call() throws Exception {
			CloverBuffer cloverBuffer = CloverBuffer.allocateDirect(Defaults.Record.RECORD_INITIAL_SIZE, Defaults.Record.RECORD_LIMIT_SIZE);
			DataRecord record = createRecord();
			DataGenerator randomGenerator = new DataGenerator(seed);
			for (int i = 0; i < NUM_REC; i++) {
				populateRecord(record, randomGenerator, i);
				cloverBuffer.clear();
				record.serialize(cloverBuffer);
				cloverBuffer.flip();
				edge.writeRecordDirect(cloverBuffer);
			}
			if (eof) {
				edge.eof();
			}
			return null;
		}
	};

	private static class ConsumentDirect implements Callable<Void> {
		private long seed;
		private BufferedFastPropagateEdge edge;
		
		/**
		 * 
		 */
		public ConsumentDirect(long seed, BufferedFastPropagateEdge edge) {
			this.seed = seed;
			this.edge = edge;
		}
		
		@Override
		public Void call() throws Exception {
			CloverBuffer cloverBuffer = CloverBuffer.allocateDirect(Defaults.Record.RECORD_INITIAL_SIZE, Defaults.Record.RECORD_LIMIT_SIZE);
			DataRecord record = createRecord();
			DataGenerator randomGenerator = new DataGenerator(seed);
			for (int i = 0; i < NUM_REC; i++) {
				cloverBuffer.clear();
				edge.readRecordDirect(cloverBuffer);
				record.deserialize(cloverBuffer);
				checkRecord(record, randomGenerator, i);
			}
			return null;
		}
	};

}
