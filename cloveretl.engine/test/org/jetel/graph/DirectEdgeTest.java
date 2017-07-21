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
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.IntegerDataField;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataFieldType;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.test.CloverTestCase;

/**
 * @author martin (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 20. 12. 2016
 */
public class DirectEdgeTest extends CloverTestCase {

	private static final long MAX_WAITING_TIME = 3000 * 1000000l; //3 seconds

	private static DataRecordMetadata metadata;
	
	public void testNonBlockingReading() throws InterruptedException, ExecutionException, IOException {
		final DirectEdge edge = new DirectEdge(null);
		edge.init();
		
		ExecutorService executorService = Executors.newCachedThreadPool();

		Callable<Void> writer = new Callable<Void>() {
			@Override
			public Void call() throws Exception {
				DataRecord record = DataRecordFactory.newRecord(getMetadata());
				for (int i = 1; i <= 10; i++) {
					record.getField(0).setValue(i);
					edge.writeRecord(record);
					Thread.sleep(1000);
				}
				return null;
			}
		};
		
		Callable<Void> reader = new Callable<Void>() {
			@Override
			public Void call() throws Exception {
				DataRecord record = DataRecordFactory.newRecord(getMetadata());
				long start = System.nanoTime();
				edge.readRecord(record);
				long elapsedTime = System.nanoTime() - start;
				assertTrue("reader thread timeouted " + elapsedTime, elapsedTime < MAX_WAITING_TIME);
				assertEquals("invalid record detected", ((IntegerDataField)record.getField(0)).getValue(), (Integer) 1);
				return null;
			}
		};

		Future<Void> writerJob = executorService.submit(writer);
		Future<Void> readerJob = executorService.submit(reader);
		
		readerJob.get(); //wait for reader thread
		writerJob.cancel(true); //reader thread succeed, cancel the writer job and wait for potential errors
		try {
			writerJob.get();
		} catch (CancellationException e) {
			//OK
		}
	}
	
	private synchronized static DataRecordMetadata getMetadata() throws ComponentNotReadyException {
		if (metadata == null) {
			metadata = new DataRecordMetadata("simpleMetadata");
			metadata.addField(new DataFieldMetadata("field1", DataFieldType.INTEGER, null));
		}
		return metadata;
	}
	
}
