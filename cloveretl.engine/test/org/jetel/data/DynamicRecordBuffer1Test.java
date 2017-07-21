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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.test.CloverTestCase;
import org.jetel.util.bytes.CloverBuffer;

/**
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 9 Nov 2011
 */
public class DynamicRecordBuffer1Test extends CloverTestCase {

	private static String bigString;

	public void testReadWriteRecord() throws IOException, InterruptedException {
		DynamicRecordBuffer dynamicBuffer = new DynamicRecordBuffer(50000);
		dynamicBuffer.init();
		
		DataRecord record = DataRecordFactory.newRecord(getMetadata());
		record.init();
		
		for (int i = 0; i < 1000; i++) {
			populateDataRecord(record, i, false);
			dynamicBuffer.writeRecord(record);
		}
		
		dynamicBuffer.setEOF();
		assertEquals(1000, dynamicBuffer.getBufferedRecords());
		
		for (int i = 0; i < 1000; i++) {
			dynamicBuffer.readRecord(record);
			checkDataRecord(record, i, false);
		}
		
		assertFalse(dynamicBuffer.readRecord());
		
		dynamicBuffer.close();
	}

	public void testReadWriteRecordDirect() throws IOException, InterruptedException {
		DynamicRecordBuffer dynamicBuffer = new DynamicRecordBuffer(50000);
		dynamicBuffer.init();
		
		DataRecord record = DataRecordFactory.newRecord(getMetadata());
		record.init();
		
		CloverBuffer recordBuffer = CloverBuffer.allocateDirect(Defaults.Record.RECORD_INITIAL_SIZE);
		
		for (int i = 0; i < 5; i++) {
			populateDataRecord(record, i, (i % 2) == 0);
			recordBuffer.clear();
			record.serialize(recordBuffer);
			recordBuffer.flip();
			dynamicBuffer.writeRecord(recordBuffer);
		}
		
		dynamicBuffer.setEOF();
		assertEquals(5, dynamicBuffer.getBufferedRecords());
		
		for (int i = 0; i < 5; i++) {
			recordBuffer.clear();
			dynamicBuffer.readRecord(recordBuffer);
			record.deserialize(recordBuffer);
			checkDataRecord(record, i, (i % 2) == 0);
		}
		
		dynamicBuffer.close();
	}

	public void testReadWriteBigRecord() throws IOException, InterruptedException {
		DynamicRecordBuffer dynamicBuffer = new DynamicRecordBuffer(50000);
		dynamicBuffer.init();
		
		DataRecord record = DataRecordFactory.newRecord(getMetadata());
		record.init();
		
		for (int iteration = 0; iteration < 3; iteration++) {
			
			for (int i = 0; i < 1000; i++) {
				populateDataRecord(record, i, false);
				dynamicBuffer.writeRecord(record);
			}
	
			for (int i = 0; i < 100; i++) {
				populateDataRecord(record, 1000 + i, true);
				dynamicBuffer.writeRecord(record);
			}
	
			assertEquals(1100, dynamicBuffer.getBufferedRecords());
			dynamicBuffer.setEOF();
			
			for (int i = 0; i < 1000; i++) {
				dynamicBuffer.readRecord(record);
				checkDataRecord(record, i, false);
			}
	
			assertEquals(100, dynamicBuffer.getBufferedRecords());
	
			for (int i = 0; i < 100; i++) {
				dynamicBuffer.readRecord(record);
				checkDataRecord(record, 1000 + i, true);
			}
	
			assertEquals(0, dynamicBuffer.getBufferedRecords());
			
			dynamicBuffer.reset();
		}

		dynamicBuffer.close();

		assertTrue(dynamicBuffer.isClosed());
	}

	public void testRandomReadWriteRecord() throws ExecutionException, InterruptedException, IOException {
		final DynamicRecordBuffer dynamicBuffer = new DynamicRecordBuffer(50000);
		dynamicBuffer.init();
		

		Callable<Void> producent = new Callable<Void>() {
			@Override
			public Void call() throws Exception {
				DataRecord record = DataRecordFactory.newRecord(getMetadata());
				record.init();
				for (int i = 0; i < 1000; i++) {
					populateDataRecord(record, i, (i % 2) == 0);
					dynamicBuffer.writeRecord(record);
				}
				dynamicBuffer.setEOF();
				return null;
			}
		};

		Callable<Void> consument = new Callable<Void>() {
			@Override
			public Void call() throws Exception {
				DataRecord record = DataRecordFactory.newRecord(getMetadata());
				record.init();
				
				for (int i = 0; i < 1000; i++) {
					dynamicBuffer.readRecord(record);
					checkDataRecord(record, i, (i % 2) == 0);
				}
				return null;
			}
		};

		Future<Void> producentFuture = Executors.newSingleThreadExecutor().submit(producent);
		Future<Void> consumentFuture = Executors.newSingleThreadExecutor().submit(consument);
		
		producentFuture.get();
		consumentFuture.get();
		
		dynamicBuffer.close();

		assertTrue(dynamicBuffer.isClosed());
	}

	public static DataRecordMetadata getMetadata() {
		DataRecordMetadata metadata = new DataRecordMetadata("md1", DataRecordMetadata.DELIMITED_RECORD);
		
		metadata.addField(new DataFieldMetadata("f1", DataFieldMetadata.STRING_FIELD, ";"));
		metadata.addField(new DataFieldMetadata("f2", DataFieldMetadata.STRING_FIELD, ";"));
		metadata.addField(new DataFieldMetadata("f3", DataFieldMetadata.STRING_FIELD, ";"));
		metadata.addField(new DataFieldMetadata("f4", DataFieldMetadata.STRING_FIELD, ";"));
		metadata.addField(new DataFieldMetadata("f5", DataFieldMetadata.STRING_FIELD, ";"));
		metadata.addField(new DataFieldMetadata("f6", DataFieldMetadata.STRING_FIELD, ";"));
		metadata.addField(new DataFieldMetadata("f7", DataFieldMetadata.STRING_FIELD, ";"));
		
		return metadata;
	}
	
	public static void populateDataRecord(DataRecord dataRecord, int seed, boolean big) {
		for (DataField field : dataRecord) {
			if (field instanceof StringDataField) {
				field.setValue(field.getMetadata().getName() + " data " + (big ? getBigString() : "") + (seed++));
			}
		}
	}

	public static void checkDataRecord(DataRecord dataRecord, int seed, boolean big) {
		for (DataField field : dataRecord) {
			if (field instanceof StringDataField) {
				Object value = field.getValue();
				String strValue = value.toString();
				DataFieldMetadata fieldMetadata = field.getMetadata();
				String fieldName = fieldMetadata.getName();
				if (!strValue.equals(fieldName + " data " + (big ? getBigString() : "") + (seed))) {
					throw new RuntimeException("unexpected record");
				}
				seed++;
			}
		}
	}

	public static String getBigString() {
		if (bigString == null) {
			StringBuffer sb = new StringBuffer();
			for (int i = 0; i < 1000 * 10; i++) {
				sb.append('a');
			}
			bigString = sb.toString();
		}
		return bigString;
	}

}
