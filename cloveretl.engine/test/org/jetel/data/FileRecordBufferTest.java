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

import org.jetel.test.CloverTestCase;
import org.jetel.util.bytes.CloverBuffer;

/**
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 9 Nov 2011
 */
public class FileRecordBufferTest extends CloverTestCase {

	private CloverBuffer recordBuffer;

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		
		recordBuffer = CloverBuffer.allocateDirect(Defaults.Record.RECORD_INITIAL_SIZE);
	}

	public void testReadWriteRecord() throws IOException, InterruptedException {
		FileRecordBuffer fileBuffer = new FileRecordBuffer(50000);
		
		DataRecord record = DataRecordFactory.newRecord(DynamicRecordBuffer1Test.getMetadata());
		record.init();
		
		
		for (int i = 0; i < 1000; i++) {
			DynamicRecordBuffer1Test.populateDataRecord(record, i, (i % 2) == 0);
			pushDataRecord(fileBuffer, record);
		}
		
		for (int i = 0; i < 1000; i++) {
			shiftDataRecord(fileBuffer, record);
			DynamicRecordBuffer1Test.checkDataRecord(record, i, (i % 2) == 0);
		}
		
		assertNull(fileBuffer.shift(recordBuffer));
		
		fileBuffer.close();
	}

	public void testReadWriteBigRecord() throws IOException, InterruptedException {
		FileRecordBuffer fileBuffer = new FileRecordBuffer(50000);
		
		DataRecord record = DataRecordFactory.newRecord(DynamicRecordBuffer1Test.getMetadata());
		record.init();
		
		for (int iteration = 0; iteration < 3; iteration++) {
			
			for (int i = 0; i < 1000; i++) {
				DynamicRecordBuffer1Test.populateDataRecord(record, i, false);
				pushDataRecord(fileBuffer, record);
			}
	
			for (int i = 0; i < 100; i++) {
				DynamicRecordBuffer1Test.populateDataRecord(record, 1000 + i, true);
				pushDataRecord(fileBuffer, record);
			}
	
			for (int innerIteration = 0; innerIteration < 3; innerIteration++) {
				for (int i = 0; i < 1000; i++) {
					shiftDataRecord(fileBuffer, record);
					DynamicRecordBuffer1Test.checkDataRecord(record, i, false);
				}
		
				for (int i = 0; i < 100; i++) {
					shiftDataRecord(fileBuffer, record);
					DynamicRecordBuffer1Test.checkDataRecord(record, 1000 + i, true);
				}
		
				fileBuffer.rewind();
			}
			fileBuffer.clear();
		}

		fileBuffer.close();
	}

	private void pushDataRecord(FileRecordBuffer fileBuffer, DataRecord record) throws IOException {
		recordBuffer.clear();
		record.serialize(recordBuffer);
		recordBuffer.flip();
		fileBuffer.push(recordBuffer);
	}
	
	private void shiftDataRecord(FileRecordBuffer fileBuffer, DataRecord record) throws IOException {
		recordBuffer.clear();
		fileBuffer.shift(recordBuffer);
		recordBuffer.flip();
		record.deserialize(recordBuffer);
	}
	
}
