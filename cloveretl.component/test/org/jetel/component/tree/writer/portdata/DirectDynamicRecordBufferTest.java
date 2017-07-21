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
package org.jetel.component.tree.writer.portdata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.Defaults;
import org.jetel.data.DynamicRecordBuffer1Test;
import org.jetel.test.CloverTestCase;
import org.jetel.util.bytes.CloverBuffer;

/**
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 7 Nov 2011
 */
public class DirectDynamicRecordBufferTest extends CloverTestCase {

	private static final int NUM_RECORDS = 10;
	
	public void testFirstUsage() throws IOException {
		DirectDynamicRecordBuffer buffer = new DirectDynamicRecordBuffer();
		buffer.init();
		
		CloverBuffer cloverBuffer = CloverBuffer.allocateDirect(Defaults.Record.RECORD_INITIAL_SIZE, Defaults.Record.RECORD_LIMIT_SIZE);
		
		DataRecord record = DataRecordFactory.newRecord(DynamicRecordBuffer1Test.getMetadata());
		record.init();
		record.reset();
		
		for (int iteration = 0; iteration < 3; iteration++) {
			List<IndexKey> positions = new ArrayList<IndexKey>();
			for (int innerIteration = 0; innerIteration < 3; innerIteration++) {
				for (int i = 0; i < NUM_RECORDS; i++) {
					DynamicRecordBuffer1Test.populateDataRecord(record, i, true);
					IndexKey indexKey = buffer.writeRaw(record);
					positions.add(0, indexKey);
				}
				
				for (int innerIteration1 = 0; innerIteration1 < 3; innerIteration1++) {
					for (int i = 0; i < NUM_RECORDS; i++) {
						IndexKey position = positions.get(NUM_RECORDS - i - 1);
						cloverBuffer.clear();
						cloverBuffer.limit(position.getLength());
						buffer.read(cloverBuffer, position.getPosition());
						record.deserialize(cloverBuffer);
						DynamicRecordBuffer1Test.checkDataRecord(record, i, true);
					}
				}
			}
			
			buffer.clear();
		}
		
		buffer.close();
	}
	
	public void testSecondUsage() throws IOException {
		DirectDynamicRecordBuffer buffer = new DirectDynamicRecordBuffer();
		buffer.init();
		
		DataRecord record = DataRecordFactory.newRecord(DynamicRecordBuffer1Test.getMetadata());
		record.init();
		record.reset();
		
		for (int iteration = 0; iteration < 3; iteration++) {

			for (int i = 0; i < 10; i++) {
				DynamicRecordBuffer1Test.populateDataRecord(record, i, true);
				buffer.write(record);
			}
	
			buffer.flushBuffer();
			buffer.loadData();
			
			for (int innerIteration = 0; innerIteration < 3; innerIteration++) {
				int counter = 0;
				while (buffer.next(record)) {
					DynamicRecordBuffer1Test.checkDataRecord(record, counter, true);
					counter++;
				}
				if (counter != 10) {
					throw new RuntimeException("incorrect number of records");
				}
				
				buffer.reset();
			}
			
			buffer.clear();
		}
		
		buffer.close();
	}

	public void testEmptyBuffer() throws IOException {
		DirectDynamicRecordBuffer buffer = new DirectDynamicRecordBuffer();
		buffer.init();
		
		DataRecord record = DataRecordFactory.newRecord(DynamicRecordBuffer1Test.getMetadata());
		record.init();
		record.reset();

		for (int iteration = 0; iteration < 3; iteration++) {
		
			//no data written
	
			buffer.flushBuffer();
			buffer.loadData();
			
			for (int innerIteration = 0; innerIteration < 3; innerIteration++) {
				while (buffer.next(record)) {
					//no data should read
					throw new RuntimeException("incorrect number of records");
				}
				
				buffer.reset();
			}
			
			buffer.clear();
		}

		buffer.close();
	}
	
}
