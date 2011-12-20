/*
 *    jETeL/Clover - Java based ETL application framework.
 *    Copyright (C) 2002-06  David Pavlis <david.pavlis@centrum.cz> and others.
 *    
 *    This library is free software; you can redistribute it and/or
 *    modify it under the terms of the GNU Lesser General Public
 *    License as published by the Free Software Foundation; either
 *    version 2.1 of the License, or (at your option) any later version.
 *    
 *    This library is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU    
 *    Lesser General Public License for more details.
 *    
 *    You should have received a copy of the GNU Lesser General Public
 *    License along with this library; if not, write to the Free Software
 *    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 * Created on 20.11.2006
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package org.jetel.data;

import java.io.IOException;

import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.test.CloverTestCase;
import org.jetel.util.bytes.CloverBuffer;

/**
 * @author david
 * @since  20.11.2006
 *
 * TODO To change the template for this generated type comment go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
public class DynamicRecordBufferTest extends CloverTestCase {

    DataRecordMetadata metadata;
    DataRecord record;
    DynamicRecordBuffer buffer;
    CloverBuffer byteBuffer1;
    CloverBuffer byteBuffer2;
    
    /* (non-Javadoc)
     * @see junit.framework.TestCase#setUp()
     */
    @Override
	protected void setUp() throws Exception {
		initEngine();

		buffer = new DynamicRecordBuffer(32000);
		buffer.init();
		
		byteBuffer1 = CloverBuffer.allocateDirect(Defaults.Record.RECORD_INITIAL_SIZE);
		byteBuffer2 = CloverBuffer.allocateDirect(Defaults.Record.RECORD_INITIAL_SIZE);
		
		byteBuffer1.asCharBuffer().append("THIS IS A TESTING DATA STRING THIS IS A TESTING DATA STRING");
		byteBuffer1.flip();
		byteBuffer2.asCharBuffer().append("THIS IS A TESTING DATA STRING *********************************************************** THIS IS A TESTING DATA STRING THIS IS A TESTING DATA STRING *****");
		byteBuffer2.flip();

		metadata = new DataRecordMetadata("in", DataRecordMetadata.DELIMITED_RECORD);

		metadata.addField(new DataFieldMetadata("Name", DataFieldMetadata.STRING_FIELD, ";"));
		metadata.addField(new DataFieldMetadata("Age", DataFieldMetadata.NUMERIC_FIELD, "|"));
		metadata.addField(new DataFieldMetadata("City", DataFieldMetadata.STRING_FIELD, "\n"));
		metadata.addField(new DataFieldMetadata("Born", DataFieldMetadata.DATE_FIELD, "\n"));
		metadata.addField(new DataFieldMetadata("Value", DataFieldMetadata.INTEGER_FIELD, "\n"));

		record = new DataRecord(metadata);
		record.init();

		SetVal.setString(record, 0, "  HELLO ");
		SetVal.setInt(record, 1, 135);
		SetVal.setString(record, 2, "Some silly longer string.");
		record.getField("Born").setNull(true);
		SetVal.setInt(record, 4, -999);
    }

  
    /**
     * Test method for {@link org.jetel.data.DynamicRecordBuffer#writeRecord(java.nio.ByteBuffer)}.
     * @throws InterruptedException 
     */
	public void testWriteRecord() throws IOException, InterruptedException {
		// first, write 100 records
		for (int i = 0; i < 3000; i++) {
			byteBuffer1.clear();
			int limit = byteBuffer1.asCharBuffer().put(String.valueOf(i)).put("THIS IS A TESTING DATA STRING THIS IS A TESTING DATA STRING").position();
			byteBuffer1.flip();
			byteBuffer1.limit(limit * 2);
			buffer.writeRecord(byteBuffer1);
			// byteBuffer1.rewind();
			// buffer.writeRecord(byteBuffer2);
			// byteBuffer2.rewind();
		}
		buffer.setEOF();
		System.out.println("has file:" + buffer.hasTempFile());
		System.out.println("buffered records:" + buffer.getBufferedRecords());
		// first, read records
		byteBuffer1.clear();
		int i = 0;
		while (buffer.readRecord(byteBuffer1)) {
			System.out.println(++i + ":" + byteBuffer1.asCharBuffer().toString());
			byteBuffer1.clear();
		}
		buffer.close();
	}
    
    /**
     * Test method for {@link org.jetel.data.DynamicRecordBuffer#readRecod(java.nio.ByteBuffer)}.
     * @throws InterruptedException 
     */
    public void testReadRecord() throws IOException, InterruptedException{
		for (int i = 0; i < 900; i++) {
			buffer.writeRecord(record);
		}
		System.out.println("has file:" + buffer.hasTempFile());
		System.out.println("buffered records:" + buffer.getBufferedRecords());
		int count;
		// WARNING, if the buffer didn't cache out more than 500 records, the following loop will freeze the test!
		// this depends on the MAX_RECORD_SIZE constant, increasing it may make this test unstable (blocked)
		for (count = 0; count < 500; count++) {
			if (buffer.readRecord(record) == null)
				break;
			System.out.println("**** " + count + "****");
			System.out.print(record);
		}
		for (int i = 0; i < 1200; i++) {
			buffer.writeRecord(record);
		}
		buffer.setEOF();
		System.out.println("has file:" + buffer.hasTempFile());
		System.out.println("buffered records:" + buffer.getBufferedRecords());
		for (int i = 0;; i++) {
			if (buffer.readRecord(record) == null)
				break;
			System.out.println("**** " + i + "****");
			System.out.print(record);
		}

		buffer.close();
    }

    /**
     * Test method for {@link org.jetel.data.DynamicRecordBuffer#isEmpty()}.
     */
    public void testIsEmpty() {
    }

}
