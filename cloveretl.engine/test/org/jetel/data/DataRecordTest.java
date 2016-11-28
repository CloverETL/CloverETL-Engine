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

import java.util.Date;

import org.jetel.data.primitive.Decimal.OutOfPrecisionException;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataFieldType;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.test.CloverTestCase;
import org.jetel.util.bytes.CloverBuffer;

/**
 * Tests the DataRecord implementation.
 *
 * @author Martin Janik, Javlin a.s. &lt;martin.janik@javlin.eu&gt;
 *
 * @version 15th December 2010
 * @since 15th December 2010
 */
public class DataRecordTest extends CloverTestCase {

	private static final int NUMBER_OF_FIELDS = 16;
	private static final int NULL_FIELDS = 0xDDBA;

	private DataRecord record;
	
	private DataRecord copySource;
	private DataRecord copyTarget1;
	private DataRecord copyTarget2;

	@Override
	protected void setUp() throws Exception {
		super.setUp();

		DataRecordMetadata metadata = new DataRecordMetadata("record");

		for (int i = 0; i < NUMBER_OF_FIELDS; i++) {
			metadata.addField(new DataFieldMetadata("f" + i, DataFieldMetadata.INTEGER_FIELD, ";"));
		}

		record = DataRecordFactory.newRecord(metadata);

		for (int i = 0; i < NUMBER_OF_FIELDS; i++) {
			record.getField(i).setValue((((1 << i) & NULL_FIELDS) != 0) ? null : i);
		}
		
		////
		
		DataRecordMetadata copySourceMeta = new DataRecordMetadata("record");
		DataFieldMetadata sourceDecimal = new DataFieldMetadata("field1", DataFieldType.DECIMAL, ",");
		sourceDecimal.setProperty(DataFieldMetadata.LENGTH_ATTR, "8");
		sourceDecimal.setProperty(DataFieldMetadata.SCALE_ATTR, "3");
		copySourceMeta.addField(sourceDecimal);
		copySourceMeta.addField(new DataFieldMetadata("field2", DataFieldType.STRING, ","));
		copySourceMeta.addField(new DataFieldMetadata("field3", DataFieldType.INTEGER, ","));
		copySourceMeta.addField(new DataFieldMetadata("field4", DataFieldType.DATE, ","));
		copySourceMeta.addField(new DataFieldMetadata("field5", DataFieldType.BOOLEAN, ","));
		
		DataRecordMetadata copyTargetMeta1 = copySourceMeta.duplicate();
		DataRecordMetadata copyTargetMeta2 = copySourceMeta.duplicate();
		
		copyTargetMeta2.getField(0).setProperty(DataFieldMetadata.LENGTH_ATTR, "2");
		copyTargetMeta2.getField(0).setProperty(DataFieldMetadata.SCALE_ATTR, "2");
		
		copySource = DataRecordFactory.newRecord(copySourceMeta);
		copyTarget1 = DataRecordFactory.newRecord(copyTargetMeta1);
		copyTarget2 = DataRecordFactory.newRecord(copyTargetMeta2);
		
		copySource.getField(0).setValue(123.01);
		copySource.getField(1).setValue("hello");
		copySource.getField(2).setValue(3);
		copySource.getField(3).setValue(new Date());
		copySource.getField(4).setValue(true);
	}

	/**
	 * Test method for {@link org.jetel.data.DataRecord#serialize(java.nio.ByteBuffer)}.
	 */
	public void testSerialize() {
		DataRecord deserializedRecord = DataRecordFactory.newRecord(record.getMetadata());

		CloverBuffer buffer = CloverBuffer.allocateDirect(Defaults.Record.RECORD_INITIAL_SIZE);
		record.serialize(buffer);
		buffer.flip();
		deserializedRecord.deserialize(buffer);

		RecordKey comparator = new RecordKey(record.getMetadata().getFieldNamesArray(), record.getMetadata());
		comparator.setEqualNULLs(true);

		assertEquals(0, comparator.compare(record, deserializedRecord));
	}
	
	public void testCopyFieldsByName() {
		copyTarget1.copyFieldsByName(copySource);
		RecordKey comparator = new RecordKey(copySource.getMetadata().getFieldNamesArray(), copySource.getMetadata());
		comparator.setEqualNULLs(true);
		assertEquals(0, comparator.compare(copySource, copyTarget1));
		
		try {
			copyTarget2.copyFieldsByName(copySource);
			fail("copyFieldsByName was expected to throw exception");
		} catch (OutOfPrecisionException e) {
			// expected
		}
	}
	
	public void testCopyFieldsByPosition() {
		copyTarget1.copyFieldsByPosition(copySource);
		RecordKey comparator = new RecordKey(copySource.getMetadata().getFieldNamesArray(), copySource.getMetadata());
		comparator.setEqualNULLs(true);
		assertEquals(0, comparator.compare(copySource, copyTarget1));
		
		try {
			copyTarget2.copyFieldsByPosition(copySource);
			fail("copyFieldsByPosition was expected to throw exception");
		} catch (OutOfPrecisionException e) {
			// expected
		}
	}

}
