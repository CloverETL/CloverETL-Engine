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

import org.jetel.metadata.DataFieldMetadata;
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

	@Override
	protected void setUp() throws Exception {
		super.setUp();

		// make sure that the optimized serialization of null values is turned on
		Defaults.Record.USE_FIELDS_NULL_INDICATORS = true;

		DataRecordMetadata metadata = new DataRecordMetadata("record");

		for (int i = 0; i < NUMBER_OF_FIELDS; i++) {
			metadata.addField(new DataFieldMetadata("f" + i, DataFieldMetadata.INTEGER_FIELD, ";"));
		}

		record = DataRecordFactory.newRecord(metadata);
		record.init();

		for (int i = 0; i < NUMBER_OF_FIELDS; i++) {
			record.getField(i).setValue((((1 << i) & NULL_FIELDS) != 0) ? null : i);
		}
	}

	/**
	 * Test method for {@link org.jetel.data.DataRecord#serialize(java.nio.ByteBuffer)}.
	 */
	public void testSerialize() {
		DataRecord deserializedRecord = DataRecordFactory.newRecord(record.getMetadata());
		deserializedRecord.init();

		CloverBuffer buffer = CloverBuffer.allocateDirect(Defaults.Record.RECORD_INITIAL_SIZE);
		record.serialize(buffer);
		buffer.flip();
		deserializedRecord.deserialize(buffer);

		RecordKey comparator = new RecordKey(record.getMetadata().getFieldNamesArray(), record.getMetadata());
		comparator.setEqualNULLs(true);
		comparator.init();

		assertEquals(0, comparator.compare(record, deserializedRecord));
	}

}
