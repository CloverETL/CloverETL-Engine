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

import java.nio.ByteBuffer;

import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.test.CloverTestCase;

/**
 * @author Martin Janik, Javlin a.s. &lt;martin.janik@javlin.eu&gt;
 *
 * @version 15th December 2010
 * @since 15th December 2010
 */
public class DataRecordTest extends CloverTestCase {

	private static final String DELIMITER = ";";

	private DataRecord record;

	protected void setUp() {
		initEngine();
		Defaults.Record.USE_FIELDS_NULL_INDICATORS = true;

		DataRecordMetadata metadata = new DataRecordMetadata("record");
		metadata.addField(createDataField(DataFieldMetadata.STRING_FIELD, "string", false));
		metadata.addField(createDataField(DataFieldMetadata.STRING_FIELD, "nullString", true));
		metadata.addField(createDataField(DataFieldMetadata.INTEGER_FIELD, "integer", false));
		metadata.addField(createDataField(DataFieldMetadata.INTEGER_FIELD, "nullInteger", true));
		metadata.addField(createDataField(DataFieldMetadata.BOOLEAN_FIELD, "boolean", false));
		metadata.addField(createDataField(DataFieldMetadata.BOOLEAN_FIELD, "nullBoolean", true));

		record = new DataRecord(metadata);
		record.init();

		record.getField(0).setValue("String value");
		record.getField(2).setValue(73);
		record.getField(4).setValue(true);
	}

	private DataFieldMetadata createDataField(char dataType, String name, boolean nullable) {
		DataFieldMetadata field = new DataFieldMetadata(name, dataType, DELIMITER);
		field.setNullable(nullable);

		return field;
	}

	/**
	 * Test method for {@link org.jetel.data.DataRecord#serialize(java.nio.ByteBuffer)}.
	 */
	public void testSerializeByteBuffer() {
		DataRecord deserializedRecord = new DataRecord(record.getMetadata());
		deserializedRecord.init();

		ByteBuffer buffer = ByteBuffer.allocateDirect(Defaults.Record.MAX_RECORD_SIZE);
		record.serialize(buffer);
		buffer.flip();
		deserializedRecord.deserialize(buffer);

		assertEquals(record, deserializedRecord);
	}

}
