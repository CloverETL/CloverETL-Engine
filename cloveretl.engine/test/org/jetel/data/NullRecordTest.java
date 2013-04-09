package org.jetel.data;

import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.test.CloverTestCase;

public class NullRecordTest extends CloverTestCase {
	
	DataRecord nullRecord, record;
	DataRecordMetadata metadata;
	
	@Override
	protected void setUp() throws Exception {
		super.setUp();
		
		metadata = new DataRecordMetadata("meta");
		metadata.addField(new DataFieldMetadata("int_field", DataFieldMetadata.INTEGER_FIELD,";"));
		metadata.addField(new DataFieldMetadata("string_field", DataFieldMetadata.STRING_FIELD,";"));
		metadata.addField(new DataFieldMetadata("date_field", DataFieldMetadata.DATE_FIELD, "\n"));
		record = DataRecordFactory.newRecord(metadata);
		record.init();
		record.getField(0).setValue(1);
		record.getField(1).setValue("test");
		record.getField(2).setNull(true);
		nullRecord = NullRecord.NULL_RECORD;
		nullRecord.init();
	}

	public void testDelField() {
		DataRecord copy = nullRecord.duplicate();
		nullRecord.delField(3);
		assertEquals(copy, nullRecord);
	}

	public void testGetField() {
		assertNull(nullRecord.getField(3).getValue());
		assertNull(nullRecord.getField("field1").getValue());
	}

	public void testSetToDefaultValue() {
		nullRecord.setToDefaultValue();
		assertNull(nullRecord.getField(3).getValue());
		assertNull(nullRecord.getField("field1").getValue());
	}

	public void testSetToNullInt() {
		nullRecord.setToDefaultValue(3);
		assertNull(nullRecord.getField(3).getValue());
		assertNull(nullRecord.getField("field1").getValue());
	}

	public void testCopyFrom() {
		nullRecord.copyFrom(record);
		assertNull(nullRecord.getField(3).getValue());
		assertNull(nullRecord.getField("field1").getValue());
		nullRecord.copyFieldsByName(record);
		assertNull(nullRecord.getField(3).getValue());
		assertNull(nullRecord.getField("field1").getValue());
		nullRecord.copyFieldsByPosition(record);
		assertNull(nullRecord.getField(3).getValue());
		assertNull(nullRecord.getField("field1").getValue());
	}

	public void testCompareTo() {
		assertEquals(-1, nullRecord.compareTo(record));
		assertEquals(1, record.compareTo(nullRecord));
		assertEquals(nullRecord, nullRecord.duplicate());
	}

	public void testHasField() {
		assertTrue(nullRecord.hasField("field1"));
	}

	public void testGetNumFields() {
		assertEquals(0, nullRecord.getNumFields());
	}

	public void testIsNull() {
		assertTrue(nullRecord.isNull());
		try{
			nullRecord.getField(1).setNull(false);
			fail("Should throw IllegalArgumentException");
		}catch (IllegalArgumentException e){
			
		}
	}

	public void testGetMetadata() {
		DataRecordMetadata tmp = nullRecord.getMetadata().duplicate();
		tmp.addField(new DataFieldMetadata("num_field", DataFieldMetadata.NUMERIC_FIELD, ";"));
		assertEquals(nullRecord.getMetadata(),tmp);
	}


}
