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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.jetel.data.DataRecordMap.DataRecordIterator;
import org.jetel.data.DataRecordMap.DataRecordLookup;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataFieldType;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.test.CloverTestCase;

/**
 * @author martin (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 30. 8. 2016
 */
public class DataRecordMapTest extends CloverTestCase {

	private static DataRecordMetadata metadata;
	private static RecordKey recordKey;
	private static RecordKey recordKeyAllFields;
	
	private static RecordKey getMapKey() {
		if (recordKey == null) {
			recordKey = new RecordKey(new String[] {"key1", "key2"}, getMetadata());
			recordKey.setEqualNULLs(true);
		}
		return recordKey;
	}

	private static RecordKey getAllFieldsKey() {
		if (recordKeyAllFields == null) {
			recordKeyAllFields = new RecordKey(new String[] {"key1", "key2", "data1", "data2"}, getMetadata());
			recordKeyAllFields.setEqualNULLs(true);
		}
		return recordKeyAllFields;
	}

	private static DataRecordMetadata getMetadata() {
		if (metadata == null) {
			metadata = new DataRecordMetadata("record");
			metadata.addField(new DataFieldMetadata("key1", DataFieldType.STRING, "|"));
			metadata.addField(new DataFieldMetadata("data1", DataFieldType.INTEGER, "|"));
			metadata.addField(new DataFieldMetadata("key2", DataFieldType.BOOLEAN, "|"));
			metadata.addField(new DataFieldMetadata("data2", DataFieldType.STRING, "|"));
		}
		return metadata;
	}

	private static DataRecord createRecord(String key1, boolean key2, int data1, String data2) {
		DataRecord record = DataRecordFactory.newRecord(getMetadata());
		record.getField("key1").setValue(key1);
		record.getField("key2").setValue(key2);
		record.getField("data1").setValue(data1);
		record.getField("data2").setValue(data2);
		return record;
	}

	private static DataRecord createKeyRecord(String key1, boolean key2) {
		DataRecord record = DataRecordFactory.newRecord(getMetadata());
		record.getField("key1").setValue(key1);
		record.getField("key2").setValue(key2);
		return record;
	}

	public void testPutAndGet1() {
		DataRecordMap map = new DataRecordMap(getMapKey(), true);
		
		assertEquals(0, map.size());
		
		DataRecord inRecord1 = createRecord("a", true, 1, "data1");
		map.put(inRecord1);
		DataRecord outRecord = map.get(getMapKey(), createKeyRecord("a", true));
		assertTrue(getAllFieldsKey().equals(inRecord1, outRecord));
		assertEquals(1, map.size());
		
		DataRecord inRecord2 = createRecord("a", false, 2, "data2");
		map.put(inRecord2);
		outRecord = map.get(getMapKey(), createKeyRecord("a", true));
		assertTrue(getAllFieldsKey().equals(inRecord1, outRecord));
		outRecord = map.get(getMapKey(), createKeyRecord("a", false));
		assertTrue(getAllFieldsKey().equals(inRecord2, outRecord));
		assertEquals(2, map.size());

		DataRecord inRecord3 = createRecord("a", true, 3, "data3");
		map.put(inRecord3);
		DataRecordIterator outRecords = map.getAll(getMapKey(), createKeyRecord("a", true));
		assertEquals(2, outRecords.size());
		assertTrue(getAllFieldsKey().equals(inRecord1, outRecords.next()));
		assertTrue(getAllFieldsKey().equals(inRecord3, outRecords.next()));
		outRecord = map.get(getMapKey(), createKeyRecord("a", false));
		assertTrue(getAllFieldsKey().equals(inRecord2, outRecord));
		assertEquals(3, map.size());

		DataRecord inRecord4 = createRecord("a", true, 4, "data4");
		map.put(inRecord4);
		outRecords = map.getAll(getMapKey(), createKeyRecord("a", true));
		assertEquals(3, outRecords.size());
		assertTrue(getAllFieldsKey().equals(inRecord1, outRecords.next()));
		assertTrue(getAllFieldsKey().equals(inRecord3, outRecords.next()));
		assertTrue(getAllFieldsKey().equals(inRecord4, outRecords.next()));
		outRecord = map.get(getMapKey(), createKeyRecord("a", false));
		assertTrue(getAllFieldsKey().equals(inRecord2, outRecord));
		assertEquals(4, map.size());

		DataRecord inRecord5 = createRecord("a", false, 5, "data5");
		map.put(inRecord5);
		outRecords = map.getAll(getMapKey(), createKeyRecord("a", true));
		assertEquals(3, outRecords.size());
		assertTrue(getAllFieldsKey().equals(inRecord1, outRecords.next()));
		assertTrue(getAllFieldsKey().equals(inRecord3, outRecords.next()));
		assertTrue(getAllFieldsKey().equals(inRecord4, outRecords.next()));
		outRecords = map.getAll(getMapKey(), createKeyRecord("a", false));
		assertEquals(2, outRecords.size());
		assertTrue(getAllFieldsKey().equals(inRecord2, outRecords.next()));
		assertTrue(getAllFieldsKey().equals(inRecord5, outRecords.next()));
		assertEquals(5, map.size());
	}

	public void testPutAndGet2() {
		DataRecordMap map = new DataRecordMap(getMapKey(), false, false);
		
		assertEquals(0, map.size());

		DataRecord inRecord1 = createRecord("a", true, 1, "data1");
		map.put(inRecord1);
		DataRecord outRecord = map.get(getMapKey(), createKeyRecord("a", true));
		assertTrue(getAllFieldsKey().equals(inRecord1, outRecord));
		assertEquals(1, map.size());
		
		DataRecord inRecord2 = createRecord("a", false, 2, "data2");
		map.put(inRecord2);
		outRecord = map.get(getMapKey(), createKeyRecord("a", true));
		assertTrue(getAllFieldsKey().equals(inRecord1, outRecord));
		outRecord = map.get(getMapKey(), createKeyRecord("a", false));
		assertTrue(getAllFieldsKey().equals(inRecord2, outRecord));
		assertEquals(2, map.size());

		DataRecord inRecord3 = createRecord("a", true, 3, "data3");
		map.put(inRecord3);
		DataRecordIterator outRecords = map.getAll(getMapKey(), createKeyRecord("a", true));
		assertEquals(1, outRecords.size());
		assertTrue(getAllFieldsKey().equals(inRecord1, outRecords.next()));
		outRecord = map.get(getMapKey(), createKeyRecord("a", false));
		assertTrue(getAllFieldsKey().equals(inRecord2, outRecord));
		assertEquals(2, map.size());

		DataRecord inRecord4 = createRecord("a", true, 4, "data4");
		map.put(inRecord4);
		outRecords = map.getAll(getMapKey(), createKeyRecord("a", true));
		assertEquals(1, outRecords.size());
		assertTrue(getAllFieldsKey().equals(inRecord1, outRecords.next()));
		outRecord = map.get(getMapKey(), createKeyRecord("a", false));
		assertTrue(getAllFieldsKey().equals(inRecord2, outRecord));
		assertEquals(2, map.size());

		DataRecord inRecord5 = createRecord("a", false, 5, "data5");
		map.put(inRecord5);
		outRecords = map.getAll(getMapKey(), createKeyRecord("a", true));
		assertEquals(1, outRecords.size());
		assertTrue(getAllFieldsKey().equals(inRecord1, outRecords.next()));
		outRecords = map.getAll(getMapKey(), createKeyRecord("a", false));
		assertEquals(1, outRecords.size());
		assertTrue(getAllFieldsKey().equals(inRecord2, outRecords.next()));
		assertEquals(2, map.size());
	}

	public void testPutAndGet3() {
		DataRecordMap map = new DataRecordMap(getMapKey(), false, true);
		
		assertEquals(0, map.size());

		DataRecord inRecord1 = createRecord("a", true, 1, "data1");
		map.put(inRecord1);
		DataRecord outRecord = map.get(getMapKey(), createKeyRecord("a", true));
		assertTrue(getAllFieldsKey().equals(inRecord1, outRecord));
		assertEquals(1, map.size());
		
		DataRecord inRecord2 = createRecord("a", false, 2, "data2");
		map.put(inRecord2);
		outRecord = map.get(getMapKey(), createKeyRecord("a", true));
		assertTrue(getAllFieldsKey().equals(inRecord1, outRecord));
		outRecord = map.get(getMapKey(), createKeyRecord("a", false));
		assertTrue(getAllFieldsKey().equals(inRecord2, outRecord));
		assertEquals(2, map.size());

		DataRecord inRecord3 = createRecord("a", true, 3, "data3");
		map.put(inRecord3);
		DataRecordIterator outRecords = map.getAll(getMapKey(), createKeyRecord("a", true));
		assertEquals(1, outRecords.size());
		assertTrue(getAllFieldsKey().equals(inRecord3, outRecords.next()));
		outRecord = map.get(getMapKey(), createKeyRecord("a", false));
		assertTrue(getAllFieldsKey().equals(inRecord2, outRecord));
		assertEquals(2, map.size());

		DataRecord inRecord4 = createRecord("a", true, 4, "data4");
		map.put(inRecord4);
		outRecords = map.getAll(getMapKey(), createKeyRecord("a", true));
		assertEquals(1, outRecords.size());
		assertTrue(getAllFieldsKey().equals(inRecord4, outRecords.next()));
		outRecord = map.get(getMapKey(), createKeyRecord("a", false));
		assertTrue(getAllFieldsKey().equals(inRecord2, outRecord));
		assertEquals(2, map.size());

		DataRecord inRecord5 = createRecord("a", false, 5, "data5");
		map.put(inRecord5);
		outRecords = map.getAll(getMapKey(), createKeyRecord("a", true));
		assertEquals(1, outRecords.size());
		assertTrue(getAllFieldsKey().equals(inRecord4, outRecords.next()));
		outRecords = map.getAll(getMapKey(), createKeyRecord("a", false));
		assertEquals(1, outRecords.size());
		assertTrue(getAllFieldsKey().equals(inRecord5, outRecords.next()));
		assertEquals(2, map.size());
	}

	public void testRemove() {
		DataRecordMap map = new DataRecordMap(getMapKey(), true);
		
		assertFalse(map.remove(createRecord("a", true, 1, "data1")));
		assertEquals(0, map.size());

		DataRecord inRecord1 = createRecord("a", true, 1, "data1");
		map.put(inRecord1);
		assertTrue(map.remove(createRecord("a", true, 3, "data3")));
		assertEquals(0, map.size());
		assertNull(map.get(getMapKey(), createKeyRecord("a", true)));

		inRecord1 = createRecord("a", true, 1, "data1x");
		map.put(inRecord1);
		
		DataRecord inRecord2 = createRecord("b", true, 2, "data2");
		map.put(inRecord2);
		DataRecord inRecord3 = createRecord("b", true, 3, "data3");
		map.put(inRecord3);
		DataRecord inRecord4 = createRecord("b", true, 4, "data4");
		map.put(inRecord4);
		assertEquals(4, map.size());
		assertFalse(map.remove(createRecord("b", false, 3, "data3")));
		assertEquals(4, map.size());
		assertTrue(map.remove(createRecord("b", true, -3, "-data3")));
		assertEquals(1, map.size());
		DataRecord outRecord = map.get(getMapKey(), createKeyRecord("a", true));
		assertTrue(getAllFieldsKey().equals(inRecord1, outRecord));
	}

	public void testDataRecordLookup() {
		DataRecordMap map = new DataRecordMap(getMapKey(), true);
		
		DataRecord inRecord1 = createRecord("a", true, 1, "data1");
		map.put(inRecord1);
		DataRecord inRecord2 = createRecord("a", true, 2, "data2");
		map.put(inRecord2);
		DataRecord inRecord3 = createRecord("b", false, 3, "data3");
		map.put(inRecord3);
		DataRecord inRecord4 = createRecord("b", true, 4, "data4");
		map.put(inRecord4);
		DataRecord inRecord5 = createRecord("b", true, 5, "data5");
		map.put(inRecord5);
		DataRecord inRecord6 = createRecord("b", true, 6, "data6");
		map.put(inRecord6);
		
		DataRecordLookup lookup = map.createDataRecordLookup(getMapKey(), createRecord("c", true, -1, null));
		assertNull(lookup.get());
		assertNull(lookup.getAndMark());
		assertNull(lookup.getAll());
		assertNull(lookup.getAllAndMark());
		assertNull(lookup.getAll());

		lookup = map.createDataRecordLookup(getMapKey(), createRecord("b", true, -1, null));
		assertTrue(getAllFieldsKey().equals(inRecord4, lookup.get()));
		assertTrue(getAllFieldsKey().equals(inRecord4, lookup.get()));
		assertTrue(getAllFieldsKey().equals(inRecord4, lookup.getAndMark()));
		assertTrue(getAllFieldsKey().equals(inRecord4, lookup.getAndMark()));
		DataRecordIterator iterator = lookup.getAll();
		for (int i = 0; i < 3; i++) {
			assertEquals(3, iterator.size());
			assertTrue(iterator.hasNext());
			assertTrue(getAllFieldsKey().equals(inRecord4, iterator.next()));
			assertTrue(iterator.hasNext());
			assertTrue(getAllFieldsKey().equals(inRecord5, iterator.next()));
			assertTrue(iterator.hasNext());
			assertTrue(getAllFieldsKey().equals(inRecord6, iterator.next()));
			assertFalse(iterator.hasNext());
			try {
				iterator.next();
				assertTrue(false);
			} catch (NoSuchElementException e) {
				//ok
			}
			iterator.reset();
		}
		
		lookup.setDataRecord(createRecord("b", false, 123, "data2"));
		assertTrue(getAllFieldsKey().equals(inRecord3, lookup.get()));
		assertTrue(getAllFieldsKey().equals(inRecord3, lookup.get()));
		assertTrue(getAllFieldsKey().equals(inRecord3, lookup.getAndMark()));
		assertTrue(getAllFieldsKey().equals(inRecord3, lookup.getAndMark()));
		iterator = lookup.getAll();
		for (int i = 0; i < 3; i++) {
			assertEquals(1, iterator.size());
			assertTrue(iterator.hasNext());
			assertTrue(getAllFieldsKey().equals(inRecord3, iterator.next()));
			assertFalse(iterator.hasNext());
			try {
				iterator.next();
				assertTrue(false);
			} catch (NoSuchElementException e) {
				//ok
			}
			iterator.reset();
		}
	}

	public void testClear() {
		DataRecordMap map = new DataRecordMap(getMapKey(), true);
		
		DataRecord inRecord1 = createRecord("a", true, 1, "data1");
		map.put(inRecord1);
		DataRecord inRecord2 = createRecord("a", true, 2, "data2");
		map.put(inRecord2);
		DataRecord inRecord3 = createRecord("b", false, 3, "data3");
		map.put(inRecord3);
		DataRecord inRecord4 = createRecord("b", true, 4, "data4");
		map.put(inRecord4);
		DataRecord inRecord5 = createRecord("b", true, 5, "data5");
		map.put(inRecord5);
		DataRecord inRecord6 = createRecord("b", true, 6, "data6");
		map.put(inRecord6);

		map.clear();
		assertEquals(0, map.size());
		assertNull(map.get(getMapKey(), createKeyRecord("a", true)));
	}

	public void testValueIterator() {
		DataRecordMap map = new DataRecordMap(getMapKey(), true);

		Iterator<DataRecord> iterator = map.valueIterator();
		assertFalse(iterator.hasNext());
		try {
			iterator.next();
			assertTrue(false);
		} catch (NoSuchElementException e) {
			//ok
		}

		DataRecordSet dataRecordSet = new DataRecordSet();
		map.put(dataRecordSet.addRecord(createRecord("a", true, 1, "data1")));
		map.put(dataRecordSet.addRecord(createRecord("a", true, 2, "data2")));
		map.put(dataRecordSet.addRecord(createRecord("b", false, 3, "data3")));
		map.put(dataRecordSet.addRecord(createRecord("b", true, 4, "data4")));
		map.put(dataRecordSet.addRecord(createRecord("b", true, 5, "data5")));
		map.put(dataRecordSet.addRecord(createRecord("b", true, 6, "data6")));

		
		iterator = map.valueIterator();
		int counter = 0;
		while (iterator.hasNext()) {
			counter++;
			dataRecordSet.assertContains(iterator.next());
		}
		assertEquals(6, counter);
		try {
			iterator.next();
			assertTrue(false);
		} catch (NoSuchElementException e) {
			//ok
		}
	}

	private class DataRecordSet {
		private List<DataRecord> records = new ArrayList<>();
		
		public DataRecord addRecord(DataRecord record) {
			records.add(record);
			return record;
		}
		
		public void assertContains(DataRecord expectedRecord) {
			for (DataRecord record : records) {
				if (getAllFieldsKey().equals(expectedRecord, record)) {
					return;
				}
			}
			assertTrue(false);
		}
	}
	
}
