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
import java.nio.charset.CharacterCodingException;
import java.util.AbstractMap;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;

import org.jetel.data.primitive.Decimal;
import org.jetel.data.primitive.IntegerDecimal;
import org.jetel.metadata.DataFieldCardinalityType;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataFieldType;
import org.jetel.test.CloverTestCase;
import org.jetel.util.bytes.CloverBuffer;
import org.jetel.util.string.CloverString;

/**
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 31 Jan 2012
 */
public class MapDataFieldTest extends CloverTestCase {

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		initEngine();
	}
	
	private DataFieldMetadata createMapMetadata() {
		return createMapMetadata(DataFieldType.STRING);
	}
	
	private DataFieldMetadata createMapMetadata(DataFieldType type) {
		DataFieldMetadata mapDataFieldMetadata = new DataFieldMetadata("mapDataField", ";");
		mapDataFieldMetadata.setDataType(type);
		mapDataFieldMetadata.setCardinalityType(DataFieldCardinalityType.MAP);
		return mapDataFieldMetadata;
	}

	public void testConstructor() {
		MapDataField mapDataField = new MapDataField(createMapMetadata());
		
		assertFalse(mapDataField.isPlain());
	}

	public void testGetSize() {
		MapDataField mapDataField = new MapDataField(createMapMetadata());
		
		assertEquals(0, mapDataField.getSize());
		
		mapDataField.putField("key");
		assertEquals(1, mapDataField.getSize());
		
		mapDataField.removeField("key");
		assertEquals(0, mapDataField.getSize());
		
		mapDataField.putField("key");
		assertEquals(1, mapDataField.getSize());
		mapDataField.clear();
		assertEquals(0, mapDataField.getSize());

		mapDataField.putField("key1");
		mapDataField.putField("key2");
		mapDataField.reset();
		assertEquals(0, mapDataField.getSize());

		mapDataField.putField("key1");
		mapDataField.putField("key2");
		mapDataField.putField("key3");
		assertEquals(3, mapDataField.getSize());
	}
	
	public void testSetNull() {
		MapDataField mapDataField = new MapDataField(createMapMetadata());
		
		mapDataField.setNull(true);
		assertTrue(mapDataField.isNull());

		mapDataField.setNull(false);
		assertFalse(mapDataField.isNull());

		mapDataField.putField("key1");
		mapDataField.putField("key2");
		mapDataField.setNull(true);
		assertTrue(mapDataField.isNull());

		mapDataField.putField("key1");
		mapDataField.putField("key2");
		mapDataField.putField("key3");
		mapDataField.setNull(false);
		assertFalse(mapDataField.isNull());
	}
	
	public void testSetToDefaultValue() {
		MapDataField mapDataField = new MapDataField(createMapMetadata());
		
		mapDataField.setNull(true);
		mapDataField.setToDefaultValue();
		assertTrue(mapDataField.isNull());

		mapDataField.setNull(false);
		mapDataField.setToDefaultValue();
		assertTrue(mapDataField.isNull());

		mapDataField.putField("key");
		mapDataField.setToDefaultValue();
		assertTrue(mapDataField.isNull());

		mapDataField.putField("key1");
		mapDataField.putField("key2");
		mapDataField.putField("key3");
		mapDataField.setToDefaultValue();
		assertTrue(mapDataField.isNull());

		mapDataField.metadata.setNullable(false);

		mapDataField.setNull(true);
		mapDataField.setToDefaultValue();
		assertFalse(mapDataField.isNull());
		assertEquals(0, mapDataField.getSize());

		mapDataField.setNull(false);
		mapDataField.setToDefaultValue();
		assertFalse(mapDataField.isNull());
		assertEquals(0, mapDataField.getSize());

		mapDataField.putField("key");
		mapDataField.setToDefaultValue();
		assertFalse(mapDataField.isNull());
		assertEquals(0, mapDataField.getSize());

		mapDataField.putField("key1");
		mapDataField.putField("key2");
		mapDataField.putField("key3");
		mapDataField.setToDefaultValue();
		assertFalse(mapDataField.isNull());
		assertEquals(0, mapDataField.getSize());
	}

	public void testContainsField() {
		MapDataField mapDataField = new MapDataField(createMapMetadata());
		
		mapDataField.setNull(true);
		assertFalse(mapDataField.containsField(null));
		assertFalse(mapDataField.containsField(""));
		assertFalse(mapDataField.containsField("key"));
		
		mapDataField.setNull(false);
		assertFalse(mapDataField.containsField(null));
		assertFalse(mapDataField.containsField(""));
		assertFalse(mapDataField.containsField("key"));
		
		mapDataField.putField("key1");
		assertFalse(mapDataField.containsField(null));
		assertFalse(mapDataField.containsField(""));
		assertFalse(mapDataField.containsField("key"));
		assertTrue(mapDataField.containsField("key1"));

		mapDataField.putField("key2");
		mapDataField.putField("key3");
		assertFalse(mapDataField.containsField(null));
		assertFalse(mapDataField.containsField(""));
		assertFalse(mapDataField.containsField("key"));
		assertTrue(mapDataField.containsField("key1"));
		assertTrue(mapDataField.containsField("key2"));
		assertTrue(mapDataField.containsField("key3"));
	}
	
	public void testPutField() {
		MapDataField mapDataField = new MapDataField(createMapMetadata());
		
		mapDataField.setNull(true);
		mapDataField.putField(null).setValue("value1");
		assertFalse(mapDataField.isNull());
		assertEquals(1, mapDataField.getSize());
		assertEquals("value1", mapDataField.getField(null).getValue().toString());

		mapDataField.putField("").setValue("value2");
		assertFalse(mapDataField.isNull());
		assertEquals(2, mapDataField.getSize());
		assertEquals("value1", mapDataField.getField(null).getValue().toString());
		assertEquals("value2", mapDataField.getField("").getValue().toString());

		mapDataField.putField("key").setValue("value3");
		assertFalse(mapDataField.isNull());
		assertEquals(3, mapDataField.getSize());
		assertEquals("value1", mapDataField.getField(null).getValue().toString());
		assertEquals("value2", mapDataField.getField("").getValue().toString());
		assertEquals("value3", mapDataField.getField("key").getValue().toString());

		mapDataField.putField(null).setValue("value4");
		assertFalse(mapDataField.isNull());
		assertEquals(3, mapDataField.getSize());
		assertEquals("value4", mapDataField.getField(null).getValue().toString());
		assertEquals("value2", mapDataField.getField("").getValue().toString());
		assertEquals("value3", mapDataField.getField("key").getValue().toString());
		
		mapDataField.reset();
		mapDataField.setNull(false);
		mapDataField.putField("key").setValue("value5");
		assertFalse(mapDataField.isNull());
		assertEquals(1, mapDataField.getSize());
		assertEquals("value5", mapDataField.getField("key").getValue().toString());
		
		mapDataField.putField("").setValue("value6");
		assertFalse(mapDataField.isNull());
		assertEquals(2, mapDataField.getSize());
		assertEquals("value5", mapDataField.getField("key").getValue().toString());
		assertEquals("value6", mapDataField.getField("").getValue().toString());
		
		mapDataField.putField(null).setValue("value7");
		assertFalse(mapDataField.isNull());
		assertEquals(3, mapDataField.getSize());
		assertEquals("value5", mapDataField.getField("key").getValue().toString());
		assertEquals("value6", mapDataField.getField("").getValue().toString());
		assertEquals("value7", mapDataField.getField(null).getValue().toString());
		
		mapDataField.putField("key").setValue("value8");
		assertFalse(mapDataField.isNull());
		assertEquals(3, mapDataField.getSize());
		assertEquals("value8", mapDataField.getField("key").getValue().toString());
		assertEquals("value6", mapDataField.getField("").getValue().toString());
		assertEquals("value7", mapDataField.getField(null).getValue().toString());
	}
	
	public void testRemoveField() {
		MapDataField mapDataField = new MapDataField(createMapMetadata());
		
		mapDataField.setNull(true);
		assertNull(mapDataField.removeField(null));
		assertNull(mapDataField.removeField(""));
		assertNull(mapDataField.removeField("key"));
		assertEquals(0, mapDataField.getSize());
		assertTrue(mapDataField.isNull());

		mapDataField.setNull(false);
		assertNull(mapDataField.removeField(null));
		assertNull(mapDataField.removeField(""));
		assertNull(mapDataField.removeField("key"));
		assertEquals(0, mapDataField.getSize());
		assertFalse(mapDataField.isNull());

		DataField field = mapDataField.putField("key");
		field.setValue("value1");
		assertNull(mapDataField.removeField(null));
		assertNull(mapDataField.removeField(""));
		assertNull(mapDataField.removeField("neco"));
		assertTrue(field == mapDataField.removeField("key"));
		assertEquals("value1", field.getValue().toString());
		assertEquals(0, mapDataField.getSize());
		
		DataField field1 = mapDataField.putField("key1");
		field1.setValue("value2");
		DataField field2 = mapDataField.putField(null);
		field2.setValue("value3");
		DataField field3 = mapDataField.putField("");
		field3.setValue("value4");
		DataField field4 = mapDataField.putField("key2");
		field4.setValue("value5");
		assertNull(mapDataField.removeField("neco"));
		assertTrue(field4 == mapDataField.removeField("key2"));
		assertEquals("value5", field4.getValue().toString());
		assertEquals(3, mapDataField.getSize());
		assertTrue(field2 == mapDataField.removeField(null));
		assertEquals("value3", field2.getValue().toString());
		assertEquals(2, mapDataField.getSize());
		assertTrue(field3 == mapDataField.removeField(""));
		assertEquals("value4", field3.getValue().toString());
		assertEquals(1, mapDataField.getSize());
		assertTrue(field1 == mapDataField.removeField("key1"));
		assertEquals("value2", field1.getValue().toString());
		assertEquals(0, mapDataField.getSize());
	}
	
	public void testGetField() {
		MapDataField mapDataField = new MapDataField(createMapMetadata());

		mapDataField.setNull(true);
		assertNull(mapDataField.getField(null));
		assertNull(mapDataField.getField(""));
		assertNull(mapDataField.getField("key"));
		
		mapDataField.setNull(false);
		assertNull(mapDataField.getField(null));
		assertNull(mapDataField.getField(""));
		assertNull(mapDataField.getField("key"));

		DataField field1 = mapDataField.putField("key");
		field1.setValue("value1");
		assertNull(mapDataField.getField(null));
		assertNull(mapDataField.getField(""));
		assertNull(mapDataField.getField("neco"));
		assertTrue(field1 == mapDataField.getField("key"));
		assertEquals("value1", field1.toString());
		
		DataField field2 = mapDataField.putField("");
		field2.setValue("value2");
		DataField field3 = mapDataField.putField(null);
		field3.setValue("value3");
		assertNull(mapDataField.getField("neco"));
		assertTrue(field3 == mapDataField.getField(null));
		assertEquals("value3", field3.toString());
		assertTrue(field1 == mapDataField.getField("key"));
		assertEquals("value1", field1.toString());
		assertTrue(field2 == mapDataField.getField(""));
		assertEquals("value2", field2.toString());
		
		mapDataField.removeField("");
		assertNull(mapDataField.getField("neco"));
		assertNull(mapDataField.getField(""));
		assertTrue(field3 == mapDataField.getField(null));
		assertEquals("value3", field3.toString());
		assertTrue(field1 == mapDataField.getField("key"));
		assertEquals("value1", field1.toString());
	}
	
	public void testClear() {
		MapDataField mapDataField = new MapDataField(createMapMetadata());
		
		mapDataField.setNull(true);
		mapDataField.clear();
		assertTrue(mapDataField.isNull());
		assertEquals(0, mapDataField.getSize());
		
		mapDataField.setNull(false);
		mapDataField.clear();
		assertFalse(mapDataField.isNull());
		assertEquals(0, mapDataField.getSize());
		
		mapDataField.putField("key");
		mapDataField.clear();
		assertFalse(mapDataField.isNull());
		assertEquals(0, mapDataField.getSize());

		mapDataField.putField("");
		mapDataField.putField(null);
		mapDataField.putField("key");
		mapDataField.putField("key1");
		mapDataField.clear();
		assertFalse(mapDataField.isNull());
		assertEquals(0, mapDataField.getSize());
	}
	
	public void testDuplicate() {
		MapDataField mapDataField = new MapDataField(createMapMetadata());
		MapDataField dupField;
		
		mapDataField.setNull(true);
		dupField = mapDataField.duplicate();
		
		assertTrue(mapDataField != dupField);
		assertTrue(mapDataField.isNull());
		assertTrue(dupField.isNull());
		assertEquals(0, mapDataField.getSize());
		assertEquals(0, dupField.getSize());
		
		mapDataField.setNull(false);
		dupField = mapDataField.duplicate();
		
		assertTrue(mapDataField != dupField);
		assertFalse(mapDataField.isNull());
		assertFalse(dupField.isNull());
		assertEquals(0, mapDataField.getSize());
		assertEquals(0, dupField.getSize());

		mapDataField.putField("key").setValue("value1");
		dupField = mapDataField.duplicate();
		
		assertTrue(mapDataField != dupField);
		assertTrue(mapDataField.getField("key") != dupField.getField("key"));
		assertFalse(mapDataField.isNull());
		assertFalse(dupField.isNull());
		assertEquals(1, mapDataField.getSize());
		assertEquals(1, dupField.getSize());
		assertEquals(mapDataField.getField("key").getValue(), dupField.getField("key").getValue());

		mapDataField.putField(null).setValue("value2");
		mapDataField.putField("").setValue("value3");
		mapDataField.putField("key1").setValue("value4");
		dupField = mapDataField.duplicate();

		assertTrue(mapDataField != dupField);
		assertTrue(mapDataField.getField("key") != dupField.getField("key"));
		assertTrue(mapDataField.getField(null) != dupField.getField(null));
		assertTrue(mapDataField.getField("") != dupField.getField(""));
		assertTrue(mapDataField.getField("key1") != dupField.getField("key1"));
		assertFalse(mapDataField.isNull());
		assertFalse(dupField.isNull());
		assertEquals(4, mapDataField.getSize());
		assertEquals(4, dupField.getSize());
		assertEquals(mapDataField.getField("key").getValue(), dupField.getField("key").getValue());
		assertEquals(mapDataField.getField(null).getValue(), dupField.getField(null).getValue());
		assertEquals(mapDataField.getField("").getValue(), dupField.getField("").getValue());
		assertEquals(mapDataField.getField("key1").getValue(), dupField.getField("key1").getValue());
	}
	
	public void testSetValue1() {
		MapDataField mapDataField = new MapDataField(createMapMetadata());
		
		mapDataField.setNull(true);
		mapDataField.setValue((Object) null);
		assertTrue(mapDataField.isNull());
		assertEquals(0, mapDataField.getSize());

		mapDataField.setNull(false);
		mapDataField.setValue((Object) null);
		assertTrue(mapDataField.isNull());
		assertEquals(0, mapDataField.getSize());

		mapDataField.putField("key1");
		mapDataField.putField("key2");
		mapDataField.putField("key3");
		mapDataField.setValue((Object) null);
		assertTrue(mapDataField.isNull());
		assertEquals(0, mapDataField.getSize());

		
		
		mapDataField.setNull(true);
		mapDataField.setValue((Object) new HashMap<String, Object>());
		assertFalse(mapDataField.isNull());
		assertEquals(0, mapDataField.getSize());

		mapDataField.setNull(false);
		mapDataField.setValue((Object) new HashMap<String, Object>());
		assertFalse(mapDataField.isNull());
		assertEquals(0, mapDataField.getSize());

		mapDataField.putField("key1");
		mapDataField.putField("key2");
		mapDataField.putField("key3");
		mapDataField.setValue((Object) new HashMap<String, Object>());
		assertFalse(mapDataField.isNull());
		assertEquals(0, mapDataField.getSize());

		

		Map<String, String> stringMap = new HashMap<String, String>();
		stringMap.put("key1", "value1");
		stringMap.put("key2", "value2");
		stringMap.put("key3", "value3");
		
		mapDataField.setNull(true);
		mapDataField.setValue((Object) stringMap);
		assertFalse(mapDataField.isNull());
		assertEquals(3, mapDataField.getSize());
		assertEquals("value1", mapDataField.getField("key1").getValue().toString());
		assertEquals("value2", mapDataField.getField("key2").getValue().toString());
		assertEquals("value3", mapDataField.getField("key3").getValue().toString());
		
		mapDataField.setNull(false);
		mapDataField.setValue((Object) stringMap);
		assertFalse(mapDataField.isNull());
		assertEquals(3, mapDataField.getSize());
		assertEquals("value1", mapDataField.getField("key1").getValue().toString());
		assertEquals("value2", mapDataField.getField("key2").getValue().toString());
		assertEquals("value3", mapDataField.getField("key3").getValue().toString());

		mapDataField.putField("key1");
		mapDataField.putField("key2");
		mapDataField.putField("key3");
		mapDataField.setValue((Object) stringMap);
		assertFalse(mapDataField.isNull());
		assertEquals(3, mapDataField.getSize());
		assertEquals("value1", mapDataField.getField("key1").getValue().toString());
		assertEquals("value2", mapDataField.getField("key2").getValue().toString());
		assertEquals("value3", mapDataField.getField("key3").getValue().toString());
	}

	public void testSetValue2() {
		MapDataField mapDataField = new MapDataField(createMapMetadata());
		
		mapDataField.setNull(true);
		mapDataField.setValue((Map<?, ?>) null);
		assertTrue(mapDataField.isNull());
		assertEquals(0, mapDataField.getSize());

		mapDataField.setNull(false);
		mapDataField.setValue((Map<?, ?>)  null);
		assertTrue(mapDataField.isNull());
		assertEquals(0, mapDataField.getSize());

		mapDataField.putField("key1");
		mapDataField.putField("key2");
		mapDataField.putField("key3");
		mapDataField.setValue((Map<?, ?>)  null);
		assertTrue(mapDataField.isNull());
		assertEquals(0, mapDataField.getSize());

		
		
		mapDataField.setNull(true);
		mapDataField.setValue(new HashMap<String, Object>());
		assertFalse(mapDataField.isNull());
		assertEquals(0, mapDataField.getSize());

		mapDataField.setNull(false);
		mapDataField.setValue(new HashMap<String, Object>());
		assertFalse(mapDataField.isNull());
		assertEquals(0, mapDataField.getSize());

		mapDataField.putField("key1");
		mapDataField.putField("key2");
		mapDataField.putField("key3");
		mapDataField.setValue(new HashMap<String, Object>());
		assertFalse(mapDataField.isNull());
		assertEquals(0, mapDataField.getSize());

		

		Map<String, String> stringMap = new HashMap<String, String>();
		stringMap.put("key1", "value1");
		stringMap.put("key2", "value2");
		stringMap.put("key3", "value3");
		
		mapDataField.setNull(true);
		mapDataField.setValue((Map<?, ?>) stringMap);
		assertFalse(mapDataField.isNull());
		assertEquals(3, mapDataField.getSize());
		assertEquals("value1", mapDataField.getField("key1").getValue().toString());
		assertEquals("value2", mapDataField.getField("key2").getValue().toString());
		assertEquals("value3", mapDataField.getField("key3").getValue().toString());
		
		mapDataField.setNull(false);
		mapDataField.setValue((Map<?, ?>) stringMap);
		assertFalse(mapDataField.isNull());
		assertEquals(3, mapDataField.getSize());
		assertEquals("value1", mapDataField.getField("key1").getValue().toString());
		assertEquals("value2", mapDataField.getField("key2").getValue().toString());
		assertEquals("value3", mapDataField.getField("key3").getValue().toString());

		mapDataField.putField("key1");
		mapDataField.putField("key2");
		mapDataField.putField("key3");
		mapDataField.setValue((Map<?, ?>) stringMap);
		assertFalse(mapDataField.isNull());
		assertEquals(3, mapDataField.getSize());
		assertEquals("value1", mapDataField.getField("key1").getValue().toString());
		assertEquals("value2", mapDataField.getField("key2").getValue().toString());
		assertEquals("value3", mapDataField.getField("key3").getValue().toString());
	}

	public void testSetValue3() {
		MapDataField mapDataField = new MapDataField(createMapMetadata());
		
		mapDataField.setNull(true);
		mapDataField.setValue((DataField) null);
		assertTrue(mapDataField.isNull());
		assertEquals(0, mapDataField.getSize());

		mapDataField.setNull(false);
		mapDataField.setValue((DataField)  null);
		assertTrue(mapDataField.isNull());
		assertEquals(0, mapDataField.getSize());

		mapDataField.putField("key1");
		mapDataField.putField("key2");
		mapDataField.putField("key3");
		mapDataField.setValue((DataField)  null);
		assertTrue(mapDataField.isNull());
		assertEquals(0, mapDataField.getSize());

		
		
		MapDataField otherMapDataField = new MapDataField(createMapMetadata());
		otherMapDataField.setNull(true);
				
		mapDataField.setNull(true);
		mapDataField.setValue(otherMapDataField);
		assertTrue(mapDataField.isNull());
		assertEquals(0, mapDataField.getSize());

		mapDataField.setNull(false);
		mapDataField.setValue(otherMapDataField);
		assertTrue(mapDataField.isNull());
		assertEquals(0, mapDataField.getSize());

		mapDataField.putField("key1");
		mapDataField.putField("key2");
		mapDataField.putField("key3");
		mapDataField.setValue(otherMapDataField);
		assertTrue(mapDataField.isNull());
		assertEquals(0, mapDataField.getSize());

		

		otherMapDataField = new MapDataField(createMapMetadata());
		otherMapDataField.setNull(false);
				
		mapDataField.setNull(true);
		mapDataField.setValue(otherMapDataField);
		assertFalse(mapDataField.isNull());
		assertEquals(0, mapDataField.getSize());

		mapDataField.setNull(false);
		mapDataField.setValue(otherMapDataField);
		assertFalse(mapDataField.isNull());
		assertEquals(0, mapDataField.getSize());

		mapDataField.putField("key1");
		mapDataField.putField("key2");
		mapDataField.putField("key3");
		mapDataField.setValue(otherMapDataField);
		assertFalse(mapDataField.isNull());
		assertEquals(0, mapDataField.getSize());

		
		
		otherMapDataField = new MapDataField(createMapMetadata());
		otherMapDataField.putField("key1").setValue("value1");
		otherMapDataField.putField("key2").setValue("value2");
				
		mapDataField.setNull(true);
		mapDataField.setValue(otherMapDataField);
		assertFalse(mapDataField.isNull());
		assertEquals(2, mapDataField.getSize());
		assertEquals("value1", mapDataField.getField("key1").getValue().toString());
		assertEquals("value2", mapDataField.getField("key2").getValue().toString());
		
		mapDataField.setNull(false);
		mapDataField.setValue(otherMapDataField);
		assertFalse(mapDataField.isNull());
		assertEquals(2, mapDataField.getSize());
		assertEquals("value1", mapDataField.getField("key1").getValue().toString());
		assertEquals("value2", mapDataField.getField("key2").getValue().toString());

		mapDataField.putField("key1");
		mapDataField.putField("key2");
		mapDataField.putField("key3");
		mapDataField.setValue(otherMapDataField);
		assertFalse(mapDataField.isNull());
		assertEquals(2, mapDataField.getSize());
		assertEquals("value1", mapDataField.getField("key1").getValue().toString());
		assertEquals("value2", mapDataField.getField("key2").getValue().toString());
	}

	public void testReset() {
		MapDataField mapDataField = new MapDataField(createMapMetadata());
		
		mapDataField.setNull(true);
		mapDataField.reset();
		assertTrue(mapDataField.isNull());
		assertEquals(0, mapDataField.getSize());

		mapDataField.setNull(false);
		mapDataField.reset();
		assertTrue(mapDataField.isNull());
		assertEquals(0, mapDataField.getSize());

		mapDataField.putField("key").setValue("value");
		mapDataField.reset();
		assertTrue(mapDataField.isNull());
		assertEquals(0, mapDataField.getSize());

		
		
		mapDataField.getMetadata().setNullable(false);
		mapDataField.getMetadata().setDefaultValue("default value");

		mapDataField.setNull(true);
		mapDataField.reset();
		assertFalse(mapDataField.isNull());
		assertEquals(0, mapDataField.getSize());

		mapDataField.setNull(false);
		mapDataField.reset();
		assertFalse(mapDataField.isNull());
		assertEquals(0, mapDataField.getSize());

		mapDataField.putField("key").setValue("value");
		mapDataField.reset();
		assertFalse(mapDataField.isNull());
		assertEquals(0, mapDataField.getSize());
	}
	
	public void testGetValue() {
		MapDataField mapDataField = new MapDataField(createMapMetadata());
		
		mapDataField.setNull(true);
		assertNull(mapDataField.getValue());
		
		mapDataField.setNull(false);
		assertNotNull(mapDataField.getValue());
		
		mapDataField.putField("key");
		assertNotNull(mapDataField.getValue());
		
		mapDataField.clear();
		assertNotNull(mapDataField.getValue());

		mapDataField.reset();
		assertNull(mapDataField.getValue());
	}
	
	public void testGetValueSize() {
		MapDataField mapDataField = new MapDataField(createMapMetadata());
		
		mapDataField.setNull(false);
		Map<String, CloverString> mapView = mapDataField.getValue(CloverString.class);
		
		assertEquals(0, mapView.size());
		
		mapDataField.putField("key1");
		assertEquals(1, mapView.size());
		
		mapView.put("key2", new CloverString("value2"));
		assertEquals(2, mapView.size());
	}
	
	public void testGetValueGet() {
		MapDataField mapDataField = new MapDataField(createMapMetadata());
		
		mapDataField.setNull(false);
		Map<String, CloverString> mapView = mapDataField.getValue(CloverString.class);
		
		assertNull(mapView.get("neco"));
		assertNull(mapView.get(""));
		assertNull(mapView.get(null));
		
		mapDataField.putField("key1").setValue("value1");
		assertNull(mapView.get("neco"));
		assertNull(mapView.get(""));
		assertNull(mapView.get(null));
		assertEquals(new CloverString("value1"), mapView.get("key1"));

		mapView.put("key2", new CloverString("value2"));
		assertNull(mapView.get("neco"));
		assertNull(mapView.get(""));
		assertNull(mapView.get(null));
		assertEquals(new CloverString("value1"), mapView.get("key1"));
		assertEquals(new CloverString("value2"), mapView.get("key2"));

		mapDataField.putField("");
		assertNull(mapView.get(""));
		
		mapView.put(null, new CloverString(""));
		assertEquals(new CloverString(""), mapView.get(null));
	}
	
	public void testGetValueContainsKey() {
		MapDataField mapDataField = new MapDataField(createMapMetadata());
		
		mapDataField.setNull(false);
		Map<String, CloverString> mapView = mapDataField.getValue(CloverString.class);
		
		assertFalse(mapView.containsKey("neco"));
		assertFalse(mapView.containsKey(""));
		assertFalse(mapView.containsKey(null));
		
		mapDataField.putField("key");
		assertFalse(mapView.containsKey(""));
		assertFalse(mapView.containsKey(null));
		assertTrue(mapView.containsKey("key"));
		
		mapView.put("key2", new CloverString("value2"));
		assertFalse(mapView.containsKey(""));
		assertFalse(mapView.containsKey(null));
		assertTrue(mapView.containsKey("key"));
		assertTrue(mapView.containsKey("key2"));
		
		mapDataField.putField("");
		assertTrue(mapView.containsKey(""));
		
		mapView.put(null, null);
		assertTrue(mapView.containsKey(null));
	}
	
	public void testGetValuePut() {
		MapDataField mapDataField = new MapDataField(createMapMetadata());
		
		mapDataField.setNull(false);
		Map<String, CloverString> mapView = mapDataField.getValue(CloverString.class);

		mapView.put("key1", new CloverString("value1"));
		assertEquals(new CloverString("value1"), mapView.get("key1"));
		assertEquals(new CloverString("value1"), mapDataField.getField("key1").getValue());
		assertEquals(1, mapView.size());
		assertEquals(1, mapDataField.getSize());
		
		mapView.put("", new CloverString("value2"));
		assertEquals(new CloverString("value2"), mapView.get(""));
		assertEquals(new CloverString("value2"), mapDataField.getField("").getValue());
		assertEquals(2, mapView.size());
		assertEquals(2, mapDataField.getSize());

		mapView.put(null, new CloverString(""));
		assertEquals(new CloverString(""), mapView.get(null));
		assertEquals(new CloverString(""), mapDataField.getField(null).getValue());
		assertEquals(3, mapView.size());
		assertEquals(3, mapDataField.getSize());

		mapView.put(null, null);
		assertNull(mapView.get(null));
		assertNull(mapDataField.getField(null).getValue());
		assertEquals(3, mapView.size());
		assertEquals(3, mapDataField.getSize());
	}
	
	public void testGetValuePutAll() {
		MapDataField mapDataField = new MapDataField(createMapMetadata());
		
		mapDataField.setNull(false);
		Map<String, CloverString> mapView = mapDataField.getValue(CloverString.class);

		try { mapView.putAll(null); assertTrue(false); } catch (NullPointerException e) { /*OK*/ }
		
		mapView.putAll(new HashMap<String, CloverString>());
		assertTrue(mapView.isEmpty());
		assertEquals(0, mapDataField.getSize());

		Map<String, CloverString> valueMap = new HashMap<String, CloverString>();
		valueMap.put("key1", new CloverString("value1"));
		valueMap.put("key2", new CloverString(""));
		valueMap.put("key3", null);
		valueMap.put("", null);
		valueMap.put(null, new CloverString("value2"));
		mapView.putAll(valueMap);
		assertEquals(5, mapDataField.getSize());
		assertEquals(new CloverString("value1"), mapDataField.getField("key1").getValue());
		assertEquals(new CloverString(""), mapDataField.getField("key2").getValue());
		assertEquals(null, mapDataField.getField("key3").getValue());
		assertEquals(null, mapDataField.getField("").getValue());
		assertEquals(new CloverString("value2"), mapDataField.getField(null).getValue());
		
		valueMap.put("key3", new CloverString("value3"));
		valueMap.put("", new CloverString(""));
		valueMap.put(null, null);
		mapView.putAll(valueMap);
		assertEquals(new CloverString("value3"), mapDataField.getField("key3").getValue());
		assertEquals(new CloverString(""), mapDataField.getField("").getValue());
		assertEquals(null, mapDataField.getField(null).getValue());
	}
	
	public void testGetValueRemove() {
		MapDataField mapDataField = new MapDataField(createMapMetadata());
		
		mapDataField.setNull(false);
		Map<String, CloverString> mapView = mapDataField.getValue(CloverString.class);
		
		assertNull(mapView.remove("neco"));
		assertNull(mapView.remove(""));
		assertNull(mapView.remove(null));
		
		mapDataField.putField("key1").setValue("value1");
		assertNull(mapView.remove(""));
		assertNull(mapView.remove(null));
		assertEquals(new CloverString("value1"), mapView.remove("key1"));
		assertEquals(0, mapDataField.getSize());
		
		mapDataField.putField("key1").setValue("value2");
		mapDataField.putField("").setValue("value3");
		mapDataField.putField(null).setValue("");
		assertEquals(new CloverString("value3"), mapView.remove(""));
		assertEquals(2, mapDataField.getSize());
		assertEquals(new CloverString("value2"), mapView.remove("key1"));
		assertEquals(1, mapDataField.getSize());
		assertEquals(new CloverString(""), mapView.remove(null));
		assertEquals(0, mapDataField.getSize());
	}
	
	public void testGetValueClear() {
		MapDataField mapDataField = new MapDataField(createMapMetadata());
		
		mapDataField.setNull(false);
		Map<String, CloverString> mapView = mapDataField.getValue(CloverString.class);
		
		mapView.clear();
		assertFalse(mapDataField.isNull());
		assertEquals(0, mapDataField.getSize());
		
		mapDataField.putField("key1").setValue("value1");
		mapDataField.putField("key2").setValue("value2");
		mapDataField.putField("").setValue("value3");
		mapDataField.putField(null).setValue("value4");
		mapView.clear();
		assertFalse(mapDataField.isNull());
		assertEquals(0, mapDataField.getSize());
	}
	
	public void testGetValueKeySetSize() {
		MapDataField mapDataField = new MapDataField(createMapMetadata());
		
		mapDataField.setNull(false);
		Map<String, CloverString> mapView = mapDataField.getValue(CloverString.class);
		Set<String> keySet = mapView.keySet(); 
		
		assertNotNull(keySet);
		assertEquals(0, keySet.size());
		
		mapDataField.putField("key1");
		assertEquals(1, keySet.size());
		
		mapDataField.putField(null).setValue("value1");
		assertEquals(2, keySet.size());
		
		mapDataField.removeField("key1");
		assertEquals(1, keySet.size());
		
		mapDataField.setNull(true);
		assertEquals(0, keySet.size());
	}

	public void testGetValueKeySetContains() {
		MapDataField mapDataField = new MapDataField(createMapMetadata());
		
		mapDataField.setNull(false);
		Map<String, CloverString> mapView = mapDataField.getValue(CloverString.class);
		Set<String> keySet = mapView.keySet();
		
		assertFalse(keySet.contains("key"));
		assertFalse(keySet.contains(""));
		assertFalse(keySet.contains(null));
		
		mapDataField.putField("key1");
		assertFalse(keySet.contains(""));
		assertFalse(keySet.contains(null));
		assertTrue(keySet.contains("key1"));
		
		mapDataField.putField(null).setValue("value1");
		assertTrue(keySet.contains(null));
		assertTrue(keySet.contains("key1"));
		
		mapDataField.removeField(null);
		assertFalse(keySet.contains(null));
		assertTrue(keySet.contains("key1"));
		
		mapDataField.clear();
		assertFalse(keySet.contains("key1"));
		assertFalse(keySet.contains(null));
	}

	public void testGetValueKeySetIterator() {
		MapDataField mapDataField = new MapDataField(createMapMetadata());
		
		mapDataField.setNull(false);
		Map<String, CloverString> mapView = mapDataField.getValue(CloverString.class);
		Set<String> keySet = mapView.keySet(); 
		Iterator<String> it = keySet.iterator();
		
		assertFalse(it.hasNext());
		try { it.next(); assertTrue(false); } catch (NoSuchElementException e) { /*OK*/ }
		try { it.remove(); assertTrue(false); } catch (IllegalStateException e) { /*OK*/ }
		
		mapDataField.putField("key");
		it = keySet.iterator();
		assertTrue(it.hasNext());
		assertTrue(it.hasNext());
		assertEquals("key", it.next());
		assertFalse(it.hasNext());
		try { it.next(); assertTrue(false); } catch (NoSuchElementException e) { /*OK*/ }
		it.remove();
		assertEquals(0, mapDataField.getSize());

		mapDataField.putField("key1");
		mapDataField.putField("key2");
		mapDataField.putField("");
		mapDataField.putField(null);
		mapDataField.putField("key3");
		it = keySet.iterator();
		assertTrue(it.hasNext());
		assertEquals("key1", it.next());
		assertTrue(it.hasNext());
		assertEquals("key2", it.next());
		assertTrue(it.hasNext());
		assertEquals("", it.next());
		assertTrue(it.hasNext());
		assertEquals(null, it.next());
		assertTrue(it.hasNext());
		assertEquals("key3", it.next());
		assertFalse(it.hasNext());

		it = keySet.iterator();
		assertTrue(it.hasNext());
		assertEquals("key1", it.next());
		it.remove();
		assertFalse(mapDataField.containsField("key1"));
		assertEquals(4, mapDataField.getSize());
		try { it.remove(); assertTrue(false); } catch (IllegalStateException e) { /*OK*/ }
		assertEquals("key2", it.next());
		assertEquals("", it.next());
		it.remove();
		assertFalse(mapDataField.containsField(""));
		assertEquals(3, mapDataField.getSize());
		try { it.remove(); assertTrue(false); } catch (IllegalStateException e) { /*OK*/ }
		assertTrue(it.hasNext());
		assertEquals(null, it.next());
		assertTrue(it.hasNext());
		assertEquals("key3", it.next());
		assertFalse(it.hasNext());
		it.remove();
		assertFalse(mapDataField.containsField("key3"));
		assertEquals(2, mapDataField.getSize());
		try { it.remove(); assertTrue(false); } catch (IllegalStateException e) { /*OK*/ }
		assertTrue(mapDataField.containsField("key2"));
		assertTrue(mapDataField.containsField(null));
	}
	
	public void testGetValueKeySetRemove() {
		MapDataField mapDataField = new MapDataField(createMapMetadata());
		
		mapDataField.setNull(false);
		Map<String, CloverString> mapView = mapDataField.getValue(CloverString.class);
		Set<String> keySet = mapView.keySet();
		
		assertFalse(keySet.remove("key"));
		assertFalse(keySet.remove(""));
		assertFalse(keySet.remove(null));
		
		mapDataField.putField("key1");
		assertFalse(keySet.remove(""));
		assertFalse(keySet.remove(null));
		assertTrue(keySet.remove("key1"));
		assertEquals(0, mapDataField.getSize());
		
		mapDataField.putField("key1");
		mapDataField.putField("");
		mapDataField.putField(null).setValue("value1");
		mapDataField.putField("key2");
		assertTrue(keySet.remove(null));
		assertEquals(3, mapDataField.getSize());
		assertFalse(mapDataField.containsField(null));
		assertFalse(keySet.remove(null));
		assertEquals(3, mapDataField.getSize());
		assertTrue(keySet.remove("key2"));
		assertEquals(2, mapDataField.getSize());
		assertFalse(mapDataField.containsField("key2"));
		assertTrue(mapDataField.containsField("key1"));
		assertTrue(mapDataField.containsField(""));
	}

	public void testGetValueKeySetClear() {
		MapDataField mapDataField = new MapDataField(createMapMetadata());
		
		mapDataField.setNull(false);
		Map<String, CloverString> mapView = mapDataField.getValue(CloverString.class);
		Set<String> keySet = mapView.keySet();
		
		keySet.clear();
		assertEquals(0, mapDataField.getSize());
		
		mapDataField.putField("key1");
		keySet.clear();
		assertEquals(0, mapDataField.getSize());

		mapDataField.putField("key1");
		mapDataField.putField("key2");
		mapDataField.putField("");
		mapDataField.putField(null);
		keySet.clear();
		assertEquals(0, mapDataField.getSize());
	}

	public void testGetValueEntrySetSize() {
		MapDataField mapDataField = new MapDataField(createMapMetadata());
		
		mapDataField.setNull(false);
		Map<String, CloverString> mapView = mapDataField.getValue(CloverString.class);
		Set<Entry<String, CloverString>> entrySet = mapView.entrySet(); 
		
		assertNotNull(entrySet);
		assertEquals(0, entrySet.size());
		
		mapDataField.putField("key1");
		assertEquals(1, entrySet.size());
		
		mapDataField.putField(null).setValue("value1");
		assertEquals(2, entrySet.size());
		
		mapDataField.removeField("key1");
		assertEquals(1, entrySet.size());
		
		mapDataField.setNull(true);
		assertEquals(0, entrySet.size());
	}

	public void testGetValueEntrySetContains() {
		MapDataField mapDataField = new MapDataField(createMapMetadata());
		
		mapDataField.setNull(false);
		Map<String, CloverString> mapView = mapDataField.getValue(CloverString.class);
		Set<Entry<String, CloverString>> entrySet = mapView.entrySet(); 
		
		assertFalse(entrySet.contains(new AbstractMap.SimpleEntry<String, CloverString>("key", new CloverString("value"))));
		assertFalse(entrySet.contains(new AbstractMap.SimpleEntry<String, CloverString>("", null)));
		assertFalse(entrySet.contains(new AbstractMap.SimpleEntry<String, CloverString>(null, new CloverString(""))));
		
		mapDataField.putField("key1").setValue("value1");
		assertFalse(entrySet.contains(new AbstractMap.SimpleEntry<String, CloverString>("", new CloverString("value"))));
		assertFalse(entrySet.contains(new AbstractMap.SimpleEntry<String, CloverString>(null, new CloverString("value"))));
		assertTrue(entrySet.contains(new AbstractMap.SimpleEntry<String, CloverString>("key1", new CloverString("value1"))));
		
		mapDataField.putField(null).setValue("");
		assertTrue(entrySet.contains(new AbstractMap.SimpleEntry<String, CloverString>(null, new CloverString(""))));
		assertTrue(entrySet.contains(new AbstractMap.SimpleEntry<String, CloverString>("key1", new CloverString("value1"))));
		
		mapDataField.removeField(null);
		assertFalse(entrySet.contains(new AbstractMap.SimpleEntry<String, CloverString>(null, new CloverString(""))));
		assertTrue(entrySet.contains(new AbstractMap.SimpleEntry<String, CloverString>("key1", new CloverString("value1"))));
		
		mapDataField.clear();
		assertFalse(entrySet.contains(new AbstractMap.SimpleEntry<String, CloverString>(null, new CloverString(""))));
		assertFalse(entrySet.contains(new AbstractMap.SimpleEntry<String, CloverString>("key1", new CloverString("value1"))));
	}

	public void testGetValueEntrySetIterator() {
		MapDataField mapDataField = new MapDataField(createMapMetadata());
		
		mapDataField.setNull(false);
		Map<String, CloverString> mapView = mapDataField.getValue(CloverString.class);
		Set<Entry<String, CloverString>> entrySet = mapView.entrySet();
		
		Iterator<Entry<String, CloverString>> it = entrySet.iterator();
		
		assertFalse(it.hasNext());
		try { it.next(); assertTrue(false); } catch (NoSuchElementException e) { /*OK*/ }
		try { it.remove(); assertTrue(false); } catch (IllegalStateException e) { /*OK*/ }
		
		mapDataField.putField("key").setValue("value");
		it = entrySet.iterator();
		assertTrue(it.hasNext());
		assertTrue(it.hasNext());
		assertEquals(new AbstractMap.SimpleEntry<String, CloverString>("key", new CloverString("value")), it.next());
		assertFalse(it.hasNext());
		try { it.next(); assertTrue(false); } catch (NoSuchElementException e) { /*OK*/ }
		it.remove();
		assertEquals(0, mapDataField.getSize());

		mapDataField.putField("key1").setValue("value1");
		mapDataField.putField("key2").setValue("value2");
		mapDataField.putField("").setValue("");
		mapDataField.putField(null).setValue("valueX");
		mapDataField.putField("key3").setValue(null);
		it = entrySet.iterator();
		assertTrue(it.hasNext());
		assertEquals(new AbstractMap.SimpleEntry<String, CloverString>("key1", new CloverString("value1")), it.next());
		assertTrue(it.hasNext());
		assertEquals(new AbstractMap.SimpleEntry<String, CloverString>("key2", new CloverString("value2")), it.next());
		assertTrue(it.hasNext());
		assertEquals(new AbstractMap.SimpleEntry<String, CloverString>("", new CloverString("")), it.next());
		assertTrue(it.hasNext());
		assertEquals(new AbstractMap.SimpleEntry<String, CloverString>(null, new CloverString("valueX")), it.next());
		assertTrue(it.hasNext());
		assertEquals(new AbstractMap.SimpleEntry<String, CloverString>("key3", null), it.next());
		assertFalse(it.hasNext());

		it = entrySet.iterator();
		assertTrue(it.hasNext());
		assertEquals(new AbstractMap.SimpleEntry<String, CloverString>("key1", new CloverString("value1")), it.next());
		it.remove();
		assertFalse(mapDataField.containsField("key1"));
		assertEquals(4, mapDataField.getSize());
		try { it.remove(); assertTrue(false); } catch (IllegalStateException e) { /*OK*/ }
		assertEquals(new AbstractMap.SimpleEntry<String, CloverString>("key2", new CloverString("value2")), it.next());
		assertEquals(new AbstractMap.SimpleEntry<String, CloverString>("", new CloverString("")), it.next());
		it.remove();
		assertFalse(mapDataField.containsField(""));
		assertEquals(3, mapDataField.getSize());
		try { it.remove(); assertTrue(false); } catch (IllegalStateException e) { /*OK*/ }
		assertTrue(it.hasNext());
		assertEquals(new AbstractMap.SimpleEntry<String, CloverString>(null, new CloverString("valueX")), it.next());
		assertTrue(it.hasNext());
		assertEquals(new AbstractMap.SimpleEntry<String, CloverString>("key3", null), it.next());
		assertFalse(it.hasNext());
		it.remove();
		assertFalse(mapDataField.containsField("key3"));
		assertEquals(2, mapDataField.getSize());
		try { it.remove(); assertTrue(false); } catch (IllegalStateException e) { /*OK*/ }
		assertTrue(mapDataField.containsField("key2"));
		assertTrue(mapDataField.containsField(null));
	}
	
	public void testGetValue1() {
		MapDataField mapDataField;
		
		mapDataField = new MapDataField(createMapMetadata(DataFieldType.BOOLEAN));
		mapDataField.setNull(false);
		Map<String, Boolean> mapView2 = mapDataField.getValue(Boolean.class);
		try { mapDataField.getValue(byte[].class); assertTrue(false); } catch (ClassCastException e) { /*OK*/ }
		mapView2.put("key", Boolean.TRUE);

		mapDataField = new MapDataField(createMapMetadata(DataFieldType.BYTE));
		try { mapDataField.getValue(Date.class); assertTrue(false); } catch (ClassCastException e) { /*OK*/ }
		mapDataField.setNull(false);
		Map<String, byte[]> mapView3 = mapDataField.getValue(byte[].class);
		mapView3.put("key", new byte[] {0});

		mapDataField = new MapDataField(createMapMetadata(DataFieldType.CBYTE));
		try { mapDataField.getValue(Decimal.class); assertTrue(false); } catch (ClassCastException e) { /*OK*/ }
		mapDataField.setNull(false);
		Map<String, byte[]> mapView4 = mapDataField.getValue(byte[].class);
		mapView4.put("key", new byte[] {0});

		mapDataField = new MapDataField(createMapMetadata(DataFieldType.DATE));
		try { mapDataField.getValue(Decimal.class); assertTrue(false); } catch (ClassCastException e) { /*OK*/ }
		mapDataField.setNull(false);
		Map<String, Date> mapView5 = mapDataField.getValue(Date.class);
		mapView5.put("key", new Date());

		mapDataField = new MapDataField(createMapMetadata(DataFieldType.DECIMAL));
		try { mapDataField.getValue(Integer.class); assertTrue(false); } catch (ClassCastException e) { /*OK*/ }
		mapDataField.setNull(false);
		Map<String, Decimal> mapView6 = mapDataField.getValue(Decimal.class);
		mapView6.put("key", new IntegerDecimal(10, 5));

		mapDataField = new MapDataField(createMapMetadata(DataFieldType.INTEGER));
		try { mapDataField.getValue(Long.class); assertTrue(false); } catch (ClassCastException e) { /*OK*/ }
		mapDataField.setNull(false);
		Map<String, Integer> mapView7 = mapDataField.getValue(Integer.class);
		mapView7.put("key", 1);

		mapDataField = new MapDataField(createMapMetadata(DataFieldType.LONG));
		try { mapDataField.getValue(Double.class); assertTrue(false); } catch (ClassCastException e) { /*OK*/ }
		mapDataField.setNull(false);
		Map<String, Long> mapView8 = mapDataField.getValue(Long.class);
		mapView8.put("key", (long) 1);

		mapDataField = new MapDataField(createMapMetadata(DataFieldType.NUMBER));
		try { mapDataField.getValue(String.class); assertTrue(false); } catch (ClassCastException e) { /*OK*/ }
		mapDataField.setNull(false);
		Map<String, Double> mapView9 = mapDataField.getValue(Double.class);
		mapView9.put("key", (double) 1);

		mapDataField = new MapDataField(createMapMetadata(DataFieldType.STRING));
		try { mapDataField.getValue(Boolean.class); assertTrue(false); } catch (ClassCastException e) { /*OK*/ }
		mapDataField.setNull(false);
		Map<String, CloverString> mapView1 = mapDataField.getValue(CloverString.class);
		mapView1.put("key", new CloverString());
	}

	@SuppressWarnings("unchecked")
	public void testGetValueDuplicate() {
		MapDataField mapDataField = new MapDataField(createMapMetadata());
		
		assertNull(mapDataField.getValueDuplicate());
		
		mapDataField.setNull(false);
		assertEquals(new HashMap<String, CloverString>(), mapDataField.getValueDuplicate());
		
		DataField firstField = mapDataField.putField("key1");
		DataField secondField = mapDataField.putField("key2");
		DataField thirdField = mapDataField.putField("key3");
		firstField.setValue("neco");
		secondField.setValue(null);
		thirdField.setValue("neco2");
		Map<String, CloverString> map = (Map<String, CloverString>) mapDataField.getValueDuplicate();
		assertEquals("neco", map.get("key1").toString());
		assertEquals(null, map.get("key2"));
		assertEquals("neco2", map.get("key3").toString());
		
		map.put("key1", new CloverString("neco3"));
		map.put("key4", new CloverString("neco4"));
		assertEquals("neco", mapDataField.getField("key1").getValue().toString());
		assertEquals(null, mapDataField.getField("key2").getValue());
		assertEquals("neco2", mapDataField.getField("key3").getValue().toString());
		
		map.clear();
		assertEquals(3, mapDataField.getSize());
	}
	
	public void testGetValueDuplicate1() {
		MapDataField mapDataField;
		
		mapDataField = new MapDataField(createMapMetadata(DataFieldType.BOOLEAN));
		mapDataField.setNull(false);
		Map<String, Boolean> mapView2 = mapDataField.getValueDuplicate(Boolean.class);
		try { mapDataField.getValue(byte[].class); assertTrue(false); } catch (ClassCastException e) { /*OK*/ }
		mapView2.put("key", Boolean.TRUE);

		mapDataField = new MapDataField(createMapMetadata(DataFieldType.BYTE));
		mapDataField.setNull(false);
		Map<String, byte[]> mapView3 = mapDataField.getValueDuplicate(byte[].class);
		try { mapDataField.getValue(String.class); assertTrue(false); } catch (ClassCastException e) { /*OK*/ }
		mapView3.put("key", new byte[] {0});

		mapDataField = new MapDataField(createMapMetadata(DataFieldType.CBYTE));
		mapDataField.setNull(false);
		Map<String, byte[]> mapView4 = mapDataField.getValueDuplicate(byte[].class);
		try { mapDataField.getValue(Date.class); assertTrue(false); } catch (ClassCastException e) { /*OK*/ }
		mapView4.put("key", new byte[] {0});

		mapDataField = new MapDataField(createMapMetadata(DataFieldType.DATE));
		mapDataField.setNull(false);
		Map<String, Date> mapView5 = mapDataField.getValueDuplicate(Date.class);
		try { mapDataField.getValue(Decimal.class); assertTrue(false); } catch (ClassCastException e) { /*OK*/ }
		mapView5.put("key", new Date());

		mapDataField = new MapDataField(createMapMetadata(DataFieldType.DECIMAL));
		mapDataField.setNull(false);
		Map<String, Decimal> mapView6 = mapDataField.getValueDuplicate(Decimal.class);
		try { mapDataField.getValue(Integer.class); assertTrue(false); } catch (ClassCastException e) { /*OK*/ }
		mapView6.put("key", new IntegerDecimal(10, 5));

		mapDataField = new MapDataField(createMapMetadata(DataFieldType.INTEGER));
		mapDataField.setNull(false);
		Map<String, Integer> mapView7 = mapDataField.getValueDuplicate(Integer.class);
		try { mapDataField.getValue(Long.class); assertTrue(false); } catch (ClassCastException e) { /*OK*/ }
		mapView7.put("key", 1);

		mapDataField = new MapDataField(createMapMetadata(DataFieldType.LONG));
		mapDataField.setNull(false);
		Map<String, Long> mapView8 = mapDataField.getValueDuplicate(Long.class);
		try { mapDataField.getValue(Double.class); assertTrue(false); } catch (ClassCastException e) { /*OK*/ }
		mapView8.put("key", (long) 1);

		mapDataField = new MapDataField(createMapMetadata(DataFieldType.NUMBER));
		mapDataField.setNull(false);
		Map<String, Double> mapView9 = mapDataField.getValueDuplicate(Double.class);
		try { mapDataField.getValue(CloverString.class); assertTrue(false); } catch (ClassCastException e) { /*OK*/ }
		mapView9.put("key", (double) 1);

		mapDataField = new MapDataField(createMapMetadata(DataFieldType.STRING));
		mapDataField.setNull(false);
		Map<String, CloverString> mapView1 = mapDataField.getValueDuplicate(CloverString.class);
		try { mapDataField.getValue(Boolean.class); assertTrue(false); } catch (ClassCastException e) { /*OK*/ }
		mapView1.put("key", new CloverString());
	}

	@SuppressWarnings("deprecation")
	public void testGetType() {
		MapDataField mapDataField;
		
		mapDataField = new MapDataField(createMapMetadata(DataFieldType.BOOLEAN));
		assertEquals(mapDataField.getType(), DataFieldType.BOOLEAN.getObsoleteIdentifier());
		
		mapDataField = new MapDataField(createMapMetadata(DataFieldType.BYTE));
		assertEquals(mapDataField.getType(), DataFieldType.BYTE.getObsoleteIdentifier());

		mapDataField = new MapDataField(createMapMetadata(DataFieldType.CBYTE));
		assertEquals(mapDataField.getType(), DataFieldType.CBYTE.getObsoleteIdentifier());

		mapDataField = new MapDataField(createMapMetadata(DataFieldType.DATE));
		assertEquals(mapDataField.getType(), DataFieldType.DATE.getObsoleteIdentifier());

		mapDataField = new MapDataField(createMapMetadata(DataFieldType.DECIMAL));
		assertEquals(mapDataField.getType(), DataFieldType.DECIMAL.getObsoleteIdentifier());

		mapDataField = new MapDataField(createMapMetadata(DataFieldType.INTEGER));
		assertEquals(mapDataField.getType(), DataFieldType.INTEGER.getObsoleteIdentifier());

		mapDataField = new MapDataField(createMapMetadata(DataFieldType.LONG));
		assertEquals(mapDataField.getType(), DataFieldType.LONG.getObsoleteIdentifier());

		mapDataField = new MapDataField(createMapMetadata(DataFieldType.STRING));
		assertEquals(mapDataField.getType(), DataFieldType.STRING.getObsoleteIdentifier());
	}

	public void testFromString() {
		MapDataField mapDataField = new MapDataField(createMapMetadata());
		try { mapDataField.fromString(new String()); assertTrue(false); } catch (UnsupportedOperationException e) { /*OK*/ }
	}

	public void testFromByteBuffer() throws CharacterCodingException {
		MapDataField mapDataField = new MapDataField(createMapMetadata());
		try { mapDataField.fromByteBuffer(ByteBuffer.allocate(100), null); assertTrue(false); } catch (UnsupportedOperationException e) { /*OK*/ }
	}

	public void testFromByteBuffer1() throws CharacterCodingException {
		MapDataField mapDataField = new MapDataField(createMapMetadata());
		try { mapDataField.fromByteBuffer(CloverBuffer.allocate(100), null); assertTrue(false); } catch (UnsupportedOperationException e) { /*OK*/ }
	}

	public void testToByteBuffer() throws CharacterCodingException {
		MapDataField mapDataField = new MapDataField(createMapMetadata());
		try { mapDataField.toByteBuffer(ByteBuffer.allocate(100), null); assertTrue(false); } catch (UnsupportedOperationException e) { /*OK*/ }
	}

	public void testToByteBuffer1() throws CharacterCodingException {
		MapDataField mapDataField = new MapDataField(createMapMetadata());
		try { mapDataField.toByteBuffer(CloverBuffer.allocate(100), null); assertTrue(false); } catch (UnsupportedOperationException e) { /*OK*/ }
	}

	public void testSerialize() {
		MapDataField mapDataField = new MapDataField(createMapMetadata());
		CloverBuffer buffer = CloverBuffer.allocate(Defaults.Record.RECORD_INITIAL_SIZE, Defaults.Record.RECORD_LIMIT_SIZE);

		mapDataField.setNull(true);
		mapDataField.serialize(buffer);
		buffer.flip();
		assertEquals(buffer.limit(), mapDataField.getSizeSerialized());
		assertEquals(1, buffer.limit());
		assertEquals(0, buffer.get());

		mapDataField.setNull(false);
		buffer.clear();
		mapDataField.serialize(buffer);
		buffer.flip();
		assertEquals(buffer.limit(), mapDataField.getSizeSerialized());
		assertEquals(1, buffer.limit());
		assertEquals(1, buffer.get());
		
		mapDataField.putField(null);
		buffer.clear();
		mapDataField.serialize(buffer);
		buffer.flip();
		assertEquals(buffer.limit(), mapDataField.getSizeSerialized());
		assertEquals(3, buffer.limit());
		assertEquals(2, buffer.get());
		assertEquals(0, buffer.get());
		assertEquals(0, buffer.get());

		mapDataField.putField("");
		buffer.clear();
		mapDataField.serialize(buffer);
		buffer.flip();
		assertEquals(buffer.limit(), mapDataField.getSizeSerialized());
		assertEquals(5, buffer.limit());
		assertEquals(3, buffer.get());
		assertEquals(0, buffer.get());
		assertEquals(0, buffer.get());
		assertEquals(1, buffer.get());
		assertEquals(0, buffer.get());

		mapDataField.putField("abc");
		buffer.clear();
		mapDataField.serialize(buffer);
		buffer.flip();
		assertEquals(buffer.limit(), mapDataField.getSizeSerialized());
		assertEquals(13, buffer.limit());
		assertEquals(4, buffer.get());
		assertEquals(0, buffer.get());
		assertEquals(0, buffer.get());
		assertEquals(1, buffer.get());
		assertEquals(0, buffer.get());
		assertEquals(4, buffer.get());
		assertEquals('a', buffer.getChar());
		assertEquals('b', buffer.getChar());
		assertEquals('c', buffer.getChar());
		assertEquals(0, buffer.get());

		mapDataField.getField(null).setValue("");
		buffer.clear();
		mapDataField.serialize(buffer);
		buffer.flip();
		assertEquals(buffer.limit(), mapDataField.getSizeSerialized());
		assertEquals(13, buffer.limit());
		assertEquals(4, buffer.get());
		assertEquals(0, buffer.get());
		assertEquals(1, buffer.get());
		assertEquals(1, buffer.get());
		assertEquals(0, buffer.get());
		assertEquals(4, buffer.get());
		assertEquals('a', buffer.getChar());
		assertEquals('b', buffer.getChar());
		assertEquals('c', buffer.getChar());
		assertEquals(0, buffer.get());
		
		mapDataField.getField("").setValue("efg");
		buffer.clear();
		mapDataField.serialize(buffer);
		buffer.flip();
		assertEquals(buffer.limit(), mapDataField.getSizeSerialized());
		assertEquals(19, buffer.limit());
		assertEquals(4, buffer.get());
		assertEquals(0, buffer.get());
		assertEquals(1, buffer.get());
		assertEquals(1, buffer.get());
		assertEquals(4, buffer.get());
		assertEquals('e', buffer.getChar());
		assertEquals('f', buffer.getChar());
		assertEquals('g', buffer.getChar());
		assertEquals(4, buffer.get());
		assertEquals('a', buffer.getChar());
		assertEquals('b', buffer.getChar());
		assertEquals('c', buffer.getChar());
	}
	
	public void testDeserialize() {
		MapDataField mapDataField = new MapDataField(createMapMetadata());
		CloverBuffer buffer = CloverBuffer.allocate(Defaults.Record.RECORD_INITIAL_SIZE, Defaults.Record.RECORD_LIMIT_SIZE);
		
		buffer.clear();
		buffer.put((byte) 0);
		buffer.flip();
		mapDataField.deserialize(buffer);
		assertTrue(mapDataField.isNull());
		assertEquals(0, mapDataField.getSize());
		
		buffer.clear();
		buffer.put((byte) 1);
		buffer.flip();
		mapDataField.deserialize(buffer);
		assertFalse(mapDataField.isNull());
		assertEquals(0, mapDataField.getSize());

		buffer.clear();
		buffer.put((byte) 2);
		buffer.put((byte) 0);
		buffer.put((byte) 0);
		buffer.flip();
		mapDataField.deserialize(buffer);
		assertFalse(mapDataField.isNull());
		assertEquals(1, mapDataField.getSize());
		assertTrue(mapDataField.containsField(null));
		assertNull(mapDataField.getField(null).getValue());

		buffer.clear();
		buffer.put((byte) 3);
		buffer.put((byte) 0);
		buffer.put((byte) 0);
		buffer.put((byte) 1);
		buffer.put((byte) 0);
		buffer.flip();
		mapDataField.deserialize(buffer);
		assertFalse(mapDataField.isNull());
		assertEquals(2, mapDataField.getSize());
		assertTrue(mapDataField.containsField(null));
		assertNull(mapDataField.getField(null).getValue());
		assertTrue(mapDataField.containsField(""));
		assertNull(mapDataField.getField("").getValue());

		buffer.clear();
		buffer.put((byte) 4);
		buffer.put((byte) 0);
		buffer.put((byte) 0);
		buffer.put((byte) 1);
		buffer.put((byte) 0);
		buffer.put((byte) 4);
		buffer.putChar('a');
		buffer.putChar('b');
		buffer.putChar('c');
		buffer.put((byte) 0);
		buffer.flip();
		mapDataField.deserialize(buffer);
		assertFalse(mapDataField.isNull());
		assertEquals(3, mapDataField.getSize());
		assertTrue(mapDataField.containsField(null));
		assertNull(mapDataField.getField(null).getValue());
		assertTrue(mapDataField.containsField(""));
		assertNull(mapDataField.getField("").getValue());
		assertTrue(mapDataField.containsField("abc"));
		assertNull(mapDataField.getField("abc").getValue());

		buffer.clear();
		buffer.put((byte) 4);
		buffer.put((byte) 0);
		buffer.put((byte) 1);
		buffer.put((byte) 1);
		buffer.put((byte) 0);
		buffer.put((byte) 4);
		buffer.putChar('a');
		buffer.putChar('b');
		buffer.putChar('c');
		buffer.put((byte) 0);
		buffer.flip();
		mapDataField.deserialize(buffer);
		assertFalse(mapDataField.isNull());
		assertEquals(3, mapDataField.getSize());
		assertTrue(mapDataField.containsField(null));
		assertEquals(new CloverString(""), mapDataField.getField(null).getValue());
		assertTrue(mapDataField.containsField(""));
		assertNull(mapDataField.getField("").getValue());
		assertTrue(mapDataField.containsField("abc"));
		assertNull(mapDataField.getField("abc").getValue());
		
		buffer.clear();
		buffer.put((byte) 4);
		buffer.put((byte) 0);
		buffer.put((byte) 1);
		buffer.put((byte) 1);
		buffer.put((byte) 4);
		buffer.putChar('e');
		buffer.putChar('f');
		buffer.putChar('g');
		buffer.put((byte) 4);
		buffer.putChar('a');
		buffer.putChar('b');
		buffer.putChar('c');
		buffer.put((byte) 0);
		buffer.flip();
		mapDataField.deserialize(buffer);
		assertFalse(mapDataField.isNull());
		assertEquals(3, mapDataField.getSize());
		assertTrue(mapDataField.containsField(null));
		assertEquals(new CloverString(""), mapDataField.getField(null).getValue());
		assertTrue(mapDataField.containsField(""));
		assertEquals(new CloverString("efg"), mapDataField.getField("").getValue());
		assertTrue(mapDataField.containsField("abc"));
		assertNull(mapDataField.getField("abc").getValue());
	}

	public void testEquals() {
		MapDataField mapDataField1 = new MapDataField(createMapMetadata());
		MapDataField mapDataField2 = new MapDataField(createMapMetadata());

		mapDataField1.setNull(true);
		mapDataField2.setNull(true);
		assertFalse(mapDataField1.equals(mapDataField2));
		
		mapDataField1.setNull(false);
		mapDataField2.setNull(true);
		assertFalse(mapDataField1.equals(mapDataField2));
		assertFalse(mapDataField2.equals(mapDataField1));

		mapDataField1.setNull(false);
		mapDataField2.setNull(false);
		assertTrue(mapDataField1.equals(mapDataField2));
		
		mapDataField1.putField(null);
		assertFalse(mapDataField1.equals(mapDataField2));
		assertFalse(mapDataField2.equals(mapDataField1));

		mapDataField2.putField(null);
		assertFalse(mapDataField1.equals(mapDataField2));
		
		mapDataField1.putField(null).setValue("");
		assertFalse(mapDataField1.equals(mapDataField2));
		assertFalse(mapDataField2.equals(mapDataField1));
		
		mapDataField2.putField(null).setValue("");
		assertTrue(mapDataField1.equals(mapDataField2));
		
		mapDataField1.putField("").setValue("abc");
		mapDataField2.putField("").setValue("cba");
		assertFalse(mapDataField1.equals(mapDataField2));
		assertFalse(mapDataField2.equals(mapDataField1));
		
		mapDataField2.putField("").setValue("abc");
		assertTrue(mapDataField1.equals(mapDataField2));
		assertTrue(mapDataField2.equals(mapDataField1));

		mapDataField1.putField("key1").setValue("value1");
		mapDataField1.putField("key2").setValue("value2");
		mapDataField2.putField("key2").setValue("value2");
		mapDataField2.putField("key1").setValue("value1");
		assertTrue(mapDataField1.equals(mapDataField2));
		assertTrue(mapDataField2.equals(mapDataField1));
	}

	public void testCompare() {
		MapDataField mapDataField1 = new MapDataField(createMapMetadata());
		MapDataField mapDataField2 = new MapDataField(createMapMetadata());
		
		try { mapDataField1.compareTo(null); assertTrue(false); } catch (UnsupportedOperationException e) { /*OK*/ }
		try { mapDataField1.compareTo(mapDataField2); assertTrue(false); } catch (UnsupportedOperationException e) { /*OK*/ }
	}

}
