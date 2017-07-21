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
package org.jetel.ctl;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.jetel.data.DataField;
import org.jetel.data.DataFieldFactory;
import org.jetel.metadata.DataFieldContainerType;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataFieldType;
import org.jetel.test.CloverTestCase;

/**
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 31.1.2012
 */
public class MapFieldWrapperTest extends CloverTestCase {
	
	private MapFieldWrapper<String> map = null;
	
	@SuppressWarnings("serial")
	private static Map<String, String> INITIAL_VALUE = Collections.unmodifiableMap(
			new HashMap<String, String>() 
			{{
				put("abc", "cba");
				put("def", null);
				put("ghi", "");
				put(null, "zzz");
				put("jkl", "lkj");
				put("mno", null);
				put("pqr", "cba");
			}}
	);
	
	@Override
	protected void setUp() throws Exception {
		super.setUp();
		initEngine();
		DataFieldMetadata fieldMetadata = new DataFieldMetadata("stringListField", DataFieldType.STRING, "|");
		fieldMetadata.setContainerType(DataFieldContainerType.MAP);
		DataField stringMapField = DataFieldFactory.createDataField(fieldMetadata, true);
		stringMapField.setValue(INITIAL_VALUE);
		map = new MapFieldWrapper<String>(stringMapField.getValue());
	}

	@Override
	protected void tearDown() throws Exception {
		super.tearDown();
		map = null;
	}

	/**
	 * Test method for {@link org.jetel.ctl.MapFieldWrapper#MapFieldWrapper(java.lang.Object)}.
	 */
	public void testMapFieldWrapper() {
		try {
			new MapFieldWrapper<Integer>(null);
			fail();
		} catch(NullPointerException ex) {}

		try {
			new MapFieldWrapper<Integer>("aaa");
			fail();
		} catch(ClassCastException ex) {}
		
		new MapFieldWrapper<Double>(new HashMap<String, Double>());
	}

	/**
	 * Test method for {@link org.jetel.ctl.MapFieldWrapper#entrySet()}.
	 */
	public void testEntrySet() {
		Set<Map.Entry<String, String>> entrySet = map.entrySet(); 
		assertEquals(INITIAL_VALUE.entrySet(), entrySet);
		
		entrySet.remove(new AbstractMap.SimpleEntry<String, String>("ghi", ""));
		assertFalse(map.containsKey("ghi"));
		
		entrySet.removeAll(Arrays.asList(new Object[] {
				new AbstractMap.SimpleEntry<String, String>("abc", "cba"),
				new AbstractMap.SimpleEntry<String, String>(null, "zzz")
		}));
		
		assertFalse(map.containsKey("abc"));
		assertFalse(map.containsKey(null));
		
		entrySet.retainAll(Arrays.asList(new Object[] {
				new AbstractMap.SimpleEntry<String, String>("abc", "cba"),
				new AbstractMap.SimpleEntry<String, String>("def", null),
				new AbstractMap.SimpleEntry<String, String>("pqr", "cba")
		}));
		
		assertEquals(2, map.size());
		assertTrue(map.containsKey("def"));
		assertTrue(map.containsKey("pqr"));
		
		Iterator<Map.Entry<String, String>> it = entrySet.iterator();
		assertTrue(it.hasNext());
		while (it.hasNext()) {
			Map.Entry<String, String> entry = it.next();
			entry.setValue("modified");
		}
		assertEquals("modified", map.get("def"));
		assertEquals("modified", map.get("pqr"));
		
		it.remove();
		assertEquals(1, map.size());
		
		entrySet.clear();
		assertTrue(map.isEmpty());
	}

	/**
	 * Test method for {@link org.jetel.ctl.MapFieldWrapper#containsKey(java.lang.Object)}.
	 */
	public void testContainsKeyObject() {
		assertTrue(map.containsKey("abc"));
		assertFalse(map.containsKey("abcd"));
		map.put("abcd", "dcba");
		assertTrue(map.containsKey("abcd"));
	}

	/**
	 * Test method for {@link org.jetel.ctl.MapFieldWrapper#get(java.lang.Object)}.
	 */
	public void testGetObject() {
		assertEquals(INITIAL_VALUE.get("abc"), map.get("abc"));
		assertEquals(INITIAL_VALUE.get("def"), map.get("def"));
		assertEquals(INITIAL_VALUE.get("ghi"), map.get("ghi"));
		map.remove("abc");
		assertEquals(null, map.get("abc"));
	}

	/**
	 * Test method for {@link org.jetel.ctl.MapFieldWrapper#keySet()}.
	 */
	public void testKeySet() {
		Set<String> expected = new HashSet<String>(Arrays.asList("abc", null, "def", "ghi", "jkl", "pqr", "mno"));
		assertEquals(expected, map.keySet());
		assertEquals(INITIAL_VALUE.size(), map.size());
		Set<String> keySet = map.keySet();
		keySet.remove("abc");
		assertEquals(INITIAL_VALUE.size() - 1, map.size());
		assertFalse(keySet.contains("abc"));
		assertFalse(map.containsKey("abc"));
		assertTrue(keySet.contains(null));
		assertFalse(keySet.contains("kkk"));
		keySet.removeAll(Arrays.asList(null, "ghi"));
		assertEquals(INITIAL_VALUE.size() - 3, map.size());
		assertFalse(map.containsKey("ghi"));
		assertFalse(map.containsKey(null));
		assertFalse(map.isEmpty());
		keySet.retainAll(Arrays.asList("def", "mno", "xyz"));
		assertTrue(map.containsKey("def"));
		assertTrue(map.containsKey("mno"));
		assertFalse(map.containsKey("xyz"));
		
		assertEquals(2, map.size());
		Iterator<String> it = keySet.iterator();
		assertTrue(it.hasNext());
		it.next();
		it.remove();
		assertEquals(1, map.size());
		
		keySet.clear();
		assertTrue(map.isEmpty());
		
		it = keySet.iterator();
		assertFalse(it.hasNext());
	}

	/**
	 * Test method for {@link org.jetel.ctl.MapFieldWrapper#put(java.lang.String, java.lang.Object)}.
	 */
	public void testPutStringV() {
		assertFalse(map.containsKey("kkk"));
		map.put("kkk", "lll");
		assertEquals("lll", map.get("kkk"));
	}

	/**
	 * Test method for {@link org.jetel.ctl.MapFieldWrapper#remove(java.lang.Object)}.
	 */
	public void testRemoveObject() {
		map.remove("xxx");
		assertEquals(INITIAL_VALUE, map);
		map.remove("abc");
		assertFalse(map.containsKey("abc"));
		map.remove(null);
		assertFalse(map.containsKey(null));
		assertEquals(INITIAL_VALUE.size() - 2, map.size());
	}

	/**
	 * Test method for {@link java.util.AbstractMap#hashCode()}.
	 */
	public void testHashCode() {
		assertEquals(INITIAL_VALUE.hashCode(), map.hashCode());
	}

	private static void assertEqualsSymmetric(Object o1, Object o2) {
		assertEquals(o1, o2);
		assertEquals(o2, o1);
	}

	/**
	 * Test method for {@link java.util.AbstractMap#equals(java.lang.Object)}.
	 */
	public void testEquals() {
		assertEqualsSymmetric(INITIAL_VALUE, map);
	}

	/**
	 * Test method for {@link java.util.AbstractMap#size()}.
	 */
	public void testSize() {
		assertEquals(INITIAL_VALUE.size(), map.size());
	}

	/**
	 * Test method for {@link java.util.AbstractMap#isEmpty()}.
	 */
	public void testIsEmpty() {
		assertFalse(map.isEmpty());
		map.clear();
		assertTrue(map.isEmpty());
	}

	/**
	 * Test method for {@link java.util.AbstractMap#containsValue(java.lang.Object)}.
	 */
	public void testContainsValue() {
		assertTrue(map.containsValue(null));
		assertTrue(map.containsValue(""));
		assertTrue(map.containsValue("cba"));
		assertTrue(map.containsValue("zzz"));
		assertFalse(map.containsValue("dcba"));
		map.put("jkl", "aaa");
		assertFalse(map.containsValue(INITIAL_VALUE.get("jkl")));
		assertTrue(map.containsValue("aaa"));
	}

	/**
	 * Test method for {@link java.util.AbstractMap#putAll(java.util.Map)}.
	 */
	public void testPutAll() {
		Map<String, String> putValues = new HashMap<String, String>();
		putValues.put("abc", "modified");
		putValues.put(null, "nullValue");
		putValues.put("xxx", "xxx");
		map.putAll(putValues);
		Map<String, String> expected = new HashMap<String, String>(INITIAL_VALUE);
		expected.putAll(putValues);
		assertEquals(expected, map);
	}

	/**
	 * Test method for {@link java.util.AbstractMap#clear()}.
	 */
	public void testClear() {
		map.clear();
		assertTrue(map.isEmpty());
	}

	/**
	 * Test method for {@link java.util.AbstractMap#values()}.
	 */
	public void testValues() {
		Collection<String> values = map.values();
		assertEquals(INITIAL_VALUE.size(), values.size());
		values.remove("");
		assertFalse(map.containsValue(""));
		values.removeAll(Arrays.asList("zzz", null));
		assertFalse(map.containsValue("zzz"));
		assertFalse(map.containsValue(null));
		values.retainAll(Arrays.asList(null, "cba"));
		assertTrue(map.containsValue("cba"));
		assertFalse(map.containsValue(null));
		assertFalse(map.containsValue("lkj"));
		
		assertEquals(2, map.size());
		Iterator<String> it = values.iterator();
		assertTrue(it.hasNext());
		it.next();
		it.remove();
		assertEquals(1, map.size());
		
		values.clear();
		assertTrue(map.isEmpty());
	}

}
