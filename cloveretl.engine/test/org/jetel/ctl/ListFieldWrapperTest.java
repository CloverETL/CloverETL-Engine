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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import org.jetel.data.DataField;
import org.jetel.data.DataFieldFactory;
import org.jetel.metadata.DataFieldCardinalityType;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataFieldType;
import org.jetel.test.CloverTestCase;

/**
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 27.1.2012
 */
public class ListFieldWrapperTest extends CloverTestCase {

	private ListFieldWrapper<String> list = null;
	private static String[] INITIAL_VALUE = {"aa", null, null, "bb", "cc"};

	protected void setUp() throws Exception {
		super.setUp();
		initEngine();
		DataFieldMetadata fieldMetadata = new DataFieldMetadata("stringListField", DataFieldType.STRING, "|");
		fieldMetadata.setCardinalityType(DataFieldCardinalityType.LIST);
		DataField stringListField = DataFieldFactory.createDataField(fieldMetadata, true);
		stringListField.setValue(Arrays.asList(INITIAL_VALUE));
		list = new ListFieldWrapper<String>(stringListField.getValue());
	}

	protected void tearDown() throws Exception {
		super.tearDown();
		list = null;
	}

	/**
	 * Test method for {@link org.jetel.ctl.ListFieldWrapper#ListFieldWrapper(java.util.List)}.
	 */
	public void testListFieldWrapper() {
		try {
			new ListFieldWrapper<Integer>(null);
			fail();
		} catch(NullPointerException ex) {}

		try {
			new ListFieldWrapper<Integer>("aaa");
			fail();
		} catch(ClassCastException ex) {}
		
		new ListFieldWrapper<Double>(new ArrayList<Double>());
	}

	/**
	 * Test method for {@link org.jetel.ctl.ListFieldWrapper#get(int)}.
	 */
	public void testGet() {
		assertEquals("aa", list.get(0));
		assertEquals(null, list.get(1));
		assertEquals(null, list.get(2));
		assertEquals("bb", list.get(3));
		assertEquals("cc", list.get(4));
		list.add("dd");
		assertEquals("dd", list.get(5));
	}

	/**
	 * Test method for {@link org.jetel.ctl.ListFieldWrapper#size()}.
	 */
	public void testSize() {
		assertEquals(5, list.size());
		list.add(null);
		list.add("ff");
		assertEquals(7, list.size());
		list.remove("ff");
		assertEquals(6, list.size());
	}

	/**
	 * Test method for {@link org.jetel.ctl.ListFieldWrapper#set(int, java.lang.Object)}.
	 */
	public void testSet() {
		assertEquals(null, list.get(1));
		list.set(1, "bb");
		assertEquals("bb", list.get(1));
		try {
			list.set(5, "xx");
			fail();
		} catch (IndexOutOfBoundsException ex2) {}
	}

	/**
	 * Test method for {@link org.jetel.ctl.ListFieldWrapper#add(int, java.lang.Object)}.
	 */
	public void testAddIntT() {
		list.add(5, "dd");
		assertEquals("dd", list.get(5));
		try {
			list.add(7, "xx");
			fail();
		} catch (IndexOutOfBoundsException ex) {}
	}

	/**
	 * Test method for {@link org.jetel.ctl.ListFieldWrapper#isEmpty()}.
	 */
	public void testIsEmpty() {
		assertEquals(false, list.isEmpty());
		list.clear();
		assertEquals(true, list.isEmpty());
	}

	/**
	 * Test method for {@link org.jetel.ctl.ListFieldWrapper#contains(java.lang.Object)}.
	 */
	public void testContains() {
		assertTrue(list.contains(null));
		assertTrue(list.contains("aa"));
		assertFalse(list.contains("xx"));
		list.add("xx");
		assertTrue(list.contains("xx"));
	}

	/**
	 * Test method for {@link org.jetel.ctl.ListFieldWrapper#iterator()}.
	 */
	public void testIterator() {
		Iterator<String> it = list.iterator();
		assertEquals("aa", it.next());
	}

	/**
	 * Test method for {@link org.jetel.ctl.ListFieldWrapper#toArray()}.
	 */
	public void testToArray() {
		list.set(1, "bb");
		list.add("xx");
		Object[] a = list.toArray();
		Object[] expected = new Object[] {"aa", "bb", null, "bb", "cc", "xx"}; 
		assertTrue(Arrays.equals(expected, a));
	}

	/**
	 * Test method for {@link org.jetel.ctl.ListFieldWrapper#toArray(S[])}.
	 */
	public void testToArraySArray() {
		list.set(1, "bb");
		list.add("xx");
		String[] a = list.toArray(new String[0]);
		String[] expected = new String[] {"aa", "bb", null, "bb", "cc", "xx"}; 
		assertTrue(Arrays.equals(expected, a));
	}

	/**
	 * Test method for {@link org.jetel.ctl.ListFieldWrapper#add(java.lang.Object)}.
	 */
	public void testAddT() {
		assertEquals(5, list.size());
		list.add("xx");
		assertEquals("xx", list.get(5));
	}

	/**
	 * Test method for {@link org.jetel.ctl.ListFieldWrapper#remove(java.lang.Object)}.
	 */
	public void testRemoveObject() {
		assertEquals(5, list.size());
		assertTrue(list.contains("bb"));
		list.remove("bb");
		assertFalse(list.contains("bb"));
		assertEquals(4, list.size());
		assertTrue(list.contains(null));
		list.remove(null);
		assertEquals(3, list.size());
		assertTrue(list.contains(null));
		list.remove(null);
		assertEquals(2, list.size());
		assertFalse(list.contains(null));
	}

	/**
	 * Test method for {@link org.jetel.ctl.ListFieldWrapper#containsAll(java.util.Collection)}.
	 */
	public void testContainsAll() {
		assertTrue(list.containsAll(Arrays.asList("aa", "cc")));
		assertTrue(list.containsAll(Arrays.asList(null, "cc")));
		assertTrue(list.containsAll(Arrays.asList(null, null)));
		assertTrue(list.containsAll(Arrays.asList(null, null, null)));
		assertTrue(list.containsAll(Arrays.asList("bb", null)));
		assertFalse(list.containsAll(Arrays.asList("xx")));
		assertFalse(list.containsAll(Arrays.asList(123)));
	}

	/**
	 * Test method for {@link org.jetel.ctl.ListFieldWrapper#addAll(java.util.Collection)}.
	 */
	public void testAddAllCollectionOfQextendsT() {
		List<String> added = Arrays.asList("aa", null, "bb", null, null, "cc", "xx");
		list.addAll(added);
		assertTrue(Arrays.equals(new Object[] {"aa", null, null, "bb", "cc", "aa", null, "bb", null, null, "cc", "xx"}, list.toArray()));
	}

	/**
	 * Test method for {@link org.jetel.ctl.ListFieldWrapper#addAll(int, java.util.Collection)}.
	 */
	public void testAddAllIntCollectionOfQextendsT() {
		List<String> added = Arrays.asList("aa", null, "bb", null, null, "cc", "xx");
		list.addAll(2, added);
		assertEquals(Arrays.asList("aa", null, "aa", null, "bb", null, null, "cc", "xx", null, "bb", "cc"), list);
	}

	/**
	 * Test method for {@link org.jetel.ctl.ListFieldWrapper#removeAll(java.util.Collection)}.
	 */
	public void testRemoveAll() {
		list.add(null);
		list.removeAll(Arrays.asList("cc"));
		assertEquals(Arrays.asList("aa", null, null, "bb", null), list);
		list.removeAll(Arrays.asList(null, null));
		assertEquals(Arrays.asList("aa", "bb"), list);
	}

	/**
	 * Test method for {@link org.jetel.ctl.ListFieldWrapper#retainAll(java.util.Collection)}.
	 */
	public void testRetainAll() {
		list.retainAll(Arrays.asList(null, "aa"));
		assertEquals(Arrays.asList("aa", null, null), list);
	}

	/**
	 * Test method for {@link org.jetel.ctl.ListFieldWrapper#clear()}.
	 */
	public void testClear() {
		list.clear();
		assertTrue(list.isEmpty());
	}

	/**
	 * Test method for {@link org.jetel.ctl.ListFieldWrapper#remove(int)}.
	 */
	public void testRemoveInt() {
		list.remove(2);
		assertEquals(Arrays.asList("aa", null, "bb", "cc"), list);
		list.remove(2);
		assertEquals(Arrays.asList("aa", null, "cc"), list);
		list.remove(0);
		assertEquals(Arrays.asList(null, "cc"), list);
		try {
			list.remove(5);
			fail();
		} catch (IndexOutOfBoundsException ex) {}
	}

	/**
	 * Test method for {@link org.jetel.ctl.ListFieldWrapper#indexOf(java.lang.Object)}.
	 */
	public void testIndexOf() {
		assertEquals(3, list.indexOf("bb"));
		assertEquals(-1, list.indexOf("xx"));
	}

	/**
	 * Test method for {@link org.jetel.ctl.ListFieldWrapper#lastIndexOf(java.lang.Object)}.
	 */
	public void testLastIndexOf() {
		assertEquals(2, list.lastIndexOf(null));
		assertEquals(-1, list.lastIndexOf("yy"));
	}

	/**
	 * Test method for {@link org.jetel.ctl.ListFieldWrapper#listIterator()}.
	 */
	public void testListIterator() {
		ListIterator<String> it = list.listIterator();
		assertEquals("aa", it.next());
	}

	/**
	 * Test method for {@link org.jetel.ctl.ListFieldWrapper#listIterator(int)}.
	 */
	public void testListIteratorInt() {
		ListIterator<String> it = list.listIterator(3);
		assertEquals("bb", it.next());
		assertFalse(list.listIterator(5).hasNext());
		try {
			list.listIterator(6);
			fail();
		} catch (IndexOutOfBoundsException ex) {}
	}

	/**
	 * Test method for {@link org.jetel.ctl.ListFieldWrapper#subList(int, int)}.
	 */
	public void testSubList() {
		assertEquals(Arrays.asList(INITIAL_VALUE), list.subList(0, 5));
		List<String> subList = list.subList(1, 4);
		assertEquals(Arrays.asList(null, null, "bb"), subList);
		subList.set(1, "kk");
		assertEquals(Arrays.asList("aa", null, "kk", "bb", "cc"), list);
		try {
			list.subList(-1, 3);
			fail();
		} catch (IndexOutOfBoundsException ex) {}
		try {
			list.subList(0, 6);
			fail();
		} catch (IndexOutOfBoundsException ex) {}
	}
	
	private static void assertEqualsSymmetric(Object o1, Object o2) {
		assertEquals(o1, o2);
		assertEquals(o2, o1);
	}

	/**
	 * Test method for {@link org.jetel.ctl.ListFieldWrapper#equals(java.lang.Object)}.
	 */
	public void testEqualsObject() {
		Object expected = null;
		expected = Arrays.asList(INITIAL_VALUE);
		assertEqualsSymmetric(expected, list);
	}

	/**
	 * Test method for {@link org.jetel.ctl.ListFieldWrapper#hashCode()}.
	 */
	public void testHashCode() {
		assertEquals(Arrays.asList(INITIAL_VALUE).hashCode(), list.hashCode());
	}

}
