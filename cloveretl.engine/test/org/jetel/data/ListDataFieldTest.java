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
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.jetel.data.primitive.Decimal;
import org.jetel.data.primitive.IntegerDecimal;
import org.jetel.exception.BadDataFormatException;
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
 * @created 17 Jan 2012
 */
public class ListDataFieldTest extends CloverTestCase {

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		initEngine();
	}

	private DataFieldMetadata createListMetadata() {
		return createListMetadata(DataFieldType.STRING);
	}
	
	private DataFieldMetadata createListMetadata(DataFieldType type) {
		DataFieldMetadata listDataFieldMetadata = new DataFieldMetadata("listDataField", ";");
		listDataFieldMetadata.setDataType(type);
		listDataFieldMetadata.setCardinalityType(DataFieldCardinalityType.LIST);
		return listDataFieldMetadata;
	}
	
	public void testGetSize() {
		ListDataField listDataField = new ListDataField(createListMetadata());

		assertTrue(listDataField.getSize() == 0);
		
		listDataField.setNull(false);
		assertTrue(listDataField.getSize() == 0);

		for (int i = 0; i < 5; i++) {
			listDataField.addField();
			assertTrue(listDataField.getSize() == i + 1);
		}
		
		for (int i = 0; i < 5; i++) {
			listDataField.removeField(0);
			assertTrue(listDataField.getSize() == 5 - i - 1);
		}
		
		listDataField.setNull(true);
		assertTrue(listDataField.getSize() == 0);
	}
	
	public void testSetNull() {
		ListDataField listDataField = new ListDataField(createListMetadata());

		listDataField.setNull(true);
		assertTrue(listDataField.isNull());

		listDataField.setNull(false);
		assertFalse(listDataField.isNull());
	}

	public void testSetNull1() {
		DataFieldMetadata fieldMetadata = createListMetadata();
		fieldMetadata.setNullable(false);
		ListDataField listDataField = new ListDataField(fieldMetadata);
		
		listDataField.addField();
		listDataField.setNull(true);
		assertFalse(listDataField.isNull());
		assertEquals(0, listDataField.getSize());
	}

	public void testSetNull2() {
		DataFieldMetadata fieldMetadata = createListMetadata();
		fieldMetadata.setNullable(false);
		fieldMetadata.setDefaultValueStr("default value"); //default value is not used
		ListDataField listDataField = new ListDataField(fieldMetadata);
		
		listDataField.addField();
		listDataField.setNull(true);
		assertFalse(listDataField.isNull());
		assertEquals(0, listDataField.getSize());
	}

	public void testSetNull3() {
		DataFieldMetadata fieldMetadata = createListMetadata();
		fieldMetadata.setNullable(true);
		fieldMetadata.setDefaultValueStr("default value");
		ListDataField listDataField = new ListDataField(fieldMetadata);
		
		listDataField.addField();
		listDataField.setNull(true);
		assertTrue(listDataField.isNull());
	}

	public void testSetToDefaultValue() {
		ListDataField listDataField = new ListDataField(createListMetadata());

		listDataField.setToDefaultValue();
		assertTrue(listDataField.isNull());
	}

	public void testSetToDefaultValue1() {
		DataFieldMetadata fieldMetadata = createListMetadata();
		fieldMetadata.setNullable(false);
		ListDataField listDataField = new ListDataField(fieldMetadata);

		listDataField.setToDefaultValue();
		assertFalse(listDataField.isNull());
		assertTrue(listDataField.getSize() == 0);
	}

	public void testSetToDefaultValue2() {
		DataFieldMetadata fieldMetadata = createListMetadata();
		fieldMetadata.setNullable(false);
		fieldMetadata.setDefaultValueStr("default value"); //default value is ignored
		ListDataField listDataField = new ListDataField(fieldMetadata);

		listDataField.setToDefaultValue();
		assertFalse(listDataField.isNull());
		assertEquals(0, listDataField.getSize());
	}

	public void testSetToDefaultValue3() {
		DataFieldMetadata fieldMetadata = createListMetadata();
		fieldMetadata.setNullable(true);
		fieldMetadata.setDefaultValueStr("default value"); //default value is ignored
		ListDataField listDataField = new ListDataField(fieldMetadata);

		listDataField.setToDefaultValue();
		assertTrue(listDataField.isNull());
		assertEquals(0, listDataField.getSize());
	}

	public void testAddField() {
		ListDataField listDataField = new ListDataField(createListMetadata());

		for (int i = 0; i < 5; i++) {
			DataField field = listDataField.addField();
			assertTrue(field == listDataField.getField(i));
			assertEquals(i + 1, listDataField.getSize());
			assertTrue(listDataField.getField(i).isNull());
		}

		listDataField.clear();
		
		for (int i = 0; i < 5; i++) {
			DataField field = listDataField.addField();
			assertTrue(field == listDataField.getField(i));
			assertEquals(i + 1, listDataField.getSize());
			assertTrue(listDataField.getField(i).isNull());
		}

	}

	public void testAddField1() {
		ListDataField listDataField = new ListDataField(createListMetadata());

		DataField firstField = listDataField.addField(0);
		assertTrue(firstField == listDataField.getField(0));
		assertEquals(1, listDataField.getSize());
		assertTrue(listDataField.getField(0).isNull());
		
		
		DataField secondField;
		try {
			secondField = listDataField.addField(2);
			throw new IllegalStateException();
		} catch (Exception e) {
			//correct
		}
		
		secondField = listDataField.addField(1);
		assertTrue(firstField == listDataField.getField(0));
		assertTrue(secondField == listDataField.getField(1));
		assertTrue(listDataField.getField(1).isNull());
		assertEquals(2, listDataField.getSize());

		DataField middleField = listDataField.addField(1);
		assertTrue(firstField == listDataField.getField(0));
		assertTrue(middleField == listDataField.getField(1));
		assertTrue(secondField == listDataField.getField(2));
		assertTrue(listDataField.getField(1).isNull());
		assertEquals(3, listDataField.getSize());
	}
	
	public void testRemoveField() {
		ListDataField listDataField = new ListDataField(createListMetadata());
		
		assertFalse(listDataField.removeField(null));
		assertFalse(listDataField.removeField(new StringDataField(new DataFieldMetadata("simpleField", ";"))));
		
		listDataField.setNull(false);

		assertFalse(listDataField.removeField(null));
		assertFalse(listDataField.removeField(new StringDataField(new DataFieldMetadata("simpleField", ";"))));

		DataField field = listDataField.addField();
		assertTrue(listDataField.removeField(field));
		assertEquals(0, listDataField.getSize());

		DataField firstField = listDataField.addField();
		DataField secondField = listDataField.addField();
		DataField thirdField = listDataField.addField();
		assertTrue(listDataField.removeField(secondField));
		assertFalse(listDataField.removeField(new StringDataField(new DataFieldMetadata("simpleField", ";"))));
		assertEquals(2, listDataField.getSize());
		assertTrue(listDataField.removeField(firstField));
		assertEquals(1, listDataField.getSize());
		
		firstField = listDataField.addField(0);
		secondField = listDataField.addField(1);
		assertTrue(listDataField.removeField(thirdField));
		assertEquals(2, listDataField.getSize());
		assertTrue(listDataField.removeField(firstField));
		assertEquals(1, listDataField.getSize());
		assertTrue(listDataField.removeField(secondField));
		assertEquals(0, listDataField.getSize());
	}

	public void testRemoveField1() {
		ListDataField listDataField = new ListDataField(createListMetadata());
		
		try { listDataField.removeField(0); assertTrue(false); } catch (IndexOutOfBoundsException e) { /*OK*/ }
		try { listDataField.removeField(-1); assertTrue(false); } catch (IndexOutOfBoundsException e) { /*OK*/ }
		try { listDataField.removeField(1); assertTrue(false); } catch (IndexOutOfBoundsException e) { /*OK*/ }
		
		listDataField.setNull(false);

		try { listDataField.removeField(0); assertTrue(false); } catch (IndexOutOfBoundsException e) { /*OK*/ }
		try { listDataField.removeField(-1); assertTrue(false); } catch (IndexOutOfBoundsException e) { /*OK*/ }
		try { listDataField.removeField(1); assertTrue(false); } catch (IndexOutOfBoundsException e) { /*OK*/ }

		DataField field = listDataField.addField();
		try { listDataField.removeField(-1); assertTrue(false); } catch (IndexOutOfBoundsException e) { /*OK*/ }
		try { listDataField.removeField(1); assertTrue(false); } catch (IndexOutOfBoundsException e) { /*OK*/ }
		assertTrue(field == listDataField.removeField(0));
		assertEquals(0, listDataField.getSize());

		DataField firstField = listDataField.addField();
		DataField secondField = listDataField.addField();
		DataField thirdField = listDataField.addField();
		try { listDataField.removeField(-1); assertTrue(false); } catch (IndexOutOfBoundsException e) { /*OK*/ }
		try { listDataField.removeField(3); assertTrue(false); } catch (IndexOutOfBoundsException e) { /*OK*/ }
		try { listDataField.removeField(4); assertTrue(false); } catch (IndexOutOfBoundsException e) { /*OK*/ }
		assertTrue(secondField == listDataField.removeField(1));
		assertEquals(2, listDataField.getSize());
		assertTrue(firstField == listDataField.removeField(0));
		assertEquals(1, listDataField.getSize());
		
		firstField = listDataField.addField(0);
		secondField = listDataField.addField(1);
		try { listDataField.removeField(-1); assertTrue(false); } catch (IndexOutOfBoundsException e) { /*OK*/ }
		try { listDataField.removeField(3); assertTrue(false); } catch (IndexOutOfBoundsException e) { /*OK*/ }
		try { listDataField.removeField(4); assertTrue(false); } catch (IndexOutOfBoundsException e) { /*OK*/ }
		assertTrue(thirdField == listDataField.removeField(2));
		assertEquals(2, listDataField.getSize());
		assertTrue(firstField == listDataField.removeField(0));
		assertEquals(1, listDataField.getSize());
		assertTrue(secondField == listDataField.removeField(0));
		assertEquals(0, listDataField.getSize());
	}

	public void testGetField() {
		ListDataField listDataField = new ListDataField(createListMetadata());
		
		try { listDataField.getField(-1); assertTrue(false); } catch (IndexOutOfBoundsException e) { /*OK*/ }
		try { listDataField.getField(0); assertTrue(false); } catch (IndexOutOfBoundsException e) { /*OK*/ }
		try { listDataField.getField(1); assertTrue(false); } catch (IndexOutOfBoundsException e) { /*OK*/ }
		try { listDataField.getField(2); assertTrue(false); } catch (IndexOutOfBoundsException e) { /*OK*/ }

		listDataField.setNull(false);

		try { listDataField.getField(-1); assertTrue(false); } catch (IndexOutOfBoundsException e) { /*OK*/ }
		try { listDataField.getField(0); assertTrue(false); } catch (IndexOutOfBoundsException e) { /*OK*/ }
		try { listDataField.getField(1); assertTrue(false); } catch (IndexOutOfBoundsException e) { /*OK*/ }
		try { listDataField.getField(2); assertTrue(false); } catch (IndexOutOfBoundsException e) { /*OK*/ }

		DataField firstField = listDataField.addField();
		try { listDataField.getField(-1); assertTrue(false); } catch (IndexOutOfBoundsException e) { /*OK*/ }
		try { listDataField.getField(1); assertTrue(false); } catch (IndexOutOfBoundsException e) { /*OK*/ }
		try { listDataField.getField(2); assertTrue(false); } catch (IndexOutOfBoundsException e) { /*OK*/ }
		assertTrue(firstField == listDataField.getField(0));

		DataField secondField = listDataField.addField();
		DataField thirdField = listDataField.addField();
		DataField fourthField = listDataField.addField();
		try { listDataField.getField(-1); assertTrue(false); } catch (IndexOutOfBoundsException e) { /*OK*/ }
		try { listDataField.getField(4); assertTrue(false); } catch (IndexOutOfBoundsException e) { /*OK*/ }
		try { listDataField.getField(5); assertTrue(false); } catch (IndexOutOfBoundsException e) { /*OK*/ }
		assertTrue(fourthField == listDataField.getField(3));
		assertTrue(thirdField == listDataField.getField(2));
		assertTrue(secondField == listDataField.getField(1));
		assertTrue(firstField == listDataField.getField(0));
	}
	
	public void testClear() {
		ListDataField listDataField = new ListDataField(createListMetadata());
		
		listDataField.clear();
		assertEquals(0, listDataField.getSize());
		assertTrue(listDataField.isNull());
		
		listDataField.setNull(false);
		listDataField.clear();
		assertEquals(0, listDataField.getSize());
		assertFalse(listDataField.isNull());

		listDataField.addField();
		listDataField.clear();
		assertEquals(0, listDataField.getSize());
		assertFalse(listDataField.isNull());

		listDataField.addField();
		listDataField.setNull(true);
		assertEquals(0, listDataField.getSize());
		assertTrue(listDataField.isNull());
		listDataField.clear();
		assertEquals(0, listDataField.getSize());
		assertTrue(listDataField.isNull());

		listDataField.addField();
		listDataField.addField();
		listDataField.addField();
		listDataField.clear();
		assertEquals(0, listDataField.getSize());
		assertFalse(listDataField.isNull());
	}
	
	public void testDuplicate() {
		ListDataField listDataField = new ListDataField(createListMetadata());
		
		ListDataField duplicate = listDataField.duplicate();
		assertTrue(duplicate.isNull());
		assertFalse(duplicate.isPlain());
		assertEquals(0, duplicate.getSize());
		
		listDataField.setNull(false);
		duplicate = listDataField.duplicate();
		assertFalse(duplicate.isNull());
		assertFalse(duplicate.isPlain());
		assertEquals(0, duplicate.getSize());
		
		DataField firstField = listDataField.addField();
		duplicate = listDataField.duplicate();
		assertFalse(duplicate.isNull());
		assertFalse(duplicate.isPlain());
		assertEquals(1, duplicate.getSize());
		assertEquals(firstField.getValue(), duplicate.getField(0).getValue());
		assertTrue(firstField != duplicate.getField(0));

		firstField.setValue("neco");
		duplicate = listDataField.duplicate();
		assertFalse(duplicate.isNull());
		assertFalse(duplicate.isPlain());
		assertEquals(1, duplicate.getSize());
		assertEquals(firstField.getValue(), duplicate.getField(0).getValue());
		assertTrue(firstField != duplicate.getField(0));

		DataField secondField = listDataField.addField();
		DataField thirdField = listDataField.addField();
		thirdField.setValue("neco2");
		duplicate = listDataField.duplicate();
		assertFalse(duplicate.isNull());
		assertFalse(duplicate.isPlain());
		assertEquals(3, duplicate.getSize());
		assertEquals(firstField.getValue(), duplicate.getField(0).getValue());
		assertEquals(secondField.getValue(), duplicate.getField(1).getValue());
		assertEquals(thirdField.getValue(), duplicate.getField(2).getValue());
		assertTrue(firstField != duplicate.getField(0));
		assertTrue(secondField != duplicate.getField(1));
		assertTrue(thirdField != duplicate.getField(2));
	}
	
	public void testSetValue() {
		ListDataField listDataField = new ListDataField(createListMetadata());

		try { listDataField.setValue(new Object()); assertTrue(false); } catch (BadDataFormatException e) { /*OK*/ }
		
		listDataField.setValue(new ArrayList<String>());
		assertFalse(listDataField.isNull());
		assertEquals(0, listDataField.getSize());
		
		List<String> stringList = new ArrayList<String>();
		stringList.add("neco");
		stringList.add(null);
		stringList.add("neco2");
		listDataField.setValue(stringList);
		assertFalse(listDataField.isNull());
		assertEquals(3, listDataField.getSize());
		assertEquals("neco", listDataField.getField(0).getValue().toString());
		assertEquals(null, listDataField.getField(1).getValue());
		assertEquals("neco2", listDataField.getField(2).getValue().toString());
		
		listDataField.setValue((Object) null);
		assertTrue(listDataField.isNull());
		assertEquals(0, listDataField.getSize());
	}
	
	public void testSetValue2() {
		ListDataField listDataField1 = new ListDataField(createListMetadata());

		ListDataField listDataField2 = new ListDataField(createListMetadata());
		
		DataField firstField = listDataField2.addField();
		DataField secondField = listDataField2.addField();
		DataField thirdField = listDataField2.addField();
		firstField.setValue("neco");
		secondField.setValue(null);
		thirdField.setValue("neco2");
		listDataField1.setValue(listDataField2);
		assertFalse(listDataField1.isNull());
		assertEquals(3, listDataField1.getSize());
		assertEquals(firstField.getValue(), listDataField1.getField(0).getValue());
		assertEquals(secondField.getValue(), listDataField1.getField(1).getValue());
		assertEquals(thirdField.getValue(), listDataField1.getField(2).getValue());
		assertTrue(firstField != listDataField1.getField(0));
		assertTrue(secondField != listDataField1.getField(1));
		assertTrue(thirdField != listDataField1.getField(2));
	}
	
	public void testReset() {
		ListDataField listDataField = new ListDataField(createListMetadata());
		listDataField.reset();
		
		assertFalse(listDataField.isPlain());
		assertTrue(listDataField.isNull());
		assertTrue(listDataField.getSize() == 0);
		
		listDataField = new ListDataField(createListMetadata(), true);
		listDataField.reset();
		
		assertTrue(listDataField.isPlain());
		assertTrue(listDataField.isNull());
		assertTrue(listDataField.getSize() == 0);

		listDataField = new ListDataField(createListMetadata(), false);
		listDataField.reset();
		
		assertFalse(listDataField.isPlain());
		assertTrue(listDataField.isNull());
		assertTrue(listDataField.getSize() == 0);
	}

	@SuppressWarnings("unchecked")
	public void testGetValue() {
		ListDataField listDataField = new ListDataField(createListMetadata());
		
		List<CloverString> listView = (List<CloverString>) listDataField.getValue();
		assertNull(listView);
		
		listDataField.setNull(false);
		listView = listDataField.getValue(CloverString.class);
		assertNotNull(listView);
		assertEquals(0, listView.size());
	}
	
	public void testGetValue1() {
		ListDataField listDataField;
		
		listDataField = new ListDataField(createListMetadata(DataFieldType.BOOLEAN));
		listDataField.setNull(false);
		List<Boolean> listView2 = listDataField.getValue(Boolean.class);
		try { listDataField.getValue(byte[].class); assertTrue(false); } catch (ClassCastException e) { /*OK*/ }
		listView2.add(Boolean.TRUE);

		listDataField = new ListDataField(createListMetadata(DataFieldType.BYTE));
		try { listDataField.getValue(Date.class); assertTrue(false); } catch (ClassCastException e) { /*OK*/ }
		listDataField.setNull(false);
		List<byte[]> listView3 = listDataField.getValue(byte[].class);
		listView3.add(new byte[] {0});

		listDataField = new ListDataField(createListMetadata(DataFieldType.CBYTE));
		try { listDataField.getValue(Decimal.class); assertTrue(false); } catch (ClassCastException e) { /*OK*/ }
		listDataField.setNull(false);
		List<byte[]> listView4 = listDataField.getValue(byte[].class);
		listView4.add(new byte[] {0});

		listDataField = new ListDataField(createListMetadata(DataFieldType.DATE));
		try { listDataField.getValue(Decimal.class); assertTrue(false); } catch (ClassCastException e) { /*OK*/ }
		listDataField.setNull(false);
		List<Date> listView5 = listDataField.getValue(Date.class);
		listView5.add(new Date());

		listDataField = new ListDataField(createListMetadata(DataFieldType.DECIMAL));
		try { listDataField.getValue(Integer.class); assertTrue(false); } catch (ClassCastException e) { /*OK*/ }
		listDataField.setNull(false);
		List<Decimal> listView6 = listDataField.getValue(Decimal.class);
		listView6.add(new IntegerDecimal(10, 5));

		listDataField = new ListDataField(createListMetadata(DataFieldType.INTEGER));
		try { listDataField.getValue(Long.class); assertTrue(false); } catch (ClassCastException e) { /*OK*/ }
		listDataField.setNull(false);
		List<Integer> listView7 = listDataField.getValue(Integer.class);
		listView7.add(1);

		listDataField = new ListDataField(createListMetadata(DataFieldType.LONG));
		try { listDataField.getValue(Double.class); assertTrue(false); } catch (ClassCastException e) { /*OK*/ }
		listDataField.setNull(false);
		List<Long> listView8 = listDataField.getValue(Long.class);
		listView8.add((long) 1);

		listDataField = new ListDataField(createListMetadata(DataFieldType.NUMBER));
		try { listDataField.getValue(String.class); assertTrue(false); } catch (ClassCastException e) { /*OK*/ }
		listDataField.setNull(false);
		List<Double> listView9 = listDataField.getValue(Double.class);
		listView9.add((double) 1);

		listDataField = new ListDataField(createListMetadata(DataFieldType.STRING));
		try { listDataField.getValue(Boolean.class); assertTrue(false); } catch (ClassCastException e) { /*OK*/ }
		listDataField.setNull(false);
		List<CloverString> listView1 = listDataField.getValue(CloverString.class);
		listView1.add(new CloverString());
	}

	public void testGetValueGet() {
		ListDataField listDataField = new ListDataField(createListMetadata());
		
		listDataField.addField().setValue("neco");
		List<CloverString> listView = listDataField.getValue(CloverString.class);
		try { listView.get(-1); assertTrue(false); } catch (IndexOutOfBoundsException e) { /*OK*/ }
		try { listView.get(1); assertTrue(false); } catch (IndexOutOfBoundsException e) { /*OK*/ }
		try { listView.get(2); assertTrue(false); } catch (IndexOutOfBoundsException e) { /*OK*/ }
		assertEquals("neco", listView.get(0).toString());
		
		listView.add(new CloverString("neco1"));
		assertEquals("neco", listView.get(0).toString());
		assertEquals("neco1", listView.get(1).toString());
	}
	
	public void testGetValueSize() {
		ListDataField listDataField = new ListDataField(createListMetadata());
		
		listDataField.setNull(false);
		List<CloverString> listView = listDataField.getValue(CloverString.class);
		assertEquals(0, listView.size());
		
		listDataField.addField().setValue("neco");
		assertEquals(1, listView.size());
		
		listView.add(new CloverString("neco1"));
		assertEquals(2, listView.size());
	}

	public void testGetValueIsEmpty() {
		ListDataField listDataField = new ListDataField(createListMetadata());
		
		listDataField.setNull(false);
		List<CloverString> listView = listDataField.getValue(CloverString.class);
		assertTrue(listView.isEmpty());
		
		listDataField.addField().setValue("neco");
		assertFalse(listView.isEmpty());
		
		listDataField.clear();
		assertTrue(listView.isEmpty());
		
		listView.add(new CloverString("neco1"));
		assertFalse(listView.isEmpty());
		
		listView.clear();
		assertTrue(listView.isEmpty());
	}
	
	public void testGetValueContains() {
		ListDataField listDataField = new ListDataField(createListMetadata());
		
		listDataField.setNull(false);
		List<CloverString> listView = listDataField.getValue(CloverString.class);
		assertFalse(listView.contains(new CloverString()));
		assertFalse(listView.contains(null));
		
		listView.add(new CloverString("neco"));
		assertFalse(listView.contains(new CloverString("neco1")));
		assertFalse(listView.contains(null));
		assertTrue(listView.contains(new CloverString("neco")));
		
		listDataField.addField().setValue("neco1");
		assertFalse(listView.contains(null));
		assertTrue(listView.contains(new CloverString("neco")));
		assertTrue(listView.contains(new CloverString("neco1")));

		listView.add(null);
		assertTrue(listView.contains(null));
		assertTrue(listView.contains(new CloverString("neco")));
		assertTrue(listView.contains(new CloverString("neco1")));
	}

	public void testGetValueIndexOf() {
		ListDataField listDataField = new ListDataField(createListMetadata());
		
		listDataField.setNull(false);
		List<CloverString> listView = listDataField.getValue(CloverString.class);
		assertEquals(-1, listView.indexOf(new CloverString()));
		assertEquals(-1, listView.indexOf(null));
		
		listView.add(new CloverString("neco"));
		assertEquals(-1, listView.indexOf(new CloverString("neco1")));
		assertEquals(-1, listView.indexOf(null));
		assertEquals(0, listView.indexOf(new CloverString("neco")));
		
		listDataField.addField().setValue("neco1");
		assertEquals(-1, listView.indexOf(null));
		assertEquals(0, listView.indexOf(new CloverString("neco")));
		assertEquals(1, listView.indexOf(new CloverString("neco1")));

		listDataField.addField().setValue("neco");
		assertEquals(-1, listView.indexOf(null));
		assertEquals(0, listView.indexOf(new CloverString("neco")));
		assertEquals(1, listView.indexOf(new CloverString("neco1")));

		listView.add(null);
		assertEquals(0, listView.indexOf(new CloverString("neco")));
		assertEquals(1, listView.indexOf(new CloverString("neco1")));
		assertEquals(3, listView.indexOf(null));
	}

	public void testGetValueLastIndexOf() {
		ListDataField listDataField = new ListDataField(createListMetadata());
		
		listDataField.setNull(false);
		List<CloverString> listView = listDataField.getValue(CloverString.class);
		assertEquals(-1, listView.lastIndexOf(new CloverString()));
		assertEquals(-1, listView.lastIndexOf(null));
		
		listView.add(new CloverString("neco"));
		assertEquals(-1, listView.lastIndexOf(new CloverString("neco1")));
		assertEquals(-1, listView.lastIndexOf(null));
		assertEquals(0, listView.lastIndexOf(new CloverString("neco")));
		
		listDataField.addField().setValue("neco1");
		assertEquals(-1, listView.lastIndexOf(null));
		assertEquals(0, listView.lastIndexOf(new CloverString("neco")));
		assertEquals(1, listView.lastIndexOf(new CloverString("neco1")));

		listDataField.addField().setValue("neco");
		assertEquals(-1, listView.lastIndexOf(null));
		assertEquals(2, listView.lastIndexOf(new CloverString("neco")));
		assertEquals(1, listView.lastIndexOf(new CloverString("neco1")));

		listView.add(null);
		assertEquals(2, listView.lastIndexOf(new CloverString("neco")));
		assertEquals(1, listView.lastIndexOf(new CloverString("neco1")));
		assertEquals(3, listView.lastIndexOf(null));
	}

	public void testGetValueSet() {
		ListDataField listDataField = new ListDataField(createListMetadata());
		
		listDataField.setNull(false);
		List<CloverString> listView = listDataField.getValue(CloverString.class);
		
		try { listView.set(-1, new CloverString()); assertTrue(false); } catch (IndexOutOfBoundsException e) { /*OK*/ }
		try { listView.set(0, new CloverString()); assertTrue(false); } catch (IndexOutOfBoundsException e) { /*OK*/ }
		try { listView.set(1, null); assertTrue(false); } catch (IndexOutOfBoundsException e) { /*OK*/ }
		
		listView.add(new CloverString("neco"));
		try { listView.set(-1, new CloverString()); assertTrue(false); } catch (IndexOutOfBoundsException e) { /*OK*/ }
		try { listView.set(1, null); assertTrue(false); } catch (IndexOutOfBoundsException e) { /*OK*/ }
		assertEquals("neco", listView.set(0, new CloverString("neco1")).toString());

		listDataField.addField().setValue(new CloverString("neco2"));
		try { listView.set(-1, new CloverString()); assertTrue(false); } catch (IndexOutOfBoundsException e) { /*OK*/ }
		try { listView.set(2, null); assertTrue(false); } catch (IndexOutOfBoundsException e) { /*OK*/ }
		assertEquals("neco1", listView.set(0, new CloverString("neco3")).toString());
		assertEquals("neco2", listView.set(1, new CloverString("neco4")).toString());

		listView.add(null);
		try { listView.set(-1, new CloverString()); assertTrue(false); } catch (IndexOutOfBoundsException e) { /*OK*/ }
		try { listView.set(3, null); assertTrue(false); } catch (IndexOutOfBoundsException e) { /*OK*/ }
		assertEquals(null, listView.set(2, new CloverString("neco5")));
	}

	public void testGetValueAdd() {
		ListDataField listDataField = new ListDataField(createListMetadata());
		
		listDataField.setNull(false);
		List<CloverString> listView = listDataField.getValue(CloverString.class);
		
		listView.add(new CloverString());
		assertEquals(new CloverString(), listView.get(0));
		assertEquals(new CloverString(), listDataField.getField(0).getValue());

		listView.add(null);
		assertEquals(null, listView.get(1));
		assertEquals(null, listDataField.getField(1).getValue());
		assertTrue(listDataField.getField(1).isNull());
		
		listView.add(new CloverString("neco"));
		assertEquals(new CloverString("neco"), listView.get(2));
		assertEquals(new CloverString("neco"), listDataField.getField(2).getValue());
	}

	public void testGetValueAdd1() {
		ListDataField listDataField = new ListDataField(createListMetadata());
		
		listDataField.setNull(false);
		List<CloverString> listView = listDataField.getValue(CloverString.class);
		
		try { listView.add(-1, new CloverString()); assertTrue(false); } catch (IndexOutOfBoundsException e) { /*OK*/ }
		try { listView.add(1, new CloverString()); assertTrue(false); } catch (IndexOutOfBoundsException e) { /*OK*/ }
		try { listView.add(2, new CloverString()); assertTrue(false); } catch (IndexOutOfBoundsException e) { /*OK*/ }

		listView.add(0, new CloverString());
		assertEquals(new CloverString(), listView.get(0));
		assertEquals(new CloverString(), listDataField.getField(0).getValue());

		listView.add(0, null);
		assertEquals(null, listView.get(0));
		assertEquals(null, listDataField.getField(0).getValue());
		assertTrue(listDataField.getField(0).isNull());

		try { listView.add(-1, new CloverString()); assertTrue(false); } catch (IndexOutOfBoundsException e) { /*OK*/ }
		try { listView.add(3, new CloverString()); assertTrue(false); } catch (IndexOutOfBoundsException e) { /*OK*/ }
		try { listView.add(4, new CloverString()); assertTrue(false); } catch (IndexOutOfBoundsException e) { /*OK*/ }

		listView.add(2, new CloverString("neco"));
		assertEquals(new CloverString("neco"), listView.get(2));
		assertEquals(new CloverString("neco"), listDataField.getField(2).getValue());
	}

	public void testGetValueRemove() {
		ListDataField listDataField = new ListDataField(createListMetadata());
		
		listDataField.setNull(false);
		List<CloverString> listView = listDataField.getValue(CloverString.class);

		try { listView.remove(-1); assertTrue(false); } catch (IndexOutOfBoundsException e) { /*OK*/ }
		try { listView.remove(0); assertTrue(false); } catch (IndexOutOfBoundsException e) { /*OK*/ }
		try { listView.remove(1); assertTrue(false); } catch (IndexOutOfBoundsException e) { /*OK*/ }

		listView.add(new CloverString());
		listDataField.addField().setValue(null);
		listView.add(new CloverString("neco"));
		
		listView.remove(1);
		assertEquals(2, listView.size());
		assertEquals(new CloverString(), listView.get(0));
		assertEquals(new CloverString("neco"), listDataField.getField(1).getValue());

		listView.remove(0);
		assertEquals(1, listView.size());
		assertEquals(new CloverString("neco"), listDataField.getField(0).getValue());
		
		listView.remove(0);
		assertEquals(0, listView.size());
	}

	public void testGetValueRemove1() {
		ListDataField listDataField = new ListDataField(createListMetadata());
		
		listDataField.setNull(false);
		List<CloverString> listView = listDataField.getValue(CloverString.class);

		assertFalse(listView.remove(null));
		assertFalse(listView.remove(new CloverString()));

		listView.add(new CloverString());
		listDataField.addField().setValue(null);
		listView.add(new CloverString("neco"));
		listDataField.addField().setValue(null);
		listView.add(new CloverString("neco"));
		
		listView.remove(new CloverString("neco"));
		assertEquals(4, listView.size());
		assertEquals(new CloverString(), listView.get(0));
		assertEquals(null, listDataField.getField(1).getValue());
		assertEquals(null, listDataField.getField(2).getValue());
		assertEquals(new CloverString("neco"), listView.get(3));

		listView.remove(new CloverString("neco"));
		assertEquals(3, listView.size());
		assertEquals(new CloverString(), listView.get(0));
		assertEquals(null, listDataField.getField(1).getValue());
		assertEquals(null, listDataField.getField(2).getValue());
		
		listView.remove(null);
		assertEquals(2, listView.size());
		assertEquals(new CloverString(), listView.get(0));
		assertEquals(null, listDataField.getField(1).getValue());

		listView.remove(new CloverString());
		assertEquals(1, listView.size());
		assertEquals(null, listDataField.getField(0).getValue());
		
		listView.remove(null);
		assertEquals(0, listView.size());
	}

	public void testGetValueClear() {
		ListDataField listDataField = new ListDataField(createListMetadata());
		
		listDataField.setNull(false);
		List<CloverString> listView = listDataField.getValue(CloverString.class);
		
		listView.clear();
		assertTrue(listView.isEmpty());
		
		listView.add(new CloverString());
		listView.add(null);
		listDataField.addField().setValue("neco");
		listView.clear();
		assertTrue(listView.isEmpty());
	}

	public void testGetValueAddAll() {
		ListDataField listDataField = new ListDataField(createListMetadata());
		
		listDataField.setNull(false);
		List<CloverString> listView = listDataField.getValue(CloverString.class);
		
		List<CloverString> collection = new ArrayList<CloverString>();
		collection.add(new CloverString());
		collection.add(null);
		collection.add(new CloverString("neco"));
		collection.add(null);
		collection.add(new CloverString("neco"));
		listView.addAll(collection);
		
		assertEquals(5, listView.size());
		assertEquals(new CloverString(), listView.get(0));
		assertEquals(null, listDataField.getField(1).getValue());
		assertEquals(new CloverString("neco"), listView.get(2));
		assertEquals(null, listDataField.getField(3).getValue());
		assertEquals(new CloverString("neco"), listView.get(4));
	}

	public void testGetValueAddAll1() {
		ListDataField listDataField = new ListDataField(createListMetadata());
		
		listDataField.setNull(false);
		List<CloverString> listView = listDataField.getValue(CloverString.class);
		
		listView.add(new CloverString("first"));
		listView.add(new CloverString("last"));
		
		List<CloverString> collection = new ArrayList<CloverString>();
		collection.add(new CloverString());
		collection.add(null);
		collection.add(new CloverString("neco"));
		collection.add(null);
		collection.add(new CloverString("neco"));
		listView.addAll(1,collection);
		
		assertEquals(7, listView.size());
		assertEquals(new CloverString("first"), listDataField.getField(0).getValue());
		assertEquals(new CloverString(), listView.get(1));
		assertEquals(null, listDataField.getField(2).getValue());
		assertEquals(new CloverString("neco"), listView.get(3));
		assertEquals(null, listDataField.getField(4).getValue());
		assertEquals(new CloverString("neco"), listView.get(5));
		assertEquals(new CloverString("last"), listView.get(6));
	}

	
	@SuppressWarnings("unchecked")
	public void testGetValueDuplicate() {
		ListDataField listDataField = new ListDataField(createListMetadata());
		
		assertNull(listDataField.getValueDuplicate());
		
		listDataField.setNull(false);
		assertEquals(new ArrayList<String>(), listDataField.getValueDuplicate());
		
		DataField firstField = listDataField.addField();
		DataField secondField = listDataField.addField();
		DataField thirdField = listDataField.addField();
		firstField.setValue("neco");
		secondField.setValue(null);
		thirdField.setValue("neco2");
		List<CloverString> list = (List<CloverString>) listDataField.getValueDuplicate();
		assertEquals("neco", list.get(0).toString());
		assertEquals(null, list.get(1));
		assertEquals("neco2", list.get(2).toString());
		
		list.set(0, new CloverString("neco3"));
		list.add(new CloverString("neco4"));
		assertEquals("neco", listDataField.getField(0).getValue().toString());
		assertEquals(null, listDataField.getField(1).getValue());
		assertEquals("neco2", listDataField.getField(2).getValue().toString());
		
		list.clear();
		assertEquals(3, listDataField.getSize());
	}
	
	public void testGetValueDuplicate1() {
		ListDataField listDataField;
		
		listDataField = new ListDataField(createListMetadata(DataFieldType.BOOLEAN));
		listDataField.setNull(false);
		List<Boolean> listView2 = listDataField.getValueDuplicate(Boolean.class);
		try { listDataField.getValue(byte[].class); assertTrue(false); } catch (ClassCastException e) { /*OK*/ }
		listView2.add(Boolean.TRUE);

		listDataField = new ListDataField(createListMetadata(DataFieldType.BYTE));
		listDataField.setNull(false);
		List<byte[]> listView3 = listDataField.getValueDuplicate(byte[].class);
		try { listDataField.getValue(String.class); assertTrue(false); } catch (ClassCastException e) { /*OK*/ }
		listView3.add(new byte[] {0});

		listDataField = new ListDataField(createListMetadata(DataFieldType.CBYTE));
		listDataField.setNull(false);
		List<byte[]> listView4 = listDataField.getValueDuplicate(byte[].class);
		try { listDataField.getValue(Date.class); assertTrue(false); } catch (ClassCastException e) { /*OK*/ }
		listView4.add(new byte[] {0});

		listDataField = new ListDataField(createListMetadata(DataFieldType.DATE));
		listDataField.setNull(false);
		List<Date> listView5 = listDataField.getValueDuplicate(Date.class);
		try { listDataField.getValue(Decimal.class); assertTrue(false); } catch (ClassCastException e) { /*OK*/ }
		listView5.add(new Date());

		listDataField = new ListDataField(createListMetadata(DataFieldType.DECIMAL));
		listDataField.setNull(false);
		List<Decimal> listView6 = listDataField.getValueDuplicate(Decimal.class);
		try { listDataField.getValue(Integer.class); assertTrue(false); } catch (ClassCastException e) { /*OK*/ }
		listView6.add(new IntegerDecimal(10, 5));

		listDataField = new ListDataField(createListMetadata(DataFieldType.INTEGER));
		listDataField.setNull(false);
		List<Integer> listView7 = listDataField.getValueDuplicate(Integer.class);
		try { listDataField.getValue(Long.class); assertTrue(false); } catch (ClassCastException e) { /*OK*/ }
		listView7.add(1);

		listDataField = new ListDataField(createListMetadata(DataFieldType.LONG));
		listDataField.setNull(false);
		List<Long> listView8 = listDataField.getValueDuplicate(Long.class);
		try { listDataField.getValue(Double.class); assertTrue(false); } catch (ClassCastException e) { /*OK*/ }
		listView8.add((long) 1);

		listDataField = new ListDataField(createListMetadata(DataFieldType.NUMBER));
		listDataField.setNull(false);
		List<Double> listView9 = listDataField.getValueDuplicate(Double.class);
		try { listDataField.getValue(CloverString.class); assertTrue(false); } catch (ClassCastException e) { /*OK*/ }
		listView9.add((double) 1);

		listDataField = new ListDataField(createListMetadata(DataFieldType.STRING));
		listDataField.setNull(false);
		List<CloverString> listView1 = listDataField.getValueDuplicate(CloverString.class);
		try { listDataField.getValue(Boolean.class); assertTrue(false); } catch (ClassCastException e) { /*OK*/ }
		listView1.add(new CloverString());
	}

	
	@SuppressWarnings("deprecation")
	public void testGetType() {
		ListDataField listDataField;
		
		listDataField = new ListDataField(createListMetadata(DataFieldType.BOOLEAN));
		assertEquals(listDataField.getType(), DataFieldType.BOOLEAN.getObsoleteIdentifier());
		
		listDataField = new ListDataField(createListMetadata(DataFieldType.BYTE));
		assertEquals(listDataField.getType(), DataFieldType.BYTE.getObsoleteIdentifier());

		listDataField = new ListDataField(createListMetadata(DataFieldType.CBYTE));
		assertEquals(listDataField.getType(), DataFieldType.CBYTE.getObsoleteIdentifier());

		listDataField = new ListDataField(createListMetadata(DataFieldType.DATE));
		assertEquals(listDataField.getType(), DataFieldType.DATE.getObsoleteIdentifier());

		listDataField = new ListDataField(createListMetadata(DataFieldType.DECIMAL));
		assertEquals(listDataField.getType(), DataFieldType.DECIMAL.getObsoleteIdentifier());

		listDataField = new ListDataField(createListMetadata(DataFieldType.INTEGER));
		assertEquals(listDataField.getType(), DataFieldType.INTEGER.getObsoleteIdentifier());

		listDataField = new ListDataField(createListMetadata(DataFieldType.LONG));
		assertEquals(listDataField.getType(), DataFieldType.LONG.getObsoleteIdentifier());

		listDataField = new ListDataField(createListMetadata(DataFieldType.STRING));
		assertEquals(listDataField.getType(), DataFieldType.STRING.getObsoleteIdentifier());
	}
	
	public void testFromString() {
		ListDataField listDataField = new ListDataField(createListMetadata());
		try { listDataField.fromString(new String()); assertTrue(false); } catch (UnsupportedOperationException e) { /*OK*/ }
	}

	public void testFromByteBuffer() throws CharacterCodingException {
		ListDataField listDataField = new ListDataField(createListMetadata());
		try { listDataField.fromByteBuffer(ByteBuffer.allocate(100), null); assertTrue(false); } catch (UnsupportedOperationException e) { /*OK*/ }
	}

	public void testFromByteBuffer1() throws CharacterCodingException {
		ListDataField listDataField = new ListDataField(createListMetadata());
		try { listDataField.fromByteBuffer(CloverBuffer.allocate(100), null); assertTrue(false); } catch (UnsupportedOperationException e) { /*OK*/ }
	}

	public void testToByteBuffer() throws CharacterCodingException {
		ListDataField listDataField = new ListDataField(createListMetadata());
		try { listDataField.toByteBuffer(ByteBuffer.allocate(100), null); assertTrue(false); } catch (UnsupportedOperationException e) { /*OK*/ }
	}

	public void testToByteBuffer1() throws CharacterCodingException {
		ListDataField listDataField = new ListDataField(createListMetadata());
		try { listDataField.toByteBuffer(CloverBuffer.allocate(100), null); assertTrue(false); } catch (UnsupportedOperationException e) { /*OK*/ }
	}

	public void testSerialize() {
		ListDataField listDataField = new ListDataField(createListMetadata());
		CloverBuffer buffer = CloverBuffer.allocate(Defaults.Record.RECORD_INITIAL_SIZE, Defaults.Record.RECORD_LIMIT_SIZE);
		
		listDataField.serialize(buffer);
		buffer.flip();
		assertEquals(buffer.limit(), listDataField.getSizeSerialized());
		assertEquals(1, buffer.limit());
		assertEquals(0, buffer.get());

		listDataField.setNull(false);
		buffer.clear();
		listDataField.serialize(buffer);
		buffer.flip();
		assertEquals(buffer.limit(), listDataField.getSizeSerialized());
		assertEquals(1, buffer.limit());
		assertEquals(1, buffer.get());
		
		listDataField.addField();
		buffer.clear();
		listDataField.serialize(buffer);
		buffer.flip();
		assertEquals(buffer.limit(), listDataField.getSizeSerialized());
		assertEquals(2, buffer.limit());
		assertEquals(2, buffer.get());
		assertEquals(0, buffer.get());

		listDataField.addField().setValue("abc");
		buffer.clear();
		listDataField.serialize(buffer);
		buffer.flip();
		assertEquals(buffer.limit(), listDataField.getSizeSerialized());
		assertEquals(9, buffer.limit());
		assertEquals(3, buffer.get());
		assertEquals(0, buffer.get());
		assertEquals(4, buffer.get());
		assertEquals('a', buffer.getChar());
		assertEquals('b', buffer.getChar());
		assertEquals('c', buffer.getChar());
	}
	
	public void testDeserialize() {
		ListDataField listDataField = new ListDataField(createListMetadata());
		CloverBuffer buffer = CloverBuffer.allocate(Defaults.Record.RECORD_INITIAL_SIZE, Defaults.Record.RECORD_LIMIT_SIZE);
		
		buffer.clear();
		buffer.put((byte) 0);
		buffer.flip();
		listDataField.deserialize(buffer);
		assertTrue(listDataField.isNull());
		assertEquals(0, listDataField.getSize());

		buffer.clear();
		buffer.put((byte) 1);
		buffer.flip();
		listDataField.deserialize(buffer);
		assertFalse(listDataField.isNull());
		assertEquals(0, listDataField.getSize());
		
		buffer.clear();
		buffer.put((byte) 2);
		buffer.put((byte) 0);
		buffer.flip();
		listDataField.deserialize(buffer);
		assertFalse(listDataField.isNull());
		assertEquals(1, listDataField.getSize());
		assertTrue(listDataField.getField(0).isNull());

		buffer.clear();
		buffer.put((byte) 3);
		buffer.put((byte) 0);
		buffer.put((byte) 4);
		buffer.putChar('a');
		buffer.putChar('b');
		buffer.putChar('c');
		buffer.flip();
		listDataField.deserialize(buffer);
		assertFalse(listDataField.isNull());
		assertEquals(2, listDataField.getSize());
		assertTrue(listDataField.getField(0).isNull());
		assertEquals(new CloverString("abc"), listDataField.getField(1).getValue());
	}

	public void testEquals() {
		ListDataField listDataField = new ListDataField(createListMetadata());
		ListDataField otherListDataField = new ListDataField(createListMetadata());
		
		listDataField.setNull(true);
		otherListDataField.setNull(true);
		assertFalse(listDataField.equals(otherListDataField));
		
		listDataField.setNull(false);
		otherListDataField.setNull(true);
		assertFalse(listDataField.equals(otherListDataField));
		
		listDataField.setNull(true);
		otherListDataField.setNull(false);
		assertFalse(listDataField.equals(otherListDataField));

		listDataField.setNull(false);
		otherListDataField.setNull(false);
		assertTrue(listDataField.equals(otherListDataField));
		
		listDataField.clear();
		otherListDataField.clear();
		listDataField.addField();
		assertFalse(listDataField.equals(otherListDataField));
		
		listDataField.clear();
		otherListDataField.clear();
		otherListDataField.addField();
		assertFalse(listDataField.equals(otherListDataField));
		
		listDataField.clear();
		otherListDataField.clear();
		listDataField.addField();
		otherListDataField.addField();
		assertTrue(listDataField.equals(otherListDataField));

		listDataField.clear();
		otherListDataField.clear();
		listDataField.addField().setValue("neco");
		otherListDataField.addField().setValue("neco");
		assertTrue(listDataField.equals(otherListDataField));

		listDataField.clear();
		otherListDataField.clear();
		otherListDataField.addField().setValue("neco");
		assertFalse(listDataField.equals(otherListDataField));

		listDataField.clear();
		listDataField.addField().setValue("neco");
		otherListDataField.clear();
		assertFalse(listDataField.equals(otherListDataField));

		listDataField.clear();
		otherListDataField.clear();
		listDataField.addField().setValue("neco");
		listDataField.addField();
		otherListDataField.addField().setValue("neco");
		otherListDataField.addField();
		assertTrue(listDataField.equals(otherListDataField));

		listDataField.clear();
		otherListDataField.clear();
		listDataField.addField().setValue("neco");
		listDataField.addField().setValue("");
		otherListDataField.addField().setValue("neco");
		otherListDataField.addField().setValue("");
		assertTrue(listDataField.equals(otherListDataField));
		
		listDataField.clear();
		assertFalse(listDataField.equals(new Object()));
		assertFalse(listDataField.equals(new StringDataField(new DataFieldMetadata("field", ";"))));
	}
	
	public void testCompareTo() {
		ListDataField listDataField = new ListDataField(createListMetadata());
		ListDataField otherListDataField = new ListDataField(createListMetadata());

		listDataField.setNull(true);
		otherListDataField.setNull(true);
		assertEquals(-1, listDataField.compareTo(otherListDataField));
		assertEquals(-1, otherListDataField.compareTo(listDataField));
		
		listDataField.setNull(false);
		otherListDataField.setNull(true);
		assertEquals(1, listDataField.compareTo(otherListDataField));
		assertEquals(-1, otherListDataField.compareTo(listDataField));
		
		listDataField.setNull(true);
		otherListDataField.setNull(false);
		assertEquals(-1, listDataField.compareTo(otherListDataField));
		assertEquals(1, otherListDataField.compareTo(listDataField));

		listDataField.setNull(false);
		otherListDataField.setNull(false);
		assertEquals(0, listDataField.compareTo(otherListDataField));
		assertEquals(0, otherListDataField.compareTo(listDataField));
		
		listDataField.clear();
		otherListDataField.clear();
		listDataField.addField();
		assertEquals(1, listDataField.compareTo(otherListDataField));
		assertEquals(-1, otherListDataField.compareTo(listDataField));
		
		listDataField.clear();
		otherListDataField.clear();
		otherListDataField.addField();
		assertEquals(-1, listDataField.compareTo(otherListDataField));
		assertEquals(1, otherListDataField.compareTo(listDataField));
		
		listDataField.clear();
		otherListDataField.clear();
		listDataField.addField();
		otherListDataField.addField();
		assertEquals(0, listDataField.compareTo(otherListDataField));
		assertEquals(0, otherListDataField.compareTo(listDataField));

		listDataField.clear();
		otherListDataField.clear();
		listDataField.addField().setValue("neco");
		otherListDataField.addField().setValue("neco");
		assertEquals(0, listDataField.compareTo(otherListDataField));
		assertEquals(0, otherListDataField.compareTo(listDataField));

		listDataField.clear();
		otherListDataField.clear();
		otherListDataField.addField().setValue("neco");
		assertEquals(-1, listDataField.compareTo(otherListDataField));
		assertEquals(1, otherListDataField.compareTo(listDataField));

		listDataField.setNull(true);
		otherListDataField.setNull(true);
		otherListDataField.addField().setValue("neco");
		assertEquals(-1, listDataField.compareTo(otherListDataField));
		assertEquals(1, otherListDataField.compareTo(listDataField));

		listDataField.clear();
		otherListDataField.clear();
		listDataField.addField().setValue("neco");
		listDataField.addField();
		otherListDataField.addField().setValue("neco");
		otherListDataField.addField();
		assertEquals(0, listDataField.compareTo(otherListDataField));
		assertEquals(0, otherListDataField.compareTo(listDataField));

		listDataField.clear();
		otherListDataField.clear();
		listDataField.addField().setValue("neco");
		listDataField.addField().setValue("");
		otherListDataField.addField().setValue("neco");
		otherListDataField.addField().setValue("");
		assertEquals(0, listDataField.compareTo(otherListDataField));
		assertEquals(0, otherListDataField.compareTo(listDataField));
		
		listDataField.clear();
		try { listDataField.compareTo(new Object()); assertTrue(false); } catch (ClassCastException e) { /*OK*/ }
		try { listDataField.compareTo(new StringDataField(new DataFieldMetadata("field", ";"))); assertTrue(false); } catch (ClassCastException e) { /*OK*/ }
	}
	
	public void testGetSizeSerialized() {
		ListDataField listDataField = new ListDataField(createListMetadata());

		assertEquals(1, listDataField.getSizeSerialized());
		
		listDataField.setNull(false);
		assertEquals(1, listDataField.getSizeSerialized());

		listDataField.addField();
		assertEquals(1 + listDataField.getField(0).getSizeSerialized(), listDataField.getSizeSerialized());

		listDataField.addField().setValue("neco");
		assertEquals(1 + listDataField.getField(0).getSizeSerialized() + listDataField.getField(1).getSizeSerialized(),
				listDataField.getSizeSerialized());

		listDataField.addField().setValue("neco jineho");
		assertEquals(1 
				+ listDataField.getField(0).getSizeSerialized()
				+ listDataField.getField(1).getSizeSerialized()
				+ listDataField.getField(2).getSizeSerialized(),
				listDataField.getSizeSerialized());
	}
	
	public void testIterator() {
		ListDataField listDataField = new ListDataField(createListMetadata());

		try { listDataField.iterator().remove(); assertTrue(false); } catch (UnsupportedOperationException e) { /*OK*/ }
		assertFalse(listDataField.iterator().hasNext());
		try { listDataField.iterator().next(); assertTrue(false); } catch (NoSuchElementException e) { /*OK*/ }
		
		listDataField.setNull(false);
		Iterator<DataField> it = listDataField.iterator();
		assertFalse(it.hasNext());
		try { it.next(); assertTrue(false); } catch (NoSuchElementException e) { /*OK*/ }

		listDataField.addField().setValue("neco");
		it = listDataField.iterator();
		assertTrue(it.hasNext());
		assertTrue(listDataField.getField(0) == it.next());
		assertFalse(it.hasNext());
		try { it.next(); assertTrue(false); } catch (NoSuchElementException e) { /*OK*/ }

		listDataField.addField();
		it = listDataField.iterator();
		assertTrue(it.hasNext());
		assertTrue(listDataField.getField(0) == it.next());
		assertTrue(it.hasNext());
		assertTrue(listDataField.getField(1) == it.next());
		assertFalse(it.hasNext());
		try { it.next(); assertTrue(false); } catch (NoSuchElementException e) { /*OK*/ }
	}
	
	public void testGeneralUsage() {
		//crate list data field
		ListDataField listDataField = new ListDataField(createListMetadata());
		
		//size is 0
		assertEquals(0, listDataField.getSize());
		
		//add a field
		listDataField.addField();
		
		//size is 1
		assertEquals(1, listDataField.getSize());
		
		//get the added data field
		StringDataField firstField = (StringDataField) listDataField.getField(0);
		
		//the first field is null
		assertTrue(firstField.isNull());
		
		//the first field is able to handle a string
		firstField.setValue("neco");
		assertEquals("neco", firstField.getValue().toString());
		
		//add second field and try the metadata type
		StringDataField secondDataField = (StringDataField) listDataField.addField();
		assertEquals(DataFieldType.STRING, secondDataField.getMetadata().getDataType());

		for (DataField field : listDataField) {
			((StringDataField) field).setValue("necojineho");
		}
		for (DataField field : listDataField) {
			assertEquals("necojineho", ((StringDataField) field).getValue().toString());
		}
	}
	
}
