/*
 *  jETeL/Clover - Java based ETL application framework.
 *  Created on Apr 10, 2003
 *  Copyright (C) 2003, 2002  David Pavlis, Wes Maciorowski
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 2 of the License, or
 *  (at your option) any later version.
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */

package org.jetel.data;

import java.nio.ByteBuffer;

import org.jetel.exception.BadDataFormatException;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.test.CloverTestCase;

/**
 * @author maciorowski
 *
 */
public class IntegerDataFieldTest  extends CloverTestCase {
	private IntegerDataField anIntegerDataField1 = null;
	private IntegerDataField anIntegerDataField2 = null;
	private IntegerDataField anIntegerDataField3 = null;
	private IntegerDataField anIntegerDataField4 = null;

protected void setUp() { 
	DataFieldMetadata fixedFieldMeta1 = new DataFieldMetadata("Field1",'i',(short)3);
	anIntegerDataField1 = new IntegerDataField(fixedFieldMeta1,5);
	
	DataFieldMetadata fixedFieldMeta2 = new DataFieldMetadata("Field2",'i',(short)3);
	fixedFieldMeta2.setNullable(false);
	anIntegerDataField2 = new IntegerDataField(fixedFieldMeta2);

	DataFieldMetadata delimFieldMeta1 = new DataFieldMetadata("Field1",'i',";");
	delimFieldMeta1.setDefaultValue(333333);
	anIntegerDataField3 = new IntegerDataField(delimFieldMeta1,5);
	
	DataFieldMetadata delimFieldMeta2 = new DataFieldMetadata("Field1",'i',",");
	delimFieldMeta2.setNullable(false);
	anIntegerDataField4 = new IntegerDataField(delimFieldMeta2);
}

protected void tearDown() {
	anIntegerDataField1 = null;
	anIntegerDataField2 = null;
	anIntegerDataField3 = null;
	anIntegerDataField4 = null;
}

/**
 *  Test for @link org.jetel.data.IntegerDataField.IntegerDataField(DataFieldMetadata _metadata)
 *
 */
public void test_1_IntegerDataField() {
	assertNotNull(anIntegerDataField2);
	assertNotNull(anIntegerDataField4);
	}


	/**
	 *  Test for @link org.jetel.data.IntegerDataField.IntegerDataField(DataFieldMetadata _metadata, int value)
	 *
	 */
	public void test_2_IntegerDataField() {
		assertNotNull(anIntegerDataField1);
		assertNotNull(anIntegerDataField3);
		assertFalse(anIntegerDataField3.isNull());
	}

	/**
	 *  Test for @link org.jetel.data.IntegerDataField.setValue(Object _value)
	 *                 org.jetel.data.IntegerDataField.setValue(double value)
	 *                 org.jetel.data.IntegerDataField.setValue(int value)
	 */
	public void test_setValue() {
		anIntegerDataField1.setValue(15.45);
		assertEquals("setValue(double value) failed",anIntegerDataField1.getDouble(), 15.0, 0.0);

		anIntegerDataField1.setValue(15);
		assertEquals("setValue(int value) failed",anIntegerDataField1.getDouble(), 15.0, 0.0);

		anIntegerDataField1.setValue(Integer.valueOf(15));
		assertEquals("setValue(Object value) failed",anIntegerDataField1.getDouble(), 15.0, 0.0);
		assertFalse(anIntegerDataField1.isNull());

		try {
			anIntegerDataField2.setNull();
			fail("anIntegerDataField2 is not nullable - BadDataFormatException should be thrown");
		} catch(BadDataFormatException e){}

		try {
			anIntegerDataField1.setNull();
			assertTrue(anIntegerDataField1.isNull());
			assertEquals("setValue(null) failed", null, anIntegerDataField1.getValue());
		} catch(BadDataFormatException e){
			fail("anIntegerDataField1 is nullable - BadDataFormatException should not be thrown");
		}
	}


	/**
	 *  Test for @link org.jetel.data.IntegerDataField.getValue()
	 *
	 */
	public void test_getValue() {
		anIntegerDataField1.setValue(17.45);
		assertEquals("getValue() failed",anIntegerDataField1.getValue(), Integer.valueOf(17));

		anIntegerDataField1.setNull();
		assertEquals(null, anIntegerDataField1.getValue());

		anIntegerDataField1.setNull();
		assertEquals("", anIntegerDataField1.toString());
	}

	/**
	 *  Test for @link org.jetel.data.IntegerDataField.toString()
	 *
	 */
	public void test_toString() {
		anIntegerDataField1.setValue(19.45);
		assertEquals("toString() failed",anIntegerDataField1.toString(), "19");

		anIntegerDataField1.setNull();
		assertEquals("", anIntegerDataField1.toString());
	}

	/**
	 *  Test for @link org.jetel.data.IntegerDataField.fromString(String valueStr)
	 *
	 */
	public void test_fromString() {
		anIntegerDataField1.fromString("123");
		assertEquals(anIntegerDataField1.getInt(),123);
		
		
		anIntegerDataField1.fromString(null);
		assertTrue(anIntegerDataField1.isNull());
		assertEquals("", anIntegerDataField1.toString());
		
		anIntegerDataField1.fromString("");
		assertTrue(anIntegerDataField1.isNull());
		assertEquals("", anIntegerDataField1.toString());
			
		try {
			anIntegerDataField2.fromString("");
			fail("Should raise an BadDataFormatException");
		} catch (BadDataFormatException e){	}
	
		try {
			anIntegerDataField1.fromString("123.234");
			fail("Should raise an BadDataFormatException");
		} catch (BadDataFormatException e){
		}

		try {
			anIntegerDataField1.fromString("r123");
			fail("Should raise an BadDataFormatException");
		} catch (BadDataFormatException e){
		}
	}
	
	
	/**
	 *  Test for @link org.jetel.data.IntegerDataField.deserialize(ByteBuffer buffer)
	 *           @link org.jetel.data.IntegerDataField.serialize(ByteBuffer buffer)
	 *
	 */
	
	public void test_serialize() {
		ByteBuffer buffer = ByteBuffer.allocateDirect(100);
		
		anIntegerDataField1.setValue(123);
		anIntegerDataField1.serialize(buffer);
		buffer.rewind();
		anIntegerDataField4.deserialize(buffer);
		assertEquals(anIntegerDataField4.getInt(),123);
		assertEquals(anIntegerDataField4.isNull(),anIntegerDataField1.isNull());
		assertEquals(anIntegerDataField4,anIntegerDataField1);
		
		buffer.rewind();
		anIntegerDataField1.setValue(1);
		anIntegerDataField1.serialize(buffer);
		buffer.rewind();
		anIntegerDataField4.deserialize(buffer);
		assertEquals(anIntegerDataField4.isNull(),anIntegerDataField1.isNull());
	
		buffer.rewind();
		anIntegerDataField1.setNull(false);
		anIntegerDataField1.serialize(buffer);
		buffer.rewind();
		anIntegerDataField4.deserialize(buffer);
		assertEquals(anIntegerDataField4.isNull(),anIntegerDataField1.isNull());
	
	
		buffer.rewind();
		anIntegerDataField1.fromString("1");
		anIntegerDataField1.serialize(buffer);
		buffer.rewind();
		anIntegerDataField4.deserialize(buffer);
		assertEquals(anIntegerDataField4.getValue(),anIntegerDataField1.getValue());
	}
	
	/**
	 *  Test for @link org.jetel.data.IntegerDataField.equals(Object obj)
	 *
	 */
	public void test_equals() {
		anIntegerDataField4.setValue(5);
		assertTrue(anIntegerDataField1.equals(anIntegerDataField4));
		
		anIntegerDataField4.setValue(7);
		assertFalse(anIntegerDataField1.equals(anIntegerDataField4));
	}
	
	/**
	 *  Test for @link org.jetel.data.IntegerDataField.compareTo(Object obj)
	 *
	 */
	public void test_1_compareTo() {
		anIntegerDataField4.setValue(5);
		assertEquals(anIntegerDataField1.compareTo(anIntegerDataField4),0);
	}
	/**
	 *  Test for @link org.jetel.data.IntegerDataField.compareTo(int compInt)
	 *
	 */
	public void test_2_compareTo() {
		assertEquals(anIntegerDataField1.compareTo(5),0);
	}


	/**
	 *  Test for @link org.jetel.data.IntegerDataField.setToDefaultValue()
	 *
	 */
	public void test_setToDefaultValue() {
		anIntegerDataField3.setToDefaultValue();
		assertEquals("333333",anIntegerDataField3.toString());
				
		try {
			anIntegerDataField4.setToDefaultValue();
			fail("Field4 is not nullable and is being set to null!");
		} catch (java.lang.RuntimeException re) {}
		
		anIntegerDataField1.setToDefaultValue();
		assertTrue(anIntegerDataField1.isNull());
	}
}
