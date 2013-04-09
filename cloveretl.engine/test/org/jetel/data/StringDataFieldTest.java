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

import org.jetel.exception.BadDataFormatException;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.test.CloverTestCase;
import org.jetel.util.bytes.CloverBuffer;

/**
 * @author maciorowski
 *
 */
public class StringDataFieldTest  extends CloverTestCase {
	private StringDataField aStringDataField1 = null;
	private StringDataField aStringDataField2 = null;
	private StringDataField aStringDataField3 = null;
	private StringDataField aStringDataField4 = null;

	

@Override
protected void setUp() throws Exception { 
	super.setUp();
	
	DataFieldMetadata fixedFieldMeta1 = new DataFieldMetadata("Field1",'S',(short)3);
	fixedFieldMeta1.setDefaultValue("abc");
	aStringDataField1 = new StringDataField(fixedFieldMeta1,"boo");

	DataFieldMetadata fixedFieldMeta2 = new DataFieldMetadata("Field2",'S',(short)3);
	fixedFieldMeta2.setNullable(false);
	aStringDataField2 = new StringDataField(fixedFieldMeta2);

	DataFieldMetadata delimFieldMeta1 = new DataFieldMetadata("Field3",'S',";");
	delimFieldMeta1.setDefaultValue("really default");
	aStringDataField3 = new StringDataField(delimFieldMeta1,"Boo3");
	
	DataFieldMetadata delimFieldMeta2 = new DataFieldMetadata("Field4",'S',",");
	delimFieldMeta2.setNullable(false);
	aStringDataField4 = new StringDataField(delimFieldMeta2);

}



@Override
protected void tearDown() {
	aStringDataField1 = null;
	aStringDataField2 = null;
	aStringDataField3 = null;
	aStringDataField4 = null;
}


/**
 *  Test for @link org.jetel.data.StringDataField.StringDataField(DataFieldMetadata _metadata)
 *
 */
public void test_1_StringDataField() {
	assertNotNull(aStringDataField2);
	assertNotNull(aStringDataField4);
	}


	/**
	 *  Test for @link org.jetel.data.StringDataField.StringDataField(DataFieldMetadata _metadata, double value)
	 *
	 */
	public void test_2_StringDataField() {
		assertNotNull(aStringDataField1);
		assertNotNull(aStringDataField3);
		assertFalse(aStringDataField3.isNull());
	}

	/**
	 *  Test for @link org.jetel.data.StringDataField.setValue(Object _value)
	 *
	 */
	public void test_setValue() {
		aStringDataField1.setValue("abcd");
		assertEquals("setValue(Object value) failed", "abcd",aStringDataField1.getValue().toString());
		assertFalse(aStringDataField1.isNull());

		try {
			aStringDataField2.setValue((String)null);
			fail("aStringDataField2 is not nullable - BadDataFormatException should be thrown");
		} catch(BadDataFormatException e){}

		try {
			aStringDataField1.setValue((String)null);
			assertTrue(aStringDataField1.isNull());
		} catch(BadDataFormatException e){
			fail("aStringDataField1 is nullable - BadDataFormatException should not be thrown");
		}
	}

	/**
	 *  Test for @link org.jetel.data.StringDataField.getValue()
	 *
	 */
	public void test_getValue() {
		aStringDataField1.setValue("abcd");
		assertEquals("getValue() failed",aStringDataField1.getValue().toString(), "abcd");

		aStringDataField1.setValue((String)null);
		assertEquals(null, aStringDataField1.getValue());

	}
	/**
	 *  Test for @link org.jetel.data.StringDataField.toString()
	 *
	 */
	public void test_toString() {
		aStringDataField1.setValue("04/10/2003");
		assertEquals("toString() failed", "04/10/2003",aStringDataField1.toString());

		aStringDataField1.setValue((String)null);
		assertEquals("", aStringDataField1.toString());
	}


	/**
	 *  Test for @link org.jetel.data.StringDataField.fromString(String valueStr)
	 *
	 */
	public void test_fromString() {
		aStringDataField1.fromString("07/10/1996");
		assertEquals(aStringDataField1.toString(),"07/10/1996");
	
		aStringDataField1.fromString(null);
		assertTrue(aStringDataField1.isNull());
	
		aStringDataField1.fromString("");
		assertTrue(aStringDataField1.isNull());
		
		try {
			aStringDataField2.fromString("");
			fail("Should raise an BadDataFormatException");
		} catch (BadDataFormatException e){	}
	
	}


	/**
	 *  Test for @link org.jetel.data.StringDataField.deserialize(ByteBuffer buffer)
	 *           @link org.jetel.data.StringDataField.serialize(ByteBuffer buffer)
	 *
	 */
	
	public void test_serialize() {
		CloverBuffer buffer = CloverBuffer.allocateDirect(100);
		
		aStringDataField1.setValue("adasdad");
		aStringDataField1.serialize(buffer);
		buffer.rewind();
		aStringDataField4.deserialize(buffer);
		assertEquals(aStringDataField4.toString(),"adasdad");
		assertEquals(aStringDataField4.isNull(),aStringDataField1.isNull());
		assertEquals(aStringDataField4,aStringDataField1);
		
		buffer.rewind();
		aStringDataField1.setNull(true);
		aStringDataField1.serialize(buffer);
		buffer.rewind();
		try {
			aStringDataField4.deserialize(buffer);
			fail("Field 4 is not nullable");
//			assertEquals(aStringDataField4.isNull(),aStringDataField1.isNull());
		} catch (BadDataFormatException e) {
		}
	
	}
	
	/**
	 *  Test for @link org.jetel.data.StringDataField.equals(Object obj)
	 *
	 */
	public void test_equals() {
		aStringDataField1.setValue("5.23423");
		aStringDataField4.setValue("5.23423");
		assertTrue(aStringDataField1.equals(aStringDataField4));
		
		aStringDataField4.setValue("7");
		assertFalse(aStringDataField1.equals(aStringDataField4));
	}
	
	/**
	 *  Test for @link org.jetel.data.StringDataField.compareTo(Object obj)
	 *
	 */
	public void test_compareTo() {
		aStringDataField1.setValue("5.654");
		aStringDataField4.setValue("5.654");
		assertEquals(aStringDataField1.compareTo(aStringDataField4),0);
	}

	/**
	 *  Test for @link org.jetel.data.StringDataField.setToDefaultValue()
	 *
	 */
	public void test_setToDefaultValue() {
		aStringDataField3.setToDefaultValue();
		assertEquals("really default",aStringDataField3.toString());
		
		try {
			aStringDataField4.setToDefaultValue();
			fail("Field4 is not nullable and is being set to null!");
		} catch (java.lang.RuntimeException re) {}

		aStringDataField1.setToDefaultValue();
		assertEquals("abc",aStringDataField1.toString());
	}
}
