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

package test.org.jetel.data;

import java.nio.ByteBuffer;

import org.jetel.data.NumericDataField;
import org.jetel.exception.BadDataFormatException;
import org.jetel.metadata.DataFieldMetadata;

import junit.framework.TestCase;

/**
 * @author maciorowski
 *
 */
public class NumericDataFieldTest  extends TestCase {
	private NumericDataField aNumericDataField1 = null;
	private NumericDataField aNumericDataField2 = null;
	private NumericDataField aNumericDataField3 = null;
	private NumericDataField aNumericDataField4 = null;

	

protected void setUp() { 
	DataFieldMetadata fixedFieldMeta1 = new DataFieldMetadata("Field1",'i',(short)3);
	aNumericDataField1 = new NumericDataField(fixedFieldMeta1,65.5);
	
	DataFieldMetadata fixedFieldMeta2 = new DataFieldMetadata("Field2",'i',(short)3);
	aNumericDataField2 = new NumericDataField(fixedFieldMeta2);

	DataFieldMetadata delimFieldMeta1 = new DataFieldMetadata("Field1",'i',";");
	aNumericDataField3 = new NumericDataField(delimFieldMeta1,5.55);
	
	DataFieldMetadata delimFieldMeta2 = new DataFieldMetadata("Field1",'i',",");
	aNumericDataField4 = new NumericDataField(delimFieldMeta2);

}



protected void tearDown() {
	aNumericDataField1 = null;
	aNumericDataField2 = null;
	aNumericDataField3 = null;
	aNumericDataField4 = null;
}


/**
 *  Test for @link org.jetel.data.NumericDataField.NumericDataField(DataFieldMetadata _metadata)
 *
 */
public void test_1_NumericDataField() {
	assertNotNull(aNumericDataField2);
	assertNotNull(aNumericDataField4);
	}


	/**
	 *  Test for @link org.jetel.data.NumericDataField.NumericDataField(DataFieldMetadata _metadata, double value)
	 *
	 */
	public void test_2_NumericDataField() {
		assertNotNull(aNumericDataField1);
		assertNotNull(aNumericDataField3);
	}

	/**
	 *  Test for @link org.jetel.data.NumericDataField.setValue(Object _value)
	 *                 org.jetel.data.NumericDataField.setValue(double value)
	 *                 org.jetel.data.NumericDataField.setValue(int value)
	 */
	public void test_setValue() {
		aNumericDataField1.setValue(15.45);
		assertEquals("setValue(double value) failed",aNumericDataField1.getDouble(), 15.45, 0.0);

		aNumericDataField1.setValue(15);
		assertEquals("setValue(int value) failed",aNumericDataField1.getDouble(), 15.0, 0.0);

		aNumericDataField1.setValue(new Double(15.1234));
		assertEquals("setValue(Object value) failed",aNumericDataField1.getDouble(), 15.1234, 0.0);
		assertFalse(aNumericDataField1.isNull());

		aNumericDataField1.setValue(null);
		assertTrue(aNumericDataField1.isNull());
	}


	/**
	 *  Test for @link org.jetel.data.NumericDataField.getValue()
	 *
	 */
	public void test_getValue() {
		aNumericDataField1.setValue(17.45);
		assertEquals("getValue() failed",aNumericDataField1.getValue(), new Double(17.45));
	}

	/**
	 *  Test for @link org.jetel.data.NumericDataField.toString()
	 *
	 */
	public void test_toString() {
		aNumericDataField1.setValue(19.45);
		assertEquals("toString() failed",aNumericDataField1.toString(), "19.45");
	}

/**
 *  Test for @link org.jetel.data.NumericDataField.fromString(String valueStr)
 *
 */
public void test_fromString() {
	aNumericDataField1.fromString("123");
	assertEquals(aNumericDataField1.getInt(),123);

	aNumericDataField1.fromString("");
	assertTrue(aNumericDataField1.isNull());
	
	try {
		aNumericDataField1.fromString("r123.43");
		fail("Should raise an BadDataFormatException");
	} catch (BadDataFormatException e){	}
}


/**
 *  Test for @link org.jetel.data.NumericDataField.deserialize(ByteBuffer buffer)
 *           @link org.jetel.data.NumericDataField.serialize(ByteBuffer buffer)
 *
 */

public void test_serialize() {
	ByteBuffer buffer = ByteBuffer.allocateDirect(100);
	
	aNumericDataField1.setValue(123.23);
	aNumericDataField1.serialize(buffer);
	buffer.rewind();
	aNumericDataField4.deserialize(buffer);
	assertEquals(aNumericDataField4.getDouble(),123.23,0.0);
	assertEquals(aNumericDataField4.isNull(),aNumericDataField1.isNull());
	assertEquals(aNumericDataField4,aNumericDataField1);
	
	buffer.rewind();
	aNumericDataField1.setNull(true);
	aNumericDataField1.serialize(buffer);
	buffer.rewind();
	aNumericDataField4.deserialize(buffer);
	assertEquals(aNumericDataField4.isNull(),aNumericDataField1.isNull());

	buffer.rewind();
	aNumericDataField1.setValue(null);
	aNumericDataField1.serialize(buffer);
	buffer.rewind();
	aNumericDataField4.deserialize(buffer);
	assertEquals(aNumericDataField4.isNull(),aNumericDataField1.isNull());
	buffer = null;
}

/**
 *  Test for @link org.jetel.data.NumericDataField.equals(Object obj)
 *
 */
public void test_equals() {
	aNumericDataField1.setValue(5.23423);
	aNumericDataField4.setValue(5.23423);
	assertTrue(aNumericDataField1.equals(aNumericDataField4));
	
	aNumericDataField4.setValue(7);
	assertFalse(aNumericDataField1.equals(aNumericDataField4));
}

/**
 *  Test for @link org.jetel.data.NumericDataField.compareTo(Object obj)
 *
 */
public void test_1_compareTo() {
	aNumericDataField1.setValue(5.654);
	aNumericDataField4.setValue(5.654);
	assertEquals(aNumericDataField1.compareTo(aNumericDataField4),0);
}
/**
 *  Test for @link org.jetel.data.NumericDataField.compareTo(double compVal)
 *
 */
public void test_2_compareTo() {
	aNumericDataField1.setValue(5.65);
	assertEquals(aNumericDataField1.compareTo(5.65),0);
}

}
