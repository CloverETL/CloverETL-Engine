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

import org.jetel.data.IntegerDataField;
import org.jetel.metadata.DataFieldMetadata;

import junit.framework.TestCase;

/**
 * @author maciorowski
 *
 */
public class IntegerDataFieldTest  extends TestCase {
	private IntegerDataField anIntegerDataField1 = null;
	private IntegerDataField anIntegerDataField2 = null;
	private IntegerDataField anIntegerDataField3 = null;
	private IntegerDataField anIntegerDataField4 = null;

protected void setUp() { 
	DataFieldMetadata fixedFieldMeta1 = new DataFieldMetadata("Field1",'i',(short)3);
	anIntegerDataField1 = new IntegerDataField(fixedFieldMeta1,5);
	
	DataFieldMetadata fixedFieldMeta2 = new DataFieldMetadata("Field2",'i',(short)3);
	anIntegerDataField2 = new IntegerDataField(fixedFieldMeta2);

	DataFieldMetadata delimFieldMeta1 = new DataFieldMetadata("Field1",'i',";");
	anIntegerDataField3 = new IntegerDataField(delimFieldMeta1,5);
	
	DataFieldMetadata delimFieldMeta2 = new DataFieldMetadata("Field1",'i',",");
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
	}

	/**
	 *  Test for @link org.jetel.data.IntegerDataField.setValue(Object _value)
	 *                 org.jetel.data.IntegerDataField.setValue(double value)
	 *                 org.jetel.data.IntegerDataField.setValue(int value)
	 */
	public void test_setValue() {
		anIntegerDataField1.setValue(15.45);
		//assertEquals(anIntegerDataField1.getDouble(),(double)15);
	}


	/**
	 *  Test for @link org.jetel.data.IntegerDataField.getValue()
	 *
	 */
	public void test_getValue() {
	}

	/**
	 *  Test for @link org.jetel.data.IntegerDataField.toString()
	 *
	 */
	public void test_toString() {
	}

	/**
	 *  Test for @link org.jetel.data.IntegerDataField.fromByteBuffer(ByteBuffer dataBuffer, CharsetDecoder decoder) throws CharacterCodingException
	 *
	 */
	public void test_fromByteBuffer() {
	}

	/**
	 *  Test for @link org.jetel.data.IntegerDataField.toByteBuffer(ByteBuffer dataBuffer, CharsetEncoder encoder) throws CharacterCodingException
	 *
	 */
	public void test_toByteBuffer() {
	}


/**
 *  Test for @link org.jetel.data.IntegerDataField.fromString(String valueStr)
 *
 */
public void test_fromString() {
}


/**
 *  Test for @link org.jetel.data.IntegerDataField.deserialize(ByteBuffer buffer)
 *           @link org.jetel.data.IntegerDataField.serialize(ByteBuffer buffer)
 *
 */

public void test_serialize() {
}

/**
 *  Test for @link org.jetel.data.IntegerDataField.equals(Object obj)
 *
 */
public void test_equals() {
}

/**
 *  Test for @link org.jetel.data.IntegerDataField.compareTo(Object obj)
 *
 */
public void test_1_compareTo() {
}
/**
 *  Test for @link org.jetel.data.IntegerDataField.compareTo(int compInt)
 *
 */
public void test_2_compareTo() {
}


}
