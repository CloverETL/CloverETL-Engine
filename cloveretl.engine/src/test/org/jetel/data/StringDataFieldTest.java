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

import org.jetel.data.StringDataField;
import org.jetel.metadata.DataFieldMetadata;

import junit.framework.TestCase;

/**
 * @author maciorowski
 *
 */
public class StringDataFieldTest  extends TestCase {
	private StringDataField aStringDataField1 = null;
	private StringDataField aStringDataField2 = null;
	private StringDataField aStringDataField3 = null;
	private StringDataField aStringDataField4 = null;

	

protected void setUp() { 
	DataFieldMetadata fixedFieldMeta1 = new DataFieldMetadata("Field1",'S',(short)3);
	aStringDataField1 = new StringDataField(fixedFieldMeta1,"boo");
	
	DataFieldMetadata fixedFieldMeta2 = new DataFieldMetadata("Field2",'S',(short)3);
	fixedFieldMeta2.setNullable(false);
	aStringDataField2 = new StringDataField(fixedFieldMeta2);

	DataFieldMetadata delimFieldMeta1 = new DataFieldMetadata("Field3",'S',";");
	aStringDataField3 = new StringDataField(delimFieldMeta1,"Boo3");
	
	DataFieldMetadata delimFieldMeta2 = new DataFieldMetadata("Field4",'S',",");
	delimFieldMeta2.setNullable(false);
	aStringDataField4 = new StringDataField(delimFieldMeta2);

}



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
 *  Test for @link org.jetel.data.StringDataField.deserialize(ByteBuffer buffer)
 *           @link org.jetel.data.StringDataField.serialize(ByteBuffer buffer)
 *
 */

public void test_serialize() {
	ByteBuffer buffer = ByteBuffer.allocateDirect(100);
	
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
	aStringDataField4.deserialize(buffer);
	assertEquals(aStringDataField4.isNull(),aStringDataField1.isNull());

	buffer.rewind();
	aStringDataField1.setValue(null);
	aStringDataField1.serialize(buffer);
	buffer.rewind();
	aStringDataField4.deserialize(buffer);
	assertEquals(aStringDataField4.isNull(),aStringDataField1.isNull());
	buffer = null;
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

}
