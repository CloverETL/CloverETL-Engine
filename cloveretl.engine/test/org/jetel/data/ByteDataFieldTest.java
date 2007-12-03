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

import org.jetel.graph.runtime.EngineInitializer;
import org.jetel.main.runGraph;
import org.jetel.metadata.DataFieldMetadata;

import junit.framework.TestCase;

/**
 * @author maciorowski
 *
 */
public class ByteDataFieldTest  extends TestCase {
    byte[] byteArray=new byte[]{0x01,0x02,0x03,0x04,0x05,0x40};
    Byte[] ByteArray=new Byte[byteArray.length+1];
    
    ByteDataField byteField;
    
    static final String TEST_STRING="THIS is test !@#$%^&*()ěščřžýíé";
    
protected void setUp() {
<<<<<<< .working
	EngineInitializer.initEngine(null, null);
=======
    EngineInitializer.initEngine(null, null);
>>>>>>> .merge-right.r3636

    int i=0;
    for(byte b: byteArray){
        ByteArray[i++]=new Byte(b);
    }
    ByteArray[byteArray.length]=0x41;
    
    byteField=new ByteDataField(new DataFieldMetadata("byte",DataFieldMetadata.BYTE_FIELD,(short)10));
}

protected void tearDown() {

}

	/**
	 *  Test for @link org.jetel.data.ByteDataField.ByteDataField(DataFieldMetadata _metadata)
	 *
	 */
	public void test_1_setValue() {
        byteField.setValue(byteArray);
        byte[] data = byteField.getByteArray();
        for (int i = 0; i < data.length; i++) {
			assertEquals(byteArray[i], data[i] );
		}
        byteField.fromString(TEST_STRING,"UTF-8");
        assertEquals(TEST_STRING, byteField.toString("UTF-8"));
	}


	/**
	 *  Test for @link org.jetel.data.ByteDataField.ByteDataField(DataFieldMetadata _metadata, byte[] value)
	 *
	 */
	public void test_2_setValue() {
        byteField.setValue(ByteArray);
        System.out.println(byteField.toString());
        
	}

	/**
	 *  Test for @link org.jetel.data.ByteDataField.setValue(Object _value)
	 *
	 */
	public void test_3_setValue() {
        byteField.setValue((Object)byteArray);
        byte[] data = (byte[])byteField.getValue();
        for (int i = 0; i < data.length; i++) {
			assertEquals(byteArray[i], data[i] );
		}
	}

	/**
	 *  Test for @link org.jetel.data.ByteDataField.setValue(byte[] value)
	 *
	 */
	public void test_4_setValue() {
	}

	/**
	 *  Test for @link org.jetel.data.ByteDataField.setValue(byte value)
	 *
	 */
	public void test_setValue() {
	}

	/**
	 *  Test for @link org.jetel.data.ByteDataField.getValue()
	 *
	 */
	public void test_getValue() {
	}

	/**
	 *  Test for @link org.jetel.data.ByteDataField.getByte(int position)
	 *
	 */
	public void test_1_getByte() {
	}

	/**
	 *  Test for @link org.jetel.data.ByteDataField.getByte()
	 *
	 */
	public void test_2_getByte() {
	}

	/**
	 *  Test for @link org.jetel.data.ByteDataField.toString()
	 *
	 */
	public void test_toString() {
	}

	/**
	 *  Test for @link org.jetel.data.ByteDataField.fromString(String valueStr)
	 *
	 */
	public void test_fromString() {
	}

	/**
	 *  Test for @link org.jetel.data.ByteDataField.fromByteBuffer(ByteBuffer dataBuffer, CharsetDecoder decoder)
	 *
	 */
	public void test_fromByteBuffer() {
	}

	/**
	 *  Test for @link org.jetel.data.ByteDataField.toByteBuffer(ByteBuffer dataBuffer, CharsetEncoder encoder)
	 *
	 */
	public void test_toByteBuffer() {
	}

	/**
	 *  Test for @link org.jetel.data.ByteDataField.deserialize(ByteBuffer buffer)
	 *           @link org.jetel.data.ByteDataField.serialize(ByteBuffer buffer)
	 *
	 */

	public void test_serialize() {
	}

	/**
	 *  Test for @link org.jetel.data.ByteDataField.equals(Object obj)
	 *
	 */
	public void test_equals() {
	}

	/**
	 *  Test for @link org.jetel.data.ByteDataField.compareTo(Object obj)
	 *
	 */
	public void test_compareTo() {
	}
	/**
	 *  Test for @link org.jetel.data.ByteDataField.setToDefaultValue()
	 *
	 */
	public void test_setToDefaultValue() {
//		anIntegerDataField3.setToDefaultValue();
//		assertEquals("333333",anIntegerDataField3.toString());
//				
//		try {
//			anIntegerDataField4.setToDefaultValue();
//			fail("Field4 is not nullable and is being set to null!");
//		} catch (java.lang.RuntimeException re) {}
//		
//		anIntegerDataField1.setToDefaultValue();
//		assertEquals("",anIntegerDataField1.toString());
	}
}
