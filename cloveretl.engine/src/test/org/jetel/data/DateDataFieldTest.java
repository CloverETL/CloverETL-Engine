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
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import org.jetel.data.DateDataField;
import org.jetel.exception.BadDataFormatException;
import org.jetel.metadata.DataFieldMetadata;

import junit.framework.TestCase;

/**
 * @author maciorowski
 *
 */
public class DateDataFieldTest extends TestCase {
	private DateDataField aDateDataField1 = null;
	private DateDataField aDateDataField2 = null;
	private DateDataField aDateDataField3 = null;
	private DateDataField aDateDataField4 = null;

	

protected void setUp() { 
	Calendar calendar = new GregorianCalendar(2003,4,10);
	Date trialTime1 = null;
	trialTime1 = calendar.getTime(); 
	DataFieldMetadata fixedFieldMeta1 = new DataFieldMetadata("Field1",'D',(short)3);
	fixedFieldMeta1.setFormatStr("MM/dd/yyyy");
	aDateDataField1 = new DateDataField(fixedFieldMeta1, trialTime1);
	
	DataFieldMetadata fixedFieldMeta2 = new DataFieldMetadata("Field2",'D',(short)3);
	fixedFieldMeta2.setFormatStr("MM/dd/yyyy");
	aDateDataField2 = new DateDataField(fixedFieldMeta2);

	calendar = new GregorianCalendar(2002,6,10);
	trialTime1 = null;
	trialTime1 = calendar.getTime(); 
	DataFieldMetadata delimFieldMeta1 = new DataFieldMetadata("Field1",'D',";");
	delimFieldMeta1.setFormatStr("MM/dd/yyyy");
	aDateDataField3 = new DateDataField(delimFieldMeta1,trialTime1);
	
	DataFieldMetadata delimFieldMeta2 = new DataFieldMetadata("Field1",'D',",");
	delimFieldMeta2.setFormatStr("MM/dd/yyyy");
	aDateDataField4 = new DateDataField(delimFieldMeta2);


}



protected void tearDown() {
	aDateDataField1 = null;
	aDateDataField2 = null;
	aDateDataField3 = null;
	aDateDataField4 = null;
}


/**
 *  Test for @link org.jetel.data.DateDataField.DateDataField(DataFieldMetadata _metadata)
 *
 */
public void test_1_DateDataField() {
	assertNotNull(aDateDataField2);
	assertNotNull(aDateDataField4);
	assertTrue(aDateDataField4.isNull());
	}


	/**
	 *  Test for @link org.jetel.data.DateDataField.DateDataField(DataFieldMetadata _metadata, Date _value)
	 *
	 */
	public void test_2_DateDataField() {
		assertNotNull(aDateDataField1);
		assertNotNull(aDateDataField3);
		assertFalse(aDateDataField1.isNull());
	}

	/**
	 *  Test for @link org.jetel.data.DateDataField.setValue(Object _value)
	 *
	 */
	public void test_setValue() {
		Calendar calendar = new GregorianCalendar(2003,4,10);
		Date trialTime1 = null;
		trialTime1 = calendar.getTime(); 
		aDateDataField1.setValue(trialTime1);
		assertEquals("setValue(Object value) failed",aDateDataField1.getValue(), trialTime1);
		assertFalse(aDateDataField1.isNull());

		aDateDataField1.setValue(null);
		assertTrue(aDateDataField1.isNull());
	}


	/**
	 *  Test for @link org.jetel.data.DateDataField.getValue()
	 *
	 */
	public void test_getValue() {
		Calendar calendar = new GregorianCalendar(2003,4,10);
		Date trialTime1 = null;
		trialTime1 = calendar.getTime(); 
		aDateDataField1.setValue(trialTime1);
		assertEquals("getValue() failed",aDateDataField1.getValue(), trialTime1);
	}

	/**
	 *  Test for @link org.jetel.data.DateDataField.toString()
	 *
	 */
	public void test_toString() {
		Calendar cal = new GregorianCalendar(2003,3,10);
		Date trialTime1 = cal.getTime(); 
		aDateDataField1.setValue(trialTime1);
		assertEquals("toString() failed",aDateDataField1.toString(), "04/10/2003");
	}


/**
 *  Test for @link org.jetel.data.DateDataField.fromString(String valueStr)
 *
 */
public void test_fromString() {
	aDateDataField1.fromString("07/10/1996");
	assertEquals(aDateDataField1.toString(),"07/10/1996");

	aDateDataField1.fromString(null);
	assertTrue(aDateDataField1.isNull());
	
	try {
		aDateDataField1.fromString("123.234");
		fail("Should raise an BadDataFormatException");
	} catch (BadDataFormatException e){	}
}


/**
 *  Test for @link org.jetel.data.DateDataField.deserialize(ByteBuffer buffer)
 *           @link org.jetel.data.DateDataField.serialize(ByteBuffer buffer)
 *
 */

public void test_serialize() {
	ByteBuffer buffer = ByteBuffer.allocateDirect(100);
	
	Calendar calendar = new GregorianCalendar(2003,4,10);
	Date trialTime1 = null;
	trialTime1 = calendar.getTime(); 
	aDateDataField1.setValue(trialTime1);
	aDateDataField1.serialize(buffer);
	buffer.rewind();
	aDateDataField4.deserialize(buffer);
	assertEquals(aDateDataField4.getValue(),aDateDataField1.getValue());
	assertEquals(aDateDataField4.isNull(),aDateDataField1.isNull());
	assertEquals(aDateDataField4,aDateDataField1);
	
	buffer.rewind();
	aDateDataField1.setNull(true);
	aDateDataField1.serialize(buffer);
	buffer.rewind();
	aDateDataField4.deserialize(buffer);
	assertEquals(aDateDataField4.isNull(),aDateDataField1.isNull());

	buffer.rewind();
	aDateDataField1.setValue(null);
	aDateDataField1.serialize(buffer);
	buffer.rewind();
	aDateDataField4.deserialize(buffer);
	assertEquals(aDateDataField4.isNull(),aDateDataField1.isNull());
	buffer = null;
}

/**
 *  Test for @link org.jetel.data.DateDataField.equals(Object obj)
 *
 */
public void test_equals() {
	Calendar calendar = new GregorianCalendar(2003,4,10);
	Date trialTime1 = null;
	trialTime1 = calendar.getTime(); 
	aDateDataField1.setValue(trialTime1);
	aDateDataField4.setValue(trialTime1);
	assertTrue(aDateDataField1.equals(aDateDataField4));
	
	Calendar calendar2 = new GregorianCalendar(2003,6,10);
	Date trialTime2 = null;
	trialTime2 = calendar2.getTime(); 
	aDateDataField4.setValue(trialTime2);
	assertFalse(aDateDataField1.equals(aDateDataField4));
}

/**
 *  Test for @link org.jetel.data.DateDataField.compareTo(Object obj)
 *
 */
public void test_compareTo() {
	Calendar calendar = new GregorianCalendar(2003,4,10);
	Date trialTime1 = null;
	trialTime1 = calendar.getTime(); 
	aDateDataField1.setValue(trialTime1);
	aDateDataField4.setValue(trialTime1);
	assertEquals(aDateDataField1.compareTo(aDateDataField4.getValue()),0);
}

}
