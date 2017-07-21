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

import java.io.UnsupportedEncodingException;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import org.jetel.exception.BadDataFormatException;
import org.jetel.metadata.DataFieldFormatType;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataFieldType;
import org.jetel.test.CloverTestCase;
import org.jetel.util.bytes.CloverBuffer;

/**
 * @author maciorowski
 *
 */
public class DateDataFieldTest extends CloverTestCase {
	private DateDataField aDateDataField1 = null;
	private DateDataField aDateDataField2 = null;
	private DateDataField aDateDataField3 = null;
	private DateDataField aDateDataField4 = null;


@Override
protected void setUp() throws Exception { 
	super.setUp();

	Calendar calendar = new GregorianCalendar(2003,4,10);
	Date trialTime1 = calendar.getTime(); 
	DataFieldMetadata fixedFieldMeta1 = new DataFieldMetadata("Field1",'D',(short)3);
	fixedFieldMeta1.setFormatStr("MM/dd/yyyy");
	aDateDataField1 = new DateDataField(fixedFieldMeta1, trialTime1);
	
	DataFieldMetadata fixedFieldMeta2 = new DataFieldMetadata("Field2",'D',(short)3);
	fixedFieldMeta2.setFormatStr("MM/dd/yyyy hh:mm");
	fixedFieldMeta2.setNullable(false);
	aDateDataField2 = new DateDataField(fixedFieldMeta2);

//	calendar = new GregorianCalendar(2002,6,10);
//	trialTime1 = calendar.getTime(); 
	DataFieldMetadata delimFieldMeta1 = new DataFieldMetadata("Field1",'D',";");
	delimFieldMeta1.setFormatStr("MM/dd/yyyy");
	delimFieldMeta1.setDefaultValueStr("03/31/2100");
	aDateDataField3 = new DateDataField(delimFieldMeta1, null);
	
	DataFieldMetadata delimFieldMeta2 = new DataFieldMetadata("Field1",'D',",");
	delimFieldMeta2.setFormatStr("hhhh");
	delimFieldMeta2.setNullable(false);
	aDateDataField4 = new DateDataField(delimFieldMeta2);
}



@Override
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
	assertTrue(aDateDataField2.getMetadata().isDateFormat());
	assertTrue(aDateDataField2.getMetadata().isTimeFormat());
	assertTrue(aDateDataField4.getMetadata().isTimeFormat());
	assertFalse(aDateDataField4.getMetadata().isDateFormat());
	assertTrue(aDateDataField1.getMetadata().isDateFormat());
	assertFalse(aDateDataField1.getMetadata().isTimeFormat());
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

		try {
			aDateDataField2.setValue(null);
			fail("aDateDataField2 is not nullable - BadDataFormatException should be thrown");
		} catch(BadDataFormatException e){}

		try {
			aDateDataField1.setValue(null);
			assertTrue(aDateDataField1.isNull());
		} catch(BadDataFormatException e){
			fail("aDateDataField1 is nullable - BadDataFormatException should not be thrown");
		}
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

		aDateDataField1.setValue(null);
		assertEquals(null, aDateDataField1.getValue());

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

		aDateDataField1.setValue(null);
		assertEquals("", aDateDataField1.toString());
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
	
		aDateDataField1.fromString("");
		assertTrue(aDateDataField1.isNull());
		
		try {
			aDateDataField2.fromString("");
			fail("Should raise an BadDataFormatException");
		} catch (BadDataFormatException e){	}
	
		try {
			aDateDataField1.fromString("123.234");
			fail("Should raise an BadDataFormatException");
		} catch (BadDataFormatException e){	}
	}

	public void test_fromStringJoda() {
		String as400DateFormat = "yyyy-MM-dd-HH.mm.ss.SSS000";
		String as400TestDate = "2002-07-10-22.36.15.129000";

		DataFieldMetadata javaDateMetadataDefault = new DataFieldMetadata("date", DataFieldMetadata.DATE_FIELD, ";");
		javaDateMetadataDefault.setFormatStr(as400DateFormat);
		DateDataField javaDateDataFieldDefault = new DateDataField(javaDateMetadataDefault);

		try {
			javaDateDataFieldDefault.fromString(as400TestDate);
			fail("The BadDataFormatException should be thrown for no prefix specified.");
		} catch (BadDataFormatException exception) {
			// OK
		}

		DataFieldMetadata javaDateMetadataPrefix = new DataFieldMetadata("date", DataFieldMetadata.DATE_FIELD, ";");
		javaDateMetadataPrefix.setFormatStr(DataFieldFormatType.JAVA.getFormatPrefixWithDelimiter() + as400DateFormat);
		DateDataField javaDateDataFieldPrefix = new DateDataField(javaDateMetadataPrefix);

		try {
			javaDateDataFieldPrefix.fromString(as400TestDate);
			fail("The BadDataFormatException should be thrown for the Java prefix specified.");
		} catch (BadDataFormatException exception) {
			// OK
		}

		DataFieldMetadata jodaDateMetadata = new DataFieldMetadata("date", DataFieldMetadata.DATE_FIELD, ";");
		jodaDateMetadata.setFormatStr(DataFieldFormatType.JODA.getFormatPrefixWithDelimiter() + as400DateFormat);
		DateDataField jodaDateDataField = new DateDataField(jodaDateMetadata);

		try {
			jodaDateDataField.fromString(as400TestDate);
		} catch (BadDataFormatException exception) {
			fail("The BadDataFormatException has been thrown even though the Joda-Time prefix was specified.");
		}
	}


	/**
	 *  Test for @link org.jetel.data.DateDataField.deserialize(ByteBuffer buffer)
	 *           @link org.jetel.data.DateDataField.serialize(ByteBuffer buffer)
	 *
	 */
	
	public void test_serialize() {
		CloverBuffer buffer = CloverBuffer.allocateDirect(100);
		
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
		aDateDataField1.setValue(null);
		aDateDataField1.serialize(buffer);
		buffer.rewind();
		aDateDataField3.deserialize(buffer);
		assertEquals(aDateDataField3.getValue(),aDateDataField1.getValue());
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
		
		assertTrue(aDateDataField3.isNull());
		aDateDataField3.setToDefaultValue();
		assertFalse(aDateDataField3.isNull());
		assertEquals(new GregorianCalendar(2100,02,31).getTime(), aDateDataField3.getValue());
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


	/**
	 *  Test for @link org.jetel.data.DateDataField.setToDefaultValue()
	 *
	 */
	public void test_setToDefaultValue() {
		aDateDataField3.setToDefaultValue();
		assertEquals("03/31/2100",aDateDataField3.toString());
			
		try {
			aDateDataField4.setToDefaultValue();
			fail("Field4 is not nullable and is being set to null!");
		} catch (java.lang.RuntimeException re) {}
	
		aDateDataField1.setToDefaultValue();
		assertEquals("",aDateDataField1.toString());
	}
	
	public void testFromStringNullValue() {
		DataFieldMetadata fieldMetadata = new DataFieldMetadata("field", DataFieldType.DATE, ";");
		DateDataField field = new DateDataField(fieldMetadata);

		field.setValue(new Date().getTime());
		field.fromString("");
		assertTrue(field.isNull());

		fieldMetadata.setNullValues(Arrays.asList("abc", "", "xxx"));

		field.setValue(new Date().getTime());
		field.fromString("abc");
		assertTrue(field.isNull());

		field.setValue(new Date().getTime());
		field.fromString("");
		assertTrue(field.isNull());

		field.setValue(new Date().getTime());
		field.fromString("xxx");
		assertTrue(field.isNull());
	}

	public void testFromByteByfferNullValue() throws CharacterCodingException, UnsupportedEncodingException {
		DataFieldMetadata fieldMetadata = new DataFieldMetadata("field", DataFieldType.DATE, ";");
		DateDataField field = new DateDataField(fieldMetadata);

		CharsetDecoder decoder = Charset.forName("US-ASCII").newDecoder();
		
		field.setValue(new Date().getTime());
		field.fromByteBuffer(BooleanDataFieldTest.getCloverBuffer(""), decoder);
		assertTrue(field.isNull());

		fieldMetadata.setNullValues(Arrays.asList("abc", "", "xxx"));

		field.setValue(new Date().getTime());
		field.fromByteBuffer(BooleanDataFieldTest.getCloverBuffer("abc"), decoder);
		assertTrue(field.isNull());

		field.setValue(new Date().getTime());
		field.fromByteBuffer(BooleanDataFieldTest.getCloverBuffer(""), decoder);
		assertTrue(field.isNull());

		field.setValue(new Date().getTime());
		field.fromByteBuffer(BooleanDataFieldTest.getCloverBuffer("xxx"), decoder);
		assertTrue(field.isNull());
	}

}
