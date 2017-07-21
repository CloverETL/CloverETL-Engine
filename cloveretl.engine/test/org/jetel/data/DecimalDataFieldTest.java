
package org.jetel.data;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.Arrays;
import java.util.Properties;

import org.jetel.data.primitive.Decimal;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataFieldType;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.test.CloverTestCase;
import org.jetel.util.bytes.CloverBuffer;

public class DecimalDataFieldTest extends CloverTestCase {
	
	private DataField field1;
	private DataFieldMetadata fieldMetadata;
	
	@Override
	protected void setUp() throws Exception {
		super.setUp();
	    
		fieldMetadata = new DataFieldMetadata("field1",	DataFieldMetadata.DECIMAL_FIELD,";");  
	}
	
	public void test_1(){
		field1 = new DecimalDataField(fieldMetadata,50,10);
		field1.setValue(8.859493587791455E25);
		System.out.println("Value set to: " + field1.getValue());
	}

	public void test_2(){
		Properties fieldProperties = new Properties();
		fieldProperties.put(DataFieldMetadata.LENGTH_ATTR, "50");
		fieldProperties.put(DataFieldMetadata.SCALE_ATTR, "10");
		fieldMetadata.setFieldProperties(fieldProperties);
		field1 = DataFieldFactory.createDataField(fieldMetadata, true);
		field1.setValue(8.859493587791455E25);
		System.out.println("Value set to: " + field1.getValue());
	}
	
	public void test_3(){
		fieldMetadata.setLocaleStr("cs.CZ");
		DataRecordMetadata recordMetadata = new DataRecordMetadata("recordMetadata");
		recordMetadata.addField(fieldMetadata);
		DataRecord record = DataRecordFactory.newRecord(recordMetadata);
		record.init();
		String number = "11,28";//NumberFormat.getInstance().format(11.28);
		record.getField(0).fromString(number);
		System.out.println(record);
		assertEquals(11.28, ((Decimal)record.getField(0).getValue()).getDouble());
	}
	
	public void test_NaN(){
		field1 = new DecimalDataField(fieldMetadata,50,10);
		field1.setValue(Float.NaN);
		assertNull(field1.getValue());
	}
	
	private DecimalDataField createField(DataFieldMetadata metadata, int length, int scale, byte[] bytes) throws CharacterCodingException {
		DecimalDataField binaryField = new DecimalDataField(metadata, length, scale);
		binaryField.fromByteBuffer(CloverBuffer.wrap(bytes), null);
		return binaryField;
	}
	
	private void checkFromByteBuffer(DataFieldMetadata metadata, int length, int scale, byte[] bytes, BigDecimal expected) throws CharacterCodingException {
		assertEquals(expected, createField(metadata, length, scale, bytes).getBigDecimal());
	}

	private void checkFromByteBuffer(DataFieldMetadata metadata, int length, int scale, byte[] bytes, double expected) throws CharacterCodingException {
		assertEquals(expected, createField(metadata, length, scale, bytes).getDouble());
	}

	private void checkFromByteBuffer(DataFieldMetadata metadata, int length, int scale, byte[] bytes, long expected) throws CharacterCodingException {
		assertEquals(expected, createField(metadata, length, scale, bytes).getLong());
	}
	
	/**
	 * Test for {@link DecimalDataField#fromByteBuffer(ByteBuffer, java.nio.charset.CharsetDecoder)}
	 */
	public void test_fromByteBuffer() throws CharacterCodingException {
		final byte[] bytes1 = {0x12, 0x34, 0x56, 0x78};
		final byte[] bytes2 = {(byte) 0x87, 0x65, 0x43, 0x21};
		final byte[] packedNegative = {0x01, 0x23, 0x45, 0x67, (byte) 0x8D};
		final byte[] packedPositive = {0x56, 0x43, 0x16, (byte) 0x90, (byte) 0x7C};
		
		final byte[] doubleBytes1 = {0x40, 0x09, 0x21, (byte) 0xFB, 0x53, (byte) 0xC8, (byte) 0xD4, (byte) 0xF1};
		final byte[] doubleBytes2 = {0x3F, (byte) 0xF3, (byte) 0xC0, (byte) 0xCA, 0x42, (byte) 0x8C, 0x59, (byte) 0xFB};

		// one extra zero byte left padding, should work nevertheless
		final byte[] packedBig = {0x00, 0x01, 0x23, 0x45, 0x67, (byte) 0x89, 0x12, 0x34, 0x56, 0x78, 0x12, 0x34, 0x56, 0x78, 0x12, 0x34, 0x56, 0x78, 0x12, 0x34, 0x56, 0x78, (byte) 0x9D};

		DataFieldMetadata bigEndianMetadata = new DataFieldMetadata("Field", 'i', (short) 4);
		bigEndianMetadata.setFormatStr("BINARY:BIG_ENDIAN");
		
		DataFieldMetadata littleEndianMetadata = new DataFieldMetadata("Field", 'i', (short) 4);
		littleEndianMetadata.setFormatStr("BINARY:LITTLE_ENDIAN");
		
		DataFieldMetadata packedMetadata = new DataFieldMetadata("Field", 'i', ";");
		packedMetadata.setFormatStr("BINARY:PACKED_DECIMAL");

		DataFieldMetadata bigEndianDoubleMetadata = new DataFieldMetadata("Field", 'i', (short) 8);
		bigEndianDoubleMetadata.setFormatStr("BINARY:DOUBLE_BIG_ENDIAN");
		
		checkFromByteBuffer(bigEndianMetadata, 9, 0, bytes1, 305419896);
		checkFromByteBuffer(bigEndianMetadata, 50, 0, bytes1, 305419896);
		checkFromByteBuffer(bigEndianMetadata, 9, 0, bytes1, 305419896.0);
		checkFromByteBuffer(bigEndianMetadata, 9, 2, bytes1, 3054198);
		checkFromByteBuffer(bigEndianMetadata, 9, 4, bytes1, 30541);
		try {
			checkFromByteBuffer(bigEndianMetadata, 8, 0, bytes1, 305419896);
			fail("Should raise a NumberFormatException");
		} catch (NumberFormatException e) {}
		
		checkFromByteBuffer(bigEndianMetadata, 9, 1, bytes1, 30541989.6);
		checkFromByteBuffer(bigEndianMetadata, 9, 7, bytes1, 30.5419896);
		checkFromByteBuffer(bigEndianMetadata, 9, 10, bytes1, 0.0305419896);
		
		checkFromByteBuffer(bigEndianMetadata, 10, 0, bytes2, -2023406815);
		
		checkFromByteBuffer(littleEndianMetadata, 10, 0, bytes1, 2018915346);

		checkFromByteBuffer(packedMetadata, 8, 0, packedNegative, -12345678);
		checkFromByteBuffer(packedMetadata, 8, 5, packedNegative, -123.45678);

		checkFromByteBuffer(packedMetadata, 15, 0, packedPositive, 564316907);
		checkFromByteBuffer(packedMetadata, 67, 5, packedPositive, 5643.16907);

		checkFromByteBuffer(bigEndianDoubleMetadata, 9, 8, doubleBytes1, 3.14159265);
		checkFromByteBuffer(bigEndianDoubleMetadata, 9, 6, doubleBytes1, 3.141592);
		checkFromByteBuffer(bigEndianDoubleMetadata, 17, 16, doubleBytes2, 1.2345678901234567);
		checkFromByteBuffer(bigEndianDoubleMetadata, 17, 14, doubleBytes2, 1.23456789012345);
		checkFromByteBuffer(bigEndianDoubleMetadata, 17, 6, doubleBytes2, 1.234567);
		
		checkFromByteBuffer(packedMetadata, 50, 5, packedBig, new BigDecimal("-1234567891234567812345678123456781234.56789"));
	}
	
	private void checkToByteBuffer(DataFieldMetadata metadata, int length, int scale, BigDecimal value, byte[] expected) throws CharacterCodingException {
		DecimalDataField binaryField = new DecimalDataField(metadata, length, scale);
		CloverBuffer dataBuffer = CloverBuffer.allocate(expected.length);
		binaryField.setValue(value);
		System.out.println(binaryField.getValue());
		binaryField.toByteBuffer(dataBuffer, null);
		byte[] resultBytes = new byte[dataBuffer.position()];
		dataBuffer.rewind();
		dataBuffer.get(resultBytes);
		assertTrue(Arrays.equals(expected, resultBytes));
	}
	
	private void checkToByteBuffer(DataFieldMetadata metadata, int length, int scale, long value, byte[] expected) throws CharacterCodingException {
		checkToByteBuffer(metadata, length, scale, BigDecimal.valueOf(value), expected);
	}
	
	private void checkToByteBuffer(DataFieldMetadata metadata, int length, int scale, double value, byte[] expected) throws CharacterCodingException {
		checkToByteBuffer(metadata, length, scale, BigDecimal.valueOf(value), expected);
	}
	
	/**
	 * Test for {@link DecimalDataField#toByteBuffer(ByteBuffer, java.nio.charset.CharsetEncoder)}
	 */
	public void test_toByteBuffer() throws CharacterCodingException {
		final byte[] bytes1 = {0x12, 0x34, 0x56, 0x78};
		final byte[] packedNegative = {0x01, 0x23, 0x45, 0x67, (byte) 0x8D};
		final byte[] packedPositive = {0x56, 0x43, 0x16, (byte) 0x90, (byte) 0x7C};
		
		final byte[] doubleBytes1 = {0x40, 0x09, 0x21, (byte) 0xFB, 0x53, (byte) 0xC8, (byte) 0xD4, (byte) 0xF1};
		final byte[] doubleBytes2 = {0x3F, (byte) 0xF3, (byte) 0xC0, (byte) 0xCA, 0x42, (byte) 0x8C, 0x59, (byte) 0xFB};
		
		final byte[] packedBig = {0x01, 0x23, 0x45, 0x67, (byte) 0x89, 0x12, 0x34, 0x56, 0x78, 0x12, 0x34, 0x56, 0x78, 0x12, 0x34, 0x56, 0x78, 0x12, 0x34, 0x56, 0x78, (byte) 0x9D};

		final byte[] bigEndianBig = {0x00, 0x00, 0x12, 0x34, 0x56, 0x78, 0x12, 0x34, 0x56, 0x78, 0x12, 0x34, 0x56, 0x78, 0x12, 0x34, 0x56, 0x78, 0x12, 0x34, 0x56, 0x78, 0x12, 0x34, 0x56, 0x78};
		final byte[] bigEndianBigNegative = {(byte) 0xFF, (byte) 0xFF, (byte) 0xED, (byte) 0xCB, (byte) 0xA9, (byte) 0x87, (byte) 0xED, (byte) 0xCB, (byte) 0xA9, (byte) 0x87, (byte) 0xED, (byte) 0xCB, (byte) 0xA9, (byte) 0x87, (byte) 0xED, (byte) 0xCB, (byte) 0xA9, (byte) 0x87, (byte) 0xED, (byte) 0xCB, (byte) 0xA9, (byte) 0x87, (byte) 0xED, (byte) 0xCB, (byte) 0xA9, (byte) 0x88};

		DataFieldMetadata bigEndianMetadata = new DataFieldMetadata("Field", 'i', ";");
		bigEndianMetadata.setFormatStr("BINARY:BIG_ENDIAN");
		
		DataFieldMetadata littleEndianMetadata = new DataFieldMetadata("Field", 'i', ";");
		littleEndianMetadata.setFormatStr("BINARY:LITTLE_ENDIAN");
		
		DataFieldMetadata packedMetadata = new DataFieldMetadata("Field", 'i', ";");
		packedMetadata.setFormatStr("BINARY:PACKED_DECIMAL");

		DataFieldMetadata bigEndianDoubleMetadata = new DataFieldMetadata("Field", 'i', (short) 8);
		bigEndianDoubleMetadata.setFormatStr("BINARY:DOUBLE_BIG_ENDIAN");
		
		checkToByteBuffer(bigEndianMetadata, 9, 0, 305419896, bytes1);
		checkToByteBuffer(bigEndianMetadata, 50, 0, 305419896, bytes1);
		checkToByteBuffer(bigEndianMetadata, 9, 3, 305419.89671111, bytes1);
		try {
			checkToByteBuffer(bigEndianMetadata, 8, 0, 305419896, bytes1);
			fail("Should raise a NumberFormatException");
		} catch (NumberFormatException e) {}
		
		checkToByteBuffer(bigEndianMetadata, 9, 7, 30.5419896, bytes1);
		checkToByteBuffer(bigEndianMetadata, 9, 10, 0.0305419896, bytes1);

		checkToByteBuffer(littleEndianMetadata, 10, 0, 2018915346, bytes1);

		checkToByteBuffer(packedMetadata, 8, 0, -12345678, packedNegative);
		checkToByteBuffer(packedMetadata, 8, 5, -123.45678, packedNegative);

		checkToByteBuffer(packedMetadata, 15, 0, 564316907, packedPositive);
		checkToByteBuffer(packedMetadata, 67, 5, 5643.16907, packedPositive);

		// the last digits are cropped
		checkToByteBuffer(bigEndianDoubleMetadata, 9, 8, 3.1415926599999, doubleBytes1);
		checkToByteBuffer(bigEndianDoubleMetadata, 17, 16, 1.2345678901234567, doubleBytes2);

		checkToByteBuffer(packedMetadata, 50, 5, new BigDecimal("-1234567891234567812345678123456781234.56789"), packedBig);
		
		// expected length: 26 to test padding
		DataFieldMetadata bigEndianFixedMetadata = new DataFieldMetadata("Field", 'i', (short) 26);
		bigEndianFixedMetadata.setFormatStr("BINARY:BIG_ENDIAN");
		checkToByteBuffer(bigEndianFixedMetadata, 57, 5, new BigDecimal("4463716781813630920036906513193799449429041869347317.57176"), bigEndianBig);
		// the number gets rescaled; also test negative number padding
		checkToByteBuffer(bigEndianFixedMetadata, 57, 5, new BigDecimal("-4463716781813630920036906513193799449429041869.34731757176"), bigEndianBigNegative);
	}
	
	public void testFromStringNullValue() {
		DataFieldMetadata fieldMetadata = new DataFieldMetadata("field", DataFieldType.DECIMAL, ";");
		DecimalDataField field = new DecimalDataField(fieldMetadata, 8, 2);

		field.setValue(123);
		field.fromString("");
		assertTrue(field.isNull());

		field.setValue(123);
		field.fromString("456");
		assertTrue(field.isNull() == false);
		assertTrue(field.getValue().getInt() == 456);
		
		fieldMetadata.setNullValues(Arrays.asList("abc", "", "xxx"));

		field.setValue(123);
		field.fromString("abc");
		assertTrue(field.isNull());

		field.setValue(123);
		field.fromString("");
		assertTrue(field.isNull());

		field.setValue(123);
		field.fromString("xxx");
		assertTrue(field.isNull());

		field.setValue(123);
		field.fromString("456");
		assertTrue(!field.isNull());
	}

	public void testFromByteByfferNullValue() throws CharacterCodingException, UnsupportedEncodingException {
		DataFieldMetadata fieldMetadata = new DataFieldMetadata("field", DataFieldType.DECIMAL, ";");
		DecimalDataField field = new DecimalDataField(fieldMetadata, 8, 2);

		CharsetDecoder decoder = Charset.forName("US-ASCII").newDecoder();
		
		field.setValue(123);
		field.fromByteBuffer(BooleanDataFieldTest.getCloverBuffer(""), decoder);
		assertTrue(field.isNull());

		field.setValue(123);
		field.fromByteBuffer(BooleanDataFieldTest.getCloverBuffer("456"), decoder);
		assertTrue(field.isNull() == false);
		assertTrue(field.getValue().getInt() == 456);
		
		fieldMetadata.setNullValues(Arrays.asList("abc", "", "xxx"));

		field.setValue(123);
		field.fromByteBuffer(BooleanDataFieldTest.getCloverBuffer("abc"), decoder);
		assertTrue(field.isNull());

		field.setValue(123);
		field.fromByteBuffer(BooleanDataFieldTest.getCloverBuffer(""), decoder);
		assertTrue(field.isNull());

		field.setValue(123);
		field.fromByteBuffer(BooleanDataFieldTest.getCloverBuffer("xxx"), decoder);
		assertTrue(field.isNull());

		field.setValue(123);
		field.fromByteBuffer(BooleanDataFieldTest.getCloverBuffer("456"), decoder);
		assertTrue(!field.isNull());
	}

}
