package org.jetel.data.primitive;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Iterator;

import junit.framework.TestCase;

import org.jetel.data.Defaults;

public class ByteArrayTest extends TestCase {
	
	private ByteArray byteArrayZero;
	private ByteArray byteArrayEmpty;
	private ByteArray byteArrayBytes;
	private ByteArray byteArraySBytes;
	private byte[] bytes;
	private String sBytes;
	private ByteBuffer dataBuffer;

	protected void setUp() throws Exception {
		super.setUp();
		Defaults.init();
		sBytes = "Kašna na vodu";
		bytes = sBytes.getBytes();
		byteArrayEmpty = new ByteArray();
		byteArrayBytes = new ByteArray(bytes);
		byteArraySBytes = new ByteArray(sBytes);
		byteArrayZero = new ByteArray(0);
		dataBuffer = ByteBuffer.allocate(bytes.length);
		dataBuffer.put(bytes);
		dataBuffer.position(0);
	}
	
	public void test_constructors() {
		assertEquals(0, byteArrayEmpty.count);
		assertEquals(0, byteArrayZero.count);
		assertEquals(ByteArray.INITIAL_BYTE_ARRAY_CAPACITY, byteArrayEmpty.value.length);
		assertEquals(bytes.length, byteArrayBytes.count);
		assertEquals(bytes.length, byteArraySBytes.count);
		for (int i=0; i<byteArrayBytes.count; i++) {
			assertEquals(bytes[i], byteArrayBytes.value[i]);
			assertEquals(bytes[i], byteArraySBytes.value[i]);
		}
	}

	public void test_setValue() {
		byteArrayBytes.setValue((byte)99);
		for (int i=0; i<byteArrayBytes.count; i++) {
			assertEquals((byte)99, byteArrayBytes.value[i]);
		}
	}
	
	public void test_fromByte() {
		byteArrayBytes.fromByte((byte)98);
		assertEquals(1, byteArrayBytes.count);
		assertEquals((byte)98, byteArrayBytes.value[0]);
	}
	
	public void test_fromByteArray() {
		byteArrayEmpty.fromByte(bytes);
		byteArrayZero.fromByte(bytes);
		assertEquals(bytes.length, byteArrayEmpty.count);
		assertEquals(bytes.length, byteArrayZero.count);
		for (int i=0; i<byteArrayEmpty.count; i++) {
			assertEquals(bytes[i], byteArrayEmpty.value[i]);
			assertEquals(bytes[i], byteArrayZero.value[i]);
		}
	}

	public void test_fromString() {
		byteArrayEmpty.fromString(sBytes);
		byteArrayZero.fromString(sBytes, "UTF-8");
		assertEquals(bytes.length, byteArrayEmpty.count);
		assertEquals(bytes.length, byteArrayZero.count);
		for (int i=0; i<byteArrayEmpty.count; i++) {
			assertEquals(bytes[i], byteArrayEmpty.value[i]);
			assertEquals(bytes[i], byteArrayZero.value[i]);
		}
	}
	
	public void test_fromByteBuffer() {
		byteArrayEmpty.fromByteBuffer(dataBuffer);
		byteArrayZero.fromByteBuffer(dataBuffer);
		assertEquals(bytes.length, byteArrayEmpty.count);
		assertEquals(bytes.length, byteArrayZero.count);
		for (int i=0; i<byteArrayEmpty.count; i++) {
			assertEquals(bytes[i], byteArrayEmpty.value[i]);
			assertEquals(bytes[i], byteArrayZero.value[i]);
		}
	}
	
	public void test_fromByteBufferFromPos2Lim() {
		int ofs = 2;
		int lim = 10;
		dataBuffer.position(ofs);
		dataBuffer.limit(lim);
		byteArrayEmpty.fromByteBufferFromPos2Lim(dataBuffer);
		assertEquals(lim-ofs, byteArrayEmpty.count);
		for (int i=ofs; i<byteArrayEmpty.count+ofs; i++) {
			assertEquals(bytes[i], byteArrayEmpty.value[i-ofs]);
		}
	}

	public void test_getByte() {
		assertEquals(byteArrayBytes.getByte(6), bytes[6]);
	}

	public void test_toString() {
		assertEquals(byteArrayBytes.toString(), sBytes);
		assertEquals(byteArrayBytes.toString("UTF-8"), sBytes);
		assertEquals(byteArrayBytes.toString(2, 8), new String(bytes, 2, 8));
		assertEquals(byteArrayBytes.toString(2, 8, "UTF-8"), new String(bytes, 2, 8));
	}
	
	public void test_toByteBuffer() {
		int ofs = 2;
		int len = 8;
		ByteBuffer dataBufferRef = ByteBuffer.allocate(bytes.length);
		ByteBuffer dataBufferRefOfs = ByteBuffer.allocate(len);
		byteArrayBytes.toByteBuffer(dataBufferRef);
		byteArrayBytes.toByteBuffer(dataBufferRefOfs, ofs, len);
		assertEquals(new String(dataBufferRef.array()), new String(dataBuffer.array()));
		assertEquals(new String(dataBufferRefOfs.array()), new String(dataBuffer.array(), ofs, len));
	}
	
	public void test_getValueDuplicate() {
		assertEquals(new String(byteArrayBytes.getValueDuplicate()), new String(bytes));
	}
	
	public void test_duplicate() {
		ByteArray byteArrayDup = byteArrayBytes.duplicate();
		assertEquals(byteArrayDup.count, byteArrayBytes.count);
		for (int i=0; i<byteArrayBytes.count; i++) {
			assertEquals(byteArrayDup.value[i], byteArrayBytes.value[i]);
		}
		assertEquals(byteArrayDup.isCompressed(), byteArrayBytes.isCompressed());
	}
	
	public void test_reset() {
		byteArrayBytes.reset();
		for (int i=0; i<byteArrayBytes.length(); i++) {
			assertEquals((byte)0, byteArrayBytes.value[i]);
		}
		assertEquals(byteArrayBytes.count, 0);
	}
	
	public void test_equals() {
		assertTrue(byteArrayBytes.equals(byteArraySBytes));
		assertFalse(byteArrayBytes.equals(byteArrayEmpty));
		assertFalse(byteArrayBytes.equals(null));
		assertFalse(byteArrayBytes.equals(""));
	}
	
	public void test_compareTo() {
		assertTrue(byteArrayBytes.compareTo(byteArraySBytes) == 0);
		assertTrue(byteArrayBytes.compareTo(byteArrayEmpty) != 0);
		byteArraySBytes.value[2] = (byte)-120;
		assertTrue(byteArrayBytes.compareTo(byteArraySBytes) > 0);
		byteArraySBytes.value[2] = (byte)120;
		assertTrue(byteArrayBytes.compareTo(byteArraySBytes) < 0);
	}
	
	public void test_appendByte() {
		byteArrayBytes.append((byte)99);
		byteArrayBytes.append((byte)99);
		assertEquals(byteArrayBytes.count, bytes.length+2);
		for (int i=0; i<byteArrayBytes.count-2; i++) {
			assertEquals(bytes[i], byteArrayBytes.value[i]);
		}
		assertEquals(byteArrayBytes.value[byteArrayBytes.count-2], (byte)99);
		assertEquals(byteArrayBytes.value[byteArrayBytes.count-1], (byte)99);
	}
	
	public void test_appendByteArray() {
		byteArrayBytes.append(bytes);
		assertEquals(byteArrayBytes.count, bytes.length*2);
		for (int i=0; i<byteArrayBytes.count; i++) {
			assertEquals(bytes[i%bytes.length], byteArrayBytes.value[i]);
		}
	}

	// TODO
	

	public void test_encodeDecodeBitString() {
		String bitSeq = "011010101011010111";
		byteArrayBytes.encodeBitString(bitSeq, '1', true);
		CharSequence seq = byteArrayBytes.decodeBitString('1', '0', 0, bitSeq.length()-1);
		assertEquals(seq.toString(), bitSeq);
		byteArrayBytes.encodeBitString(bitSeq, '0', true);
		seq = byteArrayBytes.decodeBitString('0', '1', 0, bitSeq.length()-1);
		assertEquals(seq.toString(), bitSeq);
		byteArrayBytes.encodeBitString(bitSeq, '1', false);
		seq = byteArrayBytes.decodeBitString('0', '1', 0, bitSeq.length()-1);
		assertEquals(seq.toString(), bitSeq);
		byteArrayBytes.encodeBitString(bitSeq, '0', false);
		seq = byteArrayBytes.decodeBitString('1', '0', 0, bitSeq.length()-1);
		assertEquals(seq.toString(), bitSeq);
		
		bitSeq = "10000x11";
		byteArrayBytes.encodeBitString(bitSeq, '1', true);
		seq = byteArrayBytes.decodeBitString('1', '0', 0, bitSeq.length()-1);
		bitSeq = "10000011";
		assertEquals(seq.toString(), bitSeq);
		bitSeq = "10000x11";
		byteArrayBytes.encodeBitString(bitSeq, '1', false);
		seq = byteArrayBytes.decodeBitString('0', '1', 0, bitSeq.length()-1);
		bitSeq = "10000011";
		assertEquals(seq.toString(), bitSeq);
		
		bitSeq = "011010101011010111";
		int pos = byteArraySBytes.count;
		byteArraySBytes.encodeBitStringAppend(bitSeq, '1', true);
		seq = byteArraySBytes.decodeBitString('1', '0', pos, pos+bitSeq.length()-1);
		assertEquals(seq.toString(), bitSeq);
		pos = byteArraySBytes.count;
		byteArraySBytes.encodeBitStringAppend(bitSeq, '0', true);
		seq = byteArraySBytes.decodeBitString('0', '1', pos, pos+bitSeq.length()-1);
		assertEquals(seq.toString(), bitSeq);
		pos = byteArraySBytes.count;
		byteArraySBytes.encodeBitStringAppend(bitSeq, '1', false);
		seq = byteArraySBytes.decodeBitString('0', '1', pos, pos+bitSeq.length()-1);
		assertEquals(seq.toString(), bitSeq);
		pos = byteArraySBytes.count;
		byteArraySBytes.encodeBitStringAppend(bitSeq, '0', false);
		seq = byteArraySBytes.decodeBitString('1', '0', pos, pos+bitSeq.length()-1);
		assertEquals(seq.toString(), bitSeq);
	}
	
	
	public void test_encodeDecodePackLong() {
		long lTest = 100;
		byteArrayZero.encodePackLong(lTest);
		assertEquals(byteArrayZero.decodePackLong(), lTest);
		lTest = -100;
		byteArrayZero.encodePackLong(lTest);
		assertEquals(byteArrayZero.decodePackLong(), lTest);
		lTest = Long.MAX_VALUE;
		byteArrayZero.encodePackLong(lTest);
		assertEquals(byteArrayZero.decodePackLong(), lTest);
		lTest = Long.MIN_VALUE / 2; // one bit must be free because of long range
		byteArrayZero.encodePackLong(lTest);
		assertEquals(byteArrayZero.decodePackLong(), lTest);
	}
	
	public void test_encodeDecodePackDecimal() {
		Decimal decimal = new HugeDecimal(new BigDecimal(100.65), 0, 0, false);
		byteArrayZero.encodePackDecimal(decimal);
		assertEquals(
				byteArrayZero.decodePackDecimal().getBigDecimal().unscaledValue().toString(), 
				decimal.getBigDecimal().unscaledValue().toString());
		decimal = new HugeDecimal(new BigDecimal(-100.65), 0, 0, false);
		byteArrayZero.encodePackDecimal(decimal);
		assertEquals(
				byteArrayZero.decodePackDecimal().getBigDecimal().unscaledValue().toString(), 
				decimal.getBigDecimal().unscaledValue().toString());
		decimal = new HugeDecimal(new BigDecimal(Double.MAX_VALUE), 0, 0, false);
		byteArrayZero.encodePackDecimal(decimal);
		assertEquals(
				byteArrayZero.decodePackDecimal().getBigDecimal().unscaledValue().toString(), 
				decimal.getBigDecimal().unscaledValue().toString());
		decimal = new HugeDecimal(new BigDecimal(Double.MIN_VALUE), 0, 0, false);
		byteArrayZero.encodePackDecimal(decimal);
		assertEquals(
				byteArrayZero.decodePackDecimal().getBigDecimal().unscaledValue().toString(), 
				decimal.getBigDecimal().unscaledValue().toString());
	}

	public void test_deCompressData() {
		int mult = 10;
		for (int i=0; i<mult-1; i++) byteArrayBytes.append(bytes);
		int uncompressedSize = byteArrayBytes.count;
		byteArrayBytes.compressData();
		assertTrue(uncompressedSize > byteArrayBytes.count);
		byteArrayBytes.deCompressData();
		assertEquals(uncompressedSize, byteArrayBytes.count);
		for (int i=0; i<byteArrayBytes.count; i++) {
			assertEquals(bytes[i%bytes.length], byteArrayBytes.value[i]);
		}
	}
	
	public void test_iterator() {
		int i=0;
		for (Iterator<Byte> it = byteArrayBytes.iterator(); it.hasNext(); ) {
			assertEquals(it.next().byteValue(), bytes[i++]);
		}
		assertEquals(byteArrayBytes.count, i);
		i=0;
		for (Iterator<Byte> it = byteArrayBytes.iterator(); it.hasNext(); ) {
			assertEquals(it.next().byteValue(), bytes[i++]);
			if (i==5) {
				it.remove();
				i++;
			}
		}
		assertEquals(byteArrayBytes.count, i-1);
	}
	
	protected void tearDown() throws Exception {
		super.tearDown();
	}

}
