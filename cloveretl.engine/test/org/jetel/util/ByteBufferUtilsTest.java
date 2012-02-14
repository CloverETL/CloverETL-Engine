/*
 *    jETeL/Clover - Java based ETL application framework.
 *    Copyright (C) 2002-06  David Pavlis <david.pavlis@centrum.cz> and others.
 *    
 *    This library is free software; you can redistribute it and/or
 *    modify it under the terms of the GNU Lesser General Public
 *    License as published by the Free Software Foundation; either
 *    version 2.1 of the License, or (at your option) any later version.
 *    
 *    This library is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU    
 *    Lesser General Public License for more details.
 *    
 *    You should have received a copy of the GNU Lesser General Public
 *    License along with this library; if not, write to the Free Software
 *    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 * Created on 6.12.2006
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package org.jetel.util;

import java.math.BigInteger;
import java.nio.ByteOrder;
import java.util.Arrays;

import org.jetel.test.CloverTestCase;
import org.jetel.util.bytes.ByteBufferUtils;
import org.jetel.util.bytes.CloverBuffer;

public class ByteBufferUtilsTest extends CloverTestCase {

    CloverBuffer buffer;

    @Override
	protected void setUp() throws Exception {
        super.setUp();
        buffer=CloverBuffer.allocateDirect(512);
    }

    public void testEncodeLength() {
     for(int i=0;i<=10000000;i++){
         buffer.clear();
         ByteBufferUtils.encodeLength(buffer, i);
         buffer.flip();
         assertEquals(ByteBufferUtils.lengthEncoded(i),buffer.remaining());
         int read=ByteBufferUtils.decodeLength(buffer);
         assertEquals(i, read);
         if (i%1000000==0) System.out.print("+");
         buffer.clear();
         ByteBufferUtils.encodeLength(buffer, Integer.MAX_VALUE);
         buffer.flip();
         assertEquals(ByteBufferUtils.lengthEncoded(Integer.MAX_VALUE),buffer.remaining());
         read=ByteBufferUtils.decodeLength(buffer);
         assertEquals(Integer.MAX_VALUE, read);
         
     }
        
    }
    
    public void testlengthEncode(){
        assertEquals(1, ByteBufferUtils.lengthEncoded(0));    
        assertEquals(1, ByteBufferUtils.lengthEncoded(127));
        assertEquals(2, ByteBufferUtils.lengthEncoded(128));
        assertEquals(2, ByteBufferUtils.lengthEncoded(256));
        assertEquals(3, ByteBufferUtils.lengthEncoded(2097151));
        assertEquals(4, ByteBufferUtils.lengthEncoded(268435455));
    }
    
    private void check_encodeValue(long value, ByteOrder byteOrder, byte[] expected) {
    	CloverBuffer byteBuffer = CloverBuffer.allocate(expected.length);
    	int length = ByteBufferUtils.encodeValue(byteBuffer, BigInteger.valueOf(value), byteOrder, expected.length);
    	assertEquals(expected.length, length);
    	assertTrue(Arrays.equals(expected, byteBuffer.array()));
    }
    
    private void check_decodeValue(byte[] bytes, ByteOrder byteOrder, long expected) {
    	assertEquals(expected, ByteBufferUtils.decodeValue(CloverBuffer.wrap(bytes), byteOrder).longValue());
    }
    
    public void test_decodeValue() {
        final byte[] bytes1 = {0x12, 0x34, 0x56, 0x78};
        final byte[] bytes2 = {(byte) 0x87, 0x65, 0x43, 0x21};
        final byte[] bytes3 = {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFE};
        final byte[] bytes4 = {(byte) 0xFE, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF};
        final byte[] bytes5 = {0x00, 0x02};
        final byte[] bytes6 = {0x02, 0x00, 0x00};
        
		// big endian
		check_decodeValue(bytes1, ByteOrder.BIG_ENDIAN, 305419896);
		check_decodeValue(bytes2, ByteOrder.BIG_ENDIAN, -2023406815);
		check_decodeValue(bytes3, ByteOrder.BIG_ENDIAN, -2);
		check_decodeValue(bytes4, ByteOrder.BIG_ENDIAN, -16777217);
		check_decodeValue(bytes5, ByteOrder.BIG_ENDIAN, 2);
		check_decodeValue(bytes6, ByteOrder.BIG_ENDIAN, 131072);
		
		// little endian
		check_decodeValue(bytes1, ByteOrder.LITTLE_ENDIAN, 2018915346);
		check_decodeValue(bytes2, ByteOrder.LITTLE_ENDIAN, 558065031);
		check_decodeValue(bytes3, ByteOrder.LITTLE_ENDIAN, -16777217);
		check_decodeValue(bytes4, ByteOrder.LITTLE_ENDIAN, -2);
		check_decodeValue(bytes5, ByteOrder.LITTLE_ENDIAN, 512);
		check_decodeValue(bytes6, ByteOrder.LITTLE_ENDIAN, 2);
    }
    
    public void test_encodeValue() {
        final byte[] bytes1 = {0x12, 0x34, 0x56, 0x78};
        final byte[] bytes2 = {(byte) 0x87, 0x65, 0x43, 0x21};
        final byte[] bytes3 = {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFE};
        final byte[] bytes4 = {(byte) 0xFE, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF};
        final byte[] bytes5 = {0x00, 0x02};
        final byte[] bytes6 = {0x02, 0x00, 0x00};
        
        // big endian
		check_encodeValue(305419896, ByteOrder.BIG_ENDIAN, bytes1);
		check_encodeValue(-2023406815, ByteOrder.BIG_ENDIAN, bytes2);
		check_encodeValue(-2, ByteOrder.BIG_ENDIAN, bytes3);
		check_encodeValue(-16777217, ByteOrder.BIG_ENDIAN, bytes4);
		check_encodeValue(2, ByteOrder.BIG_ENDIAN, bytes5);
		check_encodeValue(131072, ByteOrder.BIG_ENDIAN, bytes6);
		
		// little endian
		check_encodeValue(2018915346, ByteOrder.LITTLE_ENDIAN, bytes1);
		check_encodeValue(558065031, ByteOrder.LITTLE_ENDIAN, bytes2);
		check_encodeValue(-16777217, ByteOrder.LITTLE_ENDIAN, bytes3);
		check_encodeValue(-2, ByteOrder.LITTLE_ENDIAN, bytes4);
		check_encodeValue(512, ByteOrder.LITTLE_ENDIAN, bytes5);
		check_encodeValue(2, ByteOrder.LITTLE_ENDIAN, bytes6);
    }

    public void testEncodeString() {
    	CloverBuffer buffer = CloverBuffer.allocate(100);
    	
    	buffer.clear();
    	ByteBufferUtils.encodeString(buffer, null);
    	buffer.flip();
    	assertEquals(0, buffer.get());

    	buffer.clear();
    	ByteBufferUtils.encodeString(buffer, "");
    	buffer.flip();
    	assertEquals(1, buffer.get());

    	buffer.clear();
    	ByteBufferUtils.encodeString(buffer, "abc");
    	buffer.flip();
    	assertEquals(4, buffer.get());
    	assertEquals('a', buffer.getChar());
    	assertEquals('b', buffer.getChar());
    	assertEquals('c', buffer.getChar());
    }

    public void testDecodeString() {
    	CloverBuffer buffer = CloverBuffer.allocate(100);
    	
    	buffer.clear();
    	buffer.put((byte) 0);
    	buffer.flip();
    	assertNull(ByteBufferUtils.decodeString(buffer));

    	buffer.clear();
    	buffer.put((byte) 1);
    	buffer.flip();
    	assertEquals("", ByteBufferUtils.decodeString(buffer));

    	buffer.clear();
    	buffer.put((byte) 4);
    	buffer.putChar('a');
    	buffer.putChar('b');
    	buffer.putChar('c');
    	buffer.flip();
    	assertEquals("abc", ByteBufferUtils.decodeString(buffer));
    }

    public void testEncodeLengthStr() {
    	assertEquals(1, ByteBufferUtils.lengthEncoded(null));
    	assertEquals(1, ByteBufferUtils.lengthEncoded(""));
    	assertEquals(7, ByteBufferUtils.lengthEncoded("abc"));
    	assertEquals(71, ByteBufferUtils.lengthEncoded("abcd abcd abcd abcd abcd abcd abcd "));
    }
    
}
