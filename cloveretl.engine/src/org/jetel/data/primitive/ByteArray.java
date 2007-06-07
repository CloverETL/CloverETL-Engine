/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2002-04  David Pavlis <david_pavlis@hotmail.com>
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
*/
package org.jetel.data.primitive;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;

import org.jetel.exception.BadDataFormatException;
import org.jetel.util.Base64;
import org.jetel.util.ZipUtils;

/**
 * A class that represents dynamic array of bytes.<br>
 * 
 * @author J.Ausperger (OpenTech s.r.o)
 * @created May 30, 2007
 */
public class ByteArray implements Comparable, Iterable {

    /**
     * The value is used for byte storage.
     */
	protected byte[] value;

    /** 
     * The count is the number of byte used.
     */
	protected int count = 0;

	/**
	 * The value is/isn't in packed form.
	 */
	private boolean dataCompressed = false;

	/**
	 * Initial value size.
	 */
	public static final int INITIAL_BYTE_ARRAY_CAPACITY = 8;
	
	/**
	 * For packed decimal.
	 */
	private static final char[] digits = { '0', '1', '2', '3', '4', '5', '6', '7', '8',
		'9', 'A', 'B', 'C', 'D', 'E', 'F' };

	/**
	 * Constructors empty|capacity|byte[]|CharSequence.
	 */
	public ByteArray() {
		this(INITIAL_BYTE_ARRAY_CAPACITY);
	}
	
	public ByteArray(int capacity) {
		value = new byte[capacity];
	}
	
	public ByteArray(byte[] value) {
		this.value = new byte[value.length];
		fromByte(value);
	}
	
	public ByteArray(CharSequence value) {
		byte[] bValue = value.toString().getBytes();
		this.value = new byte[bValue.length];
		fromByte(bValue);
	}

    /**
     * This implements the expansion semantics of ensureCapacity with no
     * size check or synchronization.
     */
    private void expandCapacity(int minimumCapacity) {
    	int newCapacity = (value.length + 1) * 2;
        if (newCapacity < 0) {
            newCapacity = Integer.MAX_VALUE;
        } else if (minimumCapacity > newCapacity) {
        	newCapacity = minimumCapacity;
        }	
        byte newValue[] = new byte[newCapacity];
        System.arraycopy(value, 0, newValue, 0, count);
        value = newValue;
    }

    /**
     * Ensures that the capacity is at least equal to the specified minimum.
     * If the current capacity is less than the argument, then a new internal
     * array is allocated with greater capacity. The new capacity is the 
     * larger of: 
     * <ul>
     * <li>The <code>minimumCapacity</code> argument. 
     * <li>Twice the old capacity, plus <code>2</code>. 
     * </ul>
     * If the <code>minimumCapacity</code> argument is nonpositive, this
     * method takes no action and simply returns.
     *
     * @param   minimumCapacity   the minimum desired capacity.
     */
    public void ensureCapacity(int minimumCapacity) {
    	if (minimumCapacity > value.length) {
    		expandCapacity(minimumCapacity);
    	}
    }

    /**
     * Fill byte array by value 'number'.
     * 
     * @param value		the fill value
     */
	public void setValue(byte number) {
	    Arrays.fill(this.value, number);
		count = this.value.length;
	}

	/**
	 * FromXxx methods writes the value at the beginning of the byte array and 
	 * sets length of array to the size of data. 
	 */
	public void fromByte(byte data) {
		value[0] = data;
		count = 1;
	}
	
	public void fromByte(byte[] data) {
		if (this.value.length < data.length) {
			ensureCapacity(data.length);
		}
        System.arraycopy(data, 0, value, 0, data.length);
        count = data.length;
	}
	
	public void fromString(CharSequence seq) {
		fromByte(seq.toString().getBytes());
	}
	
	public void fromString(CharSequence seq, String charset) {
        try {
    		fromByte(seq.toString().getBytes(charset));
        } catch(UnsupportedEncodingException ex){
            throw new RuntimeException(ex);
        }
	}
	
	public void fromByteBuffer(ByteBuffer dataBuffer) {
		int len = dataBuffer.capacity();
		if (value.length < len) {
			ensureCapacity(len);
		}
		System.arraycopy(dataBuffer.array(), 0, value, 0, len);
		count = len;
	}

	public void fromByteBufferFromPos2Lim(ByteBuffer dataBuffer) {
		int len = dataBuffer.limit() - dataBuffer.position();
		if (value.length < len) {
			ensureCapacity(len);
		}
		byte[] tmpValue = new byte[len];
		dataBuffer.get(tmpValue);
		System.arraycopy(tmpValue, 0, value, 0, len);
		count = len;
	}
	
	/**
	 * Gets byte at the position.
	 * 
	 * @param position
	 * @return
	 */
	public byte getByte(int position) {
		return value[position];
	}
	
	/**
	 * ToXxx methods return part or whole array.
	 */
	public String toString() {
		return new String(value, 0, count);
	}
	
	public String toString(String charset) {
        try{
            return new String(value, 0, count, charset);
        }catch(UnsupportedEncodingException ex){
            throw new RuntimeException(ex);
        }
	}
	
	public String toString(int offset, int length) {
		return new String(value, offset, length);
	}
	
	public String toString(int offset, int length, String charset) {
        try{
            return new String(value, offset, length, charset);
        }catch(UnsupportedEncodingException ex){
            throw new RuntimeException(ex);
        }
	}
	
	public void toByteBuffer(ByteBuffer dataBuffer) {
		try {
			dataBuffer.put(value, 0, count);
		} catch (BufferOverflowException e) {
			throw new RuntimeException("The size of data buffer is only " + dataBuffer.limit() + ". Set appropriate parameter in defautProperties file.", e);
		}
	}
	
	public void toByteBuffer(ByteBuffer dataBuffer, int offset, int length) {
		try {
			dataBuffer.put(value, offset, length);
		} catch (BufferOverflowException e) {
			throw new RuntimeException("The size of data buffer is only " + dataBuffer.limit() + ". Set appropriate parameter in defautProperties file.", e);
		}
	}

	/**
	 * Returns internal byte array value.
	 * 
	 * @return
	 */
	public byte[] getValue() {
		return value;
	}
	
	/**
	 * Duplicates and returns internal byte array value.
	 * 
	 * @return
	 */
	public byte[] getValueDuplicate() {
        byte[] ret = new byte[count];
        System.arraycopy(value, 0, ret, 0, count);
        return ret;
	}

	/**
	 * Duplicates instance of this class.
	 * 
	 * @return
	 */
	public ByteArray duplicate() {
		ByteArray byteArray = new ByteArray(value);
		byteArray.dataCompressed = dataCompressed;
		return byteArray;
	}

	/**
	 * Sets all bytes in byte array to 0.
	 */
	public void reset() {
		setValue((byte)0);
		count = 0;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	public boolean equals(Object obj) {
		if (obj instanceof ByteArray || obj instanceof byte[]) {
			return compareTo(obj) == 0;
		} else {
		    return false;
		}
	}
	
	/*
	 * (non-Javadoc)
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	public int compareTo(Object obj) {
		byte[] byteObj;
		int oCount;
		if (obj instanceof ByteArray){
			byteObj = ((ByteArray) obj).value;
			oCount = ((ByteArray) obj).count;
		}else if (obj instanceof byte[]){
			byteObj= (byte[])obj;
			oCount = byteObj.length;
		}else {
			return -1;
		}
		 
		int compLength = count >= oCount ? count : oCount;
		for (int i = 0; i < compLength; i++) {
			if (value[i] > byteObj[i]) {
				return 1;
			} else if (value[i] < byteObj[i]) {
				return -1;
			}
		}
		// arrays seem to be the same (so far), decide according to the length
		if (count == oCount) {
			return 0;
		} else if (count > oCount) {
			return 1;
		} else {
			return -1;
		}
	}
	
	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	public int hashCode() {
	    int hash=5381;
		for (int i=0; i<count; i++){
			hash = ((hash << 5) + hash) + value[i]; 
		}
		return (hash & 0x7FFFFFFF);
	}

	/**
	 * Append methods appends data to the end of byte array.
	 */
	public void append(byte data) {
		if (count + 1 > value.length) {
			expandCapacity(count + 1);
		}
		value[count] = data;
		++count;
	}
	
	public ByteArray append(byte[] data) {
		int len = count + data.length;
		if (len > value.length) {
			expandCapacity(len);
		}
		System.arraycopy(data, 0, value, count, data.length);
		count = len;
		return this;
	}
	
	public ByteArray append(byte[] data, int offset, int len) {
        int newCount = count + len;
    	if (newCount > value.length)
    	    expandCapacity(newCount);
    	System.arraycopy(data, offset, value, count, len);
    	count = newCount;
		return this;
	}
	
	public ByteArray append(CharSequence data) {
		return append(data.toString().getBytes());
	}
	
	public ByteArray append(CharSequence data, String charset) {
		try {
			return append(data.toString().getBytes(charset));
	    } catch(UnsupportedEncodingException ex){
	        throw new RuntimeException(ex);
	    }
	}
	
	public ByteArray append(ByteBuffer dataBuffer) {
		int len = dataBuffer.capacity();
		if (value.length < len + count) {
			ensureCapacity(len + count);
		}
		System.arraycopy(dataBuffer.array(), 0, value, count, len);
		count += len;
		return this;
	}
	
	public ByteArray appendFromPos2Lim(ByteBuffer dataBuffer) {
		int len = dataBuffer.limit() - dataBuffer.position();
		if (value.length < len + count) {
			ensureCapacity(len + count);
		}
		byte[] tmpValue = new byte[len];
		dataBuffer.get(tmpValue);
		System.arraycopy(tmpValue, 0, value, count, len);
		count += len;
		return this;
	}

	/**
	 * The method removes bytes from index start to end.
	 * 
	 * @param start
	 * @param end
	 * @return
	 */
	public ByteArray delete(int start, int end) {
		if (start < 0)
		    throw new ArrayIndexOutOfBoundsException(start);
		if (end > count)
		    end = count;
		if (start > end)
		    throw new ArrayIndexOutOfBoundsException();
        int len = end - start;
        if (len > 0) {
            System.arraycopy(value, start+len, value, start, count-end);
            count -= len;
        }
		return this;
	}
	
	/**
	 * The method removes byte at the position index. 
	 * 
	 * @param index
	 * @return
	 */
	public ByteArray deleteByteAt(int index) {
        if ((index < 0) || (index >= count))
    	    throw new ArrayIndexOutOfBoundsException(index);
    	System.arraycopy(value, index+1, value, index, count-index-1);
    	count--;
		return this;
	}

	/**
	 * Replace methods replaces data. 
	 */
	public ByteArray replace(int offset, byte data) {
        if (offset < 0)
    	    throw new ArrayIndexOutOfBoundsException(offset);
    	if (offset > count-1)
    		throw new ArrayIndexOutOfBoundsException(offset);

    	value[offset] = data;
		return this;
	}
	
	public ByteArray replace(int start, int end, byte[] data) {
        if (start < 0)
    	    throw new ArrayIndexOutOfBoundsException(start);
    	if (end > count)
    	    end = count;
    	if (start > end)
    	    throw new ArrayIndexOutOfBoundsException("start > end");

    	int len = data.length;
    	int newCount = count + len - (end - start);
    	if (newCount > value.length)
    	    expandCapacity(newCount);

            System.arraycopy(value, end, value, start + len, count - end);
            System.arraycopy(data, 0, value, start, len);
            count = newCount;
		return this;
	}
	
	public ByteArray replace(int start, int end, CharSequence data) {
        if (start < 0)
    	    throw new ArrayIndexOutOfBoundsException(start);
    	if (end > count)
    	    end = count;
    	if (start > end)
    	    throw new ArrayIndexOutOfBoundsException("start > end");

    	byte[] bData = data.toString().getBytes();
    	int len = bData.length;
    	int newCount = count + len - (end - start);
    	if (newCount > value.length)
    	    expandCapacity(newCount);

            System.arraycopy(value, end, value, start + len, count - end);
            System.arraycopy(bData, 0, value, start, len);
            count = newCount;
		return this;
	}
	
	public ByteArray replace(int start, int end, CharSequence data, String charset) {
        if (start < 0)
    	    throw new ArrayIndexOutOfBoundsException(start);
    	if (end > count)
    	    end = count;
    	if (start > end)
    	    throw new ArrayIndexOutOfBoundsException("start > end");

    	byte[] bData = null;
		try {
			bData = data.toString().getBytes(charset);
		} catch (UnsupportedEncodingException e) {
	        throw new RuntimeException(e);
		}
    	int len = bData.length;
    	int newCount = count + len - (end - start);
    	if (newCount > value.length)
    	    expandCapacity(newCount);

            System.arraycopy(value, end, value, start + len, count - end);
            System.arraycopy(bData, 0, value, start, len);
            count = newCount;
		return this;
	}
	
	public ByteArray replace(int start, int end, ByteBuffer dataBuffer) {
        if (start < 0)
    	    throw new ArrayIndexOutOfBoundsException(start);
    	if (end > count)
    	    end = count;
    	if (start > end)
    	    throw new ArrayIndexOutOfBoundsException("start > end");

    	byte[] bData = new byte[dataBuffer.limit()];
    	dataBuffer.get(bData);
    	int len = bData.length;
    	int newCount = count + len - (end - start);
    	if (newCount > value.length)
    	    expandCapacity(newCount);

            System.arraycopy(value, end, value, start + len, count - end);
            System.arraycopy(bData, 0, value, start, len);
            count = newCount;
		return this;
	}

	/**
	 * Insert methods inserts data to the offset.
	 */
	public ByteArray insert(int offset, byte data) {
		int newCount = count + 1;
		if (newCount > value.length)
		    expandCapacity(newCount);
		System.arraycopy(value, offset, value, offset + 1, count - offset);
		value[offset] = data;
		count = newCount;
		return this;
	}
	
	public ByteArray insert(int index, byte data[], int offset, int len) {
        if ((index < 0) || (index > length()))
    	    throw new ArrayIndexOutOfBoundsException(index);
        if ((offset < 0) || (len < 0) || (offset > data.length - len))
            throw new ArrayIndexOutOfBoundsException(
            		"offset " + offset + ", len " + len + ", str.length " + data.length);
    	int newCount = count + len;
    	if (newCount > value.length)
    	    expandCapacity(newCount);
    	System.arraycopy(value, index, value, index + len, count - index);
    	System.arraycopy(data, offset, value, index, len);
    	count = newCount;
		return this;
	}
	
	public ByteArray insert(int dstOffset, CharSequence s, int start, int end) {
        if (s == null)
            s = "null";
    	if ((dstOffset < 0) || (dstOffset > count))
    	    throw new IndexOutOfBoundsException("dstOffset "+dstOffset);
    	byte[] sByte = s.toString().getBytes();
    	if ((start < 0) || (end < 0) || (start > end) || (end > sByte.length))
                throw new IndexOutOfBoundsException("start " + start + ", end " + end + ", s.length() " + sByte.length);
    	int len = end - start;
        if (len == 0) return this;
    	int newCount = count + len;
    	if (newCount > value.length)
    	    expandCapacity(newCount);
    	System.arraycopy(value, dstOffset, value, dstOffset + len, count - dstOffset);
    	System.arraycopy(sByte, start, value, dstOffset, end - start);
    	count = newCount;
		return this;
	}
	
	public ByteArray insert(int dstOffset, CharSequence s, int start, int end, String charset) {
        if (s == null)
            s = "null";
    	if ((dstOffset < 0) || (dstOffset > count))
    	    throw new IndexOutOfBoundsException("dstOffset "+dstOffset);
    	byte[] sByte = null;
		try {
			sByte = s.toString().getBytes(charset);
		} catch (UnsupportedEncodingException e) {
	        throw new RuntimeException(e);
		}
    	if ((start < 0) || (end < 0) || (start > end) || (end > sByte.length))
                throw new IndexOutOfBoundsException("start " + start + ", end " + end + ", s.length() " + sByte.length);
    	int len = end - start;
        if (len == 0) return this;
    	int newCount = count + len;
    	if (newCount > value.length)
    	    expandCapacity(newCount);
    	System.arraycopy(value, dstOffset, value, dstOffset + len, count - dstOffset);
    	System.arraycopy(sByte, start, value, dstOffset, end - start);
    	count = newCount;
		return this;
	}
	
	public ByteArray insert(int dstOffset, ByteBuffer data, int start, int end) {
        if (data == null)
    		return this;
    	if ((dstOffset < 0) || (dstOffset > count))
    	    throw new IndexOutOfBoundsException("dstOffset "+dstOffset);
    	byte[] sByte = new byte[end-start];
    	data.get(sByte, start, end-start);
    	if ((start < 0) || (end < 0) || (start > end) || (end > sByte.length))
                throw new IndexOutOfBoundsException("start " + start + ", end " + end + ", end-start " + sByte.length);
    	int len = end - start;
        if (len == 0) return this;
    	int newCount = count + len;
    	if (newCount > value.length)
    	    expandCapacity(newCount);
    	System.arraycopy(value, dstOffset, value, dstOffset + len, count - dstOffset);
    	System.arraycopy(sByte, start, value, dstOffset, end - start);
    	count = newCount;
		return this;
	}
	
	/**
	 * Duplicates and returns part of internal byte array. 
	 */
	public byte[] subArray(int start, int end) {
    	if ((start < 0) || (end < 0) || (start > end) || (end > count))
            throw new IndexOutOfBoundsException("start " + start + ", end " + end + ", byteArray.length() " + value.length);
		byte[] subArray = new byte[end - start];
    	System.arraycopy(value, start, subArray, 0, end - start);
		return subArray;
	}

	/**
	 * Returns length of byte array.
	 */
	public int length() {
		return count;
	}
	
	/**
	 * Sets length of byte array.
	 */
    public void setLength(int count) {
    	if (count > this.count) throw new IndexOutOfBoundsException("count " + count);
    	this.count = count;
    }

    /**
     * Gets bit from byte array at the position.
     */
    public boolean getBit(int position) {
		return (value[position >> 3] & ((byte) (1 << (position % 8)))) != 0;
	}
	
    /**
     * Sets bit to 1 at the position.
     */
	public void setBit(int position) {
		value[position >> 3] |= ((byte) (1 << (position % 8)));
	}
	
	/**
     * Sets bit to 0 at the position.
	 */
	public void resetBit(int position) {
		value[position >> 3] &= (~((byte) (1 << (position % 8))));
	}
	
	/**
	 * Make the 'bit and' over two byte array.
	 */
	public void  bitAND(byte[] data) {
		for (int i=0; i<data.length; i++) {
			if (value.length <= i) break;
			value[i] &= data[i];
		}
	}
	
	/**
	 * Make the 'bit or' over two byte array.
	 */
	public void bitOR(byte[] data) {
		for (int i=0; i<data.length; i++) {
			if (value.length <= i) break;
			value[i] |= data[i];
		}
	} 
	
	/**
	 * Make the 'bit xor' over two byte array.
	 */
	public void bitXOR(byte[] data) {
		for (int i=0; i<data.length; i++) {
			if (value.length <= i) break;
			value[i] ^= data[i];
		}
	}

	public String toBase64() {
		return Base64.encodeBytes(value, 0, count);
	}

	public void fromBase64(String data) {
		fromByte(Base64.decode(data));
	}

	/**
	 * Stores bit sequence like string "010011" to byte array.
	 * 
	 * @param seq
	 * @param bitChar
	 * @param bitCharValue
	 */
	public void encodeBitString(CharSequence seq, char bitChar, boolean bitValue) {
        if(seq == null || seq.length() == 0) {
            return;
        }
        String source = seq.toString();
        int len = source.length() >> 3 + count + 1;
        if (value.length < len) {
        	ensureCapacity(len);
        }
        len = source.length() + count;
        if (bitValue) {
        	for (int i=count; i<len; i++) {
            	if (source.charAt(i) == bitChar) {          		
        			value[i >> 3] |= ((byte) (1 << (i % 8)));
        		} else {
        			value[i >> 3] &= (~((byte) (1 << (i % 8))));
        		}
        	}
        } else {
        	for (int i=count; i<len; i++) {
            	if (source.charAt(i) != bitChar) {
           			value[i >> 3] |= ((byte) (1 << (i % 8)));
           		} else {
           			value[i >> 3] &= (~((byte) (1 << (i % 8))));
           		}
        	}
        }
	}
	
	/**
	 * Returns bit sequence from byte array.
	 * 
	 * @param trueValue
	 * @param falseValue
	 * @param start
	 * @param end
	 * @return
	 */
	public CharSequence decodeBitString(char trueValue, char falseValue, int start, int end) {
		if ((start < 0) || (end < 0) || (start >= end)) 
            throw new IndexOutOfBoundsException("start " + start + ", end " + end + ", end-start " + (end-start));
		
		StringBuilder sb = new StringBuilder(end - start);
		for (int i=start; i<end; i++) {
			sb.append(getBit(i) ? trueValue : falseValue);
		}
		return sb;
	}

	/**
	 * Convert a byte array containing a packed dicimal number to "long"
	 * 
	 * @return long number
	 * @exception BadDataFormatException
	 *                input is not packed decimal format or number would not fit in long
	 */
	public long decodePackLong() {
		long lnum = 0; // initialize
		int endloop; // loop counter
		int i; // subscript
		int nibble; // four bits
		endloop = count - 1; // don't include last byte in loop
		for (i = 0; i < endloop; i++) {
			lnum *= 10;
			nibble = (value[i] & 0xf0) >>> 4;
			if (nibble > 9)
				throw new BadDataFormatException("Invalid decimal digit: " + nibble);
			lnum += nibble;
			if (lnum < 0)
				throw new BadDataFormatException("Number too big");
			lnum *= 10;
			nibble = (value[i] & 0x0f);
			if (nibble > 9)
				throw new BadDataFormatException("Invalid decimal digit: " + nibble);
			lnum += nibble;
			if (lnum < 0)
				throw new BadDataFormatException("Number too big");
		}
		// Process the last byte.  Lower 4 bits are sign
		lnum *= 10;
		nibble = (value[i] & 0xf0) >>> 4;
		if (nibble > 9)
			throw new BadDataFormatException("Invalid decimal digit: " + nibble);
		lnum += nibble;
		if (lnum < 0)
			throw new BadDataFormatException("Number too big");
		nibble = (value[i] & 0x0f);
		if (nibble < 10)
			throw new BadDataFormatException("Invalid deciaml sign:  ");
		switch (nibble) {
		case 11:
		case 13:
			lnum *= -1; /* make nuber negative */
		}
		return lnum;
	}

	/**
	 * Convert "long" to byte array of packed decimal number
	 * 
	 * @param lnum
	 *            long number to convert
	 * @return byte array
	 */
	public void encodePackLong(long lnum) {
		int i;
		long longwork;
		value = new byte[Long.toString(lnum).length() / 2 + 1];
		i = value.length - 1;
		longwork = lnum;
		if (longwork < 0) {
			value[i] = 13; // 0x0d negative sign
			longwork *= -1;
		} else
			value[i] = 12; // 0x0c positive sign
		value[i] = (byte) (value[i] | ((longwork % 10) << 4));
		longwork /= 10;
		--i;
		while (0 != longwork) {
			value[i] = (byte) (longwork % 10);
			longwork /= 10;
			value[i] = (byte) (value[i] | ((longwork % 10) << 4));
			longwork /= 10;
			--i;
		}
		count = value.length;
	}
	
	/**
	 * Convert "Decimal" to byte array. 
	 * 
	 * @param decimal
	 *            Decimal number to convert
	 */
	public void encodePackDecimal(Decimal decimal) {
		char[] decValue = decimal.getBigDecimal().unscaledValue().toString().toCharArray();
		int i; // string index
		int j; // byte array index
		boolean nibble_ordinal = false;
		byte nibble;
		value = new byte[decValue.length / 2 + 1];
		i = decValue.length-1;
		j = value.length-1; /* byte index */
		value[j] = 12; // start with positive sign
		while (i > -1) {
			switch (decValue[i]) {
			case '0':
			case '1':
			case '2':
			case '3':
			case '4':
			case '5':
			case '6':
			case '7':
			case '8':
			case '9':
				nibble = (byte) Character.getNumericValue(decValue[i]);
				if (nibble_ordinal) {
					value[j] = (byte) (value[j] | nibble);
					nibble_ordinal ^= true;
				} else {
					value[j] = (byte) (value[j] | nibble << 4);
					nibble_ordinal ^= true;
					--j;
				}
				--i; // get next char
				break;
			case ',':
			case ' ':
			case '.':
			case '$':
			case '+':
				--i; // get next char
				break;
			case '-':
				value[value.length-1] = (byte) (value[value.length-1] & 0xf0);
				value[value.length-1] = (byte) (value[value.length-1] | 0x0d);
				--i; // get next char
				break;
			default:
				throw new BadDataFormatException("Invalid decimal digit: " + decValue[i]);
			}
		}
		count = value.length;
	}
	
	/**
	 * Convert a byte array containing a packed decimal number to Decimal value
	 * 
	 * @return Decimal number
	 * @exception BadDataFormatException
	 *                input is not packed decimal format
	 */
	public Decimal decodePackDecimal() {
		StringBuilder strbuf = new StringBuilder(count);
		int endloop; // loop counter
		int i; // subscript
		int nibble; // four bits
		endloop = count - 1;  // Don't include last byte
		for (i = 0; i < endloop; i++) {
			nibble = (value[i] & 0xf0) >>> 4;
			if (nibble > 9)
				throw new BadDataFormatException("Invalid decimal digit: " + nibble);
			strbuf.append(digits[nibble]);
			nibble = (value[i] & 0x0f);
			if (nibble > 9)
				throw new BadDataFormatException("Invalid decimal digit: " + nibble);
			strbuf.append(digits[nibble]);
		}
		// Last byte contains sign
		nibble = (value[i] & 0xf0) >>> 4;
		if (nibble > 9)
			throw new BadDataFormatException("Invalid decimal digit: " + nibble);
		strbuf.append(digits[nibble]);
		nibble = (value[i] & 0x0f);
		if (nibble < 10)
			throw new BadDataFormatException("Invalid deciaml sign:  ");
		if ( 11 == nibble || 13 == nibble )
			strbuf.insert(0, '-');
		else
			strbuf.insert(0, '+');
		return new HugeDecimal(new BigDecimal(strbuf.toString()), 0, 0, false);
	}
	
	/**
	 * Compress/Uncompress byte array. 
	 * 
	 * @param compressData
	 */
	public void compressData(boolean compressData) {
		if (this.dataCompressed == compressData) return;
		if (compressData) {
			fromByte(ZipUtils.compress(value));
		} else {
			fromByte(ZipUtils.decompress(value));
		}
		this.dataCompressed = compressData;
	}

	/**
	 * Indicates if byte array is packed or unpacked.
	 */
	public boolean isCompressed() {
		return dataCompressed;
	}

	/**
	 * Byte array iterator.
	 */
	public Iterator<Byte> iterator() {
		return new ByteIterator(this);
	}
	
	private class ByteIterator implements Iterator<Byte> {
		private ByteArray byteArray;
		private int index = 0;
		
		public ByteIterator(ByteArray byteArray) {
			this.byteArray = byteArray;
		}	
		
		public boolean hasNext() {
			return index < byteArray.count;
		}

		public Byte next() {
			if (index >= byteArray.count) return null;
			return Byte.valueOf(byteArray.value[index++]);
		}

		public void remove() {
			byteArray.deleteByteAt(index);
		}
	}
	
}