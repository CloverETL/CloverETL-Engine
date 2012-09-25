/*
 * jETeL/CloverETL - Java based ETL application framework.
 * Copyright (c) Javlin, a.s. (info@cloveretl.com)
 *  
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */
package org.jetel.data.primitive;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;

import org.jetel.exception.BadDataFormatException;
import org.jetel.util.crypto.Base64;
import org.jetel.util.file.ZipUtils;

import edu.umd.cs.findbugs.annotations.SuppressWarnings;

/**
 * A class that represents dynamic array of bytes.<br>
 * 
 * @author Jan Ausperger (jan.ausperger@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 * @created May 30, 2007
 */
@SuppressWarnings("EI")
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
	protected boolean dataCompressed = false;

	/**
	 * Initial value size.
	 */
	public static final int INITIAL_BYTE_ARRAY_CAPACITY = 8;
	
	/**
	 * For packed decimal.
	 */
	private static final char[] DIGITS = { '0', '1', '2', '3', '4', '5', '6', '7', '8',
		'9', 'A', 'B', 'C', 'D', 'E', 'F' };
	
	protected static final int INT_SIZE_BYTES = Integer.SIZE >> 3;
	protected static final int LONG_SIZE_BYTES = Long.SIZE >> 3;
	protected static final int FLOAT_SIZE_BYTES = Float.SIZE >> 3;
	protected static final int DOUBLE_SIZE_BYTES = Double.SIZE >> 3;

	/**
	 * Creates byte array with default value size.
	 */
	public ByteArray() {
		this(INITIAL_BYTE_ARRAY_CAPACITY);
	}
	
	/**
	 * Creates byte array with user defined value size.
	 * 
	 * @param capacity   defines the size of value
	 */
	public ByteArray(int capacity) {
		value = new byte[capacity];
	}
	
	/**
	 * Creates byte array from value, makes copy of value. 
	 * 
	 * @param value   byte array to be copied
	 */
	public ByteArray(byte[] value) {
		this.value = new byte[value.length];
		fromByte(value);
	}
	
	/**
	 * Creates byte array from CharSequence.
	 * 
	 * @param value   CharSequence to be copied
	 */
	public ByteArray(CharSequence value) {
		byte[] bValue = value.toString().getBytes();
		this.value = new byte[bValue.length];
		fromByte(bValue);
	}

    /**
     * This implements the expansion semantics of ensureCapacity with no
     * size check or synchronization.
     * 
     * @param minimumCapacity   the minimum desired capacity.
     */
    private final void expandCapacity(int minimumCapacity) {
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
    public final void ensureCapacity(int minimumCapacity) {
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
	
	public void setValue(byte value, int position){
		if (position >= count) throw new ArrayIndexOutOfBoundsException(position);
		this.value[position]=value;
	}
	
	
	public void setValue(ByteArray fromVal){
		ensureCapacity(fromVal.length());
		System.arraycopy(fromVal,0, value, 0, fromVal.length());
		count=fromVal.length();
	}
	
	public void setValue(byte[] fromVal){
		ensureCapacity(fromVal.length);
		System.arraycopy(fromVal,0, value, 0, fromVal.length);
		count=fromVal.length;
	}

	/**
	 * FromByte method writes the value at the beginning of the byte array and 
	 * sets length of array to the size of data.
	 * 
	 * @param data   byte array to be copied
	 */
	public void fromByte(byte[] data) {
		ensureCapacity(data.length);
        System.arraycopy(data, 0, value, 0, data.length);
        count = data.length;
	}
	
	/**
	 * FromString method writes the value at the beginning of the byte array and 
	 * sets length of array to the size of sequence.
	 * 
	 * @param seq   character sequence to be copied
	 */
	public void fromString(CharSequence seq) {
		fromByte(seq.toString().getBytes());
	}
	
	/**
	 * FromString method writes the value at the beginning of the byte array and 
	 * sets length of array to the size of sequence.
	 * 
	 * @param seq   character sequence to be copied
	 * @param charset   encoding used for char sequence
	 */
	public void fromString(CharSequence seq, String charset) {
        try {
    		fromByte(seq.toString().getBytes(charset));
        } catch(UnsupportedEncodingException ex){
            throw new RuntimeException(ex);
        }
	}
	
	/**
	 * FromByteBuffer method writes the value at the beginning of the byte array, 
	 * value is read from byte buffer position to byte buffer limit and the method 
	 * sets length of array to byte buffer limit minus byte buffer position.
	 * 
	 * @param dataBuffer   byte buffer to be copied
	 */
	public void fromByteBuffer(ByteBuffer dataBuffer) {
		int len = dataBuffer.limit() - dataBuffer.position();
		ensureCapacity(len);
		dataBuffer.get(value);
		count = len;
	}

	/**
	 * Gets byte at the position.
	 * 
	 * @param position
	 * @return
	 */
	public byte getByte(int position) {
		if (position >= count) throw new ArrayIndexOutOfBoundsException(position);
		return value[position];
	}
	
	/**
	 * ToString method returns whole array.
	 * 
	 * @return   string value to be returned
	 */
	public String toString() {
		return new String(value, 0, count);
	}
	
	/**
	 * ToString method returns whole array.
	 * 
	 * @param charset   encoding used for byte array
	 * @return   string value to be returned
	 */
	public String toString(String charset) {
        try{
            return new String(value, 0, count, charset);
        }catch(UnsupportedEncodingException ex){
            throw new RuntimeException(ex);
        }
	}
	
	/**
	 * ToString method returns part of array from offset to length+offset.
	 * 
	 * @param offset   index to the byte array
	 * @param length   count of byte to be read
	 * @return   string value to be returned
	 */
	public String toString(int offset, int length) {
		return new String(value, offset, length);
	}
	
	/**
	 * ToString method returns part of array from offset to length+offset.
	 * 
	 * @param offset   index to the byte array
	 * @param length   count of byte to be read
	 * @param charset   encoding used for byte array
	 * @return   string value to be returned
	 */
	public String toString(int offset, int length, String charset) {
        try{
            return new String(value, offset, length, charset);
        }catch(UnsupportedEncodingException ex){
            throw new RuntimeException(ex);
        }
	}
	
	public String toHexString(){
		StringBuilder str=new StringBuilder(value.length*2);
		for(int i=0;i<count; i++){
			str.append(Character.forDigit((value[i]&0xF0)>>4,16));
			str.append(Character.forDigit(value[i]&0x0F,16));
		}
		return str.toString();
	}
	
	public String fromHexString(){
		StringBuilder str=new StringBuilder(value.length/2);
		int j;
		for(int i=0;i<count/2; i++){
			j = i*2;
			str.append((char)((Character.digit(value[j], 16)<<4) + Character.digit(value[j+1], 16)));
		}
		return str.toString();
	}

	/**
	 * Puts byte array to the data buffer.
	 * 
	 * @param dataBuffer   byte array as written to the data buffer 
	 */
	public void toByteBuffer(ByteBuffer dataBuffer) {
		try {
			dataBuffer.put(value, 0, count);
		} catch (BufferOverflowException e) {
			throw new RuntimeException("The size of data buffer is only " + dataBuffer.limit() + ". Set appropriate parameter in defaultProperties file.", e);
		}
	}
	
	/**
	 * Puts byte array to the data buffer from offset to length+offset.
	 * 
	 * @param dataBuffer   byte array as written to the data buffer
	 * @param offset   index to the byte array
	 * @param length   count of byte to be read
	 */
	public void toByteBuffer(ByteBuffer dataBuffer, int offset, int length) {
		try {
			dataBuffer.put(value, offset, length);
		} catch (BufferOverflowException e) {
			throw new RuntimeException("The size of data buffer is only " + dataBuffer.limit() + ". Set appropriate parameter in defaultProperties file.", e);
		}
	}

	/**
	 * Returns internal byte array value.
	 * 
	 * @return   internal byte array.
	 */
	public byte[] getValue() {
		return value;
	}
	
	/**
	 * Returns duplicated byte array value.
	 * 
	 * @param data   internal value is copied to the data 
	 * @return   data value from param
	 */
	public byte[] getValue(byte[] data) {
		if (data.length < count) throw new ArrayIndexOutOfBoundsException(data.length);
		System.arraycopy(value, 0, data, 0, count);
		return data;
	}
	
	/**
	 * Returns duplicated byte array value.
	 * 
	 * @param data internal value is copied to the data
	 * @param maximumCount maximum count of bytes to be copied from data internal value
	 * @return   data value from param
	 */
	public byte[] getValue(byte[] data, int maximumCount) {
		if (maximumCount > count) {
			maximumCount = count;
		}
		if (data.length < maximumCount) throw new ArrayIndexOutOfBoundsException(data.length);
		System.arraycopy(value, 0, data, 0, maximumCount);
		return data;
	}
	
	/**
	 * Duplicates and returns internal byte array value.
	 * 
	 * @return   internal byte array
	 */
	public byte[] getValueDuplicate() {
        byte[] ret = new byte[count];
        System.arraycopy(value, 0, ret, 0, count);
        return ret;
	}

	/**
	 * Duplicates and returns internal byte array value as ByteBufer. 
	 * 
	 * @return   internal byte array
	 */
	public ByteBuffer getValueAsBuffer() {
        ByteBuffer ret = ByteBuffer.allocate(count);
        ret.put(value, 0, count);
        ret.flip();
        return ret;
	}

	/**
	 * Duplicates instance of this class.
	 * 
	 * @return   duplicated ByteArray 
	 */
	public ByteArray duplicate() {
		ByteArray byteArray = new ByteArray(value);
		byteArray.dataCompressed = dataCompressed;
		return byteArray;
	}

	/**
	 * Sets count index to 0.
	 */
	public void reset() {
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
	 * Appends data to the end of byte array.
	 * 
	 * @param data   byte to be appended
	 * @return   ByteArray
	 */
	public ByteArray append(byte data) {
		ensureCapacity(count+1);
		value[count] = data;
		++count;
		return this;
	}
	
	/**
	 * Appends data to the end of byte array.
	 * 
	 * @param data   byte array to be appended
	 * @return   ByteArray
	 */
	public ByteArray append(byte[] data) {
		int len = count + data.length;
		ensureCapacity(len);
		System.arraycopy(data, 0, value, count, data.length);
		count = len;
		return this;
	}
	
	/**
	 * Appends data to the end of byte array from offset to len+offset.
	 * 
	 * @param data   byte array to be appended
	 * @param offset   index to the internal byte array
	 * @param len   count of bytes to be appended
	 * @return   ByteArray
	 */
	public ByteArray append(byte[] data, int offset, int len) {
        int newCount = count + len;
		ensureCapacity(newCount);
    	System.arraycopy(data, offset, value, count, len);
    	count = newCount;
		return this;
	}
	
	/**
	 * Appends data to the end of byte array.
	 * 
	 * @param data   character sequence to be appended
	 * @return   ByteArray
	 */
	public ByteArray append(CharSequence data) {
		return append(data.toString().getBytes());
	}
	
	/**
	 * Appends data to the end of byte array.
	 * 
	 * @param data   character sequence to be appended
	 * @param charset   encoding used for byte array
	 * @return   ByteArray
	 */
	public ByteArray append(CharSequence data, String charset) {
		try {
			return append(data.toString().getBytes(charset));
	    } catch(UnsupportedEncodingException ex){
	        throw new RuntimeException(ex);
	    }
	}
	
	/**
	 * Appends data to the end of byte array.
	 * 
	 * @param dataBuffer   data buffer to be appended
	 * @return   ByteArray
	 */
	public ByteArray append(ByteBuffer dataBuffer) {
		int len = dataBuffer.remaining();
		ensureCapacity(len+count);
		dataBuffer.get(value, count, len);
		count += len;
		return this;
	}
	
	public ByteArray append(int value){
		int newlen = INT_SIZE_BYTES + count;
		ensureCapacity(newlen);
		ByteBuffer buf=ByteBuffer.wrap(this.value,count,INT_SIZE_BYTES);
		buf.putInt(value);
		count=newlen;
		return this;
	}
	
	public ByteArray append(long value){
		int newlen = LONG_SIZE_BYTES + count;
		ensureCapacity(newlen);
		ByteBuffer buf=ByteBuffer.wrap(this.value,count,LONG_SIZE_BYTES);
		buf.putLong(value);
		count=newlen;
		return this;

	}
	
	public ByteArray append(float value){
		int newlen = FLOAT_SIZE_BYTES + count;
		ensureCapacity(newlen);
		ByteBuffer buf=ByteBuffer.wrap(this.value,count,FLOAT_SIZE_BYTES);
		buf.putFloat(value);
		count=newlen;
		return this;

	}
	
	public ByteArray append(double value){
		int newlen = DOUBLE_SIZE_BYTES + count;
		ensureCapacity(newlen);
		ByteBuffer buf=ByteBuffer.wrap(this.value,count,DOUBLE_SIZE_BYTES);
		buf.putDouble(value);
		count=newlen;
		return this;
		
	}
	
	public ByteArray append(ByteArray source) {
		int len = count + source.count;
		ensureCapacity(len);
		System.arraycopy(source.value, 0, value, count, source.count);
		count = len;
		return this;
	}
	
	/**
	 * The method removes bytes from index start to end.
	 * 
	 * @param start   index to the byte array
	 * @param end   index to the byte array
	 * @return   ByteArray
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
	 * @param index   index to the byte array
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
	 * Replaces data in byte array at the offset. 
	 * 
	 * @param offset   index to the byte array
	 * @param data   byte to be replaced
	 * @return   ByteArray
	 */
	public ByteArray replace(int offset, byte data) {
        if (offset < 0)
    	    throw new ArrayIndexOutOfBoundsException(offset);
    	if (offset > count-1)
    		throw new ArrayIndexOutOfBoundsException(offset);

    	value[offset] = data;
		return this;
	}
	
	/**
	 * Replaces data in internal byte array from start to end position.
	 * 
	 * @param start   index to the byte array
	 * @param end   index to the byte array
	 * @param data   byte array to be replaced
	 * @return   ByteArray
	 */
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
	
	/**
	 * Replaces data in byte array from start to end position.
	 * 
	 * @param start   index to the byte array
	 * @param end   index to the byte array
	 * @param data   character sequence to be replaced
	 * @return   ByteArray
	 */
	public ByteArray replace(int start, int end, CharSequence data) {
		return replace(start, end, data, null);
	}
	
	/**
	 * Replaces data in byte array from start to end position.
	 * 
	 * @param start   index to the byte array
	 * @param end   index to the byte array
	 * @param data   character sequence to be replaced
	 * @param charset   an encoding used for byte array
	 * @return   ByteArray
	 */
	public ByteArray replace(int start, int end, CharSequence data, String charset) {
        if (start < 0)
    	    throw new ArrayIndexOutOfBoundsException(start);
    	if (end > count)
    	    end = count;
    	if (start > end)
    	    throw new ArrayIndexOutOfBoundsException("start > end");

    	final byte[] bData;
    	if (charset == null) {
        	bData = data.toString().getBytes();
    	} else {
    		try {
    			bData = data.toString().getBytes(charset);
    		} catch (UnsupportedEncodingException e) {
    	        throw new RuntimeException(e);
    		}
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
	
	/**
	 * Replaces data in byte array from start to end position.
	 * 
	 * @param start   index to the byte array
	 * @param end   index to the byte array
	 * @param dataBuffer   data buffer to be replaced
	 * @return   ByteArray
	 */
	public ByteArray replace(int start, int end, ByteBuffer dataBuffer) {
        if (start < 0)
    	    throw new ArrayIndexOutOfBoundsException(start);
    	if (end > count)
    	    end = count;
    	if (start > end)
    	    throw new ArrayIndexOutOfBoundsException("start > end");

    	int len = dataBuffer.limit() - dataBuffer.position();
    	int newCount = count + len - (end - start);
    	if (newCount > value.length)
    	    expandCapacity(newCount);

            System.arraycopy(value, end, value, start + len, count - end);
        	dataBuffer.get(value, start, len);
            count = newCount;
		return this;
	}

	/**
	 * Inserts data to the offset.
	 * 
	 * @param offset   index to the byte array
	 * @param data   byte to be inserted
	 * @return
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
	
	/**
	 * Inserts data to the internal byte array.
	 * 
	 * @param index   index to the internal byte array
	 * @param data  byte array to be inserted
	 * @param offset   index to the data byte array
	 * @param len   count of bytes to be inserted
	 * @return   ByteArray
	 */
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
	
	/**
	 * Inserts data to the byte array.
	 * 
	 * @param dstOffset   index to the internal byte array
	 * @param s  character sequence to be inserted
	 * @param start   index to the CharSequence
	 * @param end   index to the CharSequence
	 * @return   ByteArray
	 */
	public ByteArray insert(int dstOffset, CharSequence s, int start, int end) {
		return insert(dstOffset, s, start, end, null);
	}
	
	/**
	 * Inserts data to the byte array.
	 * 
	 * @param dstOffset   index to the internal byte array
	 * @param s   character sequence to be inserted
	 * @param start   index to the CharSequence
	 * @param end   index to the CharSequence
	 * @param charset   an encoding used for byte array
	 * @return   ByteArray
	 */
	public ByteArray insert(int dstOffset, CharSequence s, int start, int end, String charset) {
        if (s == null) throw new IllegalArgumentException("CharSequence is null");
    	if ((dstOffset < 0) || (dstOffset > count))
    	    throw new IndexOutOfBoundsException("dstOffset "+dstOffset);
    	final byte[] sByte;
    	if (charset == null) {
    		sByte = s.toString().getBytes();
    	} else {
    		try {
    			sByte = s.toString().getBytes(charset);
    		} catch (UnsupportedEncodingException e) {
    	        throw new RuntimeException(e);
    		}
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
	
	/**
	 * Inserts data to the byte array.
	 * 
	 * @param dstOffset   index to the internal byte array
	 * @param data   byte buffer to be inserted
	 * @param length   count of byte to be inserted
	 * @return   ByteArray
	 */
	public ByteArray insert(int dstOffset, ByteBuffer data, int length) {
        if (data == null)
    		return this;
    	if ((dstOffset < 0) || (dstOffset > count))
    	    throw new IndexOutOfBoundsException("dstOffset "+dstOffset);
    	if (length < 0)
                throw new IndexOutOfBoundsException("length " + length);
    	int newCount = count + length;
    	if (newCount > value.length)
    	    expandCapacity(newCount);
    	System.arraycopy(value, dstOffset, value, dstOffset + length, count - dstOffset);
    	data.get(value, dstOffset, length);
    	count = newCount;
		return this;
	}
	
	/**
	 * Duplicates and returns a part of internal byte array.
	 *  
	 * @param start   index to the byte array
	 * @param end   index to the byte array
	 * @return   a part of internal byte array
	 */
	public byte[] subArray(int start, int end) {
    	if ((start < 0) || (end < 0) || (start > end) || (end > count))
            throw new IndexOutOfBoundsException("start " + start + ", end " + end + ", byteArray.length() " + value.length);
		byte[] subArray = new byte[end - start];
    	System.arraycopy(value, start, subArray, 0, end - start);
		return subArray;
	}

	/**
	 * Returns length of byte array value.
	 * 
	 * @return length of byte array
	 */
	public int length() {
		return count;
	}
	
	/**
	 * Sets length of byte array.
	 * 
	 * @param count   changes size of byte array
	 */
    public void setLength(int count) {
    	if (count > this.count) throw new IndexOutOfBoundsException("count " + count);
    	this.count = count;
    }

    /**
     * Gets bit from byte array at the position.
	 * 
	 * @param position   bit index to byte array
     */
    public boolean getBit(int position) {
		return (value[position >> 3] & ((byte) (1 << (position % 8)))) != 0;
	}
	
    /**
     * Sets bit to 1 at the position.
	 * 
	 * @param position   bit index to byte array
     */
	public void setBit(int position) {
		value[position >> 3] |= ((byte) (1 << (position % 8)));
	}
	
	/**
     * Sets bit to 0 at the position.
	 * 
	 * @param position   bit index to byte array
	 */
	public void resetBit(int position) {
		value[position >> 3] &= (~((byte) (1 << (position % 8))));
	}
	
	/**
	 * Makes the 'bit and' over two byte array.
	 * 
	 * @param data   byte array used for 'bit and'
	 */
	public void  bitAND(byte[] data) {
		for (int i=0; i<data.length; i++) {
			if (value.length <= i) break;
			value[i] &= data[i];
		}
	}
	
	/**
	 * Makes the 'bit or' over two byte array.
	 * 
	 * @param data   byte array used for 'bit or'
	 */
	public void bitOR(byte[] data) {
		for (int i=0; i<data.length; i++) {
			if (value.length <= i) break;
			value[i] |= data[i];
		}
	} 
	
	/**
	 * Makes the 'bit xor' over internal byte array and data array.
	 * 
	 * @param data   byte array used for 'bit xor'
	 */
	public void bitXOR(byte[] data) {
		for (int i=0; i<data.length; i++) {
			if (value.length <= i) break;
			value[i] ^= data[i];
		}
	}

	/**
	 * Decodes byte array from base64 and gets its value
	 * 
	 * @return   decoded byte array
	 */
	public byte[] decodeBase64() {
		return Base64.decode(value, 0, count);
	}

	/**
	 * Decodes bytes in base64 and stores them in
	 * this ByteArray
	 * 
	 * @param base64  binary data in base64 encoding
	 */
	public void decodeBase64(String base64){
		setValue(Base64.decode(base64));
	}
	
	/**
	 * The 'data' byte array is encoded to base64 and putted to internal byte array.
	 * 
	 * @param data   byte array to be encoded
	 */
	public void encodeBase64(byte[] data) {
		fromString(Base64.encodeBytes(data));
	}

	public String encodeBase64(){
		return Base64.encodeBytes(value, 0, count);
	}
	
	
	/**
	 * Appends bit sequence like string "010011" into byte array.
	 * 
	 * @param seq   character sequence that is converted to the internal byte array.
	 * @param bitChar   represents true or false value
	 * @param bitValue   tells what bitChar value is (true or false)
	 */
	public void encodeBitStringAppend(CharSequence seq, char bitChar, boolean bitValue) {
        if(seq == null || seq.length() == 0) {
            return;
        }
        int size = ((seq.length()-1) >> 3) + count + 1;
        if (value.length < size) {
        	ensureCapacity(size);
        }
        int len = seq.length()+count;
        if (bitValue) {
        	for (int i=count; i<len; i++) {
            	if (seq.charAt(i-count) == bitChar) {          		
        			value[i >> 3] |= ((byte) (1 << (i % 8)));
        		} else {
        			value[i >> 3] &= (~((byte) (1 << (i % 8))));
        		}
        	}
        } else {
        	for (int i=count; i<len; i++) {
            	if (seq.charAt(i-count) != bitChar) {
           			value[i >> 3] |= ((byte) (1 << (i % 8)));
           		} else {
           			value[i >> 3] &= (~((byte) (1 << (i % 8))));
           		}
        	}
        }
        count = size;
	}

	/**
	 * Stores bit sequence like string "010011" into byte array.
	 * 
	 * @param seq   character sequence that is converted to the internal byte array.
	 * @param bitChar   represents true or false value
	 * @param bitValue   tells what bitChar value is (true or false)
	 */
	public void encodeBitString(CharSequence seq, char bitChar, boolean bitValue) {
        if(seq == null || seq.length() == 0) {
            return;
        }
        int size = ((seq.length()-1) >> 3) + 1;
        if (value.length < size) {
        	ensureCapacity(size);
        }
        int len = seq.length();
        if (bitValue) {
        	for (int i=0; i<len; i++) {
            	if (seq.charAt(i) == bitChar) {          		
        			value[i >> 3] |= ((byte) (1 << (i % 8)));
        		} else {
        			value[i >> 3] &= (~((byte) (1 << (i % 8))));
        		}
        	}
        } else {
        	for (int i=0; i<len; i++) {
            	if (seq.charAt(i) != bitChar) {
           			value[i >> 3] |= ((byte) (1 << (i % 8)));
           		} else {
           			value[i >> 3] &= (~((byte) (1 << (i % 8))));
           		}
        	}
        }
        count = size;
	}
	
	/**
	 * Returns bit sequence from byte array.
	 * 
	 * @param trueValue   the value that represents 'true' character
	 * @param falseValue   the value that represents 'false' character
	 * @param start   bit index to the internal byte array
	 * @param end   bit index to the internal byte array
	 * @return   CharSequence
	 */
	public CharSequence decodeBitString(char trueValue, char falseValue, int start, int end) {
		if ((start < 0) || (end < 0) || (start >= end)) 
            throw new IndexOutOfBoundsException("start " + start + ", end " + end + ", end-start " + (end-start));
		
		StringBuilder sb = new StringBuilder(end - start);
		for (int i=start; i<=end; i++) {
			sb.append(getBit(i) ? trueValue : falseValue);
		}
		return sb;
	}

	/**
	 * Converts a byte array containing a packed dicimal number to "long"
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
	 * Converts "long" to byte array of packed decimal number
	 * 
	 * @param lnum
	 *            long number to be converted
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
	 * Converts "Decimal" to byte array. 
	 * 
	 * @param decimal    Decimal number to be converted
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
	 * Converts a byte array containing a packed decimal number to Decimal value
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
			strbuf.append(DIGITS[nibble]);
			nibble = (value[i] & 0x0f);
			if (nibble > 9)
				throw new BadDataFormatException("Invalid decimal digit: " + nibble);
			strbuf.append(DIGITS[nibble]);
		}
		// Last byte contains sign
		nibble = (value[i] & 0xf0) >>> 4;
		if (nibble > 9)
			throw new BadDataFormatException("Invalid decimal digit: " + nibble);
		strbuf.append(DIGITS[nibble]);
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
	 * CompressData method compresses internal byte array. 
	 */
	public void compressData() {
		if (dataCompressed) return;
		byte[] compValue = new byte[count];
		System.arraycopy(value, 0, compValue, 0, count);
		fromByte(ZipUtils.compress(compValue));
		dataCompressed = true;
	}
	
	/**
	 * DeCompressData method decompresses internal byte array.
	 */
	public void deCompressData() {
		if (!dataCompressed) return;
		fromByte(ZipUtils.decompress(value));
		dataCompressed = false;
	}

	/**
	 * Indicates if byte array is packed or unpacked.
	 */
	public boolean isCompressed() {
		return dataCompressed;
	}

	public byte max() {
	    byte max = value[0];   // start with the first value
	    for (int i=1; i<count; i++) {
	        if (value[i] > max) {
	            max = value[i];   // new maximum
	        }
	    }
	    return max;
	}
	
	public byte min() {
	    byte min = value[0];   // start with the first value
	    for (int i=1; i<count; i++) {
	        if (value[i] < min) {
	            min = value[i];   // new maximum
	        }
	    }
	    return min;
	}
	
	
	/**
	 * Byte array iterator.
	 */
	public Iterator<Byte> iterator() {
		return new ByteIterator(this);
	}
	
	/**
	 * Internal byte iterator class.
	 * 
	 * @author ausperger
	 */
	private static class ByteIterator implements Iterator<Byte> {
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