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
package org.jetel.util.primitive;
import java.io.Serializable;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import edu.umd.cs.findbugs.annotations.SuppressWarnings;
/**
 * Class for representing strings of bits.<br>
 * Uses arrays of bytes to group bits by 8 together.
 * Uses bit manipulation operations for fast access to individual bits.
 * Once created, the bit array can't be extended (shorten or prolonged).
 *
 *@author     dpavlis
 *@created    8. April 2003
 */

@SuppressWarnings("EI")
public class BitArray implements Serializable{

	private static final long serialVersionUID = 5285503841640300317L;
	
	private  byte bits[];
	private  int length;
    private  int lengthBytes;


	/**
	 *  Constructor for the BitArray object
	 *
	 *@param  size  how many bits should the array keep
	 */
	public BitArray(int size) {
		length = size;
		if (length<1) throw new RuntimeException("Can't create ZERO length array !");
		bits = new byte[((size-1) >> 3) + 1];
        lengthBytes=bits.length;
		// init array
        Arrays.fill(bits,(byte)0);
	}

    /**
     * Creates BitArray with space for 16 bits.
     * 
     */
    public BitArray(){
        this(16);
    }
    
    private BitArray(byte[] bits,int length){
        this.bits=bits;
        this.length=length;
        this.lengthBytes=bitsLength2Bytes(length);
        
    }
    
    
    /**
     * Wraps existing byte array into BitArray
     * 
     * @param bits
     * @param length
     * @return
     * @since 23.1.2007
     */
    public static BitArray wrap(byte[] bits,int length){
        return new BitArray(bits,length);
    }

    /**
     * Utility method which makes sure we can
     * accomodate specified number of bits and then
     * resets all bits.
     * 
     * @param size
     * @since 23.1.2007
     */
    public void resize(int size){
       ensureSize(size);
       resetAll();
    }
    
    /**
     * Ensures that the BitArray can accomodate specified
     * number of bits
     * 
     * @param size
     * @since 18.1.2007
     */
    public void ensureSize(int size){
        length = size;
        lengthBytes = ((size-1) >> 3) + 1; 
        if (bits.length < lengthBytes){
            bits = new byte[lengthBytes];
        }
       
    }
    
	/**
	 *  Sets the specified bit to 1/True.
     *  The index is ZERO based.
	 *
	 *@param  index  which bit to set
	 */
	public final void set(int index) {
		bits[index >> 3] |= (byte) (1 << (index % 8));
	}


	/**
	 *  Re-sets the specified bit to 0/false.
     *  The index is ZERO based.
	 *
	 *@param  index  which bit to reset
	 */
	public final void reset(int index) {
		bits[index >> 3] &= (~((byte) (1 << (index % 8))));
	}

    
    public final void resetAll(){
        Arrays.fill(bits,0,lengthBytes,(byte)0);
    }
    
    public final void setAll(){
        Arrays.fill(bits,0,lengthBytes,(byte)0xFF);
    }

	/**
	 *  Gets state of specified bit.
     *  The index is ZERO based.
	 *
	 *@param  index  which bit to check
	 *@return        true/false according to bit state
	 */
	public final boolean isSet(int index) {
		return (bits[index >> 3] & ((byte) (1 << (index % 8)))) != 0;
	}


	/**
	 *  Gets state of specified bit<br>
	 *  Synonym for isSet()
	 *
	 *@param  index  which bit to check
	 *@return        true/false according to bit state
	 */
	public final boolean get(int index) {
		return isSet(index);
	}


	/**
	 *  Returns length of BitArray (in bits)
	 *
	 *@return    length
	 */
	public final int length() {
		return length;
	}


	/**
	 *  Formats BitArray into String where set bits
	 *  are displayed as 1 and reset as 0
	 *
	 *@return    String representation of BitArray
	 */
	@Override
	public String toString() {
		StringBuffer str = new StringBuffer(length * 2 + 1);
		str.append("|");
		for (int i = 0; i < length; i++) {
			str.append(get(i) ? "1" : "0");
			str.append("|");
		}
		return str.toString();
	}

    public static final String toString(byte[] array) {
        StringBuffer str = new StringBuffer(array.length * 8 *2  + 1);
        str.append("|");
        for (int i = 0; i < (array.length*8); i++) {
            str.append(isSet(array,i) ? "1" : "0");
            str.append("|");
        }
        return str.toString();
    }
    

	/**
	 *  Returns backing array of bytes<br>
	 *  Can be used for bulk-load operations.
 	 *
	 *@return    The byteArray value
	 */
	public final byte[] getByteArray() {
		return bits;
	}
	 
    /**
     *  Performs serialization of the internal value into ByteBuffer 
     *
     *@param  buffer  Description of Parameter
     *@since          October 29, 2002
     */
    public void serialize(ByteBuffer buffer) {
    	try {
    		buffer.put(bits,0,lengthBytes);
    	} catch (BufferOverflowException e) {
    		throw new RuntimeException("The size of data buffer is only " + buffer.limit() + ". Set appropriate parameter in defaultProperties file.", e);
    	}
    }


    /**
     *  Performs deserialization of data
     *
     *@param  buffer  Description of Parameter
     *@since          October 29, 2002
     */
    public void deserialize(ByteBuffer buffer) {
        buffer.get(bits,0,lengthBytes);
    }

    
    /**
     * How many bytes is needed to store specified number
     * of bits.
     * 
     * @param bits
     * @return
     * @since 18.1.2007
     */
    public final static int bitsLength2Bytes(int bits){
        return ((bits - 1)>> 3) + 1;
    }
    
    /**
     * Sets specified bit in array of bytes - counting
     * from left.
     * The bit parameter is ZERO based.
     * 
     * @param bytes
     * @param bit
     * @since 18.1.2007
     */
    public final static void set(byte[] bytes,int bit){
        bytes[bit >> 3] |= (byte) (1 << (bit % 8));
    }

    public final static void set(ByteBuffer bytes, int base, int bit) {
		final int index = base + (bit >> 3);
		bytes.put(index, (byte) (bytes.get(index) | (1 << (bit % 8))));
	}

    /**
     *  Gets status of specified bit in array of bytes - counting
     * from left.
     *  The bit parameter is ZERO based.
     * 
     * @param bytes
     * @param bit
     * @return  true if set otherwise false
     * @since 18.1.2007
     */
    public final static boolean isSet(byte[] bytes,int bit){
        return ((bytes[bit >> 3] & ((byte) (1 << (bit % 8)))) != 0 );
    }

    public final static boolean isSet(ByteBuffer bytes, int base, int bit) {
		final int index = base + (bit >> 3);
		return ((bytes.get(index) & ((byte) (1 << (bit % 8)))) != 0);
	}

    /**
     * Resets specified bit in array of bytes - counting
     * from left.
     *  The bit parameter is ZERO based.
     * 
     * @param bytes
     * @param bit
     * @since 18.1.2007
     */
    public final static void reset(byte[] bytes, int bit){
        bytes[bit >> 3] &= (~((byte) (1 << (bit % 8))));
    }

    public final static void reset(ByteBuffer bytes, int base, int bit) {
		final int index = base + (bit >> 3);
		bytes.put(index, (byte) (bytes.get(index) & (~((1 << (bit % 8))))));
	}

    /*
     * Extracts encoded value/number in array of bits using mask
     * which defines which bits to use when extracting value.
     * Max length of array is 8 bytes.
     */
    public final static int extractNumber(byte[]bytes, long mask){
    	if(bytes.length>8) return -1;
    	long value=0;
    	for(int i=0;i<bytes.length;i++){
    		value|= bytes[i]<< (8*i);
    	}
    	value&=mask;
    	while((mask&0x1)==0){
    		value>>=1;
    		mask>>=1;
    	}
    	return (int)value;
    }
    
    public static void encodeNumber(byte[] bytes, long mask, int value){
    	long rvalue=value;
    	long imask=mask;
    	while((imask&0x1)==0){
    		rvalue<<=1;
    		imask>>=1;
    	}
    	for(int i=0;i<bytes.length;i++){
    		bytes[i] = (byte) (bytes[i] | ((byte) ( (rvalue & mask) >> (8*i)))); 
    	}
    	
    }
    
    /**
     * How many bytes are used to store the number
     * of bits this BitArray can represent
     * 
     * @return the lengthBytes
     * @since 25.1.2007
     */
    public int getLengthBytes() {
        return lengthBytes;
    }   
    
    
    /**
     * Calculates bit AND of two byte array arguments.
     * If one is shorter than the other, then only the
     * common length of bits are processed.
     * 
     * @param first
     * @param second
     * @return
     */
    public static byte[] bitAnd(byte[] first,byte[] second){
    	final int len=Math.min(first.length, second.length);
    	byte[] result=new byte[len];
    	for(int i=0;i<len;i++){
    		result[i]=(byte)(first[i]&second[i]);
    	}
    	return result;
    }
    
    /**
     * Calculates bit OR of two byte array arguments.
     * If one is shorter than the other, then only the
     * common length of bits are processed.
     * 
     * @param first
     * @param second
     * @return
     */
    public static byte[] bitOr(byte[] first,byte[] second){
    	final int len=Math.min(first.length, second.length);
    	byte[] result=new byte[len];
    	for(int i=0;i<len;i++){
    		result[i]=(byte)(first[i]|second[i]);
    	}
    	return result;
    }
    
    /**
     * Calculates bit XOR of two byte array arguments.
     * If one is shorter than the other, then only the
     * common length of bits are processed.
     * 
     * @param first
     * @param second
     * @return
     */
    public static byte[] bitXor(byte[] first,byte[] second){
    	final int len=Math.min(first.length, second.length);
    	byte[] result=new byte[len];
    	for(int i=0;i<len;i++){
    		result[i]=(byte)(first[i] ^ second[i]);
    	}
    	return result;
    }
    
    public static byte[] bitNegate(byte[] first){
    	byte[] result=new byte[first.length];
    	for(int i=0;i<result.length;i++){
    		result[i]=(byte) ~first[i];
    	}
    	return result;
    }
}
/*
 *  End class BitArray
 */

