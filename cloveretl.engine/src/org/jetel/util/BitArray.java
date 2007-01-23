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
package org.jetel.util;
import java.lang.IndexOutOfBoundsException;
import java.nio.ByteBuffer;
import java.util.Arrays;
/**
 * Class for representing strings of bits.<br>
 * Uses arrays of bytes to group bits by 8 together.
 * Uses bit manipulation operations for fast access to individual bits.
 * Once created, the bit array can't be extended (shorten or prolonged).
 *
 *@author     dpavlis
 *@created    8. April 2003
 */

public class BitArray {

	private  byte bits[];
	private  int length;


	/**
	 *  Constructor for the BitArray object
	 *
	 *@param  size  how many bits should the array keep
	 */
	public BitArray(int size) {
		length = size;
		if (length<1) throw new RuntimeException("Can't create ZERO length array !");
		bits = new byte[(size-1) / 8 + 1];
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
     * Resizes the BitArray to new size & initializes all
     * bits to 0.
     * 
     * @param size
     * @since 18.1.2007
     */
    public void resize(int size){
        length = size;
        if (length<1) throw new RuntimeException("Can't create ZERO length array !");
        if (bits.length < ((size-1) / 8 + 1)){
            bits = new byte[(size-1) / 8 + 1];
        }
        // init array
        Arrays.fill(bits,(byte)0);
       
    }
    
	/**
	 *  Sets the specified bit to 1/True.
     *  The index is ZERO based.
	 *
	 *@param  index  which bit to set
	 */
	public final void set(int index) {
		if (index >= length) {
			throw new IndexOutOfBoundsException();
		}
		bits[index >> 3] |= (byte) (1 << (index % 8));
	}


	/**
	 *  Re-sets the specified bit to 0/false.
     *  The index is ZERO based.
	 *
	 *@param  index  which bit to reset
	 */
	public final void reset(int index) {
		if (index >= length) {
			throw new IndexOutOfBoundsException();
		}
		bits[index >> 3] &= (~((byte) (1 << (index % 8))));
	}

    
    public final void resetAll(){
        Arrays.fill(bits, (byte)0);
    }
    
    public final void setAll(){
        Arrays.fill(bits,(byte)0xFF);
    }

	/**
	 *  Gets state of specified bit.
     *  The index is ZERO based.
	 *
	 *@param  index  which bit to check
	 *@return        true/false according to bit state
	 */
	public final boolean get(int index) {
		if (index >= length) {
			throw new IndexOutOfBoundsException();
		}
		byte pattern = (byte) (1 << (index % 8));
		return (bits[index >> 3] & pattern) == pattern ? true : false;
	}


	/**
	 *  Gets state of specified bit<br>
	 *  Synonym for get()
	 *
	 *@param  index  which bit to check
	 *@return        true/false according to bit state
	 */
	public final boolean isSet(int index) {
		return get(index);
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
         buffer.put(bits);
    }


    /**
     *  Performs deserialization of data
     *
     *@param  buffer  Description of Parameter
     *@since          October 29, 2002
     */
    public void deserialize(ByteBuffer buffer) {
        buffer.get(bits);
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
        final byte pattern = (byte) (1 << (bit % 8));
        return ((bytes[bit >> 3] & pattern) == pattern );
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
}
/*
 *  End class BitArray
 */

