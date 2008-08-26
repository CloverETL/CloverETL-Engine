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
package cz.opentech.jdbc.xlsdriver.db.util;

import java.lang.IndexOutOfBoundsException;

/**
 * Class for representing strings of bits.<br>
 * Uses arrays of bytes to group bits by 8 together.
 * Uses bit manipulation operations for fast access to individual bits.
 * Once created, the bit array can't be extended (shorten or prolonged).
 *
 *@author     dpavlis
 *@author     vitaz
 *@created    8. April 2003
 */

public class BitSet {
    
    private static final int MASK_8 = 0x00000007;
    private static final int EXTENT = 8; // an extra space for realloc
    
    private static final byte[] EMPTY_BITS = new byte[0];
    
    private byte bits[] = EMPTY_BITS;
    private int size = 0;
    
    
    /**
     *  Constructor for the BitSet object
     *
     * @param  size  how many bits should the array keep
     */
    public BitSet(int size) {
        ensureSize(size);
    }
    
    // index to bits array
    private int idx(int index) {
        return index >> 3;
    }
    // position in the byte
    private int pos(int index) {
        return index & MASK_8;
    }
    
    /**
     * 
     * @param flag
     */
    public void add(boolean flag) {
        ensureSize(size + 1);
        set(size);
    }
    
    /**
     * Sets the specified bit to 1/True
     *
     * @param  index  which bit to set
     */
    public void set(int index) {
        checkBounds(index);
        bits[idx(index)] |= (byte) (1 << pos(index));
    }
    
    
    /**
     *  Re-sets the specified bit to 0/false
     *
     *@param  index  which bit to reset
     */
    public void reset(int index) {
        checkBounds(index);
        bits[idx(index)] &= ~((byte) (1 << pos(index)));
    }
    
    
    /**
     *  Gets state of specified bit
     *
     *@param  index  which bit to check
     *@return        true/false according to bit state
     */
    public boolean get(int index) {
        checkBounds(index);
        return (bits[idx(index)] & (byte) (1 << pos(index))) > 0;
    }
    
    
    /**
     *  Gets state of specified bit<br>
     *  Synonym for get()
     *
     *@param  index  which bit to check
     *@return        true/false according to bit state
     */
    public boolean isSet(int index) {
        return get(index);
    }
    
    
    /**
     * Returns length of BitArray (in bits)
     *
     * @return    length
     */
    public int size() {
        return size;
    }
        
    /**
     * Formats BitArray into String where set bits
     * are displayed as 1 and reset as 0
     *
     *@return    String representation of BitArray
     */
    public String toString() {
        StringBuffer str = new StringBuffer(size * 2 + 1);
        str.append("|");
        for (int i = 0; i < size; i++) {
            str.append(get(i) ? "1" : "0");
            str.append("|");
        }
        return str.toString();
    }
    
    private void checkBounds(int index) {
        if (index < 0) {
            throw new IndexOutOfBoundsException(index + " < 0");
        }
        if (index >= size) {
            throw new IndexOutOfBoundsException(index + ">=" + size);
        }
    }
    private void ensureSize(int newSize) {
        final int newLen = idx(newSize) + pos(newSize) > 0 ? 1 : 0;
        if (bits.length < newLen) {
            byte[] newBits = new byte[newLen + EXTENT];
            System.arraycopy(bits, 0, newBits, 0, bits.length);
            bits = newBits;
            size = newSize;
        }
    }
}
