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
package org.jetel.util.string;

import java.text.CharacterIterator;

/**
 * Bridge class between CharSequence and CharacterIterator
 * interfaces.<br>
 * Indended use is primarily for Collator and comparing of
 * string/char sequences
 * 
 * @author david pavlis
 * @since  17.10.2006
 *
 */
public final class CharSequenceCharacterIterator implements CharacterIterator {

    private final CharSequence buf;
    private int index;
    private final int len;
    
    public CharSequenceCharacterIterator(CharSequence buf){
        this.buf=buf;
        index=0;
        len=buf.length();
    }
    
    
    /* (non-Javadoc)
     * @see java.text.CharacterIterator#current()
     */
    public char current() {
        return index<len ? buf.charAt(index) : DONE;
    }

    /* (non-Javadoc)
     * @see java.text.CharacterIterator#first()
     */
    public char first() {
        if (len>0){
            index=0;
            return buf.charAt(0);
        }else{
            return DONE;
        }
    }

    /* (non-Javadoc)
     * @see java.text.CharacterIterator#getBeginIndex()
     */
    public int getBeginIndex() {
        return 0;
    }

    /* (non-Javadoc)
     * @see java.text.CharacterIterator#getEndIndex()
     */
    public int getEndIndex() {
        return len;
    }

    /* (non-Javadoc)
     * @see java.text.CharacterIterator#getIndex()
     */
    public int getIndex() {
        return index;
    }

    /* (non-Javadoc)
     * @see java.text.CharacterIterator#last()
     */
    public char last() {
        if (len>0){
            index=len-1;
            return buf.charAt(index);
        }else{
            index=len;
            return DONE;
        }
    }

    /* (non-Javadoc)
     * @see java.text.CharacterIterator#next()
     */
    public char next() {
        index++;
        if (index>=len){
            index=getEndIndex();
            return DONE;
        }
        return buf.charAt(index);
    }

    /* (non-Javadoc)
     * @see java.text.CharacterIterator#previous()
     */
    public char previous() {
        index--;
        if (index<0){
            index=0;
            return DONE;
        }
        return buf.charAt(index);
    }

    /* (non-Javadoc)
     * @see java.text.CharacterIterator#setIndex(int)
     */
    public char setIndex(int position) {
        index=position;
        if (index>len || index < 0){
            throw new IllegalArgumentException("Invalid position: "+position);
        }else if (index==len){
            return DONE;
        }
        return buf.charAt(index);
    }   

    public Object clone(){
        return new CharSequenceCharacterIterator(this.buf);
    }
    
}
