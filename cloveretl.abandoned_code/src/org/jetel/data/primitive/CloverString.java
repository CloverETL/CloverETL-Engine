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
/**
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2002-09  David Pavlis, Javlin a.s. <david.pavlis@javlin.eu>
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
*/

package org.jetel.data.primitive;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;

/**
 * @author dpavlis
 *
 */
public class CloverString implements Readable, Appendable, CharSequence, Comparable<CloverString>, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4885843404905174997L;
	
	static final int CHUNK_SIZE_L1=16;
	static final int COPY_TRESHOLD=12; 
	//static int CHUNK_SIZE_L2=64;
	
	private int length;
	private char[] dataL1;
	//char[][] dataL2;
	
	
	public CloverString(){
		dataL1=new char[CHUNK_SIZE_L1];
		length=0;
	}
	
	public CloverString(int size){
		dataL1=new char[size];
		length=0;
	}
	
	public CloverString(CharSequence seq){
		final int count=seq.length();
		dataL1=new char[count];
		for(int i=0;i<count;dataL1[i]=seq.charAt(i++));
		length=count;
	}
	
	public CloverString(char[] data){
		dataL1=data;
		length=data.length;
	}
	
	public CloverString(CloverString string){
		length=string.length;
		dataL1=new char[length];
		charArrayCopy(string.dataL1, 0, dataL1, 0, length);
	}
	
	public CloverString(String string){
		length=string.length();
		dataL1=new char[length];
		string.getChars(0, length, dataL1, 0);
	}

	public CloverString(StringBuilder string){
		length=string.length();
		dataL1=new char[length];
		string.getChars(0, length, dataL1, 0);
	}

	/* (non-Javadoc)
	 * @see java.lang.Readable#read(java.nio.CharBuffer)
	 */
	public int read(CharBuffer cb) throws IOException {
		if (length==0) return -1;
		for(int i=0;i<length;i++){
			cb.put(dataL1, 0, length);
		}
		return length;
	}

	public int put(CharBuffer cb) throws IOException {
		final int count=cb.remaining();
		if (count>dataL1.length) secureCapacity(count,false);
		cb.get(dataL1);
		length=count;
		return count;
	}
	
	private final void secureCapacity(int count, boolean preserveData) {
			int newCapacity = (dataL1.length + 1) * 2;
			if (newCapacity < 0) {
				newCapacity = Integer.MAX_VALUE;
			} else if (count > newCapacity) {
				newCapacity = count;
			}
			if (preserveData && length > 0) {
				char[] tmpdata = new char[newCapacity];
				charArrayCopy(dataL1, 0, tmpdata, 0, length);
				dataL1 = tmpdata;
			} else {
				dataL1 = new char[newCapacity];
			}
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Appendable#append(java.lang.CharSequence)
	 */
	public Appendable append(CharSequence csq) {
		final int count=csq.length();
		final int newLength=count+length;
		if(newLength>dataL1.length) secureCapacity(newLength, true);
		for(int i=0;i<count;i++){
			dataL1[length++]=csq.charAt(i);
		}
		return this;
	}

	public Appendable append(CloverString csq) {
		final int count=csq.length;
		final int newLength=length+count;
		if (newLength>dataL1.length) secureCapacity(newLength, true);
		charArrayCopy(csq.dataL1, 0, dataL1, length, count);
		length=newLength;
		return this;
	}

	
	/* (non-Javadoc)
	 * @see java.lang.Appendable#append(char)
	 */
	public Appendable append(char c){
		try{
			dataL1[length]=c;
		}catch (IndexOutOfBoundsException ex){
			secureCapacity(length+1, true);
			dataL1[length]=c;
		}finally{
			length++;
		}
		return this;
	}

	/* (non-Javadoc)
	 * @see java.lang.Appendable#append(java.lang.CharSequence, int, int)
	 */
	public Appendable append(CharSequence csq, int start, int end)  {
		final int count=end-start;
		final int newlength=length+count;
		if(newlength>dataL1.length) secureCapacity(newlength, true);
		for(int i=start;i<end;i++){
			dataL1[length++]=csq.charAt(i);
		}
		return this;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Appendable#append(java.lang.CharSequence, int, int)
	 */
	public Appendable append(char[] csq, int start, int end)  {
		final int count=end-start;
		final int newlength=length+count;
		if (newlength>dataL1.length) secureCapacity(newlength, true);
		charArrayCopy(csq, start, dataL1, length, count);
		length=newlength;
		return this;
	}

	public Appendable append(char[] csq)  {
		return append(csq,0,csq.length-1);
	}

	
	public Appendable append(String str){
		final int len=str.length();
		final int newlength=length+len;
		if (newlength>dataL1.length) secureCapacity(newlength,true);
		str.getChars(0, len, dataL1, length);
		length=newlength;
		return this;
	}
	
	public Appendable append(StringBuilder str){
		final int len=str.length();
		final int newlength=length+len;
		if (newlength>dataL1.length) secureCapacity(newlength,true);
		str.getChars(0, len, dataL1, length);
		length=newlength;
		return this;
	}
	
	public Appendable append(Object str){
		if (str instanceof CloverString){
			append((CloverString)str);
		}else if (str instanceof String){
			append((String)str);
		}else if (str instanceof CharSequence){
			append((CharSequence)str);
		}else{
			append(str.toString());
		}
		return this;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.CharSequence#charAt(int)
	 */
	public final char charAt(int index) {
		if (index<length)
			return dataL1[index];
		else
			throw new IndexOutOfBoundsException(Integer.toString(index));
	}

	/* (non-Javadoc)
	 * @see java.lang.CharSequence#length()
	 */
	public final int length() {
		return length;
	}

	
	public char[] getChars(){
		return this.dataL1;
	}
	
	
	/* (non-Javadoc)
	 * @see java.lang.CharSequence#subSequence(int, int)
	 */
	public CharSequence subSequence(int start, int end) {
		return new CloverString(slice(start,end));
	}

	
	char[] slice(int start, int end){
		int count=end-start;
		if (count<1) return new char[0];
		char[] tmp=new char[count];
		charArrayCopy(dataL1, start, tmp, 0, count);
		return tmp;
	}
	
	@Override
	public String toString(){
		return new String(dataL1,0,length);
	}

	public CloverString duplicate(){
		char[] tmp=new char[length];
		charArrayCopy(dataL1, 0, tmp, 0, length);
		return new CloverString(tmp);
	}

	public StringBuilder toStringBuilder(){
		StringBuilder tmp=new StringBuilder(length);
		return tmp.append(dataL1,0,length);
	}
	
	
	public int compareTo(CloverString o) {
		int n1 = length, n2 = o.length;
		for (int i1 = 0, i2 = 0; i1 < n1 && i2 < n2; i1++, i2++) {
			char c1 = dataL1[i1];
			char c2 = o.dataL1[i2];
			if (c1 != c2) {
				return (c1 - c2) < 0 ? -1 : 1;
			}
		}
		if ( n1 >= n2)
			if (n1 == n2)
				return 0;
			else
				return 1;
			else
				return -1;
	}
	
	public int compareTo(CharSequence o){
		int n1 = length, n2 = o.length();
		for (int i1 = 0, i2 = 0; i1 < n1 && i2 < n2; i1++, i2++) {
			char c1 = dataL1[i1];
			char c2 = o.charAt(i2);
			if (c1 != c2) {
				return (c1 - c2) < 0 ? -1 : 1;
			}
		}
		if ( n1 >= n2)
			if (n1 == n2)
				return 0;
			else
				return 1;
			else
				return -1;
	}
	
	public int compareToIgnoreCase(CloverString o){
	      int n1=length, n2=o.length;
        for (int i1=0, i2=0; i1<n1 && i2<n2; i1++, i2++) {
            char c1 = dataL1[i1];
            char c2 = o.dataL1[i2];
            if (c1 != c2) {
                c1 = Character.toUpperCase(c1);
                c2 = Character.toUpperCase(c2);
                if (c1 != c2) {
                    c1 = Character.toLowerCase(c1);
                    c2 = Character.toLowerCase(c2);
                    if (c1 != c2) {
                        return c1 - c2;
                    }
                }
            }
        }
        return n1 - n2;
    }
	
	public int compareToIgnoreCase(CharSequence o){
	      int n1=length, n2=o.length();
          for (int i1=0, i2=0; i1<n1 && i2<n2; i1++, i2++) {
              char c1 = dataL1[i1];
              char c2 = o.charAt(i2);
              if (c1 != c2) {
                  c1 = Character.toUpperCase(c1);
                  c2 = Character.toUpperCase(c2);
                  if (c1 != c2) {
                      c1 = Character.toLowerCase(c1);
                      c2 = Character.toLowerCase(c2);
                      if (c1 != c2) {
                          return c1 - c2;
                      }
                  }
              }
          }
          return n1 - n2;
      }
	
	/*
	 * Based on String's hash code method (non-Javadoc)
	 * 
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
			int h=5381;
			/*
			 * for (int i = 0; i < length; i++) { h = 31*h + dataL1[i]; }
			 */
			for (int i = 0; i < length; i++) {
				h = ((h << 5)+h)  + dataL1[i];
			}
			return (h & 0x7FFFFFFF);


	}

	@Override
	public boolean equals(Object anObject) {
		if (this == anObject) {
			return true;
		}
		if (anObject instanceof CloverString) {
			CloverString anotherString = (CloverString) anObject;
			int n = length;
			if (n == anotherString.length) {
				int i = 0;
				int j = 0;
				while (n-- != 0) {
					if (dataL1[i++] != anotherString.dataL1[j++])
						return false;
				}
				return true;
			}
		}else if (anObject instanceof CharSequence){
			CharSequence anotherString = (CharSequence) anObject;
			int n = length;
			if (n == anotherString.length()) {
				int i = 0;
				int j = 0;
				while (n-- != 0) {
					if (dataL1[i++] != anotherString.charAt(j++));
						return false;
				}
				return true;
			}
		}
		return false;
	}
	
	public void clear(){
		length=0;
	}
	
	public void setLength(int newLength){
		this.length=newLength;
	}
	
	public void serialize(ByteBuffer buffer){
		for(int i=0;i<length;buffer.putChar(dataL1[i++]));
	}
	
	public void deserialize(ByteBuffer buffer,int count){
		if(count>dataL1.length) secureCapacity(count, false);
		for(int i=0;i<count;dataL1[i++]=buffer.getChar());
		length=count;
	}
	
	private void charArrayCopy(char[] src,int srcPos, char[] dest,int destPos,int length){
		if (length<COPY_TRESHOLD){
			while(length>0){
				dest[destPos++]=src[srcPos++];
			length--;
		}
		}else{
			System.arraycopy(src, srcPos, dest, destPos, length);
		}
	}
	
	public void toUpperCase(){
		for(int i=0;i<length;i++)
			dataL1[i]=Character.toUpperCase(dataL1[i]);
	}
	
	public void toLowerCase(){
		for(int i=0;i<length;i++)
			dataL1[i]=Character.toLowerCase(dataL1[i]);
	}
}
