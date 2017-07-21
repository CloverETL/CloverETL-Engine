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

import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.nio.BufferOverflowException;
import java.nio.CharBuffer;
import java.util.Arrays;

/**
 * This class was derived from {@link StringBuilder}. We need direct access to underlying char array
 * for better cooperation with {@link CharBuffer} class.
 * 
 * Only two important methods was added, {@link #append(CharBuffer, int)} and {@link #getChars(CharBuffer)}
 * 
 * @author Kokon (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created 21 Oct 2011
 * @inspired by java.lang.StringBuilder
 */
public class CloverString implements Appendable, CharSequence, Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 4793544495376003961L;

	/**
	 * Used for memory diagnostics when the OutOfMemory is thrown.
	 */
	private static MemoryMXBean memMXB = ManagementFactory.getMemoryMXBean();

	/**
	 * The value is used for character storage.
	 */
	char value[];

	/**
	 * The count is the number of characters used.
	 */
	int count;

	/**
	 * Constructs a string builder with no characters in it and an initial capacity of 16 characters.
	 */
	public CloverString() {
		this(16);
	}

	/**
	 * Constructs a string builder with no characters in it and an initial capacity specified by the
	 * <code>capacity</code> argument.
	 * 
	 * @param capacity
	 *            the initial capacity.
	 * @throws NegativeArraySizeException
	 *             if the <code>capacity</code> argument is less than <code>0</code>.
	 */
	public CloverString(int capacity) {
		value = new char[capacity];
	}

	/**
	 * Constructs a string builder initialized to the contents of the specified string. The initial capacity of the
	 * string builder is <code>16</code> plus the length of the string argument.
	 * 
	 * @param str
	 *            the initial contents of the buffer.
	 * @throws NullPointerException
	 *             if <code>str</code> is <code>null</code>
	 */
	public CloverString(String str) {
		this(str.length() + 16);
		append(str);
	}

	/**
	 * Constructs a string builder that contains the same characters as the specified <code>CharSequence</code>. The
	 * initial capacity of the string builder is <code>16</code> plus the length of the <code>CharSequence</code>
	 * argument.
	 * 
	 * @param seq
	 *            the sequence to copy.
	 * @throws NullPointerException
	 *             if <code>seq</code> is <code>null</code>
	 */
	public CloverString(CharSequence seq) {
		this(seq.length() + 16);
		append(seq);
	}

	/**
	 * Returns the length (character count).
	 * 
	 * @return the length of the sequence of characters currently represented by this object
	 */
	@Override
	public int length() {
		return count;
	}

	/**
	 * Returns the current capacity. The capacity is the amount of storage available for newly inserted characters,
	 * beyond which an allocation will occur.
	 * 
	 * @return the current capacity
	 */
	public int capacity() {
		return value.length;
	}

	/**
	 * Ensures that the capacity is at least equal to the specified minimum. If the current capacity is less than the
	 * argument, then a new internal array is allocated with greater capacity. The new capacity is the larger of:
	 * <ul>
	 * <li>The <code>minimumCapacity</code> argument.
	 * <li>Twice the old capacity, plus <code>2</code>.
	 * </ul>
	 * If the <code>minimumCapacity</code> argument is nonpositive, this method takes no action and simply returns.
	 * 
	 * @param minimumCapacity
	 *            the minimum desired capacity.
	 */
	public void ensureCapacity(int minimumCapacity) {
		if (minimumCapacity > value.length) {
			expandCapacity(minimumCapacity);
		}
	}

	/**
	 * This implements the expansion semantics of ensureCapacity with no size check or synchronization.
	 */
	void expandCapacity(int minimumCapacity) {
		int newCapacity = (value.length + 1) * 2;
		if (newCapacity < 0) {
			newCapacity = Integer.MAX_VALUE;
		} else if (minimumCapacity > newCapacity) {
			newCapacity = minimumCapacity;
		}
		try {
			value = Arrays.copyOf(value, newCapacity);
		} catch (OutOfMemoryError err) {
			MemoryUsage mu = memMXB.getHeapMemoryUsage();
			StringBuilder sb = new StringBuilder(160);
			sb.append(err.getMessage());
			sb.append(" Heap: [commited=");
			sb.append(mu.getCommitted()/1024);
			sb.append("kB max=");
			sb.append(mu.getMax()/1024);
			sb.append("kB used=");
			sb.append(mu.getUsed()/1024);
			sb.append("kB ] Non-heap: [commited=");
			mu = memMXB.getNonHeapMemoryUsage();
			sb.append(mu.getCommitted()/1024);
			sb.append("kB max=");
			sb.append(mu.getMax()/1024);
			sb.append("kB used=");
			sb.append(mu.getUsed()/1024);
			sb.append("kB ]");
			throw new OutOfMemoryError(sb.toString());
		}
	}

	/**
	 * Attempts to reduce storage used for the character sequence. If the buffer is larger than necessary to hold its
	 * current sequence of characters, then it may be resized to become more space efficient. Calling this method may,
	 * but is not required to, affect the value returned by a subsequent call to the {@link #capacity()} method.
	 */
	public void trimToSize() {
		if (count < value.length) {
			value = Arrays.copyOf(value, count);
		}
	}

	/**
	 * Sets the length of the character sequence. The sequence is changed to a new character sequence whose length is
	 * specified by the argument. For every nonnegative index <i>k</i> less than <code>newLength</code>, the character
	 * at index <i>k</i> in the new character sequence is the same as the character at index <i>k</i> in the old
	 * sequence if <i>k</i> is less than the length of the old character sequence; otherwise, it is the null character
	 * <code>'&#92;u0000'</code>.
	 * 
	 * In other words, if the <code>newLength</code> argument is less than the current length, the length is changed to
	 * the specified length.
	 * <p>
	 * If the <code>newLength</code> argument is greater than or equal to the current length, sufficient null characters
	 * (<code>'&#92;u0000'</code>) are appended so that length becomes the <code>newLength</code> argument.
	 * <p>
	 * The <code>newLength</code> argument must be greater than or equal to <code>0</code>.
	 * 
	 * @param newLength
	 *            the new length
	 * @throws IndexOutOfBoundsException
	 *             if the <code>newLength</code> argument is negative.
	 */
	public void setLength(int newLength) {
		if (newLength < 0)
			throw new StringIndexOutOfBoundsException(newLength);
		if (newLength > value.length)
			expandCapacity(newLength);

		if (count < newLength) {
			for (; count < newLength; count++)
				value[count] = '\0';
		} else {
			count = newLength;
		}
	}

	/**
	 * Returns the <code>char</code> value in this sequence at the specified index. The first <code>char</code> value is
	 * at index <code>0</code>, the next at index <code>1</code>, and so on, as in array indexing.
	 * <p>
	 * The index argument must be greater than or equal to <code>0</code>, and less than the length of this sequence.
	 * 
	 * <p>
	 * If the <code>char</code> value specified by the index is a <a href="Character.html#unicode">surrogate</a>, the
	 * surrogate value is returned.
	 * 
	 * @param index
	 *            the index of the desired <code>char</code> value.
	 * @return the <code>char</code> value at the specified index.
	 * @throws IndexOutOfBoundsException
	 *             if <code>index</code> is negative or greater than or equal to <code>length()</code>.
	 */
	@Override
	public char charAt(int index) {
		if ((index < 0) || (index >= count))
			throw new StringIndexOutOfBoundsException(index);
		return value[index];
	}

	/**
	 * Characters are copied from this sequence into the destination character array <code>dst</code>. The first
	 * character to be copied is at index <code>srcBegin</code>; the last character to be copied is at index
	 * <code>srcEnd-1</code>. The total number of characters to be copied is <code>srcEnd-srcBegin</code>. The
	 * characters are copied into the subarray of <code>dst</code> starting at index <code>dstBegin</code> and ending at
	 * index:
	 * <p>
	 * <blockquote>
	 * 
	 * <pre>
	 * dstbegin + (srcEnd - srcBegin) - 1
	 * </pre>
	 * 
	 * </blockquote>
	 * 
	 * @param srcBegin
	 *            start copying at this offset.
	 * @param srcEnd
	 *            stop copying at this offset.
	 * @param dst
	 *            the array to copy the data into.
	 * @param dstBegin
	 *            offset into <code>dst</code>.
	 * @throws NullPointerException
	 *             if <code>dst</code> is <code>null</code>.
	 * @throws IndexOutOfBoundsException
	 *             if any of the following is true:
	 *             <ul>
	 *             <li><code>srcBegin</code> is negative <li><code>dstBegin</code> is negative <li>the <code>srcBegin
	 *             </code> argument is greater than the <code>srcEnd</code> argument. <li><code>srcEnd</code> is greater
	 *             than <code>this.length()</code>. <li><code>dstBegin+srcEnd-srcBegin</code> is greater than <code>
	 *             dst.length</code>
	 *             </ul>
	 */
	public void getChars(int srcBegin, int srcEnd, char dst[], int dstBegin) {
		if (srcBegin < 0)
			throw new StringIndexOutOfBoundsException(srcBegin);
		if ((srcEnd < 0) || (srcEnd > count))
			throw new StringIndexOutOfBoundsException(srcEnd);
		if (srcBegin > srcEnd)
			throw new StringIndexOutOfBoundsException("srcBegin > srcEnd");
		System.arraycopy(value, srcBegin, dst, dstBegin, srcEnd - srcBegin);
	}

	/**
	 * Characters are copied from this sequence to the given {@link CharBuffer}.
	 */
	public void getChars(CharBuffer charBuffer) {
		if (count > charBuffer.remaining()) {
			throw new BufferOverflowException();
		}
		charBuffer.put(value, 0, count);
	}
	
	/**
	 * The character at the specified index is set to <code>ch</code>. This sequence is altered to represent a new
	 * character sequence that is identical to the old character sequence, except that it contains the character
	 * <code>ch</code> at position <code>index</code>.
	 * <p>
	 * The index argument must be greater than or equal to <code>0</code>, and less than the length of this sequence.
	 * 
	 * @param index
	 *            the index of the character to modify.
	 * @param ch
	 *            the new character.
	 * @throws IndexOutOfBoundsException
	 *             if <code>index</code> is negative or greater than or equal to <code>length()</code>.
	 */
	public void setCharAt(int index, char ch) {
		if ((index < 0) || (index >= count))
			throw new StringIndexOutOfBoundsException(index);
		value[index] = ch;
	}

	/**
	 * Appends the string representation of the <code>Object</code> argument.
	 * <p>
	 * The argument is converted to a string as if by the method <code>String.valueOf</code>, and the characters of that
	 * string are then appended to this sequence.
	 * 
	 * @param obj
	 *            an <code>Object</code>.
	 * @return a reference to this object.
	 */
	public CloverString append(Object obj) {
		return append(String.valueOf(obj));
	}

	/**
	 * Appends the specified string to this character sequence.
	 * <p>
	 * The characters of the <code>String</code> argument are appended, in order, increasing the length of this sequence
	 * by the length of the argument. If <code>str</code> is <code>null</code>, then the four characters
	 * <code>"null"</code> are appended.
	 * <p>
	 * Let <i>n</i> be the length of this character sequence just prior to execution of the <code>append</code> method.
	 * Then the character at index <i>k</i> in the new character sequence is equal to the character at index <i>k</i> in
	 * the old character sequence, if <i>k</i> is less than <i>n</i>; otherwise, it is equal to the character at index
	 * <i>k-n</i> in the argument <code>str</code>.
	 * 
	 * @param str
	 *            a string.
	 * @return a reference to this object.
	 */
	public CloverString append(String str) {
		if (str == null)
			str = "null";
		int len = str.length();
		if (len == 0)
			return this;
		int newCount = count + len;
		if (newCount > value.length)
			expandCapacity(newCount);
		str.getChars(0, len, value, count);
		count = newCount;
		return this;
	}

	// Documentation in subclasses because of synchro difference
	public CloverString append(StringBuffer sb) {
		if (sb == null)
			return append("null");
		int len = sb.length();
		int newCount = count + len;
		if (newCount > value.length)
			expandCapacity(newCount);
		sb.getChars(0, len, value, count);
		count = newCount;
		return this;
	}

	// Appends the specified string builder to this sequence.
	public CloverString append(StringBuilder sb) {
		if (sb == null)
			return append("null");
		int len = sb.length();
		int newcount = count + len;
		if (newcount > value.length)
			expandCapacity(newcount);
		sb.getChars(0, len, value, count);
		count = newcount;
		return this;
	}

	// Appends the specified string builder to this sequence.
	public CloverString append(CloverString sb) {
		if (sb == null)
			return append("null");
		int len = sb.length();
		int newcount = count + len;
		if (newcount > value.length)
			expandCapacity(newcount);
		sb.getChars(0, len, value, count);
		count = newcount;
		return this;
	}
	
	// Appends the specified char buffer to this sequence.
	public CloverString append(CharBuffer charBuffer, int length) {
		if (charBuffer == null) {
			return append("null");
		}
		int len = Math.min(length, charBuffer.remaining());
		int newcount = count + len;
		if (newcount > value.length) {
			expandCapacity(newcount);
		}
		charBuffer.get(value, count, len);
		count = newcount;
		return this;
	}

	// Documentation in subclasses because of synchro difference
	@Override
	public CloverString append(CharSequence s) {
		if (s == null)
			s = "null";
		if (s instanceof String)
			return this.append((String) s);
		if (s instanceof StringBuffer)
			return this.append((StringBuffer) s);
        if (s instanceof StringBuilder)
            return this.append((StringBuilder) s);
        if (s instanceof CloverString)
            return this.append((CloverString) s);
		return this.append(s, 0, s.length());
	}

	/**
	 * Appends a subsequence of the specified <code>CharSequence</code> to this sequence.
	 * <p>
	 * Characters of the argument <code>s</code>, starting at index <code>start</code>, are appended, in order, to the
	 * contents of this sequence up to the (exclusive) index <code>end</code>. The length of this sequence is increased
	 * by the value of <code>end - start</code>.
	 * <p>
	 * Let <i>n</i> be the length of this character sequence just prior to execution of the <code>append</code> method.
	 * Then the character at index <i>k</i> in this character sequence becomes equal to the character at index <i>k</i>
	 * in this sequence, if <i>k</i> is less than <i>n</i>; otherwise, it is equal to the character at index
	 * <i>k+start-n</i> in the argument <code>s</code>.
	 * <p>
	 * If <code>s</code> is <code>null</code>, then this method appends characters as if the s parameter was a sequence
	 * containing the four characters <code>"null"</code>.
	 * 
	 * @param s
	 *            the sequence to append.
	 * @param start
	 *            the starting index of the subsequence to be appended.
	 * @param end
	 *            the end index of the subsequence to be appended.
	 * @return a reference to this object.
	 * @throws IndexOutOfBoundsException
	 *             if <code>start</code> or <code>end</code> are negative, or <code>start</code> is greater than
	 *             <code>end</code> or <code>end</code> is greater than <code>s.length()</code>
	 */
	@Override
	public CloverString append(CharSequence s, int start, int end) {
		if (s == null)
			s = "null";
		if ((start < 0) || (end < 0) || (start > end) || (end > s.length()))
			throw new IndexOutOfBoundsException("start " + start + ", end " + end + ", s.length() " + s.length());
		int len = end - start;
		if (len == 0)
			return this;
		int newCount = count + len;
		if (newCount > value.length)
			expandCapacity(newCount);
		for (int i = start; i < end; i++)
			value[count++] = s.charAt(i);
		count = newCount;
		return this;
	}

	/**
	 * Appends the string representation of the <code>char</code> array argument to this sequence.
	 * <p>
	 * The characters of the array argument are appended, in order, to the contents of this sequence. The length of this
	 * sequence increases by the length of the argument.
	 * <p>
	 * The overall effect is exactly as if the argument were converted to a string by the method
	 * {@link String#valueOf(char[])} and the characters of that string were then {@link #append(String) appended} to
	 * this character sequence.
	 * 
	 * @param str
	 *            the characters to be appended.
	 * @return a reference to this object.
	 */
	public CloverString append(char str[]) {
		int newCount = count + str.length;
		if (newCount > value.length)
			expandCapacity(newCount);
		System.arraycopy(str, 0, value, count, str.length);
		count = newCount;
		return this;
	}

	/**
	 * Appends the string representation of a subarray of the <code>char</code> array argument to this sequence.
	 * <p>
	 * Characters of the <code>char</code> array <code>str</code>, starting at index <code>offset</code>, are appended,
	 * in order, to the contents of this sequence. The length of this sequence increases by the value of
	 * <code>len</code>.
	 * <p>
	 * The overall effect is exactly as if the arguments were converted to a string by the method
	 * {@link String#valueOf(char[],int,int)} and the characters of that string were then {@link #append(String)
	 * appended} to this character sequence.
	 * 
	 * @param str
	 *            the characters to be appended.
	 * @param offset
	 *            the index of the first <code>char</code> to append.
	 * @param len
	 *            the number of <code>char</code>s to append.
	 * @return a reference to this object.
	 */
	public CloverString append(char str[], int offset, int len) {
		int newCount = count + len;
		if (newCount > value.length)
			expandCapacity(newCount);
		System.arraycopy(str, offset, value, count, len);
		count = newCount;
		return this;
	}

	/**
	 * Appends the string representation of the <code>boolean</code> argument to the sequence.
	 * <p>
	 * The argument is converted to a string as if by the method <code>String.valueOf</code>, and the characters of that
	 * string are then appended to this sequence.
	 * 
	 * @param b
	 *            a <code>boolean</code>.
	 * @return a reference to this object.
	 */
	public CloverString append(boolean b) {
		if (b) {
			int newCount = count + 4;
			if (newCount > value.length)
				expandCapacity(newCount);
			value[count++] = 't';
			value[count++] = 'r';
			value[count++] = 'u';
			value[count++] = 'e';
		} else {
			int newCount = count + 5;
			if (newCount > value.length)
				expandCapacity(newCount);
			value[count++] = 'f';
			value[count++] = 'a';
			value[count++] = 'l';
			value[count++] = 's';
			value[count++] = 'e';
		}
		return this;
	}

	/**
	 * Appends the string representation of the <code>char</code> argument to this sequence.
	 * <p>
	 * The argument is appended to the contents of this sequence. The length of this sequence increases by
	 * <code>1</code>.
	 * <p>
	 * The overall effect is exactly as if the argument were converted to a string by the method
	 * {@link String#valueOf(char)} and the character in that string were then {@link #append(String) appended} to this
	 * character sequence.
	 * 
	 * @param c
	 *            a <code>char</code>.
	 * @return a reference to this object.
	 */
	@Override
	public CloverString append(char c) {
		int newCount = count + 1;
		if (newCount > value.length)
			expandCapacity(newCount);
		value[count++] = c;
		return this;
	}

	/**
	 * Appends the string representation of the <code>int</code> argument to this sequence.
	 * <p>
	 * The argument is converted to a string as if by the method <code>String.valueOf</code>, and the characters of that
	 * string are then appended to this sequence.
	 * 
	 * @param i
	 *            an <code>int</code>.
	 * @return a reference to this object.
	 */
	public CloverString append(int i) {
		return append(Integer.toString(i));
	}

	final static int[] sizeTable = { 9, 99, 999, 9999, 99999, 999999, 9999999, 99999999, 999999999, Integer.MAX_VALUE };

	// Requires positive x
	static int stringSizeOfInt(int x) {
		for (int i = 0;; i++)
			if (x <= sizeTable[i])
				return i + 1;
	}

	/**
	 * Appends the string representation of the <code>long</code> argument to this sequence.
	 * <p>
	 * The argument is converted to a string as if by the method <code>String.valueOf</code>, and the characters of that
	 * string are then appended to this sequence.
	 * 
	 * @param l
	 *            a <code>long</code>.
	 * @return a reference to this object.
	 */
	public CloverString append(long l) {
		return append(Long.toString(l));
	}

	// Requires positive x
	static int stringSizeOfLong(long x) {
		long p = 10;
		for (int i = 1; i < 19; i++) {
			if (x < p)
				return i;
			p = 10 * p;
		}
		return 19;
	}

	/**
	 * Appends the string representation of the <code>float</code> argument to this sequence.
	 * <p>
	 * The argument is converted to a string as if by the method <code>String.valueOf</code>, and the characters of that
	 * string are then appended to this string sequence.
	 * 
	 * @param f
	 *            a <code>float</code>.
	 * @return a reference to this object.
	 */
	public CloverString append(float f) {
		//this code would be faster, but we have to avoid import of sun.misc.FloatingDecimal
		//new FloatingDecimal(f).appendTo(this);
		return append(String.valueOf(f));
	}

	/**
	 * Appends the string representation of the <code>double</code> argument to this sequence.
	 * <p>
	 * The argument is converted to a string as if by the method <code>String.valueOf</code>, and the characters of that
	 * string are then appended to this sequence.
	 * 
	 * @param d
	 *            a <code>double</code>.
	 * @return a reference to this object.
	 */
	public CloverString append(double d) {
		//this code would be faster, but we have to avoid import of sun.misc.FloatingDecimal
		//new FloatingDecimal(d).appendTo(this);
		return append(String.valueOf(d));
	}

	/**
	 * Removes the characters in a substring of this sequence. The substring begins at the specified <code>start</code>
	 * and extends to the character at index <code>end - 1</code> or to the end of the sequence if no such character
	 * exists. If <code>start</code> is equal to <code>end</code>, no changes are made.
	 * 
	 * @param start
	 *            The beginning index, inclusive.
	 * @param end
	 *            The ending index, exclusive.
	 * @return This object.
	 * @throws StringIndexOutOfBoundsException
	 *             if <code>start</code> is negative, greater than <code>length()</code>, or greater than
	 *             <code>end</code>.
	 */
	public CloverString delete(int start, int end) {
		if (start < 0)
			throw new StringIndexOutOfBoundsException(start);
		if (end > count)
			end = count;
		if (start > end)
			throw new StringIndexOutOfBoundsException();
		int len = end - start;
		if (len > 0) {
			System.arraycopy(value, start + len, value, start, count - end);
			count -= len;
		}
		return this;
	}

	/**
	 * Removes the <code>char</code> at the specified position in this sequence. This sequence is shortened by one
	 * <code>char</code>.
	 * 
	 * <p>
	 * Note: If the character at the given index is a supplementary character, this method does not remove the entire
	 * character. If correct handling of supplementary characters is required, determine the number of <code>char</code>
	 * s to remove by calling <code>Character.charCount(thisSequence.codePointAt(index))</code>, where
	 * <code>thisSequence</code> is this sequence.
	 * 
	 * @param index
	 *            Index of <code>char</code> to remove
	 * @return This object.
	 * @throws StringIndexOutOfBoundsException
	 *             if the <code>index</code> is negative or greater than or equal to <code>length()</code>.
	 */
	public CloverString deleteCharAt(int index) {
		if ((index < 0) || (index >= count))
			throw new StringIndexOutOfBoundsException(index);
		System.arraycopy(value, index + 1, value, index, count - index - 1);
		count--;
		return this;
	}

	/**
	 * Replaces the characters in a substring of this sequence with characters in the specified <code>String</code>. The
	 * substring begins at the specified <code>start</code> and extends to the character at index <code>end - 1</code>
	 * or to the end of the sequence if no such character exists. First the characters in the substring are removed and
	 * then the specified <code>String</code> is inserted at <code>start</code>. (This sequence will be lengthened to
	 * accommodate the specified String if necessary.)
	 * 
	 * @param start
	 *            The beginning index, inclusive.
	 * @param end
	 *            The ending index, exclusive.
	 * @param str
	 *            String that will replace previous contents.
	 * @return This object.
	 * @throws StringIndexOutOfBoundsException
	 *             if <code>start</code> is negative, greater than <code>length()</code>, or greater than
	 *             <code>end</code>.
	 */
	public CloverString replace(int start, int end, String str) {
		if (start < 0)
			throw new StringIndexOutOfBoundsException(start);
		if (start > count)
			throw new StringIndexOutOfBoundsException("start > length()");
		if (start > end)
			throw new StringIndexOutOfBoundsException("start > end");

		if (end > count)
			end = count;
		int len = str.length();
		int newCount = count + len - (end - start);
		if (newCount > value.length)
			expandCapacity(newCount);

		System.arraycopy(value, end, value, start + len, count - end);
		str.getChars(0, len, value, start);
		count = newCount;
		return this;
	}

	/**
	 * Returns a new <code>String</code> that contains a subsequence of characters currently contained in this character
	 * sequence. The substring begins at the specified index and extends to the end of this sequence.
	 * 
	 * @param start
	 *            The beginning index, inclusive.
	 * @return The new string.
	 * @throws StringIndexOutOfBoundsException
	 *             if <code>start</code> is less than zero, or greater than the length of this object.
	 */
	public String substring(int start) {
		return substring(start, count);
	}

	/**
	 * Returns a new character sequence that is a subsequence of this sequence.
	 * 
	 * <p>
	 * An invocation of this method of the form
	 * 
	 * <blockquote>
	 * 
	 * <pre>
	 * sb.subSequence(begin, end)
	 * </pre>
	 * 
	 * </blockquote>
	 * 
	 * behaves in exactly the same way as the invocation
	 * 
	 * <blockquote>
	 * 
	 * <pre>
	 * sb.substring(begin, end)
	 * </pre>
	 * 
	 * </blockquote>
	 * 
	 * This method is provided so that this class can implement the {@link CharSequence} interface.
	 * </p>
	 * 
	 * @param start
	 *            the start index, inclusive.
	 * @param end
	 *            the end index, exclusive.
	 * @return the specified subsequence.
	 * 
	 * @throws IndexOutOfBoundsException
	 *             if <tt>start</tt> or <tt>end</tt> are negative, if <tt>end</tt> is greater than <tt>length()</tt>, or
	 *             if <tt>start</tt> is greater than <tt>end</tt>
	 * @spec JSR-51
	 */
	@Override
	public CharSequence subSequence(int start, int end) {
		return substring(start, end);
	}

	/**
	 * Returns a new <code>String</code> that contains a subsequence of characters currently contained in this sequence.
	 * The substring begins at the specified <code>start</code> and extends to the character at index
	 * <code>end - 1</code>.
	 * 
	 * @param start
	 *            The beginning index, inclusive.
	 * @param end
	 *            The ending index, exclusive.
	 * @return The new string.
	 * @throws StringIndexOutOfBoundsException
	 *             if <code>start</code> or <code>end</code> are negative or greater than <code>length()</code>, or
	 *             <code>start</code> is greater than <code>end</code>.
	 */
	public String substring(int start, int end) {
		if (start < 0)
			throw new StringIndexOutOfBoundsException(start);
		if (end > count)
			throw new StringIndexOutOfBoundsException(end);
		if (start > end)
			throw new StringIndexOutOfBoundsException(end - start);
		return new String(value, start, end - start);
	}

	/**
	 * Inserts the string representation of a subarray of the <code>str</code> array argument into this sequence. The
	 * subarray begins at the specified <code>offset</code> and extends <code>len</code> <code>char</code>s. The
	 * characters of the subarray are inserted into this sequence at the position indicated by <code>index</code>. The
	 * length of this sequence increases by <code>len</code> <code>char</code>s.
	 * 
	 * @param index
	 *            position at which to insert subarray.
	 * @param str
	 *            A <code>char</code> array.
	 * @param offset
	 *            the index of the first <code>char</code> in subarray to be inserted.
	 * @param len
	 *            the number of <code>char</code>s in the subarray to be inserted.
	 * @return This object
	 * @throws StringIndexOutOfBoundsException
	 *             if <code>index</code> is negative or greater than <code>length()</code>, or <code>offset</code> or
	 *             <code>len</code> are negative, or <code>(offset+len)</code> is greater than <code>str.length</code>.
	 */
	public CloverString insert(int index, char str[], int offset, int len) {
		if ((index < 0) || (index > length()))
			throw new StringIndexOutOfBoundsException(index);
		if ((offset < 0) || (len < 0) || (offset > str.length - len))
			throw new StringIndexOutOfBoundsException("offset " + offset + ", len " + len + ", str.length " + str.length);
		int newCount = count + len;
		if (newCount > value.length)
			expandCapacity(newCount);
		System.arraycopy(value, index, value, index + len, count - index);
		System.arraycopy(str, offset, value, index, len);
		count = newCount;
		return this;
	}

	/**
	 * Inserts the string representation of the <code>Object</code> argument into this character sequence.
	 * <p>
	 * The second argument is converted to a string as if by the method <code>String.valueOf</code>, and the characters
	 * of that string are then inserted into this sequence at the indicated offset.
	 * <p>
	 * The offset argument must be greater than or equal to <code>0</code>, and less than or equal to the length of this
	 * sequence.
	 * 
	 * @param offset
	 *            the offset.
	 * @param obj
	 *            an <code>Object</code>.
	 * @return a reference to this object.
	 * @throws StringIndexOutOfBoundsException
	 *             if the offset is invalid.
	 */
	public CloverString insert(int offset, Object obj) {
		return insert(offset, String.valueOf(obj));
	}

	/**
	 * Inserts the string into this character sequence.
	 * <p>
	 * The characters of the <code>String</code> argument are inserted, in order, into this sequence at the indicated
	 * offset, moving up any characters originally above that position and increasing the length of this sequence by the
	 * length of the argument. If <code>str</code> is <code>null</code>, then the four characters <code>"null"</code>
	 * are inserted into this sequence.
	 * <p>
	 * The character at index <i>k</i> in the new character sequence is equal to:
	 * <ul>
	 * <li>the character at index <i>k</i> in the old character sequence, if <i>k</i> is less than <code>offset</code>
	 * <li>the character at index <i>k</i><code>-offset</code> in the argument <code>str</code>, if <i>k</i> is not less
	 * than <code>offset</code> but is less than <code>offset+str.length()</code>
	 * <li>the character at index <i>k</i><code>-str.length()</code> in the old character sequence, if <i>k</i> is not
	 * less than <code>offset+str.length()</code>
	 * </ul>
	 * <p>
	 * The offset argument must be greater than or equal to <code>0</code>, and less than or equal to the length of this
	 * sequence.
	 * 
	 * @param offset
	 *            the offset.
	 * @param str
	 *            a string.
	 * @return a reference to this object.
	 * @throws StringIndexOutOfBoundsException
	 *             if the offset is invalid.
	 */
	public CloverString insert(int offset, String str) {
		if ((offset < 0) || (offset > length()))
			throw new StringIndexOutOfBoundsException(offset);
		if (str == null)
			str = "null";
		int len = str.length();
		int newCount = count + len;
		if (newCount > value.length)
			expandCapacity(newCount);
		System.arraycopy(value, offset, value, offset + len, count - offset);
		str.getChars(0, len, value, offset);
		count = newCount;
		return this;
	}

	/**
	 * Inserts the string representation of the <code>char</code> array argument into this sequence.
	 * <p>
	 * The characters of the array argument are inserted into the contents of this sequence at the position indicated by
	 * <code>offset</code>. The length of this sequence increases by the length of the argument.
	 * <p>
	 * The overall effect is exactly as if the argument were converted to a string by the method
	 * {@link String#valueOf(char[])} and the characters of that string were then {@link #insert(int,String) inserted}
	 * into this character sequence at the position indicated by <code>offset</code>.
	 * 
	 * @param offset
	 *            the offset.
	 * @param str
	 *            a character array.
	 * @return a reference to this object.
	 * @throws StringIndexOutOfBoundsException
	 *             if the offset is invalid.
	 */
	public CloverString insert(int offset, char str[]) {
		if ((offset < 0) || (offset > length()))
			throw new StringIndexOutOfBoundsException(offset);
		int len = str.length;
		int newCount = count + len;
		if (newCount > value.length)
			expandCapacity(newCount);
		System.arraycopy(value, offset, value, offset + len, count - offset);
		System.arraycopy(str, 0, value, offset, len);
		count = newCount;
		return this;
	}

	/**
	 * Inserts the specified <code>CharSequence</code> into this sequence.
	 * <p>
	 * The characters of the <code>CharSequence</code> argument are inserted, in order, into this sequence at the
	 * indicated offset, moving up any characters originally above that position and increasing the length of this
	 * sequence by the length of the argument s.
	 * <p>
	 * The result of this method is exactly the same as if it were an invocation of this object's insert(dstOffset, s,
	 * 0, s.length()) method.
	 * 
	 * <p>
	 * If <code>s</code> is <code>null</code>, then the four characters <code>"null"</code> are inserted into this
	 * sequence.
	 * 
	 * @param dstOffset
	 *            the offset.
	 * @param s
	 *            the sequence to be inserted
	 * @return a reference to this object.
	 * @throws IndexOutOfBoundsException
	 *             if the offset is invalid.
	 */
	public CloverString insert(int dstOffset, CharSequence s) {
		if (s == null)
			s = "null";
		if (s instanceof String)
			return this.insert(dstOffset, (String) s);
		return this.insert(dstOffset, s, 0, s.length());
	}

	/**
	 * Inserts a subsequence of the specified <code>CharSequence</code> into this sequence.
	 * <p>
	 * The subsequence of the argument <code>s</code> specified by <code>start</code> and <code>end</code> are inserted,
	 * in order, into this sequence at the specified destination offset, moving up any characters originally above that
	 * position. The length of this sequence is increased by <code>end - start</code>.
	 * <p>
	 * The character at index <i>k</i> in this sequence becomes equal to:
	 * <ul>
	 * <li>the character at index <i>k</i> in this sequence, if <i>k</i> is less than <code>dstOffset</code>
	 * <li>the character at index <i>k</i><code>+start-dstOffset</code> in the argument <code>s</code>, if <i>k</i> is
	 * greater than or equal to <code>dstOffset</code> but is less than <code>dstOffset+end-start</code>
	 * <li>the character at index <i>k</i><code>-(end-start)</code> in this sequence, if <i>k</i> is greater than or
	 * equal to <code>dstOffset+end-start</code>
	 * </ul>
	 * <p>
	 * The dstOffset argument must be greater than or equal to <code>0</code>, and less than or equal to the length of
	 * this sequence.
	 * <p>
	 * The start argument must be nonnegative, and not greater than <code>end</code>.
	 * <p>
	 * The end argument must be greater than or equal to <code>start</code>, and less than or equal to the length of s.
	 * 
	 * <p>
	 * If <code>s</code> is <code>null</code>, then this method inserts characters as if the s parameter was a sequence
	 * containing the four characters <code>"null"</code>.
	 * 
	 * @param dstOffset
	 *            the offset in this sequence.
	 * @param s
	 *            the sequence to be inserted.
	 * @param start
	 *            the starting index of the subsequence to be inserted.
	 * @param end
	 *            the end index of the subsequence to be inserted.
	 * @return a reference to this object.
	 * @throws IndexOutOfBoundsException
	 *             if <code>dstOffset</code> is negative or greater than <code>this.length()</code>, or
	 *             <code>start</code> or <code>end</code> are negative, or <code>start</code> is greater than
	 *             <code>end</code> or <code>end</code> is greater than <code>s.length()</code>
	 */
	public CloverString insert(int dstOffset, CharSequence s, int start, int end) {
		if (s == null)
			s = "null";
		if ((dstOffset < 0) || (dstOffset > this.length()))
			throw new IndexOutOfBoundsException("dstOffset " + dstOffset);
		if ((start < 0) || (end < 0) || (start > end) || (end > s.length()))
			throw new IndexOutOfBoundsException("start " + start + ", end " + end + ", s.length() " + s.length());
		int len = end - start;
		if (len == 0)
			return this;
		int newCount = count + len;
		if (newCount > value.length)
			expandCapacity(newCount);
		System.arraycopy(value, dstOffset, value, dstOffset + len, count - dstOffset);
		for (int i = start; i < end; i++)
			value[dstOffset++] = s.charAt(i);
		count = newCount;
		return this;
	}

	/**
	 * Inserts the string representation of the <code>boolean</code> argument into this sequence.
	 * <p>
	 * The second argument is converted to a string as if by the method <code>String.valueOf</code>, and the characters
	 * of that string are then inserted into this sequence at the indicated offset.
	 * <p>
	 * The offset argument must be greater than or equal to <code>0</code>, and less than or equal to the length of this
	 * sequence.
	 * 
	 * @param offset
	 *            the offset.
	 * @param b
	 *            a <code>boolean</code>.
	 * @return a reference to this object.
	 * @throws StringIndexOutOfBoundsException
	 *             if the offset is invalid.
	 */
	public CloverString insert(int offset, boolean b) {
		return insert(offset, String.valueOf(b));
	}

	/**
	 * Inserts the string representation of the <code>char</code> argument into this sequence.
	 * <p>
	 * The second argument is inserted into the contents of this sequence at the position indicated by
	 * <code>offset</code>. The length of this sequence increases by one.
	 * <p>
	 * The overall effect is exactly as if the argument were converted to a string by the method
	 * {@link String#valueOf(char)} and the character in that string were then {@link #insert(int, String) inserted}
	 * into this character sequence at the position indicated by <code>offset</code>.
	 * <p>
	 * The offset argument must be greater than or equal to <code>0</code>, and less than or equal to the length of this
	 * sequence.
	 * 
	 * @param offset
	 *            the offset.
	 * @param c
	 *            a <code>char</code>.
	 * @return a reference to this object.
	 * @throws IndexOutOfBoundsException
	 *             if the offset is invalid.
	 */
	public CloverString insert(int offset, char c) {
		int newCount = count + 1;
		if (newCount > value.length)
			expandCapacity(newCount);
		System.arraycopy(value, offset, value, offset + 1, count - offset);
		value[offset] = c;
		count = newCount;
		return this;
	}

	/**
	 * Inserts the string representation of the second <code>int</code> argument into this sequence.
	 * <p>
	 * The second argument is converted to a string as if by the method <code>String.valueOf</code>, and the characters
	 * of that string are then inserted into this sequence at the indicated offset.
	 * <p>
	 * The offset argument must be greater than or equal to <code>0</code>, and less than or equal to the length of this
	 * sequence.
	 * 
	 * @param offset
	 *            the offset.
	 * @param i
	 *            an <code>int</code>.
	 * @return a reference to this object.
	 * @throws StringIndexOutOfBoundsException
	 *             if the offset is invalid.
	 */
	public CloverString insert(int offset, int i) {
		return insert(offset, String.valueOf(i));
	}

	/**
	 * Inserts the string representation of the <code>long</code> argument into this sequence.
	 * <p>
	 * The second argument is converted to a string as if by the method <code>String.valueOf</code>, and the characters
	 * of that string are then inserted into this sequence at the position indicated by <code>offset</code>.
	 * <p>
	 * The offset argument must be greater than or equal to <code>0</code>, and less than or equal to the length of this
	 * sequence.
	 * 
	 * @param offset
	 *            the offset.
	 * @param l
	 *            a <code>long</code>.
	 * @return a reference to this object.
	 * @throws StringIndexOutOfBoundsException
	 *             if the offset is invalid.
	 */
	public CloverString insert(int offset, long l) {
		return insert(offset, String.valueOf(l));
	}

	/**
	 * Inserts the string representation of the <code>float</code> argument into this sequence.
	 * <p>
	 * The second argument is converted to a string as if by the method <code>String.valueOf</code>, and the characters
	 * of that string are then inserted into this sequence at the indicated offset.
	 * <p>
	 * The offset argument must be greater than or equal to <code>0</code>, and less than or equal to the length of this
	 * sequence.
	 * 
	 * @param offset
	 *            the offset.
	 * @param f
	 *            a <code>float</code>.
	 * @return a reference to this object.
	 * @throws StringIndexOutOfBoundsException
	 *             if the offset is invalid.
	 */
	public CloverString insert(int offset, float f) {
		return insert(offset, String.valueOf(f));
	}

	/**
	 * Inserts the string representation of the <code>double</code> argument into this sequence.
	 * <p>
	 * The second argument is converted to a string as if by the method <code>String.valueOf</code>, and the characters
	 * of that string are then inserted into this sequence at the indicated offset.
	 * <p>
	 * The offset argument must be greater than or equal to <code>0</code>, and less than or equal to the length of this
	 * sequence.
	 * 
	 * @param offset
	 *            the offset.
	 * @param d
	 *            a <code>double</code>.
	 * @return a reference to this object.
	 * @throws StringIndexOutOfBoundsException
	 *             if the offset is invalid.
	 */
	public CloverString insert(int offset, double d) {
		return insert(offset, String.valueOf(d));
	}

	/**
	 * Returns the index within this string of the first occurrence of the specified substring. The integer returned is
	 * the smallest value <i>k</i> such that: <blockquote>
	 * 
	 * <pre>
	 * this.toString().startsWith(str, <i>k</i>)
	 * </pre>
	 * 
	 * </blockquote> is <code>true</code>.
	 * 
	 * @param str
	 *            any string.
	 * @return if the string argument occurs as a substring within this object, then the index of the first character of
	 *         the first such substring is returned; if it does not occur as a substring, <code>-1</code> is returned.
	 * @throws java.lang.NullPointerException
	 *             if <code>str</code> is <code>null</code>.
	 */
	public int indexOf(String str) {
		return indexOf(str, 0);
	}

	/**
	 * Returns the index within this string of the first occurrence of the specified substring, starting at the
	 * specified index. The integer returned is the smallest value <tt>k</tt> for which: <blockquote>
	 * 
	 * <pre>
	 * k &gt;= Math.min(fromIndex, str.length()) &amp;&amp; this.toString().startsWith(str, k)
	 * </pre>
	 * 
	 * </blockquote> If no such value of <i>k</i> exists, then -1 is returned.
	 * 
	 * @param str
	 *            the substring for which to search.
	 * @param fromIndex
	 *            the index from which to start the search.
	 * @return the index within this string of the first occurrence of the specified substring, starting at the
	 *         specified index.
	 * @throws java.lang.NullPointerException
	 *             if <code>str</code> is <code>null</code>.
	 */
	public int indexOf(String str, int fromIndex) {
		return indexOf(value, 0, count, str.toCharArray(), 0, str.length(), fromIndex);
	}

	/**
	 * Returns the index within this string of the rightmost occurrence of the specified substring. The rightmost empty
	 * string "" is considered to occur at the index value <code>this.length()</code>. The returned index is the largest
	 * value <i>k</i> such that <blockquote>
	 * 
	 * <pre>
	 * this.toString().startsWith(str, k)
	 * </pre>
	 * 
	 * </blockquote> is true.
	 * 
	 * @param str
	 *            the substring to search for.
	 * @return if the string argument occurs one or more times as a substring within this object, then the index of the
	 *         first character of the last such substring is returned. If it does not occur as a substring,
	 *         <code>-1</code> is returned.
	 * @throws java.lang.NullPointerException
	 *             if <code>str</code> is <code>null</code>.
	 */
	public int lastIndexOf(String str) {
		return lastIndexOf(str, count);
	}

	/**
	 * Returns the index within this string of the last occurrence of the specified substring. The integer returned is
	 * the largest value <i>k</i> such that: <blockquote>
	 * 
	 * <pre>
	 * k &lt;= Math.min(fromIndex, str.length()) &amp;&amp; this.toString().startsWith(str, k)
	 * </pre>
	 * 
	 * </blockquote> If no such value of <i>k</i> exists, then -1 is returned.
	 * 
	 * @param str
	 *            the substring to search for.
	 * @param fromIndex
	 *            the index to start the search from.
	 * @return the index within this sequence of the last occurrence of the specified substring.
	 * @throws java.lang.NullPointerException
	 *             if <code>str</code> is <code>null</code>.
	 */
	public int lastIndexOf(String str, int fromIndex) {
		return lastIndexOf(value, 0, count, str.toCharArray(), 0, str.length(), fromIndex);
	}

	/**
	 * Causes this character sequence to be replaced by the reverse of the sequence. If there are any surrogate pairs
	 * included in the sequence, these are treated as single characters for the reverse operation. Thus, the order of
	 * the high-low surrogates is never reversed.
	 * 
	 * Let <i>n</i> be the character length of this character sequence (not the length in <code>char</code> values) just
	 * prior to execution of the <code>reverse</code> method. Then the character at index <i>k</i> in the new character
	 * sequence is equal to the character at index <i>n-k-1</i> in the old character sequence.
	 * 
	 * <p>
	 * Note that the reverse operation may result in producing surrogate pairs that were unpaired low-surrogates and
	 * high-surrogates before the operation. For example, reversing "&#92;uDC00&#92;uD800" produces
	 * "&#92;uD800&#92;uDC00" which is a valid surrogate pair.
	 * 
	 * @return a reference to this object.
	 */
	public CloverString reverse() {
		boolean hasSurrogate = false;
		int n = count - 1;
		for (int j = (n - 1) >> 1; j >= 0; --j) {
			char temp = value[j];
			char temp2 = value[n - j];
			if (!hasSurrogate) {
				hasSurrogate = (temp >= Character.MIN_SURROGATE && temp <= Character.MAX_SURROGATE) || (temp2 >= Character.MIN_SURROGATE && temp2 <= Character.MAX_SURROGATE);
			}
			value[j] = temp2;
			value[n - j] = temp;
		}
		if (hasSurrogate) {
			// Reverse back all valid surrogate pairs
			for (int i = 0; i < count - 1; i++) {
				char c2 = value[i];
				if (Character.isLowSurrogate(c2)) {
					char c1 = value[i + 1];
					if (Character.isHighSurrogate(c1)) {
						value[i++] = c1;
						value[i] = c2;
					}
				}
			}
		}
		return this;
	}

	/**
	 * Returns a string representing the data in this sequence. A new <code>String</code> object is allocated and
	 * initialized to contain the character sequence currently represented by this object. This <code>String</code> is
	 * then returned. Subsequent changes to this sequence do not affect the contents of the <code>String</code>.
	 * 
	 * @return a string representation of this sequence of characters.
	 */
	@Override
	public String toString() {
		return new String(value, 0, count);
	}

    /**
     * Save the state of the <tt>StringBuilder</tt> instance to a stream 
     * (that is, serialize it).
     *
     * @serialData the number of characters currently stored in the string
     *             builder (<tt>int</tt>), followed by the characters in the
     *             string builder (<tt>char[]</tt>).   The length of the
     *             <tt>char</tt> array may be greater than the number of 
     *             characters currently stored in the string builder, in which
     *             case extra characters are ignored.
     */
    private void writeObject(java.io.ObjectOutputStream s)
        throws java.io.IOException {
        s.defaultWriteObject();
        s.writeInt(count);
        s.writeObject(value);
    }

    /**
     * readObject is called to restore the state of the StringBuffer from
     * a stream.
     */
    private void readObject(java.io.ObjectInputStream s)
        throws java.io.IOException, ClassNotFoundException {
        s.defaultReadObject();
        count = s.readInt();
        value = (char[]) s.readObject();
    }

	/**
	 * Code shared by String and StringBuffer to do searches. The source is the character array being searched, and the
	 * target is the string being searched for.
	 * 
	 * @param source
	 *            the characters being searched.
	 * @param sourceOffset
	 *            offset of the source string.
	 * @param sourceCount
	 *            count of the source string.
	 * @param target
	 *            the characters being searched for.
	 * @param targetOffset
	 *            offset of the target string.
	 * @param targetCount
	 *            count of the target string.
	 * @param fromIndex
	 *            the index to begin searching from.
	 */
	static int indexOf(char[] source, int sourceOffset, int sourceCount, char[] target, int targetOffset,
			int targetCount, int fromIndex) {
		if (fromIndex >= sourceCount) {
			return (targetCount == 0 ? sourceCount : -1);
		}
		if (fromIndex < 0) {
			fromIndex = 0;
		}
		if (targetCount == 0) {
			return fromIndex;
		}

		char first = target[targetOffset];
		int max = sourceOffset + (sourceCount - targetCount);

		for (int i = sourceOffset + fromIndex; i <= max; i++) {
			/* Look for first character. */
			if (source[i] != first) {
				while (++i <= max && source[i] != first)
					;
			}

			/* Found first character, now look at the rest of v2 */
			if (i <= max) {
				int j = i + 1;
				int end = j + targetCount - 1;
				for (int k = targetOffset + 1; j < end && source[j] == target[k]; j++, k++)
					;

				if (j == end) {
					/* Found whole string. */
					return i - sourceOffset;
				}
			}
		}
		return -1;
	}

	/**
	 * Code shared by String and StringBuffer to do searches. The source is the character array being searched, and the
	 * target is the string being searched for.
	 * 
	 * @param source
	 *            the characters being searched.
	 * @param sourceOffset
	 *            offset of the source string.
	 * @param sourceCount
	 *            count of the source string.
	 * @param target
	 *            the characters being searched for.
	 * @param targetOffset
	 *            offset of the target string.
	 * @param targetCount
	 *            count of the target string.
	 * @param fromIndex
	 *            the index to begin searching from.
	 */
	static int lastIndexOf(char[] source, int sourceOffset, int sourceCount, char[] target, int targetOffset,
			int targetCount, int fromIndex) {
		/*
		 * Check arguments; return immediately where possible. For consistency, don't check for null str.
		 */
		int rightIndex = sourceCount - targetCount;
		if (fromIndex < 0) {
			return -1;
		}
		if (fromIndex > rightIndex) {
			fromIndex = rightIndex;
		}
		/* Empty string always matches. */
		if (targetCount == 0) {
			return fromIndex;
		}

		int strLastIndex = targetOffset + targetCount - 1;
		char strLastChar = target[strLastIndex];
		int min = sourceOffset + targetCount - 1;
		int i = min + fromIndex;

		startSearchForLastChar: while (true) {
			while (i >= min && source[i] != strLastChar) {
				i--;
			}
			if (i < min) {
				return -1;
			}
			int j = i - 1;
			int start = j - (targetCount - 1);
			int k = strLastIndex - 1;

			while (j > start) {
				if (source[j--] != target[k--]) {
					i--;
					continue startSearchForLastChar;
				}
			}
			return start - sourceOffset + 1;
		}
	}

	@Override
	public int hashCode() {
		int h = 0;
		int len = count;
		int off = 0;
		char val[] = value;

		for (int i = 0; i < len; i++) {
			h = 31 * h + val[off++];
		}
		return h;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj instanceof CloverString) {
			CloverString anotherCloverString = (CloverString) obj;
			int n = count;
			if (n == anotherCloverString.count) {
				char v1[] = value;
				char v2[] = anotherCloverString.value;
				int i = 0;
				int j = 0;
				while (n-- != 0) {
					if (v1[i++] != v2[j++])
						return false;
				}
				return true;
			}
		}
		
		return false;
	}
	
}
