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
package org.jetel.util.bytes;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.CharBuffer;
import java.nio.DoubleBuffer;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.nio.ShortBuffer;

/**
 * This class is only implementation of {@link CloverBuffer}, at least for now.
 * It is tight wrapper around underlying {@link ByteBuffer}. Most of the operations
 * are simple delegated to the inner buffer.
 * This clover buffer ensures that no {@link BufferOverflowException} is thrown
 * while is something written to the underlying buffer. These exceptions are caught,
 * new byte buffer allocated, all data from old inner buffer are copied to the new one
 * and writing operation is repeated.
 *  
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 17 Oct 2011
 * 
 * @see CloverBuffer
 * @see org.apache.mina.core.buffer.AbstractIoBuffer This class was inspired by similar class from Apache MINA Project  
 */
public class DynamicCloverBuffer extends CloverBuffer {
	
    /** Tells if a buffer has been created from an existing buffer */
    private final boolean derived;

    /** A flag set to true if the buffer can extend automatically */
    private boolean autoExpand = true;

    /** A flag set to true if the buffer can shrink automatically */
    private boolean autoShrink;

    /** Tells if a buffer can be expanded */
    private boolean recapacityAllowed = true;

    /** The minimum number of bytes the CloverBuffer can hold */
    private int minimumCapacity;

    /** The maximum number of bytes the CloverBuffer can hold */
    private int maximumCapacity = Integer.MAX_VALUE;

    /**
     * We don't have any access to Buffer.markValue(), so we need to track it down,
     * which will cause small extra overhead.
     */
    private int mark = -1;

	/**
	 * Inner data container, which is actually wrapped.
	 * Can be substituted by new one if capacity has to be changed.
	 */
	private ByteBuffer buf;

    /**
     * Basic constructor. Simple growable wrapper of ByteBuffer is created.
     * @param buf
     */
    protected DynamicCloverBuffer(ByteBuffer buf) {
        this.buf = buf;
        this.recapacityAllowed = true;
        this.derived = false;
        this.minimumCapacity = buf.capacity();
        
    	memoryAllocated(capacity());
    }

    /**
     * Basic constructor. Simple growable wrapper of ByteBuffer is created.
     * @param buf
     * @param maximumCapacity the limit for inner buffer capacity, buffer expansion is limited
     */
    protected DynamicCloverBuffer(ByteBuffer buf, int maximumCapacity) {
        this.buf = buf;
        this.recapacityAllowed = true;
        this.derived = false;
        this.minimumCapacity = buf.capacity();
        this.maximumCapacity = maximumCapacity;

        if (this.minimumCapacity > this.maximumCapacity) {
        	throw new IllegalArgumentException("maximumCapacity cannot be smaller then minimumCapacity");
        }

    	memoryAllocated(capacity());
    }

    /**
     * Constructor for non-growing views to the given buffer
     * @param parent
     * @param buf
     */
    protected DynamicCloverBuffer(DynamicCloverBuffer parent, ByteBuffer buf) {
        this.buf = buf;
        this.recapacityAllowed = false;
        this.derived = true;
        this.minimumCapacity = parent.minimumCapacity;
        
    	memoryAllocated(capacity());
    }

	@Override
	public ByteBuffer buf() {
	    return buf;
	}
    
    /**
     * Sets the underlying NIO buffer instance.
     * 
     * @param newBuf The buffer to store within this CloverBuffer
     */
    private void buf(ByteBuffer buf) {
        this.buf = buf;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final boolean isDirect() {
        return buf.isDirect();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final boolean isReadOnly() {
        return buf.isReadOnly();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final int minimumCapacity() {
        return minimumCapacity;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final DynamicCloverBuffer minimumCapacity(int minimumCapacity) {
        if (minimumCapacity < 0) {
            throw new IllegalArgumentException("minimumCapacity: " + minimumCapacity);
        }
        this.minimumCapacity = minimumCapacity;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final int capacity() {
        return buf.capacity();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final CloverBuffer capacity(int newCapacity) {
        // Allocate a new buffer and transfer all settings to it.
        if (newCapacity > capacity()) {
            if (!recapacityAllowed) {
                throw new IllegalStateException("Resizing of this CloverBuffer is not allowed.");
            }
            // Expand:
            //// Save the state.
            int pos = position();
            int limit = limit();
            ByteOrder bo = order();

            //// Reallocate.
            ByteBuffer oldBuf = buf;
            ByteBuffer newBuf = reallocateByteBuffer(newCapacity, isDirect());
            oldBuf.clear();
            newBuf.put(oldBuf);
            buf(newBuf);
            deallocateByteBuffer(oldBuf);

            //// Restore the state.
            buf.limit(limit);
            if (mark >= 0) {
                buf.position(mark);
                buf.mark();
            }
            buf.position(pos);
            buf.order(bo);
        }

        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int maximumCapacity() {
    	return maximumCapacity;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public final boolean isAutoExpand() {
        return autoExpand && recapacityAllowed;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final boolean isAutoShrink() {
        return autoShrink && recapacityAllowed;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final boolean isDerived() {
        return derived;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final CloverBuffer setAutoExpand(boolean autoExpand) {
        if (!recapacityAllowed) {
            throw new IllegalStateException("Resizing of this CloverBuffer is not allowed.");
        }
        this.autoExpand = autoExpand;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final CloverBuffer setAutoShrink(boolean autoShrink) {
        if (!recapacityAllowed) {
            throw new IllegalStateException("Resizing of this CloverBuffer is not allowed.");
        }
        this.autoShrink = autoShrink;
        return this;
    }

    @Override
    public CloverBuffer setRecapacityAllowed(boolean recapacityAllowed) {
    	this.recapacityAllowed = recapacityAllowed;
    	return this;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public final CloverBuffer expand(int expectedRemaining) {
        expand(position(), expectedRemaining, false);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final CloverBuffer expand(int pos, int expectedRemaining) {
        expand(pos, expectedRemaining, false);
        return this;
    }

    private void expand(int pos, int expectedRemaining, boolean autoExpand) {
        int end = pos + expectedRemaining;
        int newCapacity;
        if (autoExpand) {
            newCapacity = normalizeCapacity(end);
            if (newCapacity > maximumCapacity) {
            	newCapacity = end;
            }
        } else {
            newCapacity = end;
        }
        if (newCapacity > capacity()) {
            // The buffer needs expansion.
            capacity(newCapacity);
        }

        if (newCapacity > limit()) {
            // We call limit() directly to prevent StackOverflowError
            buf.limit(newCapacity);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final CloverBuffer shrink() {

        if (!recapacityAllowed) {
            throw new IllegalStateException("Resizing of this CloverBuffer is not allowed.");
        }

        int position = position();
        int capacity = capacity();
        int limit = limit();
        if (capacity == limit) {
            return this;
        }

        int newCapacity = capacity;
        int minCapacity = Math.max(minimumCapacity, limit);
        for (;;) {
            if (newCapacity >>> 1 < minCapacity) {
                break;
            }
            newCapacity >>>= 1;
        }

        newCapacity = Math.max(minCapacity, newCapacity);

        if (newCapacity == capacity) {
            return this;
        }

        // Shrink and compact:
        //// Save the state.
        ByteOrder bo = order();

        //// Reallocate.
        ByteBuffer oldBuf = buf;
        ByteBuffer newBuf = reallocateByteBuffer(newCapacity, isDirect());
        oldBuf.position(0);
        oldBuf.limit(limit);
        newBuf.put(oldBuf);
        buf(newBuf);
        deallocateByteBuffer(oldBuf);

        //// Restore the state.
        buf.position(position);
        buf.limit(limit);
        buf.order(bo);
        mark = -1;

        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final int position() {
        return buf.position();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final CloverBuffer position(int newPosition) {
        try {
        	buf.position(newPosition);
        } catch (IllegalArgumentException e) {
            if (autoExpand(newPosition, 0)) {
            	buf.position(newPosition);
            } else {
            	throw e;
            }
        }
        if (mark > newPosition) {
            mark = -1;
        }
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final int limit() {
        return buf.limit();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final CloverBuffer limit(int newLimit) {
        try {
        	buf.limit(newLimit);
        } catch (IllegalArgumentException e) {
        	if (autoExpand(newLimit, 0)) {
            	buf.limit(newLimit);
        	} else {
        		throw e;
        	}
        }
        if (mark > newLimit) {
            mark = -1;
        }
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final CloverBuffer mark() {
        buf.mark();
        mark = position();
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final int markValue() {
        return mark;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final CloverBuffer reset() {
        buf.reset();
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final CloverBuffer clear() {
        buf.clear();
        mark = -1;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final CloverBuffer flip() {
        buf.flip();
        mark = -1;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final CloverBuffer rewind() {
        buf.rewind();
        mark = -1;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final int remaining() {
    	return buf.remaining();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final boolean hasRemaining() {
        return buf.hasRemaining();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final byte get() {
        return buf.get();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final CloverBuffer put(byte b) {
    	try {
    		buf.put(b);
    	} catch (BufferOverflowException e) {
            if (autoExpand(1)) {
            	buf.put(b);
            } else {
            	throw e;
            }
    	}
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final byte get(int index) {
        return buf.get(index);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final CloverBuffer put(int index, byte b) {
        try {
        	buf.put(index, b);
        } catch (IndexOutOfBoundsException e) {
        	if (autoExpand(index, 1)) {
            	buf.put(index, b);
        	} else {
        		throw e;
        	}
        }
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final CloverBuffer get(byte[] dst, int offset, int length) {
        buf.get(dst, offset, length);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final CloverBuffer put(ByteBuffer src) {
        try {
        	buf.put(src);
        } catch (BufferOverflowException e) {
            if (autoExpand(src.remaining())) {
            	buf.put(src);
            } else {
            	throw e;
            }
        }
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final CloverBuffer put(byte[] src, int offset, int length) {
    	try {
    		buf.put(src, offset, length);
    	} catch (BufferOverflowException e) {
            if (autoExpand(length)) {
        		buf.put(src, offset, length);
            } else {
            	throw e;
            }
    	}
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final CloverBuffer compact() {
        int remaining = remaining();
        int capacity = capacity();

        if (capacity == 0) {
            return this;
        }

        if (isAutoShrink() && remaining <= capacity >>> 2
                && capacity > minimumCapacity) {
            int newCapacity = capacity;
            int minCapacity = Math.max(minimumCapacity, remaining << 1);
            for (;;) {
                if (newCapacity >>> 1 < minCapacity) {
                    break;
                }
                newCapacity >>>= 1;
            }

            newCapacity = Math.max(minCapacity, newCapacity);

            if (newCapacity == capacity) {
                return this;
            }

            // Shrink and compact:
            //// Save the state.
            ByteOrder bo = order();

            //// Sanity check.
            if (remaining > newCapacity) {
                throw new IllegalStateException(
                        "The amount of the remaining bytes is greater than "
                                + "the new capacity.");
            }

            //// Reallocate.
            ByteBuffer oldBuf = buf;
            ByteBuffer newBuf = reallocateByteBuffer(newCapacity, isDirect());
            newBuf.put(oldBuf);
            buf(newBuf);
            deallocateByteBuffer(oldBuf);

            //// Restore the state.
            buf.order(bo);
        } else {
            buf.compact();
        }
        mark = -1;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final ByteOrder order() {
        return buf.order();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final CloverBuffer order(ByteOrder bo) {
        buf.order(bo);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final char getChar() {
        return buf.getChar();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final CloverBuffer putChar(char value) {
    	try {
    		buf.putChar(value);
    	} catch (BufferOverflowException e) {
            if (autoExpand(2)) {
        		buf.putChar(value);
            } else {
            	throw e;
            }
    	}
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final char getChar(int index) {
        return buf.getChar(index);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final CloverBuffer putChar(int index, char value) {
    	try {
    		buf.putChar(index, value);
    	} catch (IndexOutOfBoundsException e) {
    		if (autoExpand(index, 2)) {
        		buf.putChar(index, value);
    		} else {
    			throw e;
    		}
    	}
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final CharBuffer asCharBuffer() {
        return buf.asCharBuffer();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final short getShort() {
        return buf.getShort();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final CloverBuffer putShort(short value) {
    	try {
    		buf.putShort(value);
    	} catch (BufferOverflowException e) {
    		if (autoExpand(2)) {
        		buf.putShort(value);
    		} else {
    			throw e;
    		}
    	}
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final short getShort(int index) {
        return buf.getShort(index);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final CloverBuffer putShort(int index, short value) {
        try {
        	buf.putShort(index, value);
        } catch (IndexOutOfBoundsException e) {
        	if (autoExpand(index, 2)) {
            	buf.putShort(index, value);
        	} else {
        		throw e;
        	}
        }
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final ShortBuffer asShortBuffer() {
        return buf.asShortBuffer();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final int getInt() {
        return buf.getInt();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final CloverBuffer putInt(int value) {
        try {
        	buf.putInt(value);
        } catch (BufferOverflowException e) {
        	if (autoExpand(4)) {
            	buf.putInt(value);
        	} else {
        		throw e;
        	}
        }
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final int getInt(int index) {
        return buf.getInt(index);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final CloverBuffer putInt(int index, int value) {
        try {
        	buf.putInt(index, value);
        } catch (IndexOutOfBoundsException e) {
        	if (autoExpand(index, 4)) {
            	buf.putInt(index, value);
        	} else {
        		throw e;
        	}
        }
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final IntBuffer asIntBuffer() {
        return buf.asIntBuffer();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final long getLong() {
        return buf.getLong();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final CloverBuffer putLong(long value) {
        try {
        	buf.putLong(value);
        } catch (BufferOverflowException e) {
        	if (autoExpand(8)) {
            	buf.putLong(value);
        	} else {
        		throw e;
        	}
        }
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final long getLong(int index) {
        return buf.getLong(index);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final CloverBuffer putLong(int index, long value) {
        try {
        	buf.putLong(index, value);
        } catch (IndexOutOfBoundsException e) {
        	if (autoExpand(index, 8)) {
            	buf.putLong(index, value);
        	} else {
        		throw e;
        	}
        }
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final LongBuffer asLongBuffer() {
        return buf.asLongBuffer();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final float getFloat() {
        return buf.getFloat();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final CloverBuffer putFloat(float value) {
        try {
        	buf.putFloat(value);
        } catch (BufferOverflowException e) {
        	if (autoExpand(4)) {
            	buf.putFloat(value);
        	} else {
        		throw e;
        	}
        }
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final float getFloat(int index) {
        return buf.getFloat(index);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final CloverBuffer putFloat(int index, float value) {
        try {
        	buf.putFloat(index, value);
        } catch (IndexOutOfBoundsException e) {
        	if (autoExpand(index, 4)) {
            	buf.putFloat(index, value);
        	} else {
        		throw e;
        	}
        }
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final FloatBuffer asFloatBuffer() {
        return buf.asFloatBuffer();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final double getDouble() {
        return buf.getDouble();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final CloverBuffer putDouble(double value) {
        try {
        	buf.putDouble(value);
        } catch (BufferOverflowException e) {
        	if (autoExpand(8)) {
            	buf.putDouble(value);
        	} else {
        		throw e;
        	}
        }
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final double getDouble(int index) {
        return buf.getDouble(index);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final CloverBuffer putDouble(int index, double value) {
        try {
        	buf.putDouble(index, value);
        } catch (IndexOutOfBoundsException e) {
        	if (autoExpand(index, 8)) {
            	buf.putDouble(index, value);
        	} else {
        		throw e;
        	}
        }
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final DoubleBuffer asDoubleBuffer() {
        return buf.asDoubleBuffer();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final CloverBuffer asReadOnlyBuffer() {
        recapacityAllowed = false;
        return new DynamicCloverBuffer(this, this.buf.asReadOnlyBuffer());
    }

    @Override
    public boolean hasArray() {
        return buf.hasArray();
    }

    @Override
    public byte[] array() {
        return buf.array();
    }

    @Override
    public int arrayOffset() {
        return buf.arrayOffset();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final CloverBuffer duplicate() {
        recapacityAllowed = false;
        return new DynamicCloverBuffer(this, this.buf.duplicate());
    }

    /**
     * {@inheritDoc}
     * WARNING: the resulted slice is shallow copy as requested, but only until first expand of this buffer is performed
     * ISSUE: CL-2597 Recapacity of this CloverBuffer is not allowed.
     */
    @Override
    public final CloverBuffer slice() {
    	//recapacityAllowed = false; //see WARNING
        return new DynamicCloverBuffer(this, this.buf.slice());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        int h = 1;
        int p = position();
        for (int i = limit() - 1; i >= p; i--) {
            h = 31 * h + get(i);
        }
        return h;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object o) {
        if (!(o instanceof CloverBuffer)) {
            return false;
        }

        CloverBuffer that = (CloverBuffer) o;
        if (this.remaining() != that.remaining()) {
            return false;
        }

        int p = this.position();
        for (int i = this.limit() - 1, j = that.limit() - 1; i >= p; i--, j--) {
            byte v1 = this.get(i);
            byte v2 = that.get(j);
            if (v1 != v2) {
                return false;
            }
        }
        return true;
    }

    /**
     * {@inheritDoc}
     */
    public int compareTo(CloverBuffer that) {
        int n = this.position() + Math.min(this.remaining(), that.remaining());
        for (int i = this.position(), j = that.position(); i < n; i++, j++) {
            byte v1 = this.get(i);
            byte v2 = that.get(j);
            if (v1 == v2) {
                continue;
            }
            if (v1 < v2) {
                return -1;
            }

            return +1;
        }
        return this.remaining() - that.remaining();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        if (isDirect()) {
            buf.append("DirectBuffer");
        } else {
            buf.append("HeapBuffer");
        }
        buf.append("[pos=");
        buf.append(position());
        buf.append(" lim=");
        buf.append(limit());
        buf.append(" cap=");
        buf.append(capacity());
        buf.append(']');
        return buf.toString();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CloverBuffer get(byte[] dst) {
        return get(dst, 0, dst.length);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CloverBuffer put(CloverBuffer src) {
        return put(src.buf());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CloverBuffer put(byte[] src) {
        return put(src, 0, src.length);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public InputStream asInputStream() {
        return new InputStream() {
            @Override
            public int available() {
                return DynamicCloverBuffer.this.remaining();
            }

            @Override
            public synchronized void mark(int readlimit) {
            	DynamicCloverBuffer.this.mark();
            }

            @Override
            public boolean markSupported() {
                return true;
            }

            @Override
            public int read() {
                if (DynamicCloverBuffer.this.hasRemaining()) {
                    return DynamicCloverBuffer.this.get() & 0xff;
                }

                return -1;
            }

            @Override
            public int read(byte[] b, int off, int len) {
                int remaining = DynamicCloverBuffer.this.remaining();
                if (remaining > 0) {
                    int readBytes = Math.min(remaining, len);
                    DynamicCloverBuffer.this.get(b, off, readBytes);
                    return readBytes;
                }

                return -1;
            }

            @Override
            public synchronized void reset() {
            	DynamicCloverBuffer.this.reset();
            }

            @Override
            public long skip(long n) {
                int bytes;
                if (n > Integer.MAX_VALUE) {
                    bytes = DynamicCloverBuffer.this.remaining();
                } else {
                    bytes = Math
                            .min(DynamicCloverBuffer.this.remaining(), (int) n);
                }
                DynamicCloverBuffer.this.skip(bytes);
                return bytes;
            }
        };
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public OutputStream asOutputStream() {
        return new OutputStream() {
            @Override
            public void write(byte[] b, int off, int len) {
            	DynamicCloverBuffer.this.put(b, off, len);
            }

            @Override
            public void write(int b) {
            	DynamicCloverBuffer.this.put((byte) b);
            }
        };
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CloverBuffer skip(int size) {
        return position(position() + size);
    }

    /**
     * This method forwards the call to {@link #expand(int)} only when
     * <tt>autoExpand</tt> property is <tt>true</tt>.
     */
    private boolean autoExpand(int expectedRemaining) {
        if (isAutoExpand()) {
            expand(position(), expectedRemaining, true);
            return true;
        } else {
        	return false;
        }
    }

    /**
     * This method forwards the call to {@link #expand(int)} only when
     * <tt>autoExpand</tt> property is <tt>true</tt>.
     */
    private boolean autoExpand(int pos, int expectedRemaining) {
        if (isAutoExpand()) {
            expand(pos, expectedRemaining, true);
            return true;
        } else {
        	return false;
        }
    }

    @Override
	protected ByteBuffer reallocateByteBuffer(int requestedCapacity, boolean direct) {
    	if (requestedCapacity > maximumCapacity) {
    		throw new BufferOverflowException();
    	}
    	return super.reallocateByteBuffer(requestedCapacity, direct);
    }
    
}
