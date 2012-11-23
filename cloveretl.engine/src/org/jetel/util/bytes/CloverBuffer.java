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

import java.io.IOException;
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

import org.apache.log4j.Logger;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.graph.ContextProvider;
import org.jetel.graph.Node;
import org.jetel.graph.TransformationGraph;

/**
 * This class is substitution for originally very intensive using of {@link ByteBuffer} class.
 * This class is basic container for serialization form of data records ({@link DataRecord}).
 * Interface of this class is almost same as {@link ByteBuffer}.
 * But our implementation {@link DynamicCloverBuffer} ensures that writing to the buffer never throws
 * {@link BufferOverflowException}. Buffer easily grows, buffer is elastic.
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 17 Oct 2011
 * 
 * @see DynamicCloverBuffer only implementation
 * @see org.apache.mina.core.buffer.IoBuffer This class was inspired by similar class from Apache MINA Project  
 */
public abstract class CloverBuffer {

	private static final Logger logger = Logger.getLogger(CloverBuffer.class);
	
	private static final boolean DEFAULT_DIRECT = false;
	
	protected TransformationGraph associatedGraph;
	
	protected Node associatedNode;
	
	private static CloverBufferAllocator allocator = new DynamicCloverBufferAllocator();
	
	public static void setAllocator(CloverBufferAllocator allocator) {
		CloverBuffer.allocator = allocator;
	}
	
	public static CloverBufferAllocator getAllocator() {
		return allocator;
	}
	
	/**
	 * Allocates new clover buffer with given capacity. Buffer is not direct by default.
	 * @param capacity initial capacity of allocated buffer
	 * @return allocated clover buffer instance
	 */
	public static CloverBuffer allocate(int capacity) {
		return allocator.allocate(capacity, DEFAULT_DIRECT);
	}

	/**
	 * Allocates new clover buffer with given capacity. Buffer is not direct by default.
	 * @param capacity initial capacity of allocated buffer
	 * @param maximumCapacity the limit for inner buffer capacity, buffer expansion is limited
	 * @return allocated clover buffer instance
	 */
	public static CloverBuffer allocate(int capacity, int maximumCapacity) {
		return allocator.allocate(capacity, maximumCapacity, DEFAULT_DIRECT);
	}

	/**
	 * Allocates new direct clover buffer with given capacity.
	 * If JVM is out of direct memory, non-direct (heap) memory is used instead.
	 * @param capacity initial capacity of allocated buffer
	 * @return allocated clover buffer instance
	 */
	public static CloverBuffer allocateDirect(int capacity) {
		try {
			return allocator.allocate(capacity, true);
		} catch (OutOfMemoryError e) {
			return allocator.allocate(capacity, false);
		}
	}

	/**
	 * Allocates new direct clover buffer with given capacity.
	 * If JVM is out of direct memory, non-direct (heap) memory is used instead.
	 * @param capacity initial capacity of allocated buffer
	 * @param maximumCapacity the limit for inner buffer capacity, buffer expansion is limited
	 * @return allocated clover buffer instance
	 */
	public static CloverBuffer allocateDirect(int capacity, int maximumCapacity) {
		try {
			return allocator.allocate(capacity, maximumCapacity, true);
		} catch (OutOfMemoryError e) {
			return allocator.allocate(capacity, maximumCapacity, false);
		}
	}

	/**
	 * Allocates new clover buffer with given capacity.
	 * @param capacity initial capacity of allocated buffer
	 * @param direct true if underlying buffer is direct
	 * @return allocated clover buffer instance
	 */
	public static CloverBuffer allocate(int capacity, boolean direct) {
		if (direct) {
			return allocateDirect(capacity);
		} else {
			return allocate(capacity);
		}
	}

	/**
	 * Allocates new clover buffer with given capacity.
	 * @param capacity initial capacity of allocated buffer
	 * @param maximumCapacity the limit for inner buffer capacity, buffer expansion is limited
	 * @param direct true if underlying buffer is direct
	 * @return allocated clover buffer instance
	 */
	public static CloverBuffer allocate(int capacity, int maximumCapacity, boolean direct) {
		if (direct) {
			return allocateDirect(capacity, maximumCapacity);
		} else {
			return allocate(capacity, maximumCapacity);
		}
	}

    /**
     * Wraps the specified NIO {@link ByteBuffer} into CloverBuffer buffer.
     */
    public static CloverBuffer wrap(ByteBuffer nioBuffer) {
        return allocator.wrap(nioBuffer);
    }

    /**
     * Wraps the specified byte array into CloverBuffer.
     */
    public static CloverBuffer wrap(byte[] byteArray) {
        return wrap(ByteBuffer.wrap(byteArray));
    }

    /**
     * Wraps the specified byte array into CloverBuffer.
     */
    public static CloverBuffer wrap(byte[] byteArray, int offset, int length) {
        return wrap(ByteBuffer.wrap(byteArray, offset, length));
    }

	protected CloverBuffer() {
    	associatedGraph = ContextProvider.getGraph();
    	associatedNode = ContextProvider.getNode();
	}
    
    protected void memoryAllocated(int memorySize) {
    	if (associatedGraph != null) {
    		associatedGraph.getMemoryTracker().memoryAllocated(associatedNode, memorySize);
    	}
    }

    protected void memoryDeallocated(int memorySize) {
    	if (associatedGraph != null) {
    		associatedGraph.getMemoryTracker().memoryDeallocated(associatedNode, memorySize);
    	}
    }

    /**
     * That is attempt to track information about all clover buffers associated with a graph or node. 
     */
//this was considered two dangerous, 
//    @Override
//    protected void finalize() throws Throwable {
//    	memoryDeallocated(capacity());
//    }
    
    /**
     * Returns the underlying NIO buffer instance.
     */
    public abstract ByteBuffer buf();

    /**
     * @see ByteBuffer#isDirect()
     */
    public abstract boolean isDirect();

    /**
     * returns <tt>true</tt> if and only if this buffer is derived from other
     * buffer via {@link #duplicate()}, {@link #slice()} or
     * {@link #asReadOnlyBuffer()}.
     */
    public abstract boolean isDerived();

    /**
     * @see ByteBuffer#isReadOnly()
     */
    public abstract boolean isReadOnly();

    /**
     * Returns the minimum capacity of this buffer which is used to determine
     * the new capacity of the buffer shrunk by {@link #compact()} and
     * {@link #shrink()} operation. The default value is the initial capacity of
     * the buffer.
     */
    public abstract int minimumCapacity();

    /**
     * Sets the minimum capacity of this buffer which is used to determine the
     * new capacity of the buffer shrunk by {@link #compact()} and
     * {@link #shrink()} operation. The default value is the initial capacity of
     * the buffer.
     */
    public abstract CloverBuffer minimumCapacity(int minimumCapacity);

    /**
     * @see ByteBuffer#capacity()
     */
    public abstract int capacity();

    /**
     * Increases the capacity of this buffer. If the new capacity is less than
     * or equal to the current capacity, this method returns silently. If the
     * new capacity is greater than the current capacity, the buffer is
     * reallocated while retaining the position, limit, mark and the content of
     * the buffer.
     */
    public abstract CloverBuffer capacity(int newCapacity);

    /**
     * Returns maximal capacity of the buffer.
     */
    public abstract int maximumCapacity();

    /**
     * Returns <tt>true</tt> if and only if <tt>autoExpand</tt> is turned on.
     */
    public abstract boolean isAutoExpand();

    /**
     * Turns on or off <tt>autoExpand</tt>.
     */
    public abstract CloverBuffer setAutoExpand(boolean autoExpand);

    /**
     * Returns <tt>true</tt> if and only if <tt>autoShrink</tt> is turned on.
     */
    public abstract boolean isAutoShrink();

    /**
     * Turns on or off <tt>autoShrink</tt>.
     */
    public abstract CloverBuffer setAutoShrink(boolean autoShrink);

    /**
     * Turns on or off possibility of relocation underlying buffer at all.
     */
    public abstract CloverBuffer setRecapacityAllowed(boolean recapacityAllowed);

    /**
     * Changes the capacity and limit of this buffer so this buffer get the
     * specified <tt>expectedRemaining</tt> room from the current position. This
     * method works even if you didn't set <tt>autoExpand</tt> to <tt>true</tt>.
     */
    public abstract CloverBuffer expand(int expectedRemaining);

    /**
     * Changes the capacity and limit of this buffer so this buffer get the
     * specified <tt>expectedRemaining</tt> room from the specified
     * <tt>position</tt>. This method works even if you didn't set
     * <tt>autoExpand</tt> to <tt>true</tt>.
     */
    public abstract CloverBuffer expand(int position, int expectedRemaining);

    /**
     * Changes the capacity of this buffer so this buffer occupies as less
     * memory as possible while retaining the position, limit and the buffer
     * content between the position and limit. The capacity of the buffer never
     * becomes less than {@link #minimumCapacity()}. The mark is discarded once
     * the capacity changes.
     */
    public abstract CloverBuffer shrink();

    /**
     * @see java.nio.Buffer#position()
     */
    public abstract int position();

    /**
     * @see java.nio.Buffer#position(int)
     */
    public abstract CloverBuffer position(int newPosition);

    /**
     * @see java.nio.Buffer#limit()
     */
    public abstract int limit();

    /**
     * @see java.nio.Buffer#limit(int)
     */
    public abstract CloverBuffer limit(int newLimit);

    /**
     * @see java.nio.Buffer#mark()
     */
    public abstract CloverBuffer mark();

    /**
     * Returns the position of the current mark. This method returns <tt>-1</tt>
     * if no mark is set.
     */
    public abstract int markValue();

    /**
     * @see java.nio.Buffer#reset()
     */
    public abstract CloverBuffer reset();

    /**
     * @see java.nio.Buffer#clear()
     */
    public abstract CloverBuffer clear();

    /**
     * @see java.nio.Buffer#flip()
     */
    public abstract CloverBuffer flip();

    /**
     * @see java.nio.Buffer#rewind()
     */
    public abstract CloverBuffer rewind();

    /**
     * @see java.nio.Buffer#remaining()
     */
    public abstract int remaining();

    /**
     * @see java.nio.Buffer#hasRemaining()
     */
    public abstract boolean hasRemaining();

    /**
     * @see ByteBuffer#duplicate()
     */
    public abstract CloverBuffer duplicate();

    /**
     * @see ByteBuffer#slice()
     * WARNING: the resulted slice is shallow copy as requested, but only until first expand of this buffer is performed
     * ISSUE: CL-2597 Recapacity of this CloverBuffer is not allowed.
     */
    public abstract CloverBuffer slice();

    /**
     * @see ByteBuffer#asReadOnlyBuffer()
     */
    public abstract CloverBuffer asReadOnlyBuffer();

    /**
     * @see ByteBuffer#hasArray()
     */
    public abstract boolean hasArray();

    /**
     * @see ByteBuffer#array()
     */
    public abstract byte[] array();

    /**
     * @see ByteBuffer#arrayOffset()
     */
    public abstract int arrayOffset();

    /**
     * @see ByteBuffer#get()
     */
    public abstract byte get();

    /**
     * @see ByteBuffer#put(byte)
     */
    public abstract CloverBuffer put(byte b);

    /**
     * @see ByteBuffer#get(int)
     */
    public abstract byte get(int index);

    /**
     * @see ByteBuffer#put(int, byte)
     */
    public abstract CloverBuffer put(int index, byte b);

    /**
     * @see ByteBuffer#get(byte[], int, int)
     */
    public abstract CloverBuffer get(byte[] dst, int offset, int length);

    /**
     * @see ByteBuffer#get(byte[])
     */
    public abstract CloverBuffer get(byte[] dst);

    /**
     * Writes the content of the specified <tt>src</tt> into this buffer.
     */
    public abstract CloverBuffer put(ByteBuffer src);

    /**
     * Writes the content of the specified <tt>src</tt> into this buffer.
     */
    public abstract CloverBuffer put(CloverBuffer src);

    /**
     * @see ByteBuffer#put(byte[], int, int)
     */
    public abstract CloverBuffer put(byte[] src, int offset, int length);

    /**
     * @see ByteBuffer#put(byte[])
     */
    public abstract CloverBuffer put(byte[] src);

    /**
     * @see ByteBuffer#compact()
     */
    public abstract CloverBuffer compact();

    /**
     * @see ByteBuffer#order()
     */
    public abstract ByteOrder order();

    /**
     * @see ByteBuffer#order(ByteOrder)
     */
    public abstract CloverBuffer order(ByteOrder bo);

    /**
     * @see ByteBuffer#getChar()
     */
    public abstract char getChar();

    /**
     * @see ByteBuffer#putChar(char)
     */
    public abstract CloverBuffer putChar(char value);

    /**
     * @see ByteBuffer#getChar(int)
     */
    public abstract char getChar(int index);

    /**
     * @see ByteBuffer#putChar(int, char)
     */
    public abstract CloverBuffer putChar(int index, char value);

    /**
     * @see ByteBuffer#asCharBuffer()
     */
    public abstract CharBuffer asCharBuffer();

    /**
     * @see ByteBuffer#getShort()
     */
    public abstract short getShort();

    /**
     * @see ByteBuffer#putShort(short)
     */
    public abstract CloverBuffer putShort(short value);

    /**
     * @see ByteBuffer#getShort()
     */
    public abstract short getShort(int index);

    /**
     * @see ByteBuffer#putShort(int, short)
     */
    public abstract CloverBuffer putShort(int index, short value);

    /**
     * @see ByteBuffer#asShortBuffer()
     */
    public abstract ShortBuffer asShortBuffer();

    /**
     * @see ByteBuffer#getInt()
     */
    public abstract int getInt();

    /**
     * @see ByteBuffer#putInt(int)
     */
    public abstract CloverBuffer putInt(int value);

    /**
     * @see ByteBuffer#getInt(int)
     */
    public abstract int getInt(int index);

    /**
     * @see ByteBuffer#putInt(int, int)
     */
    public abstract CloverBuffer putInt(int index, int value);

    /**
     * @see ByteBuffer#asIntBuffer()
     */
    public abstract IntBuffer asIntBuffer();

    /**
     * @see ByteBuffer#getLong()
     */
    public abstract long getLong();

    /**
     * @see ByteBuffer#putLong(int, long)
     */
    public abstract CloverBuffer putLong(long value);

    /**
     * @see ByteBuffer#getLong(int)
     */
    public abstract long getLong(int index);

    /**
     * @see ByteBuffer#putLong(int, long)
     */
    public abstract CloverBuffer putLong(int index, long value);

    /**
     * @see ByteBuffer#asLongBuffer()
     */
    public abstract LongBuffer asLongBuffer();

    /**
     * @see ByteBuffer#getFloat()
     */
    public abstract float getFloat();

    /**
     * @see ByteBuffer#putFloat(float)
     */
    public abstract CloverBuffer putFloat(float value);

    /**
     * @see ByteBuffer#getFloat(int)
     */
    public abstract float getFloat(int index);

    /**
     * @see ByteBuffer#putFloat(int, float)
     */
    public abstract CloverBuffer putFloat(int index, float value);

    /**
     * @see ByteBuffer#asFloatBuffer()
     */
    public abstract FloatBuffer asFloatBuffer();

    /**
     * @see ByteBuffer#getDouble()
     */
    public abstract double getDouble();

    /**
     * @see ByteBuffer#putDouble(double)
     */
    public abstract CloverBuffer putDouble(double value);

    /**
     * @see ByteBuffer#getDouble(int)
     */
    public abstract double getDouble(int index);

    /**
     * @see ByteBuffer#putDouble(int, double)
     */
    public abstract CloverBuffer putDouble(int index, double value);

    /**
     * @see ByteBuffer#asDoubleBuffer()
     */
    public abstract DoubleBuffer asDoubleBuffer();

    /**
     * Returns an {@link InputStream} that reads the data from this buffer.
     * {@link InputStream#read()} returns <tt>-1</tt> if the buffer position
     * reaches to the limit.
     */
    public abstract InputStream asInputStream();

    /**
     * Returns an {@link OutputStream} that appends the data into this buffer.
     * Please note that the {@link OutputStream#write(int)} will throw a
     * {@link BufferOverflowException} instead of an {@link IOException} in case
     * of buffer overflow. Please set <tt>autoExpand</tt> property by calling
     * {@link #setAutoExpand(boolean)} to prevent the unexpected runtime
     * exception.
     */
    public abstract OutputStream asOutputStream();

    // ////////////////////////
    // Skip or fill methods //
    // ////////////////////////

    /**
     * Forwards the position of this buffer as the specified <code>size</code>
     * bytes.
     */
    public abstract CloverBuffer skip(int size);

    /**
     * Normalizes the specified capacity of the buffer to power of 2, which is
     * often helpful for optimal memory usage and performance. If it is greater
     * than or equal to {@link Integer#MAX_VALUE}, it returns
     * {@link Integer#MAX_VALUE}. If it is zero, it returns zero.
     */
    protected static int normalizeCapacity(int requestedCapacity) {
        if (requestedCapacity < 0) {
            return Integer.MAX_VALUE;
        }

        int newCapacity = Integer.highestOneBit(requestedCapacity);
        newCapacity <<= (newCapacity < requestedCapacity ? 1 : 0);
        return newCapacity < 0 ? Integer.MAX_VALUE : newCapacity;
    }

    /**
     * Request to prepare a new byte buffer. Memory tracker is updated.
     */
    protected ByteBuffer reallocateByteBuffer(int capacity, boolean direct) {
    	memoryAllocated(capacity);
    	return allocateByteBuffer(capacity, direct);
    }

    /**
     * Event listener, that an underlying byte buffer is no more used.
     * Memory tracker is updated. 
     */
    protected void deallocateByteBuffer(ByteBuffer byteBuffer) {
    	memoryDeallocated(byteBuffer.capacity());
    }

    /**
     * Allocates new {@link ByteBuffer} instance with given capacity.
     * Resulted {@link ByteBuffer} is direct if and only if direct buffer is requested
     * and if usage of direct memory is allowed (Defaults.USE_DIRECT_MEMORY = true)
     * and if free direct memory is still available.
     * @param capacity capacity of requested {@link ByteBuffer}
     * @param direct <code>true</code> if direct buffer is requested
     * @return new {@link ByteBuffer}
     */
    public static ByteBuffer allocateByteBuffer(int capacity, boolean direct) {
    	if (direct && Defaults.USE_DIRECT_MEMORY && isDirectMemoryAvailable()) {
    		try {
    			return ByteBuffer.allocateDirect(capacity);
    		} catch (OutOfMemoryError e) {
    			logger.debug("Engine is out of direct memory. Heap memory is going to be used.");
    			lastDirectMemoryAllocationFail = System.currentTimeMillis();
        		return ByteBuffer.allocate(capacity);
    		}
    	} else {
    		return ByteBuffer.allocate(capacity);
    	}
    }
    
    /**
     * This simple method should decide whether some direct memory is available.
     * The direct memory is considered unavailable if an unsuccessful attempt to allocate direct
     * memory was performed in last 10 seconds.
     */
    private static long lastDirectMemoryAllocationFail = 0;
    private static boolean isDirectMemoryAvailable() {
    	if (lastDirectMemoryAllocationFail == 0) {
    		return true;
    	} else {
	    	long currentTime = System.currentTimeMillis();
	    	if (currentTime - lastDirectMemoryAllocationFail > 10000) {
	    		lastDirectMemoryAllocationFail = 0;
	    		return true;
	    	} else {
	    		return false;
	    	}
    	}
    }
    
}
