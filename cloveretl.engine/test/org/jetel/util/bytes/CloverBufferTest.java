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

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;

import org.jetel.data.Defaults;
import org.jetel.test.CloverTestCase;

/**
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 20 Oct 2011
 * 
 * @see org.apache.mina.core.buffer.IoBufferTestr This test was inspired by similar class from Apache MINA Project  
 */
public class CloverBufferTest extends CloverTestCase {

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		
		initEngine();
	}
	
    public void testNormalizeCapacity() {
        // A few sanity checks
        assertEquals(Integer.MAX_VALUE, CloverBufferImpl.normalizeCapacity(-10));
        assertEquals(0, CloverBufferImpl.normalizeCapacity(0));
        assertEquals(Integer.MAX_VALUE, CloverBufferImpl.normalizeCapacity(Integer.MAX_VALUE));
        assertEquals(Integer.MAX_VALUE, CloverBufferImpl.normalizeCapacity(Integer.MIN_VALUE));
        assertEquals(Integer.MAX_VALUE, CloverBufferImpl.normalizeCapacity(Integer.MAX_VALUE - 10));

        // A sanity check test for all the powers of 2
        for (int i = 0; i < 30; i++) {
            int n = 1 << i;

            assertEquals(n, CloverBufferImpl.normalizeCapacity(n));

            if (i > 1) {
                // test that n - 1 will be normalized to n (notice that n = 2^i)
                assertEquals(n, CloverBufferImpl.normalizeCapacity(n - 1));
            }

            // test that n + 1 will be normalized to 2^(i + 1)
            assertEquals(n << 1, CloverBufferImpl.normalizeCapacity(n + 1));
        }

        // The first performance test measures the time to normalize integers
        // from 0 to 2^27 (it tests 2^27 integers)
        long time = System.currentTimeMillis();

        for (int i = 0; i < 1 << 27; i++) {
            int n = CloverBufferImpl.normalizeCapacity(i);

            // do a simple superfluous test to prevent possible compiler or JVM
            // optimizations of not executing non used code/variables
            if (n == -1) {
                System.out.println("n should never be -1");
            }
        }

        long time2 = System.currentTimeMillis();
        //System.out.println("Time for performance test 1: " + (time2 - time) + "ms");

        // The second performance test measures the time to normalize integers
        // from Integer.MAX_VALUE to Integer.MAX_VALUE - 2^27 (it tests 2^27
        // integers)
        time = System.currentTimeMillis();
        for (int i = Integer.MAX_VALUE; i > Integer.MAX_VALUE - (1 << 27); i--) {
            int n = CloverBufferImpl.normalizeCapacity(i);

            // do a simple superfluous test to prevent possible compiler or JVM
            // optimizations of not executing non used code/variables
            if (n == -1) {
                System.out.println("n should never be -1");
            }
        }

        time2 = System.currentTimeMillis();
        //System.out.println("Time for performance test 2: " + (time2 - time) + "ms");
    }
    
    public void testAutoExpand1() { 
    	CloverBuffer buffer = CloverBuffer.allocate(8, false); 
        buffer.setAutoExpand(true); 
         
        assertTrue("Should AutoExpand", buffer.isAutoExpand()); 
         
        CloverBuffer slice = buffer.slice(); 
        assertTrue("Should AutoExpand", buffer.isAutoExpand()); 
        assertFalse("Should *NOT* AutoExpand", slice.isAutoExpand()); 
    } 

    /**
     * This class extends the AbstractIoBuffer class to have direct access to
     * the protected IoBuffer.normalizeCapacity() method and to expose it for
     * the tests.
     */
    private static class CloverBufferImpl extends DynamicCloverBuffer {

		protected CloverBufferImpl(ByteBuffer buf) {
			super(buf);
		}

		public static int normalizeCapacity(int requestedCapacity) {
            return CloverBuffer.normalizeCapacity(requestedCapacity);
        }
        
    }
    
    public void testAllocate() throws Exception {
        for (int i = 10; i < 1048576 * 2; i = i * 11 / 10) // increase by 10%
        {
        	CloverBuffer buf = CloverBuffer.allocate(i);
            assertEquals(0, buf.position());
            assertEquals(buf.capacity(), buf.remaining());
            assertTrue(buf.capacity() >= i);
            assertTrue(buf.capacity() < i * 2);
        }
    }

    public void testAutoExpand() throws Exception {
    	CloverBuffer buf = CloverBuffer.allocate(2);

        buf.put((byte) 0);
        buf.put((byte) 0);
        assertEquals(2, buf.position());
        assertEquals(2, buf.limit());
        assertEquals(2, buf.capacity());

        buf.setAutoExpand(false);
        try {
            buf.put(3, (byte) 0);
            fail("Buffer can't auto expand, with autoExpand property set at false");
        } catch (IndexOutOfBoundsException e) {
            // Expected Exception as auto expand property is false
            assertTrue(true);
        }

        buf.setAutoExpand(true);
        buf.put(3, (byte) 0);
        assertEquals(2, buf.position());
        assertEquals(4, buf.limit());
        assertEquals(4, buf.capacity());

        // Make sure the buffer is doubled up.
        buf = CloverBuffer.allocate(1).setAutoExpand(true);
        int lastCapacity = buf.capacity();
        for (int i = 0; i < 1048576; i ++) {
            buf.put((byte) 0);
            if (lastCapacity != buf.capacity()) {
                assertEquals(lastCapacity * 2, buf.capacity());
                lastCapacity = buf.capacity();
            }
        }
    }

    public void testAutoExpandMark() throws Exception {
    	CloverBuffer buf = CloverBuffer.allocate(4).setAutoExpand(true);

        buf.put((byte) 0);
        buf.put((byte) 0);
        buf.put((byte) 0);

        // Position should be 3 when we reset this buffer.
        buf.mark();

        // Overflow it
        buf.put((byte) 0);
        buf.put((byte) 0);

        assertEquals(5, buf.position());
        buf.reset();
        assertEquals(3, buf.position());
    }

    public void testWrapNioBuffer() throws Exception {
        ByteBuffer nioBuf = ByteBuffer.allocate(10);
        nioBuf.position(3);
        nioBuf.limit(7);

        CloverBuffer buf = CloverBuffer.wrap(nioBuf);
        assertEquals(3, buf.position());
        assertEquals(7, buf.limit());
        assertEquals(10, buf.capacity());
    }

    public void testWrapSubArray() throws Exception {
        byte[] array = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };

        CloverBuffer buf = CloverBuffer.wrap(array, 3, 4);
        assertEquals(3, buf.position());
        assertEquals(7, buf.limit());
        assertEquals(10, buf.capacity());

        buf.clear();
        assertEquals(0, buf.position());
        assertEquals(10, buf.limit());
        assertEquals(10, buf.capacity());
    }

    public void testDuplicate() throws Exception {
    	CloverBuffer original;
    	CloverBuffer duplicate;

        // Test if the buffer is duplicated correctly.
        original = CloverBuffer.allocate(16);
        original.position(4);
        original.limit(10);
        duplicate = original.duplicate();
        original.put(4, (byte) 127);
        assertEquals(4, duplicate.position());
        assertEquals(10, duplicate.limit());
        assertEquals(16, duplicate.capacity());
        assertNotSame(original.buf(), duplicate.buf());
        assertSame(original.buf().array(), duplicate.buf().array());
        assertEquals(127, duplicate.get(4));

        // Test a duplicate of a duplicate.
        original = CloverBuffer.allocate(16);
        duplicate = original.duplicate().duplicate();
        assertNotSame(original.buf(), duplicate.buf());
        assertSame(original.buf().array(), duplicate.buf().array());

        // Try to expand.
        original = CloverBuffer.allocate(16);
        original.setAutoExpand(true);
        duplicate = original.duplicate();
        assertFalse(original.isAutoExpand());

        try {
            original.setAutoExpand(true);
            fail("Derived buffers and their parent can't be expanded");
        } catch (IllegalStateException e) {
            // Expected an Exception, signifies test success
            assertTrue(true);
        }

        try {
            duplicate.setAutoExpand(true);
            fail("Derived buffers and their parent can't be expanded");
        } catch (IllegalStateException e) {
            // Expected an Exception, signifies test success
            assertTrue(true);
        }
    }

    public void testSlice() throws Exception {
    	CloverBuffer original;
    	CloverBuffer slice;

        // Test if the buffer is sliced correctly.
        original = CloverBuffer.allocate(16);
        original.position(4);
        original.limit(10);
        slice = original.slice();
        original.put(4, (byte) 127);
        assertEquals(0, slice.position());
        assertEquals(6, slice.limit());
        assertEquals(6, slice.capacity());
        assertNotSame(original.buf(), slice.buf());
        assertEquals(127, slice.get(0));
    }

    public void testReadOnlyBuffer() throws Exception {
    	CloverBuffer original;
    	CloverBuffer duplicate;

        // Test if the buffer is duplicated correctly.
        original = CloverBuffer.allocate(16);
        original.position(4);
        original.limit(10);
        duplicate = original.asReadOnlyBuffer();
        original.put(4, (byte) 127);
        assertEquals(4, duplicate.position());
        assertEquals(10, duplicate.limit());
        assertEquals(16, duplicate.capacity());
        assertNotSame(original.buf(), duplicate.buf());
        assertEquals(127, duplicate.get(4));

        // Try to expand.
        try {
            original = CloverBuffer.allocate(1);
            duplicate = original.asReadOnlyBuffer();
            duplicate.put((byte) 1);
            duplicate.put((byte) 1);
            fail("ReadOnly buffer's can't be expanded");
        } catch (ReadOnlyBufferException e) {
            // Expected an Exception, signifies test success
            assertTrue(true);
        }
    }
    
    public void testMaximumCapacity() {
    	try {
    		CloverBuffer.allocate(1, 2);
    		assert false;
    	} catch (IllegalArgumentException e) {
    		//that is correct
    	}

    	try {
    		CloverBuffer.allocate(Defaults.Record.RECORD_INITIAL_SIZE, Defaults.Record.RECORD_INITIAL_SIZE - 1);
    		assert false;
    	} catch (IllegalArgumentException e) {
    		//that is correct
    	}

    	CloverBuffer.allocate(1, 1);
    	
		CloverBuffer buffer = CloverBuffer.allocate(Defaults.Record.RECORD_INITIAL_SIZE, Defaults.Record.RECORD_LIMIT_SIZE);
    	
    	for (int i = 0; i < Defaults.Record.RECORD_LIMIT_SIZE; i++) {
        	buffer.put((byte) 0);
    	}
    	
    	try {
    		buffer.put((byte) 0);
    		assert false;
    	} catch (Exception e) {
    		//that is correct
    	}
    	
    	buffer.clear();
    	buffer.limit(100);
    	buffer.shrink();

    	for (int i = 0; i < Defaults.Record.RECORD_LIMIT_SIZE; i++) {
        	buffer.put((byte) 0);
    	}
    	
    	try {
    		buffer.put((byte) 0);
    		assert false;
    	} catch (Exception e) {
    		//that is correct
    	}
    }

    public void testRecapacityAllowed() {
    	ByteBuffer underlyingBuffer = ByteBuffer.allocate(100);
    	CloverBuffer cloverBuffer = CloverBuffer.wrap(underlyingBuffer);
    	cloverBuffer.setRecapacityAllowed(false);
    	try {
    		cloverBuffer.expand(101);
    		assert false;
    	} catch (IllegalStateException e) {
    		//correct
    	}
    	
    	for (int i = 0; i < 100; i++) {
    		cloverBuffer.put((byte) 0);
    	}
    	
    	try {
    		cloverBuffer.put((byte) 0);
    		assert false;
    	} catch (BufferOverflowException e) {
    		//correct
    	}
    	
    	assertEquals(underlyingBuffer, cloverBuffer.buf());
    }
 
    public void testUseDirectMemory() {
    	boolean formerUseDirectMemory = Defaults.USE_DIRECT_MEMORY;
    	Defaults.USE_DIRECT_MEMORY = false;
    	
    	assertFalse(CloverBuffer.allocate(100).isDirect());
    	assertFalse(CloverBuffer.allocateDirect(100).isDirect());

    	Defaults.USE_DIRECT_MEMORY = true;
    	
    	assertFalse(CloverBuffer.allocate(100).isDirect());
    	assertTrue(CloverBuffer.allocateDirect(100).isDirect());

    	Defaults.USE_DIRECT_MEMORY = formerUseDirectMemory;
    }
 
    public void testMaxDirectMemoryAllocation() throws InterruptedException {
    	long formerLimitSize = Defaults.CLOVER_BUFFER_DIRECT_MEMORY_LIMIT_SIZE;
    	try {
    		execGC();
    		Defaults.CLOVER_BUFFER_DIRECT_MEMORY_LIMIT_SIZE = 1000;
    		CloverBuffer cb1 = CloverBuffer.allocate(500, true);
    		assertTrue(cb1.isDirect());
    		CloverBuffer cb2 = CloverBuffer.allocate(500, true);
    		assertTrue(cb2.isDirect());
    		CloverBuffer cb3 = CloverBuffer.allocate(1, true);
    		assertFalse(cb3.isDirect());
    		
    		cb1 = null;
    		execGC();
    		CloverBuffer cb4 = CloverBuffer.allocate(500, false);
    		assertFalse(cb4.isDirect());
    		CloverBuffer cb5 = CloverBuffer.allocate(500, true);
    		assertTrue(cb5.isDirect());
    		CloverBuffer cb6 = CloverBuffer.allocate(1, true);
    		assertFalse(cb6.isDirect());
    		
    		cb2 = cb3 = cb4 = cb5 = cb6 = null;
    		execGC();
    		CloverBuffer cb7 = CloverBuffer.allocate(5000, false);
    		assertFalse(cb7.isDirect());
    		CloverBuffer cb8 = CloverBuffer.allocate(1001, true);
    		assertFalse(cb8.isDirect());
    		
    	} finally {
    		Defaults.CLOVER_BUFFER_DIRECT_MEMORY_LIMIT_SIZE = formerLimitSize;
    	}
    }

    private void execGC() throws InterruptedException {
    	//cleanup the already allocated clover buffers - this is potential issue
    	//this is not good enough
    	for (int i = 0; i < 5; i++) {
	    	System.gc();
	    	Thread.sleep(10);
    	}
    }
}
