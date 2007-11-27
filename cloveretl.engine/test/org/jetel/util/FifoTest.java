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

import junit.framework.TestCase;

import org.jetel.util.primitive.Queue;

/**
 *  A test case for the FIFO class.
 *
 *  @author	Sven Boden
 *  @since  11 Nov 2004
 */
public final class FifoTest extends TestCase {

    public FifoTest(String name) {
   		super(name);
    }

    /**
     *  Check isEmpty() and removeAll().
     */
    public void testNormalEmpty() {
    	Queue fifo = new Queue(10);
    	assertEquals(true, fifo.isEmpty());

    	fifo.add(new Integer(0));
    	assertEquals(false, fifo.isEmpty());

		fifo.removeAll();
		assertEquals(true, fifo.isEmpty());
    }

	/**
	 *  Put in elements and take them out
	 */
	public void testNormalCase1() {
		Queue fifo = new Queue(4);
		Integer result = null;

		fifo.add(new Integer(1));
		fifo.add(new Integer(2));
		fifo.add(new Integer(3));

        result = (Integer)fifo.get();
        assertEquals(new Integer(1), result);

		result = (Integer)fifo.get();
		assertEquals(new Integer(2), result);

		result = (Integer)fifo.get();
		assertEquals(new Integer(3), result);

		assertEquals(true, fifo.isEmpty());

		result = (Integer)fifo.get();
		assertEquals(null, result);
	}

	/**
	 *  Put in elements and take them out
	 */
	public void testNormalCase2() {
		Queue fifo = new Queue(2);
		Integer result = null;

		fifo.add(new Integer(1));
		fifo.add(new Integer(2));
		assertFalse(fifo.add(new Integer(3)));

		result = (Integer)fifo.get();
		assertEquals(new Integer(1), result);

		result = (Integer)fifo.get();
		assertEquals(new Integer(2), result);

		// Element 3 was never added, hence will not be retrieved.
		result = (Integer)fifo.get();
		assertEquals(null, result);

		assertEquals(true, fifo.isEmpty());

		result = (Integer)fifo.get();
		assertEquals(null, result);
	}

	/**
	 *  Put in elements and take them out. But now try to roll over the
	 *  queue, this is transparant to the user of the queue.
	 */
	public void testNormalCase3() {
		Queue fifo = new Queue(4);
		Integer result = null;

		fifo.add(new Integer(1));
		fifo.add(new Integer(2));
		fifo.add(new Integer(3));

		result = (Integer)fifo.get();
		assertEquals(new Integer(1), result);

		result = (Integer)fifo.get();
		assertEquals(new Integer(2), result);

		fifo.add(new Integer(4));
		fifo.add(new Integer(5));
		fifo.add(new Integer(6));

		// queue is now full
		assertFalse(fifo.add(new Integer(7)));


		result = (Integer)fifo.get();
		assertEquals(new Integer(3), result);

		fifo.add(new Integer(7));

		result = (Integer)fifo.get();
		assertEquals(new Integer(4), result);

		result = (Integer)fifo.get();
		assertEquals(new Integer(5), result);

		result = (Integer)fifo.get();
		assertEquals(new Integer(6), result);

		result = (Integer)fifo.get();
		assertEquals(new Integer(7), result);

		// again empty
		assertEquals(true, fifo.isEmpty());

		fifo.add(new Integer(8));
		fifo.add(new Integer(9));
		fifo.add(new Integer(10));

		result = (Integer)fifo.get();
		assertEquals(new Integer(8), result);

		result = (Integer)fifo.get();
		assertEquals(new Integer(9), result);

		result = (Integer)fifo.get();
		assertEquals(new Integer(10), result);

		assertEquals(true, fifo.isEmpty());
	}

	public void testNormalCase4() {
		Queue fifo = new Queue(50000);
		Integer result = null;

		// add 40000
		for ( int idx = 0; idx < 40000; idx++ )  {
		    fifo.add(new Integer(idx));
		}

		// remove 20000
		for ( int idx = 0; idx < 20000; idx++ )  {
			result = (Integer)fifo.get();
			assertEquals(new Integer(idx), result);
		}

		// add 20000
		for ( int idx = 40000; idx < 40000 + 20000; idx++ )  {
			fifo.add(new Integer(idx));
		}

		// remove 40000
		for ( int idx = 20000; idx < 20000 + 40000; idx++ )  {
			result = (Integer)fifo.get();
			assertEquals(new Integer(idx), result);
		}

		assertEquals(true, fifo.isEmpty());

		// add 40000
		for ( int idx = 0; idx < 40000; idx++ )  {
			fifo.add(new Integer(idx));
		}

		assertEquals(false, fifo.isEmpty());

		// remove 40000
		for ( int idx = 0; idx < 40000; idx++ )  {
			result = (Integer)fifo.get();
			assertEquals(new Integer(idx), result);
		}

		assertEquals(true, fifo.isEmpty());

	}

	/**
	 *  Create a small queue
	 */
	public void testSmallFifo1() {
		Queue fifo = new Queue(0);
		Integer result = null;

		assertFalse(fifo.add(new Integer(1)));
	}

	/**
	 *  Create a small queue
	 */
	public void testSmallFifo2() {
		Queue fifo = new Queue(1);
		Integer result = null;

		fifo.add(new Integer(1));
		assertFalse(fifo.add(new Integer(2)));
	}

}