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

import org.jetel.test.CloverTestCase;
import org.jetel.util.primitive.Queue;

/**
 *  A test case for the FIFO class.
 *
 *  @author	Sven Boden
 *  @since  11 Nov 2004
 */
public final class FifoTest extends CloverTestCase {

    public FifoTest(String name) {
   		super(name);
    }

    /**
     *  Check isEmpty() and removeAll().
     */
    public void testNormalEmpty() {
    	Queue fifo = new Queue(10);
    	assertEquals(true, fifo.isEmpty());

    	fifo.add(Integer.valueOf(0));
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

		fifo.add(Integer.valueOf(1));
		fifo.add(Integer.valueOf(2));
		fifo.add(Integer.valueOf(3));

        result = (Integer)fifo.get();
        assertEquals(Integer.valueOf(1), result);

		result = (Integer)fifo.get();
		assertEquals(Integer.valueOf(2), result);

		result = (Integer)fifo.get();
		assertEquals(Integer.valueOf(3), result);

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

		fifo.add(Integer.valueOf(1));
		fifo.add(Integer.valueOf(2));
		assertFalse(fifo.add(Integer.valueOf(3)));

		result = (Integer)fifo.get();
		assertEquals(Integer.valueOf(1), result);

		result = (Integer)fifo.get();
		assertEquals(Integer.valueOf(2), result);

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

		fifo.add(Integer.valueOf(1));
		fifo.add(Integer.valueOf(2));
		fifo.add(Integer.valueOf(3));

		result = (Integer)fifo.get();
		assertEquals(Integer.valueOf(1), result);

		result = (Integer)fifo.get();
		assertEquals(Integer.valueOf(2), result);

		fifo.add(Integer.valueOf(4));
		fifo.add(Integer.valueOf(5));
		fifo.add(Integer.valueOf(6));

		// queue is now full
		assertFalse(fifo.add(Integer.valueOf(7)));


		result = (Integer)fifo.get();
		assertEquals(Integer.valueOf(3), result);

		fifo.add(Integer.valueOf(7));

		result = (Integer)fifo.get();
		assertEquals(Integer.valueOf(4), result);

		result = (Integer)fifo.get();
		assertEquals(Integer.valueOf(5), result);

		result = (Integer)fifo.get();
		assertEquals(Integer.valueOf(6), result);

		result = (Integer)fifo.get();
		assertEquals(Integer.valueOf(7), result);

		// again empty
		assertEquals(true, fifo.isEmpty());

		fifo.add(Integer.valueOf(8));
		fifo.add(Integer.valueOf(9));
		fifo.add(Integer.valueOf(10));

		result = (Integer)fifo.get();
		assertEquals(Integer.valueOf(8), result);

		result = (Integer)fifo.get();
		assertEquals(Integer.valueOf(9), result);

		result = (Integer)fifo.get();
		assertEquals(Integer.valueOf(10), result);

		assertEquals(true, fifo.isEmpty());
	}

	public void testNormalCase4() {
		Queue fifo = new Queue(50000);
		Integer result = null;

		// add 40000
		for ( int idx = 0; idx < 40000; idx++ )  {
		    fifo.add(Integer.valueOf(idx));
		}

		// remove 20000
		for ( int idx = 0; idx < 20000; idx++ )  {
			result = (Integer)fifo.get();
			assertEquals(Integer.valueOf(idx), result);
		}

		// add 20000
		for ( int idx = 40000; idx < 40000 + 20000; idx++ )  {
			fifo.add(Integer.valueOf(idx));
		}

		// remove 40000
		for ( int idx = 20000; idx < 20000 + 40000; idx++ )  {
			result = (Integer)fifo.get();
			assertEquals(Integer.valueOf(idx), result);
		}

		assertEquals(true, fifo.isEmpty());

		// add 40000
		for ( int idx = 0; idx < 40000; idx++ )  {
			fifo.add(Integer.valueOf(idx));
		}

		assertEquals(false, fifo.isEmpty());

		// remove 40000
		for ( int idx = 0; idx < 40000; idx++ )  {
			result = (Integer)fifo.get();
			assertEquals(Integer.valueOf(idx), result);
		}

		assertEquals(true, fifo.isEmpty());

	}

	/**
	 *  Create a small queue
	 */
	public void testSmallFifo1() {
		Queue fifo = new Queue(0);

		assertFalse(fifo.add(Integer.valueOf(1)));
	}

	/**
	 *  Create a small queue
	 */
	public void testSmallFifo2() {
		Queue fifo = new Queue(1);

		fifo.add(Integer.valueOf(1));
		assertFalse(fifo.add(Integer.valueOf(2)));
	}

}