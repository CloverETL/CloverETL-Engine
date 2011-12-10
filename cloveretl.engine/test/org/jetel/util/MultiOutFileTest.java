/*
 *    jETeL/Clover - Java based ETL application framework.
 *    Copyright (C) 2004-08 Javlin Consulting <info@javlinconsulting.cz>
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

/**
 * @author Agata Vackova (agata.vackova@javlinconsulting.cz) ; 
 * (c) JavlinConsulting s.r.o.
 *  www.javlinconsulting.cz
 *
 * @since Feb 5, 2008
 *
 */

public class MultiOutFileTest extends CloverTestCase {


	/* (non-Javadoc)
	 * @see junit.framework.TestCase#setUp()
	 */
	@Override
	protected void setUp() throws Exception {
		super.setUp();
	}

	public void testMulti() {
		MultiOutFile out = new MultiOutFile("test$.txt");
		assertTrue(out.hasNext());
		for (int i = 0; i < 5; i++) {
			assertEquals("test" + i + ".txt", out.next());
		}
		out.reset();
		assertTrue(out.hasNext());
		for (int i = 0; i < 5; i++) {
			assertEquals("test" + i + ".txt", out.next());
		}
	}
	
	public void testSingle() throws Exception {
		MultiOutFile out = new MultiOutFile("test.txt");
		assertTrue(out.hasNext());
		assertEquals("test.txt", out.next());
		assertFalse(out.hasNext());
	}
	
	/* (non-Javadoc)
	 * @see junit.framework.TestCase#tearDown()
	 */
	@Override
	protected void tearDown() throws Exception {
		super.tearDown();
	}

}
