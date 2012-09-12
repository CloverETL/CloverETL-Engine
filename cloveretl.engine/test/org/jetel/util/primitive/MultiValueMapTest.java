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
package org.jetel.util.primitive;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;

import org.jetel.test.CloverTestCase;

/**
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 12.9.2012
 */
public class MultiValueMapTest extends CloverTestCase {

	
	@Override
	protected void setUp() throws Exception {
		super.setUp();
		initEngine();
	}

	public void testConstructor() {
		try {
			new MultiValueMap<String, Integer>(null);
			assertTrue(false);
		} catch (NullPointerException e) {
			//OK
		}
	}
	
	public void testPutValue() {
		MultiValueMap<String, Integer> map = new MultiValueMap<String, Integer>(new LinkedHashMap<String, List<Integer>>());

		map.putValue("abc", 123);
		assertTrue(Arrays.equals(new Integer[] { 123 }, map.get("abc").toArray(new Integer[0])));
		
		map.clear();
		List<Integer> list = new LinkedList<Integer>();
		list.add(1);
		list.add(2);
		map.put("abc", list);
		map.putValue("abc", 3);
		assertTrue(Arrays.equals(new Integer[] { 1, 2, 3 }, map.get("abc").toArray(new Integer[0])));
	}
}
