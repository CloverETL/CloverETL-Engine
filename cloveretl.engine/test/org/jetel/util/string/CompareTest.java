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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.jetel.test.CloverTestCase;

/**
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 9. 4. 2014
 */
public class CompareTest extends CloverTestCase {

	public void testEquals() {
		try {
			Compare.equals((String) null, (List<String>) null);
			assertTrue(false);
		} catch (Exception e) {
			//OK
		}

		try {
			Compare.equals((String) "abc", (List<String>) null);
			assertTrue(false);
		} catch (Exception e) {
			//OK
		}

		try {
			Compare.equals((String) null, Arrays.asList(""));
			assertTrue(false);
		} catch (Exception e) {
			//OK
		}

		assertFalse(Compare.equals((String) null, new ArrayList<String>()));
		assertFalse(Compare.equals((String) "", new ArrayList<String>()));
		assertFalse(Compare.equals((String) "abc", new ArrayList<String>()));

		assertTrue(Compare.equals((String) "", Arrays.asList("")));
		assertFalse(Compare.equals((String) "abc", Arrays.asList("")));

		assertFalse(Compare.equals((String) "", Arrays.asList("abc")));
		assertTrue(Compare.equals((String) "abc", Arrays.asList("abc")));

		assertTrue(Compare.equals((String) "", Arrays.asList("abc", "")));
		assertTrue(Compare.equals((String) "abc", Arrays.asList("abc", "")));

		assertTrue(Compare.equals((String) "", Arrays.asList("", "abc", "xxx")));
		assertTrue(Compare.equals((String) "abc", Arrays.asList("", "xxx", "abc")));
		assertFalse(Compare.equals((String) "yyy", Arrays.asList("", "xxx", "abc")));

		assertTrue(Compare.equals((String) "", Arrays.asList("", "abc", "xxx", "")));
		assertTrue(Compare.equals((String) "abc", Arrays.asList("", "xxx", "abc", "")));
		assertFalse(Compare.equals((String) "yyy", Arrays.asList("", "xxx", "abc", "")));
	}
	
}
