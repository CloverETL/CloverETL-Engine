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
package org.jetel.util;

import junit.framework.TestCase;

/**
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 9.7.2012
 */
public class CompareUtilsTest extends TestCase {

	public void testEquals() {
		
		assertTrue(CompareUtils.equals(null, null));
		assertFalse(CompareUtils.equals(null, new Object()));
		assertFalse(CompareUtils.equals(new Object(), new Object()));
		
		Object o = new Object();
		assertTrue(CompareUtils.equals(o, o));

		assertTrue(CompareUtils.equals(new Integer("123456"), new Integer("123456")));
		assertFalse(CompareUtils.equals(new Integer("123456"), new Integer("123457")));
	}
	
	public void testCompare() {
		assertTrue(CompareUtils.compare((String) null, (String) null) == 0);
		assertTrue(CompareUtils.compare(null, "abc") < 0);
		assertTrue(CompareUtils.compare("abc", null) > 0);
		assertTrue(CompareUtils.compare("abc", "abc") == 0);
		assertTrue(CompareUtils.compare("abc", "bcd") < 0);
		assertTrue(CompareUtils.compare("bcd", "abc") > 0);
	}
	
}
