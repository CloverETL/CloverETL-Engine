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
package org.jetel.util.protocols;

import org.jetel.test.CloverTestCase;

/**
 * @author martin (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 24. 10. 2017
 */
public class UserInfoTest extends CloverTestCase {

	public void testFromString() {
		UserInfo userInfo;
		
		userInfo = UserInfo.fromString(null);
		assertEquals("", userInfo.getUser());
		assertEquals(null, userInfo.getPassword());
		
		userInfo = UserInfo.fromString("");
		assertEquals("", userInfo.getUser());
		assertEquals(null, userInfo.getPassword());

		userInfo = UserInfo.fromString("a");
		assertEquals("a", userInfo.getUser());
		assertEquals(null, userInfo.getPassword());

		userInfo = UserInfo.fromString(":");
		assertEquals("", userInfo.getUser());
		assertEquals(null, userInfo.getPassword());

		userInfo = UserInfo.fromString("a:");
		assertEquals("a", userInfo.getUser());
		assertEquals(null, userInfo.getPassword());

		userInfo = UserInfo.fromString(":a");
		assertEquals("", userInfo.getUser());
		assertEquals("a", userInfo.getPassword());

		userInfo = UserInfo.fromString("a:b");
		assertEquals("a", userInfo.getUser());
		assertEquals("b", userInfo.getPassword());

		userInfo = UserInfo.fromString("te%24st1:test%3A");
		assertEquals("te$st1", userInfo.getUser());
		assertEquals("test:", userInfo.getPassword());

	}
	
}
