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
package org.jetel.util.exec;

import static org.junit.Assert.*;

import org.junit.Test;

/**
 * @author reichman (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created Oct 10, 2018
 */
public class PlatformUtilsTest {

	@Test
	public void testValid() {
		testJavaMajorVersion("1.8.0_181", 8);
		testJavaMajorVersion("9.0.4", 9);
		testJavaMajorVersion("10.0.2", 10);
		testJavaMajorVersion("11", 11);
		testJavaMajorVersion("10.2.b", 10);
	}
	
	@Test
	public void testEmpty() {
		testJavaMajorVersion("", -1);
		testJavaMajorVersion("  ", -1);
		testJavaMajorVersion(null, -1);
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void testInvalid_1() {
		PlatformUtils.getJavaMajorVersion("a.a.a");
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void testInvalid_2() {
		PlatformUtils.getJavaMajorVersion("aaaa");
	}
	
	private void testJavaMajorVersion(String javaVersion, int expectedMajorVersion) {
		int majorVersion = PlatformUtils.getJavaMajorVersion(javaVersion);
		assertEquals(expectedMajorVersion, majorVersion);
	}
}
