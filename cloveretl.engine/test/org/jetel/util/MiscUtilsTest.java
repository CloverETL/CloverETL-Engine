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

import java.util.Locale;

import org.jetel.test.CloverTestCase;

/**
 * @author Martin Zatopek (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 5.7.2010
 */
public class MiscUtilsTest extends CloverTestCase {
	
	@Override
	protected void setUp() throws Exception {
		initEngine();
	}

	public void testLocale() {
		Locale csLocale = MiscUtils.createLocale("cs");
		assertEquals(csLocale, new Locale("cs"));
		assertEquals(MiscUtils.localeToString(csLocale), "cs");
		
		Locale csCZLocale = MiscUtils.createLocale("cs.CZ");
		assertEquals(csCZLocale, new Locale("cs", "CZ"));
		assertEquals(MiscUtils.localeToString(csCZLocale), "cs.CZ");
		
		Locale enLocale = MiscUtils.createLocale("en");
		assertEquals(enLocale, new Locale("en"));
		assertEquals(MiscUtils.localeToString(enLocale), "en");
		
		Locale enUSLocale = MiscUtils.createLocale("en.US");
		assertEquals(enUSLocale, new Locale("en", "US"));
		assertEquals(MiscUtils.localeToString(enUSLocale), "en.US");
	}
	
	public void testExceptionChainToMessage() {
		assertEquals("", MiscUtils.exceptionChainToMessage(null, null));
		
		assertEquals("", MiscUtils.exceptionChainToMessage("", null));
		
		assertEquals("abc", MiscUtils.exceptionChainToMessage("abc", null));
		
		assertEquals("", MiscUtils.exceptionChainToMessage(null, new Exception()));

		assertEquals("", MiscUtils.exceptionChainToMessage("", new Exception()));

		assertEquals("abc", MiscUtils.exceptionChainToMessage("abc", new Exception()));

		assertEquals("first", MiscUtils.exceptionChainToMessage(null, new Exception("first")));

		assertEquals("first", MiscUtils.exceptionChainToMessage("", new Exception("first")));
		
		assertEquals("abc\nCaused by: first", MiscUtils.exceptionChainToMessage("abc", new Exception("first")));

		assertEquals("abc\nCaused by: first", MiscUtils.exceptionChainToMessage("abc", new Exception("first", new Exception())));

		assertEquals("abc\nCaused by: first", MiscUtils.exceptionChainToMessage("abc", new Exception("first", new Exception(""))));

		assertEquals("abc\nCaused by: first", MiscUtils.exceptionChainToMessage("abc", new Exception("first", new Exception("first"))));

		assertEquals("abc\nCaused by: first\nCaused by: second", MiscUtils.exceptionChainToMessage("abc", new Exception("first", new Exception("second"))));

		assertEquals("abc\nCaused by: first\nCaused by: second", MiscUtils.exceptionChainToMessage("abc", new Exception("first", new Exception(null, new Exception("second")))));

		assertEquals("abc\nCaused by: first\nCaused by: second", MiscUtils.exceptionChainToMessage("abc", new Exception("first", new Exception("", new Exception("second")))));

		assertEquals("abc\nCaused by: first\nCaused by: second", MiscUtils.exceptionChainToMessage("abc", new Exception("first", new Exception("first", new Exception("second")))));
	}

	public void testGetEnvSafe() {
		assertTrue(MiscUtils.getEnvSafe(null) == null);
		assertTrue(MiscUtils.getEnvSafe("") == null);
		assertTrue(MiscUtils.getEnvSafe("___XXX___") == null);
		if (!System.getenv().isEmpty()) {
			assertEquals(MiscUtils.getEnvSafe(System.getenv().keySet().iterator().next()), System.getenv(System.getenv().keySet().iterator().next()));
		}
	}

}
