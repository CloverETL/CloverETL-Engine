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
package org.jetel.util.formatter;

import org.jetel.test.CloverTestCase;


/**
 * @author csochor (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created Jul 1, 2010
 */
public class BooleanFormatterFactoryTest extends CloverTestCase {

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		initEngine();
	}

	public void testMF() throws ParseBooleanException{
		final BooleanFormatter bf = BooleanFormatterFactory.createFormatter("$M$F$");
		assertEquals(true, bf.parseBoolean("M"));
		assertEquals(false, bf.parseBoolean("F"));
		
		assertFormatException(bf,"MM");
		assertFormatException(bf,"");
		
		assertEquals("M", bf.formatBoolean(true));
		assertEquals("F", bf.formatBoolean(false));
	}

	public void testM() throws ParseBooleanException{
		final BooleanFormatter bf = BooleanFormatterFactory.createFormatter("$M$$");
		assertEquals(true, bf.parseBoolean("M"));
		assertEquals(false, bf.parseBoolean(""));
		
		assertFormatException(bf,"MM");
		assertFormatException(bf," ");
		assertFormatException(bf,"F");
		
		assertEquals("M", bf.formatBoolean(true));
		assertEquals("", bf.formatBoolean(false));
	}
	
	public void testMAll() throws ParseBooleanException{
		final BooleanFormatter bf = BooleanFormatterFactory.createFormatter("$M$.*$");
		assertEquals(true, bf.parseBoolean("M"));
		assertEquals(false, bf.parseBoolean("F"));
		assertEquals(false, bf.parseBoolean("pingpong"));
		
		assertEquals("M", bf.formatBoolean(true));
		assertEquals("false", bf.formatBoolean(false));
	}
	
	public void testPinguin() throws ParseBooleanException{
		final BooleanFormatter bf = BooleanFormatterFactory.createFormatter("Pinguin");
		assertEquals(true, bf.parseBoolean("Pinguin"));
		assertEquals(false, bf.parseBoolean("false"));
		assertEquals(false, bf.parseBoolean("F"));
		
		assertFormatException(bf,"true");
		
		assertEquals("Pinguin", bf.formatBoolean(true));
		assertEquals("false", bf.formatBoolean(false));
	}
	
	public void testABCD() throws ParseBooleanException{
		final BooleanFormatter bf = BooleanFormatterFactory.createFormatter("?a|b?c|d?b?b?");
		assertEquals(true, bf.parseBoolean("a"));
		assertEquals(true, bf.parseBoolean("b"));
		assertEquals(false, bf.parseBoolean("c"));
		assertEquals(false, bf.parseBoolean("d"));
		
		assertFormatException(bf,"aa");
		assertFormatException(bf,"true");
		assertFormatException(bf,"false");
		
		assertEquals("b", bf.formatBoolean(true));
		assertEquals("b", bf.formatBoolean(false));
	}
	
	private void assertFormatException(BooleanFormatter bf, String string) {
		try{
			bf.parseBoolean(string);
		} catch (ParseBooleanException e) {
			//OK
			return;
		}
		fail("ParseBooleanException expected");
	}

}
