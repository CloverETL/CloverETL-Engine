/*
 *  jETeL/Clover - Java based ETL application framework.
 *  Copyright (C) 2002  David Pavlis
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 2 of the License, or
 *  (at your option) any later version.
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 * Created on Mar 26, 2003
 *
 */
package org.jetel.util;

import org.jetel.test.CloverTestCase;
import org.jetel.util.string.StringUtils;

/**
 * @author Wes Maciorowski
 * @version 1.0
 * 
 * JUnit tests for org.jetel.util.StringUtils class.
 * 
 */

public class StringUtilsTest extends CloverTestCase {

	private String controlString1;

	private String resultString1;

	private String controlString2;

	private String resultString2;

	private Object[] messages3;

	private int[] sizes3;

	private String resultString3;

	private Object[] messages4;

	private int[] sizes4;

	private String resultString4;

	private Object[] messages5;

	private int[] sizes5;

	private String resultString5;

	@Override
	protected void setUp() {
		initEngine();
	    
		controlString1 = "ala\n\rr\t\n";

		resultString1 = "ala\\n\\rr\\t\\n";

		controlString2 = "\\\nn\\nn\\nn\\\\r";

		resultString2 = "\\\\\\nn\\\\nn\\\\nn\\\\\\\\r";

		// for test_formatString test1

		messages3 = new Object[1];

		messages3[0] = "blah blah";

		sizes3 = new int[1]; // the following sizes should return

		sizes3[0] = 7; // "blah bl"

		resultString3 = "blah bl";

		// for test_formatString test2

		messages4 = new Object[1];

		messages4[0] = "blah blah";

		sizes4 = new int[1]; // the following sizes should return

		sizes4[0] = 15; // "blah bl"

		resultString4 = "      blah blah";

		// for test_formatString test3

		messages5 = new Object[6];

		messages5[0] = "blah blah";

		messages5[1] = "";

		messages5[2] = "";

		messages5[3] = "blah blah";

		messages5[4] = "blah blah";

		messages5[5] = "blah blah";

		sizes5 = new int[6]; // the following sizes should return

		sizes5[0] = 7; // "blah bl"

		sizes5[1] = 7; // " "

		sizes5[2] = -7; // " "

		sizes5[3] = -5; // "blah "

		sizes5[4] = 0; // ""

		sizes5[5] = 15; // " blah blah"

		resultString5 = "blah bl              blah       blah blah";

	}

	@Override
	protected void tearDown() {

		controlString1 = null;

		resultString1 = null;

		controlString2 = null;

		resultString2 = null;

		messages3 = null;

		sizes3 = null;

		resultString3 = null;

	}

	/**
	 * Test for
	 * 
	 * @link org.jetel.util.StringUtils.specCharToString(String controlString)
	 * 
	 */

	public void test_specCharToString() {

		String tmp = null;

		tmp = StringUtils.specCharToString(controlString1);

		assertEquals(resultString1, tmp);

		tmp = StringUtils.specCharToString(controlString2);

		assertEquals(resultString2, tmp);

	}

	/**
	 * Test for
	 * 
	 * @link org.jetel.util.StringUtils.stringToSpecChar(String controlString)
	 * 
	 */

	public void test_stringToSpecChar() {

		String tmp = null;

		tmp = StringUtils.stringToSpecChar(resultString1);

		System.out.println("Resultstr1: " + resultString1);

		assertEquals(controlString1, tmp);

		tmp = StringUtils.stringToSpecChar(resultString2);

		System.out.println("Resultstr2: " + resultString2);

		System.out.println("Ctrl: " + tmp);

		assertEquals(controlString2, tmp);

	}

	/**
	 * Test for
	 * 
	 * @link org.jetel.util.StringUtils.formatString(Object[] messages, int[] sizes)
	 * 
	 */

	public void test_1_formatString() {

		String tmp = null;

		tmp = StringUtils.formatString(messages3, sizes3);

		assertEquals(resultString3, tmp);

	}

	/**
	 * Test for
	 * 
	 * @link org.jetel.util.StringUtils.formatString(Object[] messages, int[] sizes)
	 * 
	 */

	public void test_2_formatString() {

		String tmp = null;

		tmp = StringUtils.formatString(messages4, sizes4);

		assertEquals(resultString4, tmp);

	}

	/**
	 * Test for
	 * 
	 * @link org.jetel.util.StringUtils.formatString(Object[] messages, int[] sizes)
	 * 
	 */

	public void test_3_formatString() {

		String tmp = null;

		tmp = StringUtils.formatString(messages5, sizes5);

		assertEquals(resultString5, tmp);

	}

	public void test_isNumber() {
		String str = "123456789";
		assertTrue(StringUtils.isNumber(str));
		System.out.println("Oryginal: " + str + ", parsed: " + Double.parseDouble(str));

		str = "1234.56789";
		assertTrue(StringUtils.isNumber(str));
		System.out.println("Oryginal: " + str + ", parsed: " + Double.parseDouble(str));

		assertFalse(StringUtils.isNumber("123,456789"));
		assertFalse(StringUtils.isNumber("123.45678.9"));

		str = "1234e56789";
		assertTrue(StringUtils.isNumber(str));
		System.out.println("Oryginal: " + str + ", parsed: " + Double.parseDouble(str));

		str = "1234E+56789";
		assertTrue(StringUtils.isNumber(str));
		System.out.println("Oryginal: " + str + ", parsed: " + Double.parseDouble(str));

		str = "1234e-56789";
		assertTrue(StringUtils.isNumber(str));
		System.out.println("Oryginal: " + str + ", parsed: " + Double.parseDouble(str));

		str = "-1234E+56789";
		assertTrue(StringUtils.isNumber(str));
		System.out.println("Oryginal: " + str + ", parsed: " + Double.parseDouble(str));

		assertFalse(StringUtils.isNumber("123456789e"));
		assertFalse(StringUtils.isNumber("123456789e-"));
		assertFalse(StringUtils.isNumber("123456dj789e"));
		assertFalse(StringUtils.isNumber("d123456789"));

		str = "1234E+56";
		assertTrue(StringUtils.isNumber(str));
		System.out.println("Oryginal: " + str + ", parsed: " + Double.parseDouble(str));
	}

	public void testSplit() throws Exception {
		String str = "f1;f2;f3";
		String[] splitResult = StringUtils.split(str);
		assertEquals(3, splitResult.length);
		assertEquals("f1", splitResult[0]);
		assertEquals("f2", splitResult[1]);
		assertEquals("f3", splitResult[2]);

		str = "f1;f2; f3;";
		splitResult = StringUtils.split(str);
		assertEquals(3, splitResult.length);
		assertEquals("f1", splitResult[0]);
		assertEquals("f2", splitResult[1]);
		assertEquals("f3", splitResult[2]);

		str = "f1|f2:	f3; f4:f5|f6:";
		splitResult = StringUtils.split(str);
		assertEquals(6, splitResult.length);
		assertEquals("f1", splitResult[0]);
		assertEquals("f2", splitResult[1]);
		assertEquals("f3", splitResult[2]);
		assertEquals("f4", splitResult[3]);
		assertEquals("f5", splitResult[4]);
		assertEquals("f6", splitResult[5]);

		str = "f1;f2:=my_value;f3;";
		splitResult = StringUtils.split(str);
		assertEquals(3, splitResult.length);
		assertEquals("f1", splitResult[0]);
		assertEquals("f2:=my_value", splitResult[1]);
		assertEquals("f3", splitResult[2]);

		str = "f1:f2:=my_value;f3;f\":4\"";
		splitResult = StringUtils.split(str);
		assertEquals(4, splitResult.length);
		assertEquals("f1", splitResult[0]);
		assertEquals("f2:=my_value", splitResult[1]);
		assertEquals("f3", splitResult[2]);
		assertEquals("f\":4\"", splitResult[3]);

		str = "f1;f2:=\"my_values:1;2\";f3;";
		splitResult = StringUtils.split(str);
		assertEquals(3, splitResult.length);
		assertEquals("f1", splitResult[0]);
		assertEquals("f2:=\"my_values:1;2\"", splitResult[1]);
		assertEquals("f3", splitResult[2]);

		str = "f1;f2=\"my_values1\"+my_values2;f3;";
		splitResult = StringUtils.split(str);
		assertEquals(3, splitResult.length);
		assertEquals("f1", splitResult[0]);
		assertEquals("f2=\"my_values1\"+my_values2", splitResult[1]);
		assertEquals("f3", splitResult[2]);

		str = "curentrecno=random(\"0\",\"10\");caudrecno=random(\"\",\"\");maudrecno=random(\"\",\"\")";
		splitResult = StringUtils.split(str);
		assertEquals(3, splitResult.length);
		assertEquals("curentrecno=random(\"0\",\"10\")", splitResult[0]);
		assertEquals("caudrecno=random(\"\",\"\")", splitResult[1]);
		assertEquals("maudrecno=random(\"\",\"\")", splitResult[2]);

		str = "string=random(\"3\",\"5\");date=random(\"01-02-2002 12:00:00\",\"29-02-2008 12:00:00\")";
		splitResult = StringUtils.split(str);
		assertEquals(2, splitResult.length);
		assertEquals("string=random(\"3\",\"5\")", splitResult[0]);
		assertEquals("date=random(\"01-02-2002 12:00:00\",\"29-02-2008 12:00:00\")", splitResult[1]);
	}
	
	public void testTranslateBinarySearch(){
		assertEquals("autogus", StringUtils.translateBinarySearch("autobus", "b", "g"));
		assertEquals("agtobgs", StringUtils.translateBinarySearch("autobus", "u", "g"));
		assertEquals("atobs", StringUtils.translateBinarySearch("autobus", "u", ""));
		assertEquals("", StringUtils.translateBinarySearch("autobus", "autobus", ""));
		assertEquals("autobus", StringUtils.translateBinarySearch("autobus", "autobus", "autobus"));
		assertEquals("aaaaaaa", StringUtils.translateBinarySearch("autobus", "autobus", "aaaaaaa"));
		assertEquals("", StringUtils.translateBinarySearch("", "autobus", "sdfas"));
		assertEquals("", StringUtils.translateBinarySearch("", "", ""));
		assertEquals("butocus", StringUtils.translateBinarySearch("autobus", "ab", "bc"));
	}

	public void testTranslateSequentialSearch(){
		assertEquals("autogus", StringUtils.translateSequentialSearch("autobus", "b", "g"));
		assertEquals("agtobgs", StringUtils.translateSequentialSearch("autobus", "u", "g"));
		assertEquals("atobs", StringUtils.translateSequentialSearch("autobus", "u", ""));
		assertEquals("", StringUtils.translateSequentialSearch("autobus", "autobus", ""));
		assertEquals("autobus", StringUtils.translateSequentialSearch("autobus", "autobus", "autobus"));
		assertEquals("aaaaaaa", StringUtils.translateSequentialSearch("autobus", "autobus", "aaaaaaa"));
		assertEquals("", StringUtils.translateSequentialSearch("", "autobus", "sdfas"));
		assertEquals("", StringUtils.translateSequentialSearch("", "", ""));
		assertEquals("butocus", StringUtils.translateSequentialSearch("autobus", "ab", "bc"));
	}

	public void testTranslateMapSearch(){
		assertEquals("autogus", StringUtils.translateMapSearch("autobus", "b", "g"));
		assertEquals("agtobgs", StringUtils.translateMapSearch("autobus", "u", "g"));
		assertEquals("atobs", StringUtils.translateMapSearch("autobus", "u", ""));
		assertEquals("", StringUtils.translateMapSearch("autobus", "autobus", ""));
		assertEquals("autobus", StringUtils.translateMapSearch("autobus", "autobus", "autobus"));
		assertEquals("aaaaaaa", StringUtils.translateMapSearch("autobus", "autobus", "aaaaaaa"));
		assertEquals("", StringUtils.translateMapSearch("", "autobus", "sdfas"));
		assertEquals("", StringUtils.translateMapSearch("", "", ""));
		assertEquals("butocus", StringUtils.translateMapSearch("autobus", "ab", "bc"));
	}


}

/*
 * End class testStringUtils
 */
