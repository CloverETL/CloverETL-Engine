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
package test.org.jetel.util;

import junit.framework.TestCase;
import org.jetel.util.StringUtils;

/**
 * @author Wes Maciorowski
 * @version 1.0
 * 
 * JUnit tests for org.jetel.util.StringUtils class.
 * 
 */

public class StringUtilsTest extends TestCase {

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

	protected void setUp() {

		controlString1 = "ala\n\rr\t\n";

		resultString1 = "ala\\n\\rr\\t\\n";

		controlString2 = "\\\nn\\nn\\nn\\\\r";

		resultString2 = "\\\\nn\\nn\\nn\\\\r";

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

		sizes5[1] = 7; // "       "

		sizes5[2] = -7; // "       "

		sizes5[3] = -5; // "blah "

		sizes5[4] = 0; // ""

		sizes5[5] = 15; // "      blah blah"

		resultString5 = "blah bl              blah       blah blah";

	}

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
	 *  Test for @link org.jetel.util.StringUtils.specCharToString(String controlString)
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
	 *  Test for @link org.jetel.util.StringUtils.stringToSpecChar(String controlString)
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
	 *  Test for @link org.jetel.util.StringUtils.formatString(Object[] messages, int[] sizes)
	 *
	 */

	public void test_1_formatString() {

		String tmp = null;

		tmp = StringUtils.formatString(messages3, sizes3);

		assertEquals(resultString3, tmp);

	}

	/**
	 *  Test for @link org.jetel.util.StringUtils.formatString(Object[] messages, int[] sizes)
	 *
	 */

	public void test_2_formatString() {

		String tmp = null;

		tmp = StringUtils.formatString(messages4, sizes4);

		assertEquals(resultString4, tmp);

	}

	/**
	 *  Test for @link org.jetel.util.StringUtils.formatString(Object[] messages, int[] sizes)
	 *
	 */

	public void test_3_formatString() {

		String tmp = null;

		tmp = StringUtils.formatString(messages5, sizes5);

		assertEquals(resultString5, tmp);

	}

}

/*
 *  End class testStringUtils
 */
