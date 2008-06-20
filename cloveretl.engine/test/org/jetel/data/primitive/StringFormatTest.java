package org.jetel.data.primitive;

import org.jetel.test.CloverTestCase;

/**
 * 
 * @author Martin Varecha <martin.varecha@@javlinconsulting.cz>
 * (c) JavlinConsulting s.r.o.
 * www.javlinconsulting.cz
 * @created Nov 7, 2007
 */
public class StringFormatTest extends CloverTestCase {

	private static void t(String regexp, String expected, String input, String outputFormat) {
		String result = StringFormat.create(regexp).format(input, outputFormat);
		assertEquals(expected, result);
	}

	public void testFormat() throws Exception {
		t("(\\d*)(?:\\D*)(\\d*)", "00420123456789", "00420- -123456789", null);
		t("(?:\\d*)(?:\\D*)(\\d*)", "+420 123456789", "00420- -123456789", "+420 $1");
		t(".*", "abcd", "abcd", null);
		t("(\\w+)(\\s)\\s*(?:\\w+\\s+)?(\\w+)", "Martin Varecha", "Martin Bubak Varecha", null);
		t("(\\w+)(\\s)\\s*(?:\\w+\\s+)?(\\w+)", "Martin Varecha", "Martin Varecha", null);
		t("(\\w+)\\s*(?:\\w+\\s+)?(\\w+)", "Varecha Martin", "Martin Varecha", "$2 $1");
		t("(\\w+)\\s*(?:\\w+\\s+)?(\\w+)", "lastName:Varecha firstName:Martin", "Martin Bubak Varecha", "lastName:$2 firstName:$1");
		assertTrue(StringFormat.create("\\w*").matches("bubak"));
		assertTrue(StringFormat.create("\\W*").matches("/'[];,"));
	}
}