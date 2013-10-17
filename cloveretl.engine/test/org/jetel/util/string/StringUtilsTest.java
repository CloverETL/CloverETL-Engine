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
package org.jetel.util.string;

import java.nio.CharBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

import org.jetel.test.CloverTestCase;

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
	protected void setUp() throws Exception {
		super.setUp();
	    
		controlString1 = "a\u0007la\n\rr\t\n";

		resultString1 = "a\\u0007la\\n\\rr\\t\\n";

		controlString2 = "\\\nn\\nn\\nn\\\\r\u0007";

		resultString2 = "\\\\\\nn\\\\nn\\\\nn\\\\\\\\r\\u0007";

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

		assertEquals(";", StringUtils.stringToSpecChar("\\u003B"));
		assertEquals("\\", StringUtils.stringToSpecChar("\\"));
		assertEquals("\\u", StringUtils.stringToSpecChar("\\u"));
		assertEquals("a\\", StringUtils.stringToSpecChar("a\\"));
		assertEquals("\\a", StringUtils.stringToSpecChar("\\a"));
		assertEquals("\\u00XX", StringUtils.stringToSpecChar("\\u00XX"));
		assertEquals(";a", StringUtils.stringToSpecChar("\\u003ba"));
		assertEquals("b;a", StringUtils.stringToSpecChar("b\\u003ba"));
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
		
		assertFalse(StringUtils.isNumber(""));
		assertEquals(-1, StringUtils.isInteger(""));
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


	public void testMetaphone() {
		assertEquals("ATL", StringUtils.metaphone("ADELLE", 10));
		assertEquals("ANTRSN", StringUtils.metaphone("Anderson", 10));
		assertEquals("ARLN", StringUtils.metaphone("Arlene", 10));
		assertEquals("AKST", StringUtils.metaphone("August", 10));
		assertEquals("B0N", StringUtils.metaphone("BETHANIE", 10));
		assertEquals("BRTN", StringUtils.metaphone("BRITTANY", 10));
		assertEquals("BF", StringUtils.metaphone("BUFFY", 10));
		assertEquals("KRLN", StringUtils.metaphone("CAROLINE", 10));
		assertEquals("XRS", StringUtils.metaphone("CHRISSIE", 10));
		assertEquals("KLNTN", StringUtils.metaphone("Clinton", 10));
		assertEquals("KLBRT", StringUtils.metaphone("Colbert", 10));
		assertEquals("SN0", StringUtils.metaphone("CYNTHIA", 10));
		assertEquals("TLX", StringUtils.metaphone("DELICIA", 10));
		assertEquals("TKSTR", StringUtils.metaphone("Dexter", 10));
		assertEquals("TN", StringUtils.metaphone("DIANA", 10));
		assertEquals("TLN", StringUtils.metaphone("DYLAN", 10));
		assertEquals("ELFS", StringUtils.metaphone("Elvis", 10));
		assertEquals("ESMRLT", StringUtils.metaphone("Esmeralda", 10));
		assertEquals("E0N", StringUtils.metaphone("ETHAN", 10));
		assertEquals("FRNK", StringUtils.metaphone("FRANKIE", 10));
		assertEquals("JNFF", StringUtils.metaphone("GENEVIEVE", 10));
		assertEquals("KRHM", StringUtils.metaphone("GRAHAM", 10));
		assertEquals("KRFN", StringUtils.metaphone("Griffin", 10));
		assertEquals("KWNTLN", StringUtils.metaphone("Gwendoline", 10));
		assertEquals("HNTRSN", StringUtils.metaphone("Henderson", 10));
		assertEquals("IRM", StringUtils.metaphone("Irma", 10));
		assertEquals("JMS", StringUtils.metaphone("James", 10));
		assertEquals("JNT", StringUtils.metaphone("JANNETTE", 10));
		assertEquals("JKLN", StringUtils.metaphone("JAQUELINE", 10));
		assertEquals("JRMN", StringUtils.metaphone("JERMAINE", 10));
		assertEquals("JN0N", StringUtils.metaphone("JOHNATHAN", 10));
		assertEquals("JLN", StringUtils.metaphone("JULIANA", 10));
		assertEquals("KLN", StringUtils.metaphone("KAILYN", 10));
		assertEquals("K0LN", StringUtils.metaphone("KATHLEEN", 10));
		assertEquals("KRMT", StringUtils.metaphone("KERMIT", 10));
		assertEquals("KPLNK", StringUtils.metaphone("Kipling", 10));
		assertEquals("KRSTN", StringUtils.metaphone("Krystine", 10));
		assertEquals("LS", StringUtils.metaphone("LACEY", 10));
		assertEquals("LRNS", StringUtils.metaphone("Lawrence", 10));
		assertEquals("LNT", StringUtils.metaphone("LINDA", 10));
		assertEquals("LSNT", StringUtils.metaphone("Lucinda", 10));
		assertEquals("LNTS", StringUtils.metaphone("Lyndsay", 10));
		assertEquals("MKNS", StringUtils.metaphone("MACKENZIE", 10));
		assertEquals("MRKRT", StringUtils.metaphone("MARGARET", 10));
		assertEquals("MKNS", StringUtils.metaphone("MCKENZIE", 10));
		assertEquals("MXL", StringUtils.metaphone("MITCHELL", 10));
		assertEquals("MNTK", StringUtils.metaphone("MONTAGUE", 10));
		assertEquals("NLSN", StringUtils.metaphone("NELSON", 10));
		assertEquals("NRWT", StringUtils.metaphone("Norwood", 10));
		assertEquals("PTNS", StringUtils.metaphone("Patience", 10));
		assertEquals("PKSTN", StringUtils.metaphone("Paxton", 10));
		assertEquals("PS", StringUtils.metaphone("PEACE", 10));
		assertEquals("PRSKH", StringUtils.metaphone("PORSCHE", 10));
		assertEquals("RJNLT", StringUtils.metaphone("Reginald", 10));
		assertEquals("RT", StringUtils.metaphone("RODDY", 10));
		assertEquals("RLNT", StringUtils.metaphone("ROLLAND", 10));
		assertEquals("RSFLT", StringUtils.metaphone("Roosvelt", 10));
		assertEquals("SXFRL", StringUtils.metaphone("SACHEVERELL", 10));
		assertEquals("XNN", StringUtils.metaphone("SHANNON", 10));
		assertEquals("SN", StringUtils.metaphone("SIENNA", 10));
		assertEquals("SK", StringUtils.metaphone("SKY", 10));
		assertEquals("TLL", StringUtils.metaphone("TALLULAH", 10));
		assertEquals("0MS", StringUtils.metaphone("THOMAS", 10));
		assertEquals("TM", StringUtils.metaphone("TOMMY", 10));
		assertEquals("TRFRT", StringUtils.metaphone("Trafford", 10));
	}
	
	public void testNYSIIS() throws Exception {
		assertEquals("BRAN", StringUtils.NYSIIS("Brian"));
		assertEquals("BRAN", StringUtils.NYSIIS("Brown"));
		assertEquals("BRAN", StringUtils.NYSIIS("Brun"));
		assertEquals("CAP", StringUtils.NYSIIS("Capp"));
		assertEquals("CAP", StringUtils.NYSIIS("Cope"));
		assertEquals("CAP", StringUtils.NYSIIS("Copp"));
		assertEquals("CAP", StringUtils.NYSIIS("Kipp"));
		assertEquals("DAN", StringUtils.NYSIIS("Dane"));
		assertEquals("DAN", StringUtils.NYSIIS("Dean"));
		assertEquals("DAD", StringUtils.NYSIIS("Dent"));
		assertEquals("DAN", StringUtils.NYSIIS("Dionne"));
		assertEquals("SNAT", StringUtils.NYSIIS("Smith"));
		assertEquals("SNAT", StringUtils.NYSIIS("Schmit"));
		assertEquals("SNAD", StringUtils.NYSIIS("Schmidt"));
		assertEquals("TRANAN", StringUtils.NYSIIS("Trueman"));
		assertEquals("TRANAN", StringUtils.NYSIIS("Truman"));
	}
	
	public void testStripComments() {
		String transform = "//comment\n"
			+ "function a() { /* comment */\n"
			+ "// comment\n"
			+ "		metoda(A);\n"
			+ "/*\n"
			+ "	multiline comment\n"
			+ " one more line\n"
			+ " end of comment */\n"
			+ "}\n";
		System.out.println(CommentsProcessor.stripComments(transform));
	}

	public void testReplaceVariables(){
		final Properties vars = getVariablesPropertiesDefault();
		assertEquals("", StringUtils.replaceVariables("", vars, "${", "}"));
		assertEquals("skldfja f  fj", StringUtils.replaceVariables("skldfja f  fj", vars, "${", "}"));
		assertEquals("skldfja ikarus  fj", StringUtils.replaceVariables("skldfja ${bus}  fj", vars, "${", "}"));
		assertEquals("skldfja ikaruspinguin  fj", StringUtils.replaceVariables("skldfja ${bus}${animal}  fj", vars, "${", "}"));
		assertEquals("skldfja ikarusjjjjpinguin  fj", StringUtils.replaceVariables("skldfja ${bus}jjjj${animal}  fj", vars, "${", "}"));
		assertEquals("skldfja   fj", StringUtils.replaceVariables("skldfja ${empty}  fj", vars, "${", "}"));
		assertEquals("", StringUtils.replaceVariables("${empty}", vars, "${", "}"));
		assertEquals("ikarus", StringUtils.replaceVariables("${bus}", vars, "${", "}"));
		assertEquals("pinguinikarus", StringUtils.replaceVariables("${animal}${bus}", vars, "${", "}"));
		assertEquals("", StringUtils.replaceVariables("${empty}", vars, "${", "}"));
	}

	public void testIndexOf() {
		assertEquals("abcdedfg".indexOf("cde", 0), StringUtils.indexOf("abcdedfg", "cde", 0));
		assertEquals("abcdedfg".indexOf("cde", 2), StringUtils.indexOf("abcdedfg", "cde", 2));
		assertEquals("abcdedfg".indexOf("cde", 4), StringUtils.indexOf("abcdedfg", "cde", 4));
		assertEquals("abcdedfg".indexOf("abc", 0), StringUtils.indexOf("abcdedfg", "abc", 0));
		assertEquals("abcdedfg".indexOf("abc", 100), StringUtils.indexOf("abcdedfg", "abc", 100));
		assertEquals("abcdedfg".indexOf("dfg", 0), StringUtils.indexOf("abcdedfg", "dfg", 0));
		assertEquals("abcdedfg".indexOf("dfg", 5), StringUtils.indexOf("abcdedfg", "dfg", 5));
		assertEquals("abcdedfg".indexOf("cba", 0), StringUtils.indexOf("abcdedfg", "cba", 0));
		assertEquals("abcdedfg".indexOf("cba", 1), StringUtils.indexOf("abcdedfg", "cba", 1));
		assertEquals("aaaaaaaaaaabc".indexOf("aaaa", 0), StringUtils.indexOf("aaaaaaaaaaabc", "aaaa", 0));
		assertEquals("aaaaaaaaaaabc".indexOf("aaaa", 6), StringUtils.indexOf("aaaaaaaaaaabc", "aaaa", 6));
		assertEquals("aaaaaaaaaaabc".indexOf("aaaa", 100), StringUtils.indexOf("aaaaaaaaaaabc", "aaaa", 100));
		assertEquals("aaaaaaaaaaabc".indexOf("abc", 0), StringUtils.indexOf("aaaaaaaaaaabc", "abc", 0));
		assertEquals("aaaaaaaaaaabc".indexOf("abc", 10), StringUtils.indexOf("aaaaaaaaaaabc", "abc", 10));
	}
	
	public void testStringAbbreviation() {
		assertEquals(StringUtils.abbreviateString("THIS-IS_SOME. TEST OF #WAYS1_2_222", 1, false, false).toString(), "TISTOW");
		assertEquals(StringUtils.abbreviateString("THIS-IS_SOME. TEST OF #WAYS1_2_222", 1, false, true).toString(), "TISTOW2222");
		assertEquals(StringUtils.abbreviateString("THIS-IS_SOME. TEST OF #WAYS1_2_222", 2, false, true).toString(), "TH_IS_SO_TE_OF_WA2222");
	}
	
	public void testSplitWithDelimiter() {
		assertTrue(Arrays.equals(StringUtils.split("a b c", " "), new String[] {"a", "b", "c"}));
		assertTrue(Arrays.equals(StringUtils.split("a  b c", " "), new String[] {"a", "", "b", "c"}));
		assertTrue(Arrays.equals(StringUtils.split("a \"b\" c", " "), new String[] {"a", "\"b\"", "c"}));
		assertTrue(Arrays.equals(StringUtils.split("a \"b d\" c", " "), new String[] {"a", "\"b d\"", "c"}));
		assertTrue(Arrays.equals(StringUtils.split("a \"b \\\" d\" c", " "), new String[] {"a", "\"b \\\" d\"", "c"}));

		assertTrue(Arrays.equals(StringUtils.split("a 'b' c", " "), new String[] {"a", "'b'", "c"}));
		assertTrue(Arrays.equals(StringUtils.split("a 'b d' c", " "), new String[] {"a", "'b d'", "c"}));
		assertTrue(Arrays.equals(StringUtils.split("a 'b \\\" d\' c", " "), new String[] {"a", "'b \\\" d'", "c"}));

		assertTrue(Arrays.equals(StringUtils.split("martin", "rt"), new String[] {"ma", "in"}));
		assertTrue(Arrays.equals(StringUtils.split("martrtin", "rt"), new String[] {"ma", "", "in"}));
		assertTrue(Arrays.equals(StringUtils.split("martartin", "rt"), new String[] {"ma", "a", "in"}));
		

		assertTrue(Arrays.equals(StringUtils.split("\"-P:testUvozovek=uvozovky \\\" Text\" -contexturl \"/media/javlin/clover 3.1 workspace 1/Map Of Records\"", " "),
				new String[] {"\"-P:testUvozovek=uvozovky \\\" Text\"", "-contexturl", "\"/media/javlin/clover 3.1 workspace 1/Map Of Records\""}));
	}
	
	public void testIsQuoted() {
		assertTrue(StringUtils.isQuoted("\"a\""));
		assertTrue(StringUtils.isQuoted("'a'"));
		assertTrue(StringUtils.isQuoted("\"martin\""));
		assertTrue(StringUtils.isQuoted("\"'\""));

		assertFalse(StringUtils.isQuoted("\"'\"a"));
		assertFalse(StringUtils.isQuoted("\""));
		assertFalse(StringUtils.isQuoted("'"));
		assertFalse(StringUtils.isQuoted(""));
		assertFalse(StringUtils.isQuoted("martin"));
	}
	
	public void testUnquote2() {
		assertEquals(StringUtils.unquote2("dummy"), "dummy");
		assertEquals(StringUtils.unquote2("\"dummy\""), "dummy");
		assertEquals(StringUtils.unquote2("a\"dummy\""), "a\"dummy\"");
		assertEquals(StringUtils.unquote2("\"dummy\"a"), "\"dummy\"a");
		assertEquals(StringUtils.unquote2("\""), "\"");
		assertEquals(StringUtils.unquote2("\\"), "\\");
		assertEquals(StringUtils.unquote2("\"\""), "");
		assertEquals(StringUtils.unquote2("\"I said: \\\"hello\\\"\""), "I said: \"hello\"");
		assertEquals(StringUtils.unquote2("I said: \\\"hello\\\""), "I said: \\\"hello\\\"");
		
		assertEquals(StringUtils.unquote2("\"-P:testUvozovek=uvozovky \\\" Text\""), "-P:testUvozovek=uvozovky \" Text");
		assertEquals(StringUtils.unquote2("\"/media/javlin/clover 3.1 workspace 1/Map Of Records\""), "/media/javlin/clover 3.1 workspace 1/Map Of Records");
	}
	
	private Properties getVariablesPropertiesDefault() {
		final Properties ret = new Properties();
		ret.put("bus","ikarus");
		ret.put("animal","pinguin");
		ret.put("empty", "");
		return ret;
	}

	private static void assertArraysEquals(Object[] expected, Object[] actual) {
		boolean equal = Arrays.equals(expected, actual); 
		assertTrue(
				equal ? "" : "Expected: " + Arrays.toString(expected) + ", actual: " + Arrays.toString(actual),
				equal
		);
	}
	
	public void testNormalizeNames() {
		assertEquals("Milasek", StringUtils.normalizeName("Milášek"));
		assertEquals("Mil_ek", StringUtils.normalizeName("Mil%$ek"));
		assertEquals("Mil___ek", StringUtils.normalizeName("Mil%_$ek"));
		assertEquals("_0Mil___ek", StringUtils.normalizeName("0Mil%_$ek"));
		assertEquals(null, StringUtils.normalizeName(null));
		assertEquals("_", StringUtils.normalizeName(""));
		assertEquals("_", StringUtils.normalizeName("_"));
		assertEquals("_", StringUtils.normalizeName("#"));
		assertEquals("__", StringUtils.normalizeName("__"));
		assertEquals("a", StringUtils.normalizeName("%a%"));
		assertEquals("a", StringUtils.normalizeName("%$@%a&*%"));
		assertEquals("a_a", StringUtils.normalizeName("%$@%a!@#$%a&*%"));
		assertEquals("_0", StringUtils.normalizeName("%0%"));
		assertEquals("_0", StringUtils.normalizeName("%$@%0&*%"));
		assertEquals("_0_5", StringUtils.normalizeName("%$@%0!@#$%^5&*%"));
		assertEquals("Milan_Krivanek", StringUtils.normalizeName("\"Milan Křivánek\""));
		assertEquals("_0Milan_Krivanek", StringUtils.normalizeName("\"0Milan Křivánek\""));
		assertEquals("Zlutoucky_kun_upel_dabelske_ody", StringUtils.normalizeName("Žluťoučký kůň úpěl ďábelské ódy"));
		
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < 500; i++) {
			sb.append('a');
		}
		String trimmed = StringUtils.normalizeName(sb.toString());
		assertTrue("The length of the normalized name is greater than 256", trimmed.length() <= 256);
		
		assertArraysEquals(
				new String[] {"unique", "unique1", "unique_1", "unique_2", "unique1_1", "unique_1_1"}, 
				StringUtils.normalizeNames("unique", "unique1", "unique_1", "unique", "unique1", "unique_1")
		);
		assertArraysEquals(
				new String[] {"Milan_Krivanek", "Milan_Krivanek_1"}, 
				StringUtils.normalizeNames("Milan Křivánek", "Milan_Krivanek")
		);
		assertArraysEquals(
				new String[] {"Milan_Krivanek_1", "Milan_Krivanek_2"}, 
				StringUtils.normalizeNames(Arrays.asList("Milan Křivánek", "Milan_Krivanek"), Arrays.asList("Milan_Krivanek"))
		);
	}
	
	public void trimLeading(String trimmed, String expected) {
		assertTrue(StringUtils.trimLeading(new CloverString(trimmed)).equals(new CloverString(expected)));
		assertTrue(StringUtils.trimLeading(new StringBuilder(trimmed)).toString().equals(expected));
		assertTrue(StringUtils.trimLeading(CharBuffer.wrap(trimmed)).toString().equals(expected));
	}

	public void trimTrailing(String trimmed, String expected) {
		assertTrue(StringUtils.trimTrailing(new CloverString(trimmed)).equals(new CloverString(expected)));
		assertTrue(StringUtils.trimTrailing(new StringBuilder(trimmed)).toString().equals(expected));
		assertTrue(StringUtils.trimTrailing(CharBuffer.wrap(trimmed)).toString().equals(expected));
	}

	public void testTrimLeading() {
		trimLeading("", "");
		trimLeading(" ", "");
		trimLeading("   ", "");

		trimLeading("abc", "abc");
		trimLeading(" abc", "abc");
		trimLeading("       abc", "abc");
		trimLeading("	abc", "abc");
		trimLeading("				abc", "abc");
		trimLeading("    	   		   abc", "abc");
	
		trimLeading(" 	 	 	    abc    	  		   ", "abc    	  		   ");
		trimLeading(" 	 	 	    a b c    	  		   ", "a b c    	  		   ");
		trimLeading(" 	 	 	    a	bc    	  		   ", "a	bc    	  		   ");
	}

	public void testTrimTrailing() {
		trimTrailing("", "");
		trimTrailing(" ", "");
		trimTrailing("   ", "");

		trimTrailing("abc ", "abc");
		trimTrailing("abc       ", "abc");
		trimTrailing("abc	", "abc");
		trimTrailing("abc				", "abc");
		trimTrailing("abc    	  		   ", "abc");
	
		trimTrailing(" 	 	 	    abc    	  		   ", " 	 	 	    abc");
		trimTrailing(" 	 	 	    a b c    	  		   ", " 	 	 	    a b c");
		trimTrailing(" 	 	 	    a	bc    	  		   ", " 	 	 	    a	bc");
	}

	public void testEqualsWithNulls() {
		assertTrue(StringUtils.equalsWithNulls(null, null));
		assertFalse(StringUtils.equalsWithNulls("abc", null));
		assertFalse(StringUtils.equalsWithNulls(null, "abc"));
		assertTrue(StringUtils.equalsWithNulls("abc", "abc"));
		assertFalse(StringUtils.equalsWithNulls("abc", "cba"));
		assertFalse(StringUtils.equalsWithNulls("abcdefghtjki", "cba"));
	}
	
	public void testBytesToHexString() {
		assertNull(StringUtils.bytesToHexString(null));
		
		assertEquals("", StringUtils.bytesToHexString(new byte[0]));
		
		byte[] bytes = new byte[1];
		assertEquals("00", StringUtils.bytesToHexString(bytes));

		bytes[0] = 1;
		assertEquals("01", StringUtils.bytesToHexString(bytes));

		bytes[0] = 10;
		assertEquals("0a", StringUtils.bytesToHexString(bytes));

		bytes[0] = 15;
		assertEquals("0f", StringUtils.bytesToHexString(bytes));

		bytes[0] = 16;
		assertEquals("10", StringUtils.bytesToHexString(bytes));

		bytes[0] = 127;
		assertEquals("7f", StringUtils.bytesToHexString(bytes));

		bytes = new byte[] { 1, 10, 15, 16, 127, -127, 0, 63, 64, 65};
		assertEquals("010a0f107f81003f4041", StringUtils.bytesToHexString(bytes));
	}

	public void testHexStringToBytes() {
		assertNull(StringUtils.hexStringToBytes(null));
		
		assertTrue(Arrays.equals(new byte[0], StringUtils.hexStringToBytes("")));
		
		byte[] bytes = new byte[1];
		assertTrue(Arrays.equals(bytes, StringUtils.hexStringToBytes("00")));

		bytes[0] = 1;
		assertTrue(Arrays.equals(bytes, StringUtils.hexStringToBytes("01")));

		bytes[0] = 10;
		assertTrue(Arrays.equals(bytes, StringUtils.hexStringToBytes("0a")));

		bytes[0] = 15;
		assertTrue(Arrays.equals(bytes, StringUtils.hexStringToBytes("0F")));

		bytes[0] = 16;
		assertTrue(Arrays.equals(bytes, StringUtils.hexStringToBytes("10")));

		bytes[0] = 127;
		assertTrue(Arrays.equals(bytes, StringUtils.hexStringToBytes("7F")));

		bytes = new byte[] { 1, 10, 15, 16, 127, -127, 0, 63, 64, 65};
		assertTrue(Arrays.equals(bytes, StringUtils.hexStringToBytes("010A0F107f81003F4041")));
	}

	public void testJoin() {
		assertEquals(null, StringUtils.join(null, null));
		assertEquals(null, StringUtils.join(null, ""));
		assertEquals(null, StringUtils.join(null, "abc"));
		assertEquals("", StringUtils.join(new ArrayList<String>(), null));
		assertEquals("", StringUtils.join(new ArrayList<String>(), ""));
		assertEquals("", StringUtils.join(new ArrayList<String>(), "abc"));
		assertEquals("def", StringUtils.join(Arrays.asList("def"), null));
		assertEquals("def", StringUtils.join(Arrays.asList("def"), ""));
		assertEquals("def", StringUtils.join(Arrays.asList("def"), "abc"));
		assertEquals("defnullghi", StringUtils.join(Arrays.asList("def", "ghi"), null));
		assertEquals("defghi", StringUtils.join(Arrays.asList("def", "ghi"), ""));
		assertEquals("defabcghi", StringUtils.join(Arrays.asList("def", "ghi"), "abc"));
		assertEquals("ja nebojsa", StringUtils.join(Arrays.asList("ja", "nebojsa"), " "));
		assertEquals("jedna;dva;;tri;null;ctyri", StringUtils.join(Arrays.asList("jedna", "dva", "", "tri", null, "ctyri"), ";"));
	}
	
	public void testRemoveNonPrintable() {
		String input;
		
		// remove non-printable
		input = "A\u000b\u000bH\u000b\u000bO\u000b\u000bJ";
		assertEquals("AHOJ", StringUtils.removeNonPrintable(input));
		
		// remove ASCII control characters
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i <= 31; i++) { // space (32) is considered printable 
			sb.appendCodePoint(i);
		}
		sb.appendCodePoint(127); // delete (127)
		assertEquals("", StringUtils.removeNonPrintable(sb.toString()));

		// preserve non-ASCII characters
		input = "\u017Elu\u0165ou\u010Dk\u00FD k\u016F\u0148 \u00FAp\u011Bl \u010F\u00E1belsk\u00E9 \u00F3dy";
		assertEquals(input, StringUtils.removeNonPrintable(input));

		// preserve space
		input = "a b";
		assertEquals(input, StringUtils.removeNonPrintable(input));

		// empty string
		input = "";
		assertEquals(input, StringUtils.removeNonPrintable(input));

		// null
		input = null;
		assertEquals(input, StringUtils.removeNonPrintable(input));

		// blank string
		input = "   ";
		assertEquals(input, StringUtils.removeNonPrintable(input));
	}
	
}

/*
 * End class testStringUtils
 */
