package org.jetel.data.primitive;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import junit.framework.TestCase;

/**
 * String formatter/validator.
 * 
 * @author Martin Varecha <martin.varecha@@javlinconsulting.cz> 
 * (c) JavlinConsulting s.r.o. 
 * www.javlinconsulting.cz
 * @created Nov 5, 2007
 */
public class StringFormat {

	Pattern regExpPattern = null;

	public StringFormat(String regExp) {
		this.regExpPattern = Pattern.compile(regExp);
	}

	/**
	 * Returns true if entire specified string matches with regExp.
	 * 
	 * @param string
	 * @return
	 */
	public boolean matches(String string) {
		boolean retval = regExpPattern.matcher(string).matches();
		return retval;
	}

	/**
	 * This method formats specified text due to regExp and outputFormat. There
	 * are 3 possibilities:
	 * <ul>
	 * <li>regExp doesnt contain groups: 
	 * 			method returns whole content of first matching region</li>
	 * <li>regExp contains groups and outputFormat!=null: 
	 * 			method returns text with injected groups of first matching region; 
	 * 			injected groups should be specified in outputFormat by its index $1 $2 etc.</li>
	 * <li>regExp contains groups and outputFormat==null: 
	 * 			method returns concatenated all groups of first matching region</li>
	 * </ul>
	 * If there is no matching region in specified string, returns null
	 * 
	 * @param inputText
	 * @param outputformat -
	 *            any text mixed with group indexes $1 $2 etc.; 
	 *            may be null as described above
	 * @return
	 */
	public String format(String text, String outputFormat) {
		StringBuilder result = new StringBuilder();
		Matcher m = regExpPattern.matcher(text);
		if (m.find()) {
			if (m.groupCount() == 0)
				result.append(m.group());
			else {
				if (outputFormat != null)
					result.append(m.replaceFirst(outputFormat));
				else {
					for (int i = 1; i <= m.groupCount(); i++) {
						result.append(m.group(i));
					}// for
				}
			}
		} else {
			return null;
		}
		return result.toString();
	}

	/**
	 * Creates new instance with specified regExp.
	 * 
	 * @param regExp
	 * @return
	 */
	public static StringFormat create(String regExp) {
		return new StringFormat(regExp);
	}

	/**
	 * Test case
	 */
	public static class Test extends TestCase {

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
			this.assertTrue(StringFormat.create("\\w*").matches("bubak"));
			this.assertTrue(StringFormat.create("\\W*").matches("/'[];,"));
		}
	}

	public Pattern getPattern() {
		return regExpPattern;
	}
}
