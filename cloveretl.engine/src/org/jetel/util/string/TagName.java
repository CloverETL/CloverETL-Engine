package org.jetel.util.string;

/**
 * Utility to generate valid XML tag name from arbitrary string. Even that XML allows non-ASCII characters
 * as a tag name, this utility encodes them as well as any special ASCII characters. These characters are encoded as
 * "_xhhhh" where hhhh is the Unicode of the character as a hexa value. 
 * 
 * @author jan.michalica
 */
public class TagName {

	private static final char ENC_SEQ_CHAR = '_';
	private static final String ENC_SEQ_START = ENC_SEQ_CHAR + "x";
	
	/**
	 * Encodes given input string using unicodes.
	 * <pre>
	 * Example:
	 * input: "@Funny*Tag-Name()"
	 * output: "_x0040Funny_x002aTag-Name_x0028_x0029"
	 * </pre>
	 * @param s
	 * @return
	 */
	public static String encode(String s) {
		
		if (s == null) {
			return null;
		}
		
		StringBuilder sb = new StringBuilder(s.length());
		for (int i = 0; i < s.length(); ++i) {
			char c = s.charAt(i);
			if (!('0' <= c && c <= '9' || 'a' <= c && c <= 'z' || 'A' <= c && c <= 'Z' || c == '-' || c == '.') ||
					c == ENC_SEQ_CHAR || (i == 0 && ('0' <= c && c <= '9' || c == '-' || c == '.'))) {
				sb.append(ENC_SEQ_START);
				String scode = Integer.toHexString(c);
				for (int j = 0; j < 4 - scode.length(); ++j) {
					sb.append('0');
				}
				sb.append(scode);
			} else {
				sb.append(c);
			}
		}
		return sb.toString();
	}
	
	/**
	 * Decodes given input string previously encoded by this class.
	 * e.g.:
	 * input: "tag_x0020with_x0020spaces"
	 * output: "tag with spaces"
	 * @param s
	 * @return
	 */
	public static String decode(String s) {
		
		if  (s == null) {
			return null;
		}
		
		StringBuilder sb = new StringBuilder(s.length());
		int pos = 0;
		while (pos < s.length()) {
			char c = s.charAt(pos++);
			if (c == ENC_SEQ_CHAR) {
				try {
					c = (char)Integer.parseInt(s.substring(pos + 1, pos + 5), 16);
					pos += 5;
				} catch (RuntimeException e) {
					// malformed sequence, ignore
				}
			}
			sb.append(c);
		}
		return sb.toString();
	}
}
