package org.jetel.ctl.extensions;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.text.Normalizer;
import java.text.Normalizer.Form;

import org.jetel.ctl.Stack;
import org.jetel.ctl.TransformLangExecutorRuntimeException;
import org.jetel.util.string.UnicodeBlanks;

/**
 * Library wrapper for additional string manipulation functions for CTL2. 
 * 
 * @author Branislav Repcek (branislav.repcek@javlin.eu)
 */
public class StringLibExt extends TLFunctionLibraryExt {

	private static final String LIBRARY_NAME = "StringLib";

	public StringLibExt() {
		super(LIBRARY_NAME);
	}
	
	/**
	 * Pad input string with spaces from right.
	 * 
	 * @param context function call context.
	 * @param input string to pad. Null is allowed and is returned without change.
	 * @param targetLength required target length of the result. Only non-negative length is allowed.
	 * 
	 * @return Input padded with spaces from right. Input longer than the target length is returned unchanged.
	 */
	@TLFunctionAnnotation("Right pad input string to specified length.")
	@CTL2FunctionDeclaration(impl = RPadFunction.class)
	public static final String rpad(TLFunctionCallContext context, String input, Integer targetLength) {
		return rpad(context, input, targetLength, " ");
	}
	
	/**
	 * Pad input string with specified characters from right.
	 * 
	 * @param context function call context.
	 * @param input string to pad. Null is allowed and is returned without change.
	 * @param targetLength required target length of the result. Only non-negative length is allowed.
	 * @param filler character to use as padding. Only one character long strings are allowed.
	 * 
	 * @return Input padded with specified character from right. Input longer than the target length is returned unchanged.
	 */
	@TLFunctionAnnotation("Right pad input string to specified length.")
	@CTL2FunctionDeclaration(impl = RPadFunction.class)
	public static final String rpad(TLFunctionCallContext context, String input, Integer targetLength, String filler) {
		
		if (targetLength < 0) {
			throw new IllegalArgumentException("Negative target string length is not allowed.");
		}

		if (filler == null) {
			throw new NullPointerException("Filler string cannot be null.");
		}
		
		if (filler.length() != 1) {
			throw new IllegalArgumentException("Filler string has to be one character long.");
		}
		
		if (input == null) {
			return null;
		}
		
		if (input.length() >= targetLength) {
			return input;
		}

		StringBuilder builder = new StringBuilder(input);
		for (int i = 0; i < targetLength - input.length(); ++i) {
			builder.append(filler);
		}
		
		return builder.toString();
	}
	
	/**
	 * Pad input string with spaces from left.
	 * 
	 * @param context function call context.
	 * @param input string to pad. Null is allowed and is returned without change.
	 * @param targetLength required target length of the result. Only non-negative length is allowed.
	 * 
	 * @return Input padded with spaces from left. Input longer than the target length is returned unchanged.
	 */
	@TLFunctionAnnotation("Left pad input string to specified length.")
	@CTL2FunctionDeclaration(impl = LPadFunction.class)
	public static final String lpad(TLFunctionCallContext context, String input, Integer targetLength) {
		return lpad(context, input, targetLength, " ");
	}
	
	/**
	 * Pad input string with specified characters from left.
	 * 
	 * @param context function call context.
	 * @param input string to pad. Null is allowed and is returned without change.
	 * @param targetLength required target length of the result. Only non-negative length is allowed.
	 * @param filler character to use as padding. Only one character long strings are allowed.
	 * 
	 * @return Input padded with specified character from left. Input longer than the target length is returned unchanged.
	 */
	@TLFunctionAnnotation("Left pad input string to specified length.")
	@CTL2FunctionDeclaration(impl = LPadFunction.class)
	public static final String lpad(TLFunctionCallContext context, String input, Integer targetLength, String filler) {
		
		if (targetLength < 0) {
			throw new IllegalArgumentException("Negative target string length is not allowed.");
		}
		
		if (filler == null) {
			throw new NullPointerException("Filler string cannot be null.");
		}
		
		if (filler.length() != 1) {
			throw new IllegalArgumentException("Filler string has to be one character long.");
		}
		
		if (input == null) {
			return null;
		}
		
		if (input.length() >= targetLength) {
			return input;
		}

		StringBuilder builder = new StringBuilder();
		for (int i = 0; i < targetLength - input.length(); ++i) {
			builder.append(filler);
		}
		
		return builder.append(input).toString();
	}
	
	/**
	 * <p>Find the last occurrence of substring within given string searching backwards from given position.</p>
	 * 
	 * <p>Function gracefully handles null values. Both searchStr and input can be null. -1 (not found) is returned in
	 * such case.</p>
	 * 
	 * @param context function call context.
	 * @param input string to search in. May be null.
	 * @param searchStr string to search for. May be null.
	 * @param fromIndex starting point of the search. Negative values are treated as zero. If start is greater than the
	 *        length of the input, the whole input is searched.
	 *        
	 * @return Index of the last occurrence of searchStr within input string. -1 is returned if the search string is not
	 *         found in the input string.
	 *         
	 * @see String#lastIndexOf(String, int)
	 */
	@TLFunctionAnnotation("Index of the last occurrence of specified string searching backwards from given index.")
	@CTL2FunctionDeclaration(impl = LastIndexOfFunction.class)
	public static final int lastIndexOf(TLFunctionCallContext context, String input, String searchStr, Integer fromIndex) {
		if (searchStr == null) {
			throw new NullPointerException("Search string is null");
		}
		
		if (input == null) {
			return -1;
		}

		if (fromIndex < 0) {
			fromIndex = 0;
		}
		
		if (fromIndex > input.length()) {
			fromIndex = input.length();
		}

		return input.lastIndexOf(searchStr, fromIndex);
	}

	/**
	 * <p>Find the last occurrence of substring within given string.</p>
	 * 
	 * <p>Function gracefully handles null values. Input can be null - -1 (not found) is returned in
	 * such case.</p>
	 * 
	 * @param context function call context.
	 * @param input string to search in. May be null.
	 * @param searchStr string to search for. May not be null.
	 *        
	 * @return Index of the last occurrence of searchStr within input string. -1 is returned if the search string is not
	 *         found in the input string.
	 *         
	 * @see #lastIndexOf(TLFunctionCallContext, String, String, int)
	 */
	@TLFunctionAnnotation("Index of the last occurrence of specified string.")
	@CTL2FunctionDeclaration(impl = LastIndexOfFunction.class)
	public static final int lastIndexOf(TLFunctionCallContext context, String input, String searchStr) {

		if (input == null) {
			return -1;
		}
		
		return lastIndexOf(context, input, searchStr, input.length());
	}

	/**
	 * <p>Test if specified string starts with given prefix.</p>
	 * 
	 * <p>
	 * Function gracefully handles null input:
	 * <ul>
	 *   <li>null input string has no prefix, false is returned</li>
	 *   <li>Empty prefix is prefix of any non-null input string, true is returned if input is not null</li>
	 * </ul>
	 * </p>
	 * 
	 * @param context function call context.
	 * @param input input string to test. May be null.
	 * @param prefix prefix to test against. May not be null.
	 * 
	 * @return true if input starts with specified prefix, false otherwise.
	 * 
	 * @see String#startsWith(String)
	 */
	@TLFunctionAnnotation("Test if string starts with given prefix.")
	@CTL2FunctionDeclaration(impl = StartsWithFunction.class)
	public static final boolean startsWith(TLFunctionCallContext context, String input, String prefix) {
		if (prefix == null) {
			throw new NullPointerException("Prefix is null");
		}
		
		if (input == null) {
			return false;
		}
		
		if (prefix.equals("")) {
			return true;
		}
		
		return input.startsWith(prefix);
	}
	
	/**
	 * <p>Test if specified string ends with given suffix.</p>
	 * 
	 * <p>
	 * Function gracefully handles null input:
	 * <ul>
	 *   <li>null input string has no suffix, false is returned</li>
	 *   <li>Empty suffix is suffix of any non-null input string, true is returned if input is not null</li>
	 * </ul>
	 * </p>
	 * 
	 * @param context function call context.
	 * @param input input string to test. May be null.
	 * @param suffix suffix to test against. May not be null.
	 * 
	 * @return true if input ends with specified suffix, false otherwise.
	 * 
	 * @see String#endsWith(String)
	 */
	@TLFunctionAnnotation("Test if string ends with given suffix.")
	@CTL2FunctionDeclaration(impl = EndsWithFunction.class)
	public static final boolean endsWith(TLFunctionCallContext context, String input, String suffix) {
		if (suffix == null) {
			throw new NullPointerException("Suffix is null");
		}
		
		if (input == null) {
			return false;
		}
		
		if (suffix.equals("")) {
			return true;
		}

		return input.endsWith(suffix);
	}

	/**
	 * <p>Converts the specified character (Unicode code point) to its UTF-16 representation.</p>
	 * 
	 * <p>If the specified code point is a BMP (Basic Multilingual Plane or Plane 0) value, the resulting string contains
	 * one character with the same value as codePoint.
	 * If the specified code point is a supplementary code point, the result contains corresponding surrogate pair.</p>
	 * 
	 * @param context function call context
	 * @param codePoint a Unicode code point. Cannot be null.
	 * 
	 * @return A string containing Unicode representation of specified code point.
	 * 
	 * @see Character#toChars(int)
	 */
	@TLFunctionAnnotation("Convert Unicode code point to character.")
	@CTL2FunctionDeclaration(impl = CodePointToCharFunction.class)
	public static final String codePointToChar(TLFunctionCallContext context, Integer codePoint) {
		return String.valueOf(Character.toChars(codePoint));
	}
	
	/**
	 * <p>Returns code point at the given position in the input string.</p>
	 * 
	 * <p>If the char value at the given index is in the high-surrogate range, the following index is less than the
	 * length of the CharSequence, and the char value at the following index is in the low-surrogate range,
	 * then the supplementary code point corresponding to this surrogate pair is returned.
	 * Otherwise, the char value at the given index is returned.</p>
	 * 
	 * @param context function call context.
	 * @param input a Unicode string.
	 * @param index the index of the code point to return. Has to be at least zero and length(input) - 1 at maximum.
	 * 
	 * @return Code point at specified position in the input string.
	 * 
	 * @see Character#codePointAt(CharSequence, int)
	 */
	@TLFunctionAnnotation("Returns the code point at the given index.")
	@CTL2FunctionDeclaration(impl = CodePointAtFunction.class)
	public static final int codePointAt(TLFunctionCallContext context, String input, Integer index) {
		return Character.codePointAt(input, index);
	}
	
	/**
	 * <p>Determine the number of characters needed to represent the specified Unicode code point.</p>
	 * 
	 * <p>If the specified character is equal to or greater than 0x10000, then the method returns 2. 
	 * Otherwise, the method returns 1.</p>
	 * <p>Note that code point is not validated. If validation is desired, isValidCodePoint should be used.</p>
	 *  
	 * @param context function call context.
	 * @param codePoint code point to test. Cannot be null.
	 * 
	 * @return number of characters needed to store specified Unicode code point.
	 * 
	 * @see Character#charCount(int)
	 */
	@TLFunctionAnnotation("Return number of characters defined by given code point.")
	@CTL2FunctionDeclaration(impl = CodePointLengthFunction.class)
	public static final int codePointLength(TLFunctionCallContext context, Integer codePoint) {
		return Character.charCount(codePoint);
	}
	
	/**
	 * Determine if specified value is valid Unicode code point value.
	 * 
	 * @param context function call context.
	 * @param codePoint value to test. May be null.
	 * 
	 * @return true if given value is valid Unicode code point, false otherwise. Null value is considered invalid.
	 * 
	 * @see Character#isValidCodePoint(int)
	 */
	@TLFunctionAnnotation("Determine if given value is valid Unicode code point.")
	@CTL2FunctionDeclaration(impl = IsValidCodePointFunction.class)
	public static final boolean isValidCodePoint(TLFunctionCallContext context, Integer codePoint) {
		return Character.isValidCodePoint(codePoint);
	}

	/**
	 * <p>Normalize input using specified normalization form.</p>
	 * 
	 * <p>Following normalization forms are supported:
	 * <ul>
	 *   <li>NFD: canonical Unicode decomposition</li>
	 *   <li>NFC: canonical Unicode decomposition followed by canonical composition</li>
	 *   <li>NFKD: compatibility decomposition</li>
	 *   <li>NFKC: compatibility decomposition followed by canonical composition</li>
	 * </ul>
	 * </p>
	 * <p>Function gracefully handles null input - null is simply passed through.</p>
	 * 
	 * @param context function call context.
	 * @param input input string to normalize. May be null.
	 * @param form specifies algorithm to use. Algorithm name is case insensitive. Cannot be null.
	 * 
	 * @return normalized input string or null if input is also null.
	 * 
	 * @see Normalizer#normalize(CharSequence, Form)
	 */
	@TLFunctionAnnotation("Perform Unicode normalization of given string.")
	@CTL2FunctionDeclaration(impl = UnicodeNormalizeFunction.class)
	public static final String unicodeNormalize(TLFunctionCallContext context, String input, String form) {
		
		if (form == null) {
			throw new NullPointerException("Null form is not allowed.");
		}
		
		Form normalizerForm;
		try {
			normalizerForm = Form.valueOf(form.toUpperCase());
		} catch (IllegalArgumentException iae) {
			throw new IllegalArgumentException("Unsupported normalization form '" + form + "'.", iae);
		}
		
		if (input == null) {
			return null;
		}

		return Normalizer.normalize(input, normalizerForm);
	}
	
	/**
	 * <p>Determine if input string is Unicode normalized according to the given form.</p>
	 * 
	 * <p>Following normalization forms are supported:
	 * <ul>
	 *   <li>NFD: canonical Unicode decomposition</li>
	 *   <li>NFC: canonical Unicode decomposition followed by canonical composition</li>
	 *   <li>NFKD: compatibility decomposition</li>
	 *   <li>NFKC: compatibility decomposition followed by canonical composition</li>
	 * </ul>
	 * </p>
	 * <p>Function gracefully handles null input - null is simply passed through.</p>
	 * 
	 * @param context function call context.
	 * @param input input string to normalize. May be null.
	 * @param form specifies algorithm to use. Algorithm name is case insensitive. Cannot be null.
	 * 
	 * @return true if input is normalized with respect to the selected form of if input is null. False is returned otherwise.
	 * 
	 * @see Normalizer#isNormalized(CharSequence, Form)
	 */
	@TLFunctionAnnotation("Determine if given string is Unicode normalized.")
	@CTL2FunctionDeclaration(impl = IsUnicodeNormalizedFunction.class)
	public static final boolean isUnicodeNormalized(TLFunctionCallContext context, String input, String form) {

		if (form == null) {
			throw new NullPointerException("Null form is not allowed.");
		}
		
		Form normalizerForm;
		try {
			normalizerForm = Form.valueOf(form.toUpperCase());
		} catch (IllegalArgumentException iae) {
			throw new IllegalArgumentException("Unsupported normalization form '" + form + "'.", iae);
		}
		
		if (input == null) {
			return true;
		}
		
		return Normalizer.isNormalized(input, normalizerForm);
	}

	/**
	 * <p>Test if input string contains specified substring.</p>
	 *  
	 * <p>This function handles null inputs gracefully. The function returns false for null input since it does not have
	 * any substrings.<p>
	 *  
	 * @param context function call context.
	 * @param input input string. May be null.
	 * @param substring substring to search for. Cannot be null.
	 * 
	 * @return true if input contains specified substring, false otherwise.
	 * 
	 * @see String#contains(CharSequence)
	 */
	@TLFunctionAnnotation("Test if given substring is contained within input string.")
	@CTL2FunctionDeclaration(impl = ContainsFunction.class)
	public static final boolean contains(TLFunctionCallContext context, String input, String substring) {
		if (substring == null) {
			throw new NullPointerException("Substring is null");
		}
		
		if (input == null) {
			return false;
		}
		
		if (substring.isEmpty()) {
			return true;
		}
		
		return input.contains(substring);
	}
	
	/**
	 * <p>Returns a substring beginning at specified position and extending to the end of the string.</p>
	 * 
	 * <p>Function gracefully handles null values. Null input results into null output.</p>
	 * 
	 * @param context function call context
	 * @param input input string. May be null.
	 * @param beginIndex the beginning index of the resulting substring. Value is inclusive. Negative values result in
	 *        whole input being returned, values larger then the length of the input result in empty string.
	 * 
	 * @return the specified substring.
	 * 
	 * @see String#substring(int)
	 */
	@TLFunctionAnnotation("Returns a substring beginning at specified position and extending to the end of the string.")
	@CTL2FunctionDeclaration(impl = SubstringFunction.class)
	public static final String substring(TLFunctionCallContext context, String input, Integer beginIndex) {
		
		if (input == null) {
			return null;
		}
		
		if (beginIndex < 0) {
			return input;
		}
		
		if (beginIndex > input.length()) {
			return "";
		}
		
		return input.substring(beginIndex);
	}
	
	/**
	 * <p>Concatenate list of strings into one string separating non-blank elements with separators.</p>
	 * 
	 * <p>Example:
	 * <code>concatWithSeparator(context, ", ", "first", "", null, "second")</code>
	 * will result in <code>"first, second"</code>
	 * </p>
	 *  
	 * @param context function call context.
	 * @param separator separator to use between the elements. Long separator (with more than one character) can be used.
	 *        null separator is not allowed. 
	 * @param arguments elements to concatenate into the output. Blank values (as determined by isBlank function) are
	 *        ignored and do not appear on the output.
	 *        
	 * @return string created as concatenation of the specified values with non-blank values separated by specified separator.
	 *         The function never returns null - empty argument list results in an empty string.
	 */
	@TLFunctionAnnotation("Concatenate strings separating them with given separator while ignoring blank values.")
	@CTL2FunctionDeclaration(impl = ConcatWithSeparatorFunction.class)
	public static final String concatWithSeparator(TLFunctionCallContext context, String separator, String... arguments) {
		if (separator == null) {
			throw new NullPointerException("Separator cannot be null.");
		}
		
		if (arguments == null) {
			return "";
		}
		
		StringBuilder builder = new StringBuilder();
		for (String s: arguments) {
			if (UnicodeBlanks.isBlank(s)) {
				continue;
			}
			if (builder.length() != 0) {
				builder.append(separator);
			}
			
			builder.append(s);
		}
		
		return builder.toString();
	}
	
	/**
	 * <p>Translate input into application/x-www-form-urlencoded format using specified character encoding.</p>
	 *  
	 * <p>Note that this function does not try to interpret the input as URL. Therefore the input does not need to have
	 * valid (or known) protocol etc. Compare this to escapeURL function.<p>
	 * 
	 * @param context function call context.
	 * @param input input string to translate. Null value is handled gracefully and will result into null output.
	 * @param encoding encoding to use when translating the characters. Character encodings supported by
	 *        {@link java.nio.charset.Charset} class can be used. UTF-8 is recommended. Encoding cannot be null.
	 * 
	 * @return translated input string.
	 * 
	 * @see URLEncoder#encode(String, String)
	 */
	@TLFunctionAnnotation("Translate input into URL encoded string using specified character encoding.")
	@CTL2FunctionDeclaration(impl = EscapeUrlFragmentFunction.class)
	public static final String escapeUrlFragment(TLFunctionCallContext context, String input, String encoding) {
		if (encoding == null) {
			throw new NullPointerException("Encoding is null");
		}
		
		if (input == null) {
			return null;
		}
		
		try {
			return URLEncoder.encode(input, encoding);
		} catch (UnsupportedEncodingException e) {
			throw new TransformLangExecutorRuntimeException("Unsupported encoding.", e);
		} catch (Exception e) {
			throw new TransformLangExecutorRuntimeException("Unable to encode the input string.", e);
		}
	}
	
	/**
	 * <p>Translate input into application/x-www-form-urlencoded format using UTF-8 character encoding.</p>
	 * 
	 * <p>Note that this function does not try to interpret the input as URL. Therefore the input does not need to have
	 * valid (or known) protocol etc. Compare this to escapeURL function.<p>
	 * 
	 * @param context function call context.
	 * @param input input string to translate. Null value is handled gracefully and will result into null output.
	 * 
	 * @return translated input string.
	 */
	@TLFunctionAnnotation("Translate input into URL encoded string with UTF-8 character encoding.")
	@CTL2FunctionDeclaration(impl = EscapeUrlFragmentFunction.class)
	public static final String escapeUrlFragment(TLFunctionCallContext context, String input) {
		
		return escapeUrlFragment(context, input, "UTF-8");
	}
	
	/**
	 * <p>Decode application/x-www-form-urlencoded string using specified character encoding.</p>
	 *  
	 * <p>Note that this function does not try to interpret the input as URL. Therefore the input does not need to have
	 * valid (or known) protocol etc. Compare this to escapeURL function.<p>
	 * 
	 * @param context function call context.
	 * @param input input string to translate. Null value is handled gracefully and will result into null output.
	 * @param encoding encoding to use when translating the characters. Character encodings supported by
	 *        {@link java.nio.charset.Charset} class can be used. UTF-8 is recommended. Encoding cannot be null.
	 * 
	 * @return translated input string.
	 * 
	 * @see URLDecoder#decode(String, String)
	 */
	@TLFunctionAnnotation("Decode URL encoded string using specified character encoding.")
	@CTL2FunctionDeclaration(impl = UnescapeUrlFragmentFunction.class)
	public static final String unescapeUrlFragment(TLFunctionCallContext context, String input, String encoding) {
		if (encoding == null) {
			throw new NullPointerException("Encoding is null");
		}
		
		if (input == null) {
			return null;
		}
		
		try {
			return URLDecoder.decode(input, encoding);
		} catch (UnsupportedEncodingException e) {
			throw new TransformLangExecutorRuntimeException("Unsupported encoding.", e);
		} catch (Exception e) {
			throw new TransformLangExecutorRuntimeException("Unable to decode the input string.", e);
		}
	}
	
	/**
	 * <p>Decode application/x-www-form-urlencoded string using UTF-8 character encoding.</p>
	 *  
	 * <p>Note that this function does not try to interpret the input as URL. Therefore the input does not need to have
	 * valid (or known) protocol etc. Compare this to escapeURL function.<p>
	 * 
	 * @param context function call context.
	 * @param input input string to translate. Null value is handled gracefully and will result into null output.
	 * 
	 * @return translated input string.
	 */
	@TLFunctionAnnotation("Decode UTF-8 URL encoded string.")
	@CTL2FunctionDeclaration(impl = UnescapeUrlFragmentFunction.class)
	public static final String unescapeUrlFragment(TLFunctionCallContext context, String input) {
		return unescapeUrlFragment(context, input, "UTF-8");
	}

//	/**
//	 * <p>Convert all whitespace separated words into title case.</p>
//	 * 
//	 * <p>Title case words have capital first letter followed by all lowercase letters.</p>
//	 * 
//	 * @param context function call context.
//	 * @param input string to capitalize. Function is null safe - null results in null output.
//	 * 
//	 * @return input string with all words converted to title case.
//	 * 
//	 * @see WordUtils#capitalizeFully(String)
//	 */
//	@TLFunctionAnnotation("Convert all whitespace separated words into title case.")
//	@CTL2FunctionDeclaration(impl = CapitalizeWordsFunction.class)
//	public static final String capitalizeWords(TLFunctionCallContext context, String input) {
//		return WordUtils.capitalizeFully(input);
//	}
//	
//	/**
//	 * <p>Uncapitalize all whitespace separated words in the input. Only the first letter of each word is changed to
//	 * lowercase.</p>
//	 * 
//	 * @param context function call context.
//	 * @param input input string. Null values are handled gracefully and result into null output.
//	 * 
//	 * @return uncapitalized input string.
//	 * 
//	 * @see WordUtils#uncapitalize(String)
//	 */
//	@TLFunctionAnnotation("Change first letter of each whitespace separated word into lower case.")
//	@CTL2FunctionDeclaration(impl = UncapitalizeWordsFunction.class)
//	public static final String uncapitalizeWords(TLFunctionCallContext context, String input) {
//		return WordUtils.uncapitalize(input);
//	}
	
	public class RPadFunction implements TLFunctionPrototype {

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			// Stack layout (top is first): [filler], targetLength, input
			
			switch (context.getParams().length) {
				case 2: {
					int length = stack.popInt();;
					String input = stack.popString();
					stack.push(rpad(context, input, length));
					break;
				}
					
				case 3: {
					String filler = stack.popString();
					int length = stack.popInt();;
					String input = stack.popString();
					stack.push(rpad(context, input, length, filler));
					break;
				}
					
				default:
					throw new TransformLangExecutorRuntimeException("Unsupported number of arguments: " + stack.length());
			}
		}

		@Override
		public void init(TLFunctionCallContext context) {
		}
	}

	public class LPadFunction implements TLFunctionPrototype {

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			// Stack layout (top is first): [filler], targetLength, input
			
			switch (context.getParams().length) {
				case 2: {
					int length = stack.popInt();;
					String input = stack.popString();
					stack.push(lpad(context, input, length));
					break;
				}
					
				case 3: {
					String filler = stack.popString();
					int length = stack.popInt();;
					String input = stack.popString();
					stack.push(lpad(context, input, length, filler));
					break;
				}
					
				default:
					throw new TransformLangExecutorRuntimeException("Unsupported number of arguments: " + stack.length());
			}
		}

		@Override
		public void init(TLFunctionCallContext context) {
		}
	}
	public class LastIndexOfFunction implements TLFunctionPrototype {

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
		
			// Stack layout: [fromIndex], what, input
			switch (context.getParams().length) {
				case 2: {
					String what = stack.popString();
					String input = stack.popString();
					stack.push(lastIndexOf(context, input, what));
					break;
				}
					
				case 3: {
					int fromIndex = stack.popInt();
					String what = stack.popString();
					String input = stack.popString();
					stack.push(lastIndexOf(context, input, what, fromIndex));
					break;
				}
					
				default:
					throw new TransformLangExecutorRuntimeException("Unsupported number of arguments: " + stack.length());
			}
			
		}
		
		@Override
		public void init(TLFunctionCallContext context) {
		}
	}
	
	public class SubstringFunction implements TLFunctionPrototype {

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			// Stack layout: beginIndex, input
			int beginIndex = stack.popInt();
			String input = stack.popString();
			stack.push(substring(context, input, beginIndex));
		}

		@Override
		public void init(TLFunctionCallContext context) {
		}
	}
	
	public class ConcatWithSeparatorFunction implements TLFunctionPrototype {

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			// Stack layout: [arg_n], ..., arg_1, separator

			// Below is basically a copy of code from concatWithSeparator, but modified to use stack instead of array
			String separator = (String) context.getParamValue(0);
			
			if (separator == null) {
				for (int i = 0; i < context.getParams().length; ++i) {
					stack.pop();
				}
				throw new IllegalArgumentException("Separator cannot be null.");
			}
			
			if (context.getParams().length == 1) {
				// Only one argument and this has to be separator
				stack.popString(); // Throw away the separator argument
				stack.push("");
				return;
			}
			
			StringBuilder builder = new StringBuilder();
			Object[] stackElements = stack.getStackContents();
			
			for (int i = 1; i < stackElements.length; ++i) {
				String arg = (String) stackElements[i];
				if (UnicodeBlanks.isBlank(arg)) {
					continue;
				}

				if (builder.length() != 0) {
					builder.append(separator);
				}
				
				builder.append(arg);
			}

			//Now pop everything from the stack
			for (int i = 0; i < context.getParams().length; ++i) {
				stack.pop();
			}
			
			stack.push(builder.toString());
		}

		@Override
		public void init(TLFunctionCallContext context) {
		}
	}
	
	public class EscapeUrlFragmentFunction implements TLFunctionPrototype {

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			// Stack layout: [encoding], input
			switch (context.getParams().length) {
				case 1:
					stack.push(escapeUrlFragment(context, stack.popString()));
					break;
					
				case 2: {
					String encoding = stack.popString();
					String input = stack.popString();
					stack.push(escapeUrlFragment(context, input, encoding));
					break;
				}
					
				default:
					throw new TransformLangExecutorRuntimeException("Unsupported number of arguments: " + stack.length());
			}
		}

		@Override
		public void init(TLFunctionCallContext context) {
		}
	}

	public class UnescapeUrlFragmentFunction implements TLFunctionPrototype {

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			// Stack layout: [encoding], input
			switch (context.getParams().length) {
				case 1:
					stack.push(unescapeUrlFragment(context, stack.popString()));
					break;
					
				case 2: {
					String encoding = stack.popString();
					String input = stack.popString();
					stack.push(unescapeUrlFragment(context, input, encoding));
					break;
				}
					
				default:
					throw new TransformLangExecutorRuntimeException("Unsupported number of arguments: " + stack.length());
			}
		}

		@Override
		public void init(TLFunctionCallContext context) {
		}
	}
	
	public class StartsWithFunction implements TLFunctionPrototype {

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {

			// Stack layout: prefix, input
			String prefix = stack.popString();
			String input = stack.popString();
			stack.push(startsWith(context, input, prefix));
		}

		@Override
		public void init(TLFunctionCallContext arg0) {
		}
	}
	
	public class EndsWithFunction implements TLFunctionPrototype {

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {

			// Stack layout: suffix, input
			String suffix = stack.popString();
			String input = stack.popString();
			stack.push(endsWith(context, input, suffix));
		}

		@Override
		public void init(TLFunctionCallContext arg0) {
		}
	}

	public class CodePointToCharFunction implements TLFunctionPrototype {

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			// Stack layout: codePoint
			stack.push(codePointToChar(context, stack.popInt()));
		}

		@Override
		public void init(TLFunctionCallContext context) {
		}
	}
	
	public class CodePointAtFunction implements TLFunctionPrototype {

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			// Stack layout: index, input
			int index = stack.popInt();
			String input = stack.popString();
			
			stack.push(codePointAt(context, input, index));
		}

		@Override
		public void init(TLFunctionCallContext context) {
		}
	}
	
	public class CodePointLengthFunction implements TLFunctionPrototype {

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			// Stack layout: codePoint
			stack.push(codePointLength(context, stack.popInt()));
		}

		@Override
		public void init(TLFunctionCallContext context) {
		}
	}
	
	public class IsValidCodePointFunction implements TLFunctionPrototype {

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			// Stack layout: codePoint
			stack.push(isValidCodePoint(context, stack.popInt()));
		}

		@Override
		public void init(TLFunctionCallContext context) {
		}
	}
	
	public class UnicodeNormalizeFunction implements TLFunctionPrototype {

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			// Stack layout: form, input
			String form = stack.popString();
			String input = stack.popString();
			stack.push(unicodeNormalize(context, input, form));
		}

		@Override
		public void init(TLFunctionCallContext context) {
		}
	}
	
	public class IsUnicodeNormalizedFunction implements TLFunctionPrototype {

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			// Stack layout: form, input
			String form = stack.popString();
			String input = stack.popString();
			stack.push(isUnicodeNormalized(context, input, form));
		}

		@Override
		public void init(TLFunctionCallContext context) {
		}
	}

	public class ContainsFunction implements TLFunctionPrototype {

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			// Stack layout: substring, input
			String substr = stack.popString();
			String input = stack.popString();
			stack.push(contains(context, input, substr));
		}

		@Override
		public void init(TLFunctionCallContext context) {
		}
	}

//	public class CapitalizeWordsFunction implements TLFunctionPrototype {
//
//		@Override
//		public void execute(Stack stack, TLFunctionCallContext context) {
//			// Stack layout: input
//			stack.push(capitalizeWords(context, stack.popString()));
//		}
//
//		@Override
//		public void init(TLFunctionCallContext context) {
//		}
//	}
//
//	public class UncapitalizeWordsFunction implements TLFunctionPrototype {
//
//		@Override
//		public void execute(Stack stack, TLFunctionCallContext context) {
//			// Stack layout: input
//			stack.push(uncapitalizeWords(context, stack.popString()));
//		}
//
//		@Override
//		public void init(TLFunctionCallContext context) {
//		}
//	}

}
