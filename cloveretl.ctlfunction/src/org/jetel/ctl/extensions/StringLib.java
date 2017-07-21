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
package org.jetel.ctl.extensions;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jetel.ctl.Stack;
import org.jetel.ctl.TransformLangExecutorRuntimeException;
import org.jetel.ctl.data.TLType;
import org.jetel.data.DataRecord;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.util.file.FileUtils;
import org.jetel.util.formatter.DateFormatter;
import org.jetel.util.property.PropertyRefResolver;
import org.jetel.util.property.RefResFlag;
import org.jetel.util.string.StringUtils;

public class StringLib extends TLFunctionLibrary {

	@Override
	public TLFunctionPrototype getExecutable(String functionName) {
		if (functionName != null) {
			switch (functionName) {
				case "concat": return new ConcatFunction(); //$NON-NLS-1$
				case "upperCase": return new UpperCaseFunction(); //$NON-NLS-1$
				case "lowerCase": return new LowerCaseFunction(); //$NON-NLS-1$
				case "substring": return new SubstringFunction(); //$NON-NLS-1$
				case "left": return new LeftFunction(); //$NON-NLS-1$
				case "right": return new RightFunction(); //$NON-NLS-1$
				case "trim": return new TrimFunction(); //$NON-NLS-1$
				case "length": return new LengthFunction(); //$NON-NLS-1$
				case "replace": return new ReplaceFunction(); //$NON-NLS-1$
				case "split": return new SplitFunction(); //$NON-NLS-1$
				case "charAt": return new CharAtFunction(); //$NON-NLS-1$
				case "isBlank": return new IsBlankFunction(); //$NON-NLS-1$
				case "isAscii": return new IsAsciiFunction(); //$NON-NLS-1$
				case "isNumber": return new IsNumberFunction(); //$NON-NLS-1$
				case "isInteger": return new IsIntegerFunction(); //$NON-NLS-1$
				case "isLong": return new IsLongFunction(); //$NON-NLS-1$
				case "isDate": return new IsDateFunction(); //$NON-NLS-1$
				case "isDecimal": return new IsDecimalFunction(); //$NON-NLS-1$
				case "removeDiacritic": return new RemoveDiacriticFunction(); //$NON-NLS-1$
				case "removeBlankSpace": return new RemoveBlankSpaceFunction(); //$NON-NLS-1$
				case "removeNonPrintable": return new RemoveNonPrintableFunction(); //$NON-NLS-1$
				case "removeNonAscii": return new RemoveNonAsciiFunction(); //$NON-NLS-1$
				case "resolveParams": return new ResolveParamsFunction(); //$NON-NLS-1$
				case "getAlphanumericChars": return new GetAlphanumericCharsFunction(); //$NON-NLS-1$
				case "translate": return new TranslateFunction(); //$NON-NLS-1$
				case "join": return new JoinFunction(); //$NON-NLS-1$
				case "indexOf": return new IndexOfFunction(); //$NON-NLS-1$
				case "countChar": return new CountCharFunction(); //$NON-NLS-1$
				case "find": return new FindFunction(); //$NON-NLS-1$
				case "matches": return new MatchesFunction(); //$NON-NLS-1$
				case "matchGroups": return new MatchGroupsFunction(); //$NON-NLS-1$
				case "chop": return new ChopFunction(); //$NON-NLS-1$
				case "cut": return new CutFunction(); //$NON-NLS-1$
				case "isUrl": return new IsUrlFunction(); //$NON-NLS-1$
				case "getUrlProtocol": return new GetUrlProtocolFunction(); //$NON-NLS-1$
				case "getUrlUserInfo": return new GetUrlUserInfo(); //$NON-NLS-1$
				case "getUrlHost": return new GetUrlHostFunction(); //$NON-NLS-1$
				case "getUrlPort": return new GetUrlPortFunction(); //$NON-NLS-1$
				case "getUrlPath": return new GetUrlPathFunction(); //$NON-NLS-1$
				case "getUrlQuery": return new GetUrlQueryFunction(); //$NON-NLS-1$
				case "getUrlRef": return new GetUrlRefFunction(); //$NON-NLS-1$
				case "toAbsolutePath": return new ToAbsolutePathFunction(); //$NON-NLS-1$
				case "toProjectUrl": return new ToProjectUrlFunction(); //$NON-NLS-1$
				case "escapeUrl": return new EscapeUrlFunction(); //$NON-NLS-1$
				case "unescapeUrl": return new UnescapeUrlFunction(); //$NON-NLS-1$
				case "getFileExtension": return new GetFileExtensionFunction(); //$NON-NLS-1$
				case "getFileName": return new GetFileNameFunction(); //$NON-NLS-1$
				case "getFileNameWithoutExtension": return new GetFileNameWithoutExtensionFunction(); //$NON-NLS-1$
				case "getFilePath": return new GetFilePathFunction(); //$NON-NLS-1$
				case "normalizePath": return new NormalizePathFunction(); //$NON-NLS-1$
				case "reverse": return new ReverseFunction(); //$NON-NLS-1$
				case "isEmpty": return new IsEmptyFunction(); //$NON-NLS-1$
			}
		}

		throw new IllegalArgumentException(CtlExtensionsMessages.getString("StringLib.unknown_function") + functionName + "'"); //$NON-NLS-1$ //$NON-NLS-2$
	}
	
	private static String LIBRARY_NAME = "String"; //$NON-NLS-1$

	@Override
	public String getName() {
		return LIBRARY_NAME;
	}

	
	// CONCAT
	@TLFunctionAnnotation("Concatenates two or more strings.")
	public static final String concat(TLFunctionCallContext context, String... operands) {
		final StringBuilder buf = new StringBuilder();

		for (int i = 0; i < operands.length; i++) {
			buf.append(operands[i]);
		}

		return buf.toString();
	}

	static class ConcatFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
		}
		
		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			String[] args = new String[context.getParams().length];
			for (int i=args.length-1; i>=0; i--) {
				args[i] = stack.popString();
			}
			stack.push(concat(context, args));
		}

	}

	// UPPERCASE
	@TLFunctionAnnotation("Returns input string in uppercase")
	public static final String upperCase(TLFunctionCallContext context, String input) {
		if (input == null){
			return null;
		}
		return input.toUpperCase();
	}

	static class UpperCaseFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			stack.push(upperCase(context, stack.popString()));
		}
	}

	// LOWERCASE
	@TLFunctionAnnotation("Returns input string in lowercase")
	public static final String lowerCase(TLFunctionCallContext context, String input) {
		if (input == null){
			return null;
		}
		return input.toLowerCase();
	}

	static class LowerCaseFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			stack.push(lowerCase(context, stack.popString()));
		}

	}

	// SUBSTRING
	@TLFunctionAnnotation("Returns a substring of a given string")
	public static final String substring(TLFunctionCallContext context, String input, Integer beginIndex, Integer length) {
		if (beginIndex < 0) {
			throw new IllegalArgumentException("Begin index is negative");
		}
		
		if (length < 0) {
			throw new IllegalArgumentException("Length is negative");
		}
		
		if (input == null) {
			return null;
		}
		
		if (beginIndex > input.length()) {
			return "";
		}
		
		int endIndex = beginIndex + length;
		if (endIndex > input.length()) {
			return input.substring(beginIndex);
		}
		
		return input.substring(beginIndex, endIndex);
	}

	/**
	 * <p>Returns a substring beginning at specified position and extending to the end of the string.</p>
	 * 
	 * <p>Function gracefully handles null values. Null input results in null output.</p>
	 * 
	 * @param context function call context
	 * @param input input string. May be null.
	 * @param beginIndex the beginning index of the resulting substring. Value is inclusive. 
	 * 		Only non-negative values are allowed. Values larger then the length of the input result in empty string.
	 * 
	 * @return the specified substring.
	 * 
	 * @see String#substring(int)
	 */
	@TLFunctionAnnotation("Returns a substring beginning at specified position and extending to the end of the string.")
	public static final String substring(TLFunctionCallContext context, String input, Integer beginIndex) {
		if (beginIndex < 0) {
			throw new IllegalArgumentException("Begin index is negative");
		}
		
		if (input == null) {
			return null;
		}
		
		if (beginIndex > input.length()) {
			return "";
		}
		
		return input.substring(beginIndex);
	}
	
	static class SubstringFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			switch (context.getParams().length) {
				case 3: {
					final int length = stack.popInt();
					final int from = stack.popInt();
					final String input = stack.popString();
					stack.push(substring(context, input, from, length));
					break;
				}
				case 2: {
					final int from = stack.popInt();
					final String input = stack.popString();
					stack.push(substring(context, input, from));
					break;
				}
				default:
					throw new TransformLangExecutorRuntimeException("Unsupported number of arguments: " + stack.length());
			}
		}
	}

	// LEFT
	@TLFunctionAnnotation("Returns prefix of the specified length")
	public static final String left(TLFunctionCallContext context, String input, Integer length) {
		return left(context, input, length, false);
	}

	@TLFunctionAnnotation("Returns prefix of the specified length. If input string is shorter than specified length " +
		"and 3th argument is true, right side of result is padded with blank spaces so that the result has specified length.")
	public static final String left(TLFunctionCallContext context, String input, Integer length, Boolean spacePad) {
		if (input == null) {
			return null;
		}
		if (input.length() < length) {
			if (spacePad) {
				return String.format("%-" + length + "s", input); //$NON-NLS-1$ //$NON-NLS-2$
			}
			return input;
		}
		return input.substring(0, length);
	}
	
	static class LeftFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			int params = context.getParams().length;
			final boolean spacePad = params > 2 ? stack.popBoolean() : false;
			final int length = stack.popInt();
			final String input = stack.popString();
			if (params > 2) {
				stack.push(left(context, input, length, spacePad));
			} else {
				stack.push(left(context, input, length));
			}
		}
	}

	// RIGHT
	@TLFunctionAnnotation("Returns suffix of the specified length")
	public static final String right(TLFunctionCallContext context, String input, int length) {
		if( input == null){
			return null;
		}
		return StringLib.right(context, input, length, false);
	}

	@TLFunctionAnnotation("Returns suffix of the specified length. If input string is shorter than specified length " +
			"and 3th argument is true, left side of result is padded with blank spaces so that the result has specified length.")
	public static final String right(TLFunctionCallContext context, String input, int length, boolean spacePad) {
		if (input == null && !spacePad) {
			return null;
		}
		if (input == null && spacePad) {
			return String.format("%"+length+"s","");
		}
		if (input.length() < length) {
			if (spacePad) {
				return String.format("%"+length+"s", input); //$NON-NLS-1$ //$NON-NLS-2$
			}
			return input;
		}
		return input.substring(input.length() - length, input.length());
	}

	static class RightFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			int params = context.getParams().length;
			final boolean spacePad = params > 2 ? stack.popBoolean() : false;
			final int length = stack.popInt();
			final String input = stack.popString();
			if (params > 2) {
				stack.push(right(context, input, length, spacePad));
			} else {
				stack.push(right(context, input, length));
			}
		}
	}

	// TRIM
	@TLFunctionAnnotation("Removes leading and trailing whitespaces from a string.")
	public static final String trim(TLFunctionCallContext context, String input) {
		if (input == null){
			return null;
		}
		StringBuilder buf = new StringBuilder(input);
		return StringUtils.trim(buf).toString();

	}
	static class TrimFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			stack.push(trim(context, stack.popString()));
		}
	}

	// LENGTH
	@TLFunctionAnnotation("Returns number of characters in the input string")
	public static final Integer length(TLFunctionCallContext context, String input) {
		if (input == null){
			return 0;
		}
		return input.length();
	}

	@TLFunctionAnnotation("Returns number of elements in the input list")
	public static final <E> Integer length(TLFunctionCallContext context, List<E> input) {
		if (input == null){
			return 0;
		}
		return input.size();
	}

	@TLFunctionAnnotation("Returns number of mappings in the input map")
	public static final <K,V> Integer length(TLFunctionCallContext context, Map<K, V> input) {
		if (input == null){
			return 0;
		}
		return input.size();
	}

	@TLFunctionAnnotation("Returns number of bytes in the input byte array")
	public static Integer length(TLFunctionCallContext context, byte[] input) {
		if (input == null){
			return 0;
		}
		return input.length;
	}

	@TLFunctionAnnotation("Returns number of fields in the input record")
	public static final Integer length(TLFunctionCallContext context, DataRecord input) {
		if (input == null){
			return 0;
		}
		return input.getNumFields();
	}

	static class LengthFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			if (context.getParams()[0].isString()) {
				stack.push(length(context, stack.popString()));
				return;
			}

			if (context.getParams()[0].isList()) {
				stack.push(length(context, stack.popList()));
				return;
			}

			if (context.getParams()[0].isMap()) {
				stack.push(length(context, stack.popMap()));
				return;
			}

			if (context.getParams()[0].isRecord()) {
				stack.push(length(context, stack.popRecord()));
				return;
			}
			
			if (context.getParams()[0].isByteArray()) {
				stack.push(length(context, stack.popByteArray()));
				return;
			}

			throw new TransformLangExecutorRuntimeException(
					CtlExtensionsMessages.getString("StringLib.unknown_type") + context.getParams()[0].name()); //$NON-NLS-1$
		}

	}

	// REPLACE
	@TLFunctionInitAnnotation
	public static final void replaceInit(TLFunctionCallContext context) {
		context.setCache(new TLRegexpCache(context, 1));
	}
	
	@TLFunctionAnnotation("Replaces matches of a regular expression")
	public static final String replace(TLFunctionCallContext context, String input, String regex, String replacement) {
		if (input == null){
			return null;
		}
		Matcher m; 
		m = ((TLRegexpCache)context.getCache()).getCachedMatcher(context, regex).reset(input);
		return m.replaceAll(replacement);
	}

	static class ReplaceFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
			replaceInit(context);
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			final String replacement = stack.popString();
			final String regex = stack.popString();
			final String input = stack.popString();
			stack.push(replace(context, input, regex, replacement));
		}

	}

	// SPLIT
	@TLFunctionInitAnnotation
	public static final void splitInit(TLFunctionCallContext context) {
		context.setCache(new TLRegexpCache(context, 1));
	}

	@TLFunctionAnnotation("Splits the string around regular expression matches")
	public static final List<String> split(TLFunctionCallContext context, String input, String regex) {
		return split(context, input, regex, 0);
	}

	/**
	 * <p>Splits this string around matches of the given regular expression.</p>
	 * 
	 * <p> The array returned by this method contains each substring of the input that is terminated by another substring
	 * that matches the given expression or is terminated by the end of the string.
	 * The substrings in the array are in the order in which they occur the input. If the expression does not match
	 * any part of the input then the resulting array has just one element, namely the input string.</p>
	 * <p>The limit parameter controls the number of times the pattern is applied and therefore affects the length
	 * of the resulting array. If the limit is greater than zero then the pattern will be applied at most limit - 1 times,
	 * the array's length will be no greater than limit, and the array's last entry will contain all input beyond
	 * the last matched delimiter. If limit is non-positive then the pattern will be applied as many times as possible
	 * and the array can have any length.
	 * If limit is zero then the pattern will be applied as many times as possible, the array can have any length,
	 * and trailing empty strings will be discarded.</p> 
	 * 
	 * @param context function call context.
	 * @param input input string to split. May be null.
	 * @param regex regular expression specifying the delimiters. Cannot be null.
	 * @param limit the result threshold as described above.
	 * 
	 * @return the array of strings obtained by splitting input string around matches of the given regular expression.
	 *         null input string results in an empty array. This function never returns null.
	 * 
	 * @see String#split(String, int)
	 */
	@TLFunctionAnnotation("Split string around matches of given regular expression.")
	public static final List< String > split(TLFunctionCallContext context, String input, String regex, Integer limit) {
		
		if (regex == null) {
			throw new IllegalArgumentException("Null regular expression is not allowed.");
		}
		
		List< String > result = new ArrayList< String >();
		if (input == null) {
			return result;
		}
		
		final Pattern pattern = ((TLRegexpCache) context.getCache()).getCachedPattern(context, regex);
		Collections.addAll(result, pattern.split(input, limit));
		
		return result;
	}
	
	static class SplitFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
			splitInit(context);
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			// Stack layout: [limit,] regexp, input
			int limit = 0;
			String regexp = null;
			String input = null;
			
			switch (context.getParams().length) {
			case 3:
				limit = stack.popInt();
				// no break here
			case 2:
				regexp = stack.popString();
				input = stack.popString();
				break;
			default:
				throw new IllegalArgumentException("Unsupported number of arguments for split function (" + context.getParams().length + " parameters found).");
			}
			
			stack.push(split(context, input, regexp, limit));
		}

	}

	// CHAR AT
	@TLFunctionAnnotation("Returns character at the specified position of input string")
	public static final String charAt(TLFunctionCallContext context, String input, int position) {
		return String.valueOf(input.charAt(position));
	}

	static class CharAtFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			int pos = stack.popInt();
			String input = stack.popString();
			stack.push(charAt(context, input, pos));
		}
	}

	// IS BLANK
	@TLFunctionAnnotation("Checks if the string contains only whitespace characters")
	public static final boolean isBlank(TLFunctionCallContext context, String input) {
		return input == null || input.length() == 0
				|| StringUtils.isBlank(input);
	}

	static class IsBlankFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			stack.push(isBlank(context, stack.popString()));
		}

	}
	
	// IS ASCII
	@TLFunctionAnnotation("Checks if the string contains only characters from the US-ASCII encoding")
	public static final boolean isAscii(TLFunctionCallContext context, String input) {
		return StringUtils.isAscii(input);
	}

	static class IsAsciiFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			stack.push(isAscii(context, stack.popString()));
		}

	}

	// IS NUMBER
	@TLFunctionAnnotation("Checks if the string can be parsed into a double number")
	public static final boolean isNumber(TLFunctionCallContext context, String input) {
		return StringUtils.isNumber(input);
	}

	static class IsNumberFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			stack.push(isNumber(context, stack.popString()));
		}
	}

	// IS DECIMAL
	/**
	 * Test if given string represents a decimal number (same as {@link #isNumber(TLFunctionCallContext, String)}).
	 * 
	 * @param context function call context
	 * @param value value to parse
	 * 
	 * @return <code>true</code> if provided string is a valid decimal number according to the default format,
	 * 			<code>false</code> otherwise or if the string is <code>null</code>.
	 */
	@TLFunctionAnnotation("Test if string represents a decimal number.")
	public static boolean isDecimal(TLFunctionCallContext context, String value) {
		return StringUtils.isNumber(value);
	}
	
	public static class IsDecimalFunction implements TLFunctionPrototype {

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			stack.push(isDecimal(context, stack.popString()));
		}

		@Override
		public void init(TLFunctionCallContext context) {
		}
	}

	// IS INTEGER
	@TLFunctionAnnotation("Checks if the string can be parsed into an integer number")
	public static final boolean isInteger(TLFunctionCallContext context, String input) {
		int result = StringUtils.isInteger(input);
		return result == 0 || result == 1;
	}

	static class IsIntegerFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			stack.push(isInteger(context, stack.popString()));
		}
	}

	// IS LONG
	@TLFunctionAnnotation("Checks if the string can be parsed into a long number")
	public static final boolean isLong(TLFunctionCallContext context, String input) {
		int result = StringUtils.isInteger(input);
		return result >= 0 && result < 3;
	}

	static class IsLongFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			stack.push(isLong(context, stack.popString()));
		}

	}

	// IS DATE
	@TLFunctionInitAnnotation
    public static final void isDateInit(TLFunctionCallContext context) {
    	context.setCache(new TLDateFormatLocaleCache(context, 1, 2, 3));
    }	
	
	@TLFunctionAnnotation("Checks if the string can be parsed into a date with specified pattern")
	public static final boolean isDate(TLFunctionCallContext context, String input, String pattern) {
		return isDate(context, input, pattern, null, null, null);
	}
	
	@TLFunctionAnnotation("Checks if the string can be parsed into a date with specified pattern and strict parsing flag")
	public static final boolean isDate(TLFunctionCallContext context, String input, String pattern, Boolean strict) {
		return isDate(context, input, pattern, null, null, strict);
	}

	@TLFunctionAnnotation("Checks if the string can be parsed into a date with specified pattern and locale")
	public static final boolean isDate(TLFunctionCallContext context, String input, String pattern, String locale) {
		return isDate(context, input, pattern, locale, null, null);
	}
	
	@TLFunctionAnnotation("Checks if the string can be parsed into a date with specified pattern, locale and strict parsing flag")
	public static final boolean isDate(TLFunctionCallContext context, String input, String pattern, String locale, Boolean strict) {
		return isDate(context, input, pattern, locale, null, strict);
	}

	@TLFunctionAnnotation("Checks if the string can be parsed into a date with specified pattern, locale and time zone")
	public static final boolean isDate(TLFunctionCallContext context, String input, String pattern, String locale, String timeZone) {
		return isDate(context, input, pattern, locale, timeZone, null);
	}
	
	@TLFunctionAnnotation("Checks if the string can be parsed into a date with specified pattern, locale, time zone and strict parsing flag")
	public static final boolean isDate(TLFunctionCallContext context, String input, String pattern, String locale, String timeZone, Boolean strict) {
		DateFormatter formatter = ((TLDateFormatLocaleCache) context.getCache()).getCachedLocaleFormatter(context, pattern, locale, timeZone, 1, 2, 3);
		if (strict != null && strict) {
			try {
				formatter.parseDateExactMatch(input);
			} catch (IllegalArgumentException e) {
				return false;
			}
			return true;
		} else {
			return formatter.tryParse(input);
		}
		
	}

	static class IsDateFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
			isDateInit(context);
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			String locale = null;
			String timeZone = null;
			Boolean strict = false;
			
			TLType[] params = context.getParams();
			boolean strictParamPresent = params[params.length - 1].isBoolean();
			
			if (strictParamPresent) {
				strict = stack.popBoolean();
			}

			if (params.length > 4 || (params.length == 4 && !strictParamPresent)) {
				timeZone = stack.popString();
			}
			if (params.length > 3 || (params.length == 3 && !strictParamPresent)) {
				locale = stack.popString();
			}

			final String pattern = stack.popString();
			final String input = stack.popString();

			stack.push(isDate(context, input, pattern, locale, timeZone, strict));
		}

	}

	// REMOVE DIACRITIC
	@TLFunctionAnnotation("Strips diacritic from characters.")
	public static final String removeDiacritic(TLFunctionCallContext context, String input) {
		return StringUtils.removeDiacritic(input);
	}

	static class RemoveDiacriticFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			stack.push(removeDiacritic(context, stack.popString()));
		}
	}

	// REMOVE BLANK SPACE
	@TLFunctionAnnotation("Removes whitespace characters")
	public static final String removeBlankSpace(TLFunctionCallContext context, String input) {
		return StringUtils.removeBlankSpace(input);
	}

	static class RemoveBlankSpaceFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			stack.push(removeBlankSpace(context, stack.popString()));
		}
	}

	// REMOVE NONPRINTABLE CHARS
	@TLFunctionAnnotation("Removes nonprintable characters")
	public static final String removeNonPrintable(TLFunctionCallContext context, String input) {
		return StringUtils.removeNonPrintable(input);
	}

	static class RemoveNonPrintableFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			stack.push(removeNonPrintable(context, stack.popString()));
		}

	}

	// REMOVE NONASCII CHARS
	@TLFunctionAnnotation("Removes nonascii characters")
	public static final String removeNonAscii(TLFunctionCallContext context, String input) {
		return StringUtils.removeNonAscii(input);
	}

	static class RemoveNonAsciiFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			stack.push(removeNonAscii(context, stack.popString()));
		}

	}

	// GET ALPHANUMERIC CHARS
	@TLFunctionAnnotation("Extracts only alphanumeric characters from input string")
	public static final String getAlphanumericChars(TLFunctionCallContext context, String input) {
		return getAlphanumericChars(context, input, true, true);
	}

	@TLFunctionAnnotation("Extracts letters, numbers or both from input string")
	public static final String getAlphanumericChars(TLFunctionCallContext context, String input,
			boolean takeAlpha, boolean takeNumeric) {
		return StringUtils.getOnlyAlphaNumericChars(input, takeAlpha,
				takeNumeric);
	}

	static class GetAlphanumericCharsFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			if (context.getParams().length == 1) {
				stack.push(getAlphanumericChars(context, stack.popString()));
			} else {
				final boolean takeNumeric = stack.popBoolean();
				final boolean takeAlpha = stack.popBoolean();
				final String input = stack.popString();
				stack.push(getAlphanumericChars(context, input, takeAlpha,
								takeNumeric));
			}
		}

	}

	// TRANSLATE
	@TLFunctionAnnotation("Replaces occurences of characters")
	public static final String translate(TLFunctionCallContext context, String input, String match,
			String replacement) {
		CharSequence seq = StringUtils.translateSequentialSearch(input,
				match, replacement);
		if (seq == null){
			return null;
		}else{
			return seq.toString();
		}
	}

	static class TranslateFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			final String replacement = stack.popString();
			final String match = stack.popString();
			final String input = stack.popString();

			stack.push(translate(context, input, match, replacement));
		}

	}

	// JOIN
	@TLFunctionAnnotation("Concatenates list elements into a string using delimiter.")
	public static final <E> String join(TLFunctionCallContext context, String delimiter, List<E> values) {
		if (delimiter == null){
			delimiter ="";
		}
		StringBuffer buf = new StringBuffer();
		for (int i=0; i<values.size(); i++) {
			buf.append(values.get(i) == null ? CtlExtensionsMessages.getString("StringLib.null") : values.get(i).toString()); //$NON-NLS-1$
			if (i < values.size()-1) {
				buf.append(delimiter);
			}
		}

		return buf.toString();
	}

	@TLFunctionAnnotation("Concatenates all mappings into a string using delimiter.")
	public static final <K,V> String join(TLFunctionCallContext context, String delimiter, Map<K,V> values) {
		if (delimiter == null){
			delimiter = "";
		}
		StringBuffer buf = new StringBuffer();
		Set<K> keys = values.keySet();
		for (Iterator<K> it = keys.iterator(); it.hasNext();) {
			K key = it.next();
			V value = values.get(key);
			
			buf.append(String.valueOf(key)).append("=").append( //$NON-NLS-1$
					value == null ? CtlExtensionsMessages.getString("StringLib.null") : value.toString()); //$NON-NLS-1$
			if (it.hasNext()) {
				buf.append(delimiter);
			}
			
		}

		return buf.toString();
	}

	static class JoinFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			if (context.getParams()[1].isList()) {
				final List<Object> values = stack.popList();
				final String delim = stack.popString();
				stack.push(join(context, delim, values));
			} else {
				final Map<Object, Object> values = stack.popMap();
				final String delim = stack.popString();
				stack.push(join(context, delim, values));
			}
		}

	}

	// INDEX OF
	@TLFunctionAnnotation("Returns the first occurence of a specified string")
	public static final Integer indexOf(TLFunctionCallContext context, String input, String pattern) {
		return indexOf(context, input, pattern, 0);
	}

	@TLFunctionAnnotation("Returns the first occurence of a specified string")
	public static final Integer indexOf(TLFunctionCallContext context, String input, String pattern, Integer from) {
		if (pattern == null) {
			throw new NullPointerException("Search string is null");
		}
		if (input == null) {
			return -1;
		}
		return StringUtils.indexOf(input, pattern, from);
	}

	static class IndexOfFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			int from = 0;

			if (context.getParams().length > 2) {
				from = stack.popInt();
			}

			final String pattern = stack.popString();
			final String input = stack.popString();
			stack.push(indexOf(context, input, pattern, from));
		}

	}

	// COUNTCHAR
	@TLFunctionAnnotation("Calculates the number of occurences of the specified character")
	public static final Integer countChar(TLFunctionCallContext context, String input, String character) {
		return StringUtils.count(input, character.charAt(0));
	}

	static class CountCharFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			String character = stack.popString();
			String input = stack.popString();
			stack.push(countChar(context, input, character));
		}
	}

	// FIND
	@TLFunctionInitAnnotation
	public static final void findInit(TLFunctionCallContext context) {
		context.setCache(new TLRegexpCache(context, 1));
	}
	
	@TLFunctionAnnotation("Finds and returns all occurences of regex in specified string")
//	@TLFunctionParametersAnnotation({"input","regex_pattern"})
	public static final List<String> find(TLFunctionCallContext context, String input, String pattern) {
		
		Matcher m = ((TLRegexpCache)context.getCache()).getCachedMatcher(context, pattern).reset(input);
		
		final List<String> ret = new ArrayList<String>();

		while (m.find()) {
			ret.add(m.group());
		}

		return ret;
	}
	
	@TLFunctionAnnotation("Finds and returns n-th group(s) of regex occurence(s) in specified string.")
//	@TLFunctionParametersAnnotation({"input","regex_pattern","groupNum"})
	public static final List<String> find(TLFunctionCallContext context, String input, String pattern, int groupNo) {
		Matcher m = ((TLRegexpCache)context.getCache()).getCachedMatcher(context, pattern).reset(input);
		
		final List<String> ret = new ArrayList<String>();

		if (m.groupCount()>= groupNo){
			while (m.find()) {
				ret.add(m.group(groupNo));
			}
		}else{
			throw new TransformLangExecutorRuntimeException(
					CtlExtensionsMessages.getString("StringLib.wrong_regexp_group_number")); //$NON-NLS-1$
		}
		return ret;
		
	}

	

	static class FindFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
			findInit(context);
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			switch(context.getParams().length){
			case 2:
			{
				final String pattern = stack.popString();
				final String input = stack.popString();
				stack.push(find(context, input, pattern));
			}
				break;
			case 3:
			{
				final Integer group = stack.popInt();
				final String pattern = stack.popString();
				final String input = stack.popString();
				stack.push(find(context, input, pattern,group));
			}
			}
			return;
		}
	}
	
	// MATCHES
	@TLFunctionInitAnnotation
	public static final void matchesInit(TLFunctionCallContext context) {
		// moved to IntegralLib, so that it can be called from the interpreter and compiler
		IntegralLib.matchesInit(context);
	}
	
	@TLFunctionAnnotation("Tries to match entire input with specified pattern.")
//	@TLFunctionParametersAnnotation({"input","regex_pattern"})
	public static final Boolean matches(TLFunctionCallContext context, String input, String pattern) {
		return IntegralLib.matches(context, input, pattern);			
	}

	static class MatchesFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
			matchesInit(context);
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			final String pattern = stack.popString();
			final String input = stack.popString();
			stack.push(matches(context, input, pattern));
			return;
		}
	}
	
	// MATCH GROUPS
	@TLFunctionInitAnnotation
	public static final void matchGroupsInit(TLFunctionCallContext context) {
		context.setCache(new TLRegexpCache(context, 1));
	}
	
	@TLFunctionAnnotation("Tries to match entire input with specified pattern.")
	public static final List<String> matchGroups(TLFunctionCallContext context, String input, String pattern) {
		if(input == null){
			return null;
		}
		final Matcher m = ((TLRegexpCache)context.getCache()).getCachedPattern(context, pattern).matcher(input);
		if (m.matches()) {
			return new AbstractList<String>() {

				@Override
				public String get(int index) {
					return m.group(index);
				}

				@Override
				public int size() {
					return m.groupCount() + 1; // group 0 is not included in groupCount()
				}
				
			};
		} else {
			return null;
		}
	}

	static class MatchGroupsFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
			matchGroupsInit(context);
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			final String pattern = stack.popString();
			final String input = stack.popString();
			stack.push(matchGroups(context, input, pattern));
			return;
		}
	}
	
	// CHOP
	@TLFunctionInitAnnotation()
	public static final void chopInit(TLFunctionCallContext context) {
		context.setCache(new TLRegexpCache(context, 1));
	}
	
	private static final Pattern chopPattern = Pattern.compile("[\r\n]+"); //$NON-NLS-1$
	
	@TLFunctionAnnotation("Removes new line characters from input string")
	public static final String chop(TLFunctionCallContext context, String input) {
		final Matcher m = chopPattern.matcher(input);
		return m.replaceAll(""); //$NON-NLS-1$
	}

	@TLFunctionAnnotation("Removes regexp pattern from input string")
	public static final String chop(TLFunctionCallContext context, String input, String pattern) {
		final Pattern p = ((TLRegexpCache)context.getCache()).getCachedPattern(context, pattern);
		final Matcher m = p.matcher(input);
		return m.replaceAll(""); //$NON-NLS-1$

	}
	static class ChopFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
			chopInit(context);
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			if (context.getParams().length == 1) {
				stack.push(chop(context, stack.popString()));
			} else {
				final String pattern = stack.popString();
				final String input = stack.popString();
				stack.push(chop(context, input, pattern));

			}
		}

	}

	//CUT
	@TLFunctionAnnotation("Cuts substring from specified string based on list consisting of pairs position,length")
	public static final List<String> cut(TLFunctionCallContext context, String input, List<Integer> indexes) {
		if (indexes.size() % 2 != 0) {
			throw new TransformLangExecutorRuntimeException(
					CtlExtensionsMessages.getString("StringLib.bad_count_of_indices") + indexes.size() //$NON-NLS-1$
							+ CtlExtensionsMessages.getString("StringLib.even_number_expected")); //$NON-NLS-1$
		}
		final Iterator<Integer> it = indexes.iterator();
		final List<String> ret = new ArrayList<String>();

		while (it.hasNext()) {
			final Integer from = it.next();
			final Integer length = it.next();
			ret.add(input.substring(from, from + length));
		}

		return ret;
	}

	static class CutFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			final List<Object> indices = stack.popList();
			final String input = stack.popString();
			stack.push(cut(context, input, TLFunctionLibrary.<Integer>convertTo(indices)));
		}
	}

	// URL parsing
	
    //   foo://username:password@example.com:8042/over/there/index.dtb?type=animal;name=narwhal#nose
    //   \_/   \_______________/ \_________/ \__/\___________________/ \______________________/ \__/
    //    |           |               |       |            |                    |                |
    //  scheme    userinfo         hostname  port        path                 query             ref

		   
	@TLFunctionAnnotation("Checks whether specified string is valid URL")
	public static final boolean isUrl(TLFunctionCallContext context, String url) {
		try {
			FileUtils.getUrl(url);
			return true;
		} catch (MalformedURLException e) {
			return false;
		}
	}
	
	static class IsUrlFunction implements TLFunctionPrototype {
	
		@Override
		public void init(TLFunctionCallContext context) {
		}
		
		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			final String url = stack.popString();
			stack.push(isUrl(context, url));
		}
	}
	
	@TLFunctionAnnotation("Parses out protocol name from specified URL")
	public static final String getUrlProtocol(TLFunctionCallContext context, String url) {
		try {
			return FileUtils.getUrl(url).getProtocol();
		} catch (MalformedURLException e) {
			return null;
		}
	}
	
	static class GetUrlProtocolFunction implements TLFunctionPrototype {
	
		@Override
		public void init(TLFunctionCallContext context) {
		}
		
		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			final String url = stack.popString();
			stack.push(getUrlProtocol(context, url));
		}
	}
	
	@TLFunctionAnnotation("Parses out user info from specified URL")
	public static final String getUrlUserInfo(TLFunctionCallContext context, String url) {
		try {
			String ui = FileUtils.getUrl(url).getUserInfo();
			return ui == null ? "" : ui; //$NON-NLS-1$
		} catch (MalformedURLException e) {
			return null;
		}
	}
	
	static class GetUrlUserInfo implements TLFunctionPrototype {
	
		@Override
		public void init(TLFunctionCallContext context) {
		}
		
		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			final String url = stack.popString();
			stack.push(getUrlUserInfo(context, url));
		}
	}

	@TLFunctionAnnotation("Parses out host name from specified URL")
	public static final String getUrlHost(TLFunctionCallContext context, String url) {
		try {
			return FileUtils.getUrl(url).getHost();
		} catch (MalformedURLException e) {
			return null;
		}
	}
	
	static class GetUrlHostFunction implements TLFunctionPrototype {
	
		@Override
		public void init(TLFunctionCallContext context) {
		}
		
		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			final String url = stack.popString();
			stack.push(getUrlHost(context, url));
		}
	}
	
	@TLFunctionAnnotation("Parses out port number from specified URL. Returns -1 if port not defined, -2 if URL has invalid syntax.")
	public static final int getUrlPort(TLFunctionCallContext context, String url) {
		try {
			return FileUtils.getUrl(url).getPort();
		} catch (MalformedURLException e) {
			return -2;
		}
	}
	
	static class GetUrlPortFunction implements TLFunctionPrototype {
	
		@Override
		public void init(TLFunctionCallContext context) {
		}
		
		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			final String url = stack.popString();
			stack.push(getUrlPort(context, url));
		}
	}

	@TLFunctionAnnotation("Parses out path part of specified URL")
	public static final String getUrlPath(TLFunctionCallContext context, String url) {
		try {
			return FileUtils.getUrl(url).getPath();
		} catch (MalformedURLException e) {
			return null;
		}
	}
	
	static class GetUrlPathFunction implements TLFunctionPrototype {
	
		@Override
		public void init(TLFunctionCallContext context) {
		}
		
		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			final String url = stack.popString();
			stack.push(getUrlPath(context, url));
		}
	}

	@TLFunctionAnnotation("Parses out query (parameters) from specified URL")
	public static final String getUrlQuery(TLFunctionCallContext context, String url) {
		try {
			String q = FileUtils.getUrl(url).getQuery();
			return q == null ? "" : q; //$NON-NLS-1$
		} catch (MalformedURLException e) {
			return null;
		}
	}
	
	static class GetUrlQueryFunction implements TLFunctionPrototype {
	
		@Override
		public void init(TLFunctionCallContext context) {
		}
		
		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			final String url = stack.popString();
			stack.push(getUrlQuery(context, url));
		}
	}
	
	@TLFunctionAnnotation("Parses out fragment after \"#\" character, also known as ref, reference or anchor, from specified URL")
	public static final String getUrlRef(TLFunctionCallContext context, String url) {
		try {
			String query = FileUtils.getUrl(url).getRef();
			return query == null ? "" : query; //$NON-NLS-1$
		} catch (MalformedURLException e) {
			return null;
		}
	}
	
	static class GetUrlRefFunction implements TLFunctionPrototype {
	
		@Override
		public void init(TLFunctionCallContext context) {
		}
		
		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			final String url = stack.popString();
			stack.push(getUrlRef(context, url));
		}
	}
	
	@TLFunctionAnnotation("Converts the URL passed as argument to the absolute file path")
	public static final String toAbsolutePath(TLFunctionCallContext context, String url) {
		URL contextUrl = context.getGraph().getRuntimeContext().getContextURL();
		if (contextUrl == null) {
			try {
				contextUrl = new File(".").toURI().toURL(); //$NON-NLS-1$
			} catch (MalformedURLException e) {}
		}
		try {
			File file = FileUtils.getJavaFile(contextUrl, url);
			if (file != null) {
				try {
					return file.getCanonicalPath().replace('\\', '/');
				} catch (IOException e) {
					return file.getAbsolutePath().replace('\\', '/');
				}
			}
		} catch (JetelRuntimeException ex) {}
		
		return null;
	}
	
	static class ToAbsolutePathFunction implements TLFunctionPrototype {
	
		@Override
		public void init(TLFunctionCallContext context) {
		}
		
		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			final String url = stack.popString();
			stack.push(toAbsolutePath(context, url));
		}
	}
	
	@TLFunctionAnnotation("Converts the argument to an absolute URL with respect to the project URL")
	public static final String toProjectUrl(TLFunctionCallContext context, String url) {
		if (url == null) {
			return null;
		}
		URL contextUrl = context.getGraph().getRuntimeContext().getContextURL();
		try {
			return FileUtils.getAbsoluteURL(contextUrl, url);
		} catch (MalformedURLException ex) {
			throw new JetelRuntimeException(ex);
		}
	}
	
	static class ToProjectUrlFunction implements TLFunctionPrototype {
	
		@Override
		public void init(TLFunctionCallContext context) {
		}
		
		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			final String url = stack.popString();
			stack.push(toProjectUrl(context, url));
		}
	}
	

	// ESCAPE URL
	
	@TLFunctionAnnotation("Escapes any illegal characters within components of specified URL (the URL is parsed first).")
	public static final String escapeUrl(TLFunctionCallContext context, String urlStr) {
		try {
			// parse input string
			URL url = FileUtils.getUrl(urlStr);
			// create URI representation of the URL which handles character quoting
			return new URI(url.getProtocol(), url.getUserInfo(), url.getHost(), url.getPort(), url.getPath(), url.getQuery(), url.getRef()).toASCIIString();
		} catch (MalformedURLException e) {
			throw new TransformLangExecutorRuntimeException(CtlExtensionsMessages.getString("StringLib.failed_to_pass_input_as_URL"), e); //$NON-NLS-1$
		} catch (URISyntaxException e) {
			throw new TransformLangExecutorRuntimeException(CtlExtensionsMessages.getString("StringLib.failed_to_escape_URL"), e); //$NON-NLS-1$
		}
	}
	
	static class EscapeUrlFunction implements TLFunctionPrototype {
	
		@Override
		public void init(TLFunctionCallContext context) {
		}
		
		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			final String url = stack.popString();
			stack.push(escapeUrl(context, url));
		}
	}

	
	// UNESCAPE URL
	
	@TLFunctionAnnotation("Decodes sequence of escaped octets with sequence of characters that it represents in UTF-8, e.g. \"%20\" is replaced with \" \".")
	public static final String unescapeUrl(TLFunctionCallContext context, String url) {
		try {
			// try to parse passed string as URL and convert it to URI which handles escaping of characters
			URI uri = FileUtils.getUrl(url).toURI();
			
			// get unescaped parts of the URL
			String scheme = uri.getScheme();
			String authority = uri.getAuthority();
			String path = uri.getPath();
			String query = uri.getQuery();
			String fragment = uri.getFragment();
			
			// reconstruct URL from unescaped parts and return its String representation
			StringBuilder sb = new StringBuilder();
			if (scheme != null) sb.append(scheme).append("://"); //$NON-NLS-1$
			if (authority != null) sb.append(authority);
			if (path != null) sb.append(path);
			if (query != null) sb.append('?').append(query);
			if (fragment != null) sb.append('#').append(fragment);
			
			return FileUtils.getUrl(sb.toString()).toString();
		} catch (MalformedURLException e) {
			throw new TransformLangExecutorRuntimeException(CtlExtensionsMessages.getString("StringLib.failed_to_unescape_URL"), e); //$NON-NLS-1$
		} catch (URISyntaxException e) {
			throw new TransformLangExecutorRuntimeException(CtlExtensionsMessages.getString("StringLib.failed_to_unescape_URL_detailed"), e); //$NON-NLS-1$
		}
	}
	
	static class UnescapeUrlFunction implements TLFunctionPrototype {
	
		@Override
		public void init(TLFunctionCallContext context) {
		}
		
		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			final String url = stack.popString();
			stack.push(unescapeUrl(context, url));
		}
	}
	
	// RESOLVE PARAMS
	
	@TLFunctionAnnotation("Resolves parameters in a given string.")
	public static final String resolveParams(TLFunctionCallContext context, String value) {
		return resolveParams(context, value, false);
	}

	@TLFunctionAnnotation("Resolves parameters in a given string.")
	public static final String resolveParams(TLFunctionCallContext context, String value, boolean resolveSpecialChars) {
		RefResFlag refResFlag = null;
		if (resolveSpecialChars) {
			refResFlag = RefResFlag.REGULAR;
		} else {
			refResFlag = RefResFlag.SPEC_CHARACTERS_OFF;
		}
		PropertyRefResolver refResolver = ((TLPropertyRefResolverCache) context.getCache()).getCachedPropertyRefResolver();
		return refResolver.resolveRef(value, refResFlag);
	}

	static class ResolveParamsFunction implements TLFunctionPrototype {
	
		@Override
		public void init(TLFunctionCallContext context) {
			resolveParamsInit(context);
		}
		
		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			if (context.getParams().length == 1) {
				stack.push(resolveParams(context, stack.popString()));
			} else if (context.getParams().length == 2) {
				final boolean resolveSpecialChars = stack.popBoolean();
				final String value = stack.popString();
				stack.push(resolveParams(context, value, resolveSpecialChars));
			}
		}
	}
	
	@TLFunctionInitAnnotation()
	public static final void resolveParamsInit(TLFunctionCallContext context) {
		PropertyRefResolver refResolver = context.getGraph().getPropertyRefResolver();
		context.setCache(new TLPropertyRefResolverCache(refResolver));
	}
	
	// GET FILE EXTENSION FUNCTION

	@TLFunctionAnnotation("Returns the extension of a filename.")
	public static final String getFileExtension(TLFunctionCallContext context, String url) {
		return FileUtils.getFileExtension(url);
	}

	static class GetFileExtensionFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			final String filename = stack.popString();
			stack.push(getFileExtension(context, filename));
		}
	}

	// GET FILE NAME FUNCTION

	@TLFunctionAnnotation("Returns the name minus the path from a full filename.")
	public static final String getFileName(TLFunctionCallContext context, String url) {
		return FileUtils.getFileName(url);
	}

	static class GetFileNameFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			final String filename = stack.popString();
			stack.push(getFileName(context, filename));
		}
	}

	// GET FILE NAME WITHOUT EXTENSION FUNCTION

		@TLFunctionAnnotation("Returns the base name, minus the full path and extension, from a full filename.")
		public static final String getFileNameWithoutExtension(TLFunctionCallContext context, String url) {
			return FileUtils.getBaseName(url);
		}

		static class GetFileNameWithoutExtensionFunction implements TLFunctionPrototype {

			@Override
			public void init(TLFunctionCallContext context) {
			}

			@Override
			public void execute(Stack stack, TLFunctionCallContext context) {
				final String filename = stack.popString();
				stack.push(getFileNameWithoutExtension(context, filename));
			}
		}

		// GET FILE PATH FUNCTION

		@TLFunctionAnnotation("Returns the path, minus the filename, from a full filename.")
		public static final String getFilePath(TLFunctionCallContext context, String url) {
			return FileUtils.getFilePath(url);
		}

		static class GetFilePathFunction implements TLFunctionPrototype {

			@Override
			public void init(TLFunctionCallContext context) {
			}

			@Override
			public void execute(Stack stack, TLFunctionCallContext context) {
				final String filename = stack.popString();
				stack.push(getFilePath(context, filename));
			}
		}

		// NORMALIZE PATH FUNCTION

		@TLFunctionAnnotation("Normalizes a path, removing double and single dot path segments.")
		public static final String normalizePath(TLFunctionCallContext context, String url) {
			return FileUtils.normalize(url);
		}

		static class NormalizePathFunction implements TLFunctionPrototype {

			@Override
			public void init(TLFunctionCallContext context) {
			}

			@Override
			public void execute(Stack stack, TLFunctionCallContext context) {
				final String filename = stack.popString();
				stack.push(normalizePath(context, filename));
			}
		}

	// REVERSE CHARS FUNCTION

	@TLFunctionAnnotation("Reverses the order of characters in string.")
	public static final String reverse(TLFunctionCallContext context, String value) {
		if (value != null) {
			if (value.length()<2) return value;
			StringBuilder newVal = new StringBuilder(value);
			newVal.reverse(); // handles surrogate pairs
			return newVal.toString();
		} else {
			return null;
		}
	}

	static class ReverseFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			final String value = stack.popString();
			stack.push(reverse(context, value));
		}
	}
	
	@TLFunctionAnnotation("Checks if the string is null or of zero length.")
	public static final boolean isEmpty(TLFunctionCallContext context, String input) {
		return StringUtils.isEmpty(input);
	}
	
	static class IsEmptyFunction implements TLFunctionPrototype{
		
		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			stack.push(isEmpty(context, stack.popString()));
		}
	}
	
}
