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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jetel.ctl.Stack;
import org.jetel.ctl.TransformLangExecutorRuntimeException;
import org.jetel.data.DataRecord;
import org.jetel.util.MiscUtils;
import org.jetel.util.date.DateFormatter;
import org.jetel.util.date.DateFormatterFactory;
import org.jetel.util.string.StringUtils;

public class StringLib extends TLFunctionLibrary {

	@Override
	public TLFunctionPrototype getExecutable(String functionName) {
		TLFunctionPrototype ret = 
			"concat".equals(functionName) ? new ConcatFunction() :
			"upperCase".equals(functionName) ? new UpperCaseFunction() :
			"lowerCase".equals(functionName) ? new LowerCaseFunction() :
			"substring".equals(functionName) ? new SubstringFunction() :
			"left".equals(functionName) ? new LeftFunction() :
			"right".equals(functionName) ? new RightFunction() :
			"trim".equals(functionName) ? new TrimFunction() : 
			"length".equals(functionName) ? new LengthFunction() :
			"replace".equals(functionName) ? new ReplaceFunction() :
			"split".equals(functionName) ? new SplitFunction() :
			"charAt".equals(functionName) ? new CharAtFunction() :
			"isBlank".equals(functionName) ? new IsBlankFunction() :
			"isAscii".equals(functionName) ? new IsAsciiFunction() :
			"isNumber".equals(functionName) ? new IsNumberFunction() :
			"isInteger".equals(functionName) ? new IsIntegerFunction() :
			"isLong".equals(functionName) ? new IsLongFunction() :
			"isDate".equals(functionName) ? new IsDateFunction() :
			"removeDiacritic".equals(functionName) ? new RemoveDiacriticFunction() :
			"removeBlankSpace".equals(functionName) ? new RemoveBlankSpaceFunction() :
			"removeNonPrintable".equals(functionName) ? new RemoveNonPrintableFunction() :
			"removeNonAscii".equals(functionName) ? new RemoveNonAsciiFunction() :
			"getAlphanumericChars".equals(functionName) ? new GetAlphanumericCharsFunction() :
			"translate".equals(functionName) ? new TranslateFunction() :
			"join".equals(functionName) ? new JoinFunction() :
			"indexOf".equals(functionName) ? new IndexOfFunction() :
			"countChar".equals(functionName) ? new CountCharFunction() :
			"find".equals(functionName) ? new FindFunction() :
			"matches".equals(functionName) ? new MatchesFunction() :
			"chop".equals(functionName) ? new ChopFunction() :
			"cut".equals(functionName) ? new CutFunction() : null;

		if (ret == null) {
    		throw new IllegalArgumentException("Unknown function '" + functionName + "'");
    	}

		return ret;
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

	class ConcatFunction implements TLFunctionPrototype {

		public void init(TLFunctionCallContext context) {
		}
		
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
		return input.toUpperCase();
	}

	class UpperCaseFunction implements TLFunctionPrototype {

		public void init(TLFunctionCallContext context) {
		}

		public void execute(Stack stack, TLFunctionCallContext context) {
			stack.push(upperCase(context, stack.popString()));
		}
	}

	// LOWERCASE
	@TLFunctionAnnotation("Returns input string in lowercase")
	public static final String lowerCase(TLFunctionCallContext context, String input) {
		return input.toLowerCase();
	}

	class LowerCaseFunction implements TLFunctionPrototype {

		public void init(TLFunctionCallContext context) {
		}

		public void execute(Stack stack, TLFunctionCallContext context) {
			stack.push(lowerCase(context, stack.popString()));
		}

	}

	// SUBSTRING
	@TLFunctionAnnotation("Returns a substring of a given string")
	public static final String substring(TLFunctionCallContext context, String input, int from, int length) {
		return input.substring(from, from+length);
	}

	class SubstringFunction implements TLFunctionPrototype {

		public void init(TLFunctionCallContext context) {
		}

		public void execute(Stack stack, TLFunctionCallContext context) {
			final int length = stack.popInt();
			final int from = stack.popInt();
			final String input = stack.popString();
			stack.push(substring(context, input, from, length));
		}
	}

	// LEFT
	@TLFunctionAnnotation("Returns prefix of the specified length")
	public static final String left(TLFunctionCallContext context, String input, int length) {
		return input.length() < length ? "" : input.substring(0, length);
	}

	class LeftFunction implements TLFunctionPrototype {

		public void init(TLFunctionCallContext context) {
		}

		public void execute(Stack stack, TLFunctionCallContext context) {
			final int length = stack.popInt();
			final String input = stack.popString();
			stack.push(left(context, input, length));
		}
	}

	// RIGHT
	@TLFunctionAnnotation("Returns suffix of the specified length")
	public static final String right(TLFunctionCallContext context, String input, int length) {
		return input.substring(input.length() - length, input.length());
	}

	class RightFunction implements TLFunctionPrototype {

		public void init(TLFunctionCallContext context) {
		}

		public void execute(Stack stack, TLFunctionCallContext context) {
			final int length = stack.popInt();
			final String input = stack.popString();
			stack.push(right(context, input, length));
		}
	}

	// TRIM
	@TLFunctionAnnotation("Removes leading and trailing whitespaces from a string.")
	public static final String trim(TLFunctionCallContext context, String input) {
		StringBuilder buf = new StringBuilder(input);
		return StringUtils.trim(buf).toString();

	}
	class TrimFunction implements TLFunctionPrototype {

		public void init(TLFunctionCallContext context) {
		}

		public void execute(Stack stack, TLFunctionCallContext context) {
			stack.push(trim(context, stack.popString()));
		}
	}

	// LENGTH
	@TLFunctionAnnotation("Returns number of characters in the input string")
	public static final Integer length(TLFunctionCallContext context, String input) {
		return input.length();
	}

	@TLFunctionAnnotation("Returns number of elements in the input list")
	public static final <E> Integer length(TLFunctionCallContext context, List<E> input) {
		return input.size();
	}

	@TLFunctionAnnotation("Returns number of mappings in the input map")
	public static final <K,V> Integer length(TLFunctionCallContext context, Map<K, V> input) {
		return input.size();
	}

	@TLFunctionAnnotation("Returns number of bytes in the input byte array")
	public static Integer length(TLFunctionCallContext context, byte[] input) {
		return input.length;
	}

	@TLFunctionAnnotation("Returns number of fields in the input record")
	public static final Integer length(TLFunctionCallContext context, DataRecord input) {
		return input.getNumFields();
	}

	class LengthFunction implements TLFunctionPrototype {

		public void init(TLFunctionCallContext context) {
		}

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
					"length - Unknown type: " + context.getParams()[0].name());
		}

	}

	// REPLACE
	@TLFunctionInitAnnotation
	public static final void replaceInit(TLFunctionCallContext context) {
		context.setCache(new TLRegexpCache(context, 1));
	}
	
	@TLFunctionAnnotation("Replaces matches of a regular expression")
	public static final String replace(TLFunctionCallContext context, String input, String regex, String replacement) {
		Matcher m; 
		m = ((TLRegexpCache)context.getCache()).getCachedPattern(context, regex).matcher(input);
		return m.replaceAll(replacement);
	}

	class ReplaceFunction implements TLFunctionPrototype {

		public void init(TLFunctionCallContext context) {
			replaceInit(context);
		}

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
		final Pattern p = ((TLRegexpCache)context.getCache()).getCachedPattern(context, regex);
		final String[] strArray = p.split(input);
		final List<String> list = new ArrayList<String>();
		for (String item : strArray) {
			list.add(item);
		}
		return list;
	}

	class SplitFunction implements TLFunctionPrototype {

		public void init(TLFunctionCallContext context) {
			splitInit(context);
		}

		public void execute(Stack stack, TLFunctionCallContext context) {
			final String regex = stack.popString();
			final String input = stack.popString();
			stack.push(split(context, input, regex));
		}

	}

	// CHAR AT
	@TLFunctionAnnotation("Returns character at the specified position of input string")
	public static final String charAt(TLFunctionCallContext context, String input, int position) {
		return String.valueOf(input.charAt(position));
	}

	class CharAtFunction implements TLFunctionPrototype {

		public void init(TLFunctionCallContext context) {
		}

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

	class IsBlankFunction implements TLFunctionPrototype {

		public void init(TLFunctionCallContext context) {
		}

		public void execute(Stack stack, TLFunctionCallContext context) {
			stack.push(isBlank(context, stack.popString()));
		}

	}

	// IS ASCII
	@TLFunctionAnnotation("Checks if the string contains only characters from the US-ASCII encoding")
	public static final boolean isAscii(TLFunctionCallContext context, String input) {
		return StringUtils.isAscii(input);

	}

	class IsAsciiFunction implements TLFunctionPrototype {

		public void init(TLFunctionCallContext context) {
		}

		public void execute(Stack stack, TLFunctionCallContext context) {
			stack.push(isAscii(context, stack.popString()));
		}

	}

	// IS NUMBER
	@TLFunctionAnnotation("Checks if the string can be parsed into a double number")
	public static final boolean isNumber(TLFunctionCallContext context, String input) {
		return StringUtils.isNumber(input);
	}

	class IsNumberFunction implements TLFunctionPrototype {

		public void init(TLFunctionCallContext context) {
		}

		public void execute(Stack stack, TLFunctionCallContext context) {
			stack.push(isNumber(context, stack.popString()));
		}
	}

	// IS INTEGER
	@TLFunctionAnnotation("Checks if the string can be parsed into an integer number")
	public static final boolean isInteger(TLFunctionCallContext context, String input) {
		int result = StringUtils.isInteger(input);
		return result == 0 || result == 1;
	}

	class IsIntegerFunction implements TLFunctionPrototype {

		public void init(TLFunctionCallContext context) {
		}

		public void execute(Stack stack, TLFunctionCallContext context) {
			stack.push(isInteger(context, stack.popString()));
		}
	}

	// IS LONG
	@TLFunctionAnnotation("Checks if the string can be parsed into a long number")
	public static final boolean isLong(TLFunctionCallContext context, String input) {
		int result = StringUtils.isInteger(input);
		return result > 0 || result < 3;
	}

	class IsLongFunction implements TLFunctionPrototype {

		public void init(TLFunctionCallContext context) {
		}

		public void execute(Stack stack, TLFunctionCallContext context) {
			stack.push(isLong(context, stack.popString()));
		}

	}

	// IS DATE
	@TLFunctionAnnotation("Checks if the string can be parsed into a date with specified pattern")
	public static final boolean isDate(TLFunctionCallContext context, String input, String pattern) {
		return isDate(context, input, pattern, Locale.getDefault().getDisplayName(),
				false);
	}
	
	@TLFunctionAnnotation("Checks if the string can be parsed into a date with specified pattern. Allows changing parser strictness")
	public static final boolean isDate(TLFunctionCallContext context, String input, String pattern, boolean lenient) {
		return isDate(context, input, pattern, Locale.getDefault().getDisplayName(),
				lenient);
	}

	@TLFunctionAnnotation("Checks if the string can be parsed into a date with specified pattern and locale.")
	public static final boolean isDate(TLFunctionCallContext context, String input, String pattern, String locale) {
		return isDate(context, input, pattern, locale, false);
	}

	@TLFunctionAnnotation("Checks if the string can be parsed into a date with specified pattern and locale. Allows changing parser strictness.")
	public static final boolean isDate(TLFunctionCallContext context, String input, String pattern, String locale,
			boolean lenient) {
		// in lenient mode, empty string is not a valid date
		if ("".equals(input)) {
			return lenient;
		}

		DateFormatter formatter = DateFormatterFactory.createFormatter(pattern, MiscUtils.createLocale(locale));
		formatter.setLenient(lenient);

		return formatter.tryParse(input);
	}

	class IsDateFunction implements TLFunctionPrototype {

		public void init(TLFunctionCallContext context) {
		}

		public void execute(Stack stack, TLFunctionCallContext context) {

			boolean lenient = false;
			String locale = Locale.getDefault().getDisplayName();

			if (context.getParams().length > 2) {

				if (context.getParams().length > 3) {
					// if 4 params, it's (string,string,string,bool)
					lenient = stack.popBoolean();
					locale = stack.popString();
				} else {
					// if 3 params it's (string,string,string) or
					// (string,string,bool) if locale is skipped
					if (context.getParams()[2].isString()) {
						locale = stack.popString();
					} else {
						lenient = stack.popBoolean();
					}
				}
			}

			final String pattern = stack.popString();
			final String input = stack.popString();

			stack.push(isDate(context, input, pattern, locale, lenient));
		}

	}

	// REMOVE DIACRITIC
	@TLFunctionAnnotation("Strips diacritic from characters.")
	public static final String removeDiacritic(TLFunctionCallContext context, String input) {
		return StringUtils.removeDiacritic(input);
	}

	class RemoveDiacriticFunction implements TLFunctionPrototype {

		public void init(TLFunctionCallContext context) {
		}

		public void execute(Stack stack, TLFunctionCallContext context) {
			stack.push(removeDiacritic(context, stack.popString()));
		}
	}

	// REMOVE BLANK SPACE
	@TLFunctionAnnotation("Removes whitespace characters")
	public static final String removeBlankSpace(TLFunctionCallContext context, String input) {
		return StringUtils.removeBlankSpace(input);
	}

	class RemoveBlankSpaceFunction implements TLFunctionPrototype {

		public void init(TLFunctionCallContext context) {
		}

		public void execute(Stack stack, TLFunctionCallContext context) {
			stack.push(removeBlankSpace(context, stack.popString()));
		}
	}

	// REMOVE NONPRINTABLE CHARS
	@TLFunctionAnnotation("Removes nonprintable characters")
	public static final String removeNonPrintable(TLFunctionCallContext context, String input) {
		return StringUtils.removeNonPrintable(input);
	}

	class RemoveNonPrintableFunction implements TLFunctionPrototype {

		public void init(TLFunctionCallContext context) {
		}

		public void execute(Stack stack, TLFunctionCallContext context) {
			stack.push(removeNonPrintable(context, stack.popString()));
		}

	}

	// REMOVE NONASCII CHARS
	@TLFunctionAnnotation("Removes nonascii characters")
	public static final String removeNonAscii(TLFunctionCallContext context, String input) {
		return StringUtils.removeNonAscii(input);
	}

	class RemoveNonAsciiFunction implements TLFunctionPrototype {

		public void init(TLFunctionCallContext context) {
		}

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

	class GetAlphanumericCharsFunction implements TLFunctionPrototype {

		public void init(TLFunctionCallContext context) {
		}

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
		return String.valueOf(StringUtils.translateSequentialSearch(input,
				match, replacement));
	}

	class TranslateFunction implements TLFunctionPrototype {

		public void init(TLFunctionCallContext context) {
		}

		public void execute(Stack stack, TLFunctionCallContext context) {
			final String replacement = stack.popString();
			final String match = stack.popString();
			final String input = stack.popString();

			stack.push(translate(context, input, match, replacement));
		}

	}

	// JOIN
	@TLFunctionAnnotation("Concatenets list elements into a string using delimiter.")
	public static final <E> String join(TLFunctionCallContext context, String delimiter, List<E> values) {
		StringBuffer buf = new StringBuffer();
		for (int i=0; i<values.size(); i++) {
			buf.append(values.get(i) == null ? "null" : values.get(i).toString());
			if (i < values.size()-1) {
				buf.append(delimiter);
			}
		}

		return buf.toString();
	}

	@TLFunctionAnnotation("Concatenates all mappings into a string using delimiter.")
	public static final <K,V> String join(TLFunctionCallContext context, String delimiter, Map<K,V> values) {
		StringBuffer buf = new StringBuffer();
		Set<K> keys = values.keySet();
		for (Iterator<K> it = keys.iterator(); it.hasNext();) {
			K key = it.next();
			V value = values.get(key);
			buf.append(key.toString()).append("=").append(
					value == null ? "null" : value.toString());
			if (it.hasNext()) {
				buf.append(delimiter);
			}
			
		}

		return buf.toString();
	}

	class JoinFunction implements TLFunctionPrototype {

		public void init(TLFunctionCallContext context) {
		}

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
	public static final Integer indexOf(TLFunctionCallContext context, String input, String pattern, int from) {
		return StringUtils.indexOf(input, pattern, from);
	}

	class IndexOfFunction implements TLFunctionPrototype {

		public void init(TLFunctionCallContext context) {
		}

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

	class CountCharFunction implements TLFunctionPrototype {

		public void init(TLFunctionCallContext context) {
		}

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
	public static final List<String> find(TLFunctionCallContext context, String input, String pattern) {
		
		Matcher m = ((TLRegexpCache)context.getCache()).getCachedPattern(context, pattern).matcher(input);
		
		final List<String> ret = new ArrayList<String>();

		while (m.find()) {
			ret.add(m.group());
			int i = 0;
			while (i < m.groupCount()) {
				ret.add(m.group(++i));
			}
		}

		return ret;
	}

	class FindFunction implements TLFunctionPrototype {

		public void init(TLFunctionCallContext context) {
			findInit(context);
		}

		public void execute(Stack stack, TLFunctionCallContext context) {
			final String pattern = stack.popString();
			final String input = stack.popString();
			stack.push(find(context, input, pattern));
			return;
		}
	}
	
	// MATCHES
	@TLFunctionInitAnnotation
	public static final void matchesInit(TLFunctionCallContext context) {
		context.setCache(new TLRegexpCache(context, 1));
	}
	
	@TLFunctionAnnotation("Tries to match entire input with specified pattern.")
	public static final Boolean matches(TLFunctionCallContext context, String input, String pattern) {
		
		Matcher m = ((TLRegexpCache)context.getCache()).getCachedPattern(context, pattern).matcher(input);
		return m.matches();
	}

	class MatchesFunction implements TLFunctionPrototype {

		public void init(TLFunctionCallContext context) {
			matchesInit(context);
		}

		public void execute(Stack stack, TLFunctionCallContext context) {
			final String pattern = stack.popString();
			final String input = stack.popString();
			stack.push(matches(context, input, pattern));
			return;
		}
	}
	
	// CHOP
	@TLFunctionInitAnnotation()
	public static final void chopInit(TLFunctionCallContext context) {
		context.setCache(new TLRegexpCache(context, 1));
	}
	
	private static final Pattern chopPattern = Pattern.compile("[\r\n]+");
	
	@TLFunctionAnnotation("Removes new line characters from input string")
	public static final String chop(TLFunctionCallContext context, String input) {
		final Matcher m = chopPattern.matcher(input);
		return m.replaceAll("");
	}

	@TLFunctionAnnotation("Removes pattern from input string")
	public static final String chop(TLFunctionCallContext context, String input, String pattern) {
		final Pattern p = ((TLRegexpCache)context.getCache()).getCachedPattern(context, pattern);
		final Matcher m = p.matcher(input);
		return m.replaceAll("");

	}
	class ChopFunction implements TLFunctionPrototype {

		public void init(TLFunctionCallContext context) {
			chopInit(context);
		}

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
					"Incorrect number of indices: " + indexes.size()
							+ ". Must be an even number");
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

	class CutFunction implements TLFunctionPrototype {

		public void init(TLFunctionCallContext context) {
		}

		public void execute(Stack stack, TLFunctionCallContext context) {
			final List<Object> indices = stack.popList();
			final String input = stack.popString();
			stack.push(cut(context, input, TLFunctionLibrary.<Integer>convertTo(indices)));
		}
	}
	
	
}
