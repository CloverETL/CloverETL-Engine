/*
 * jETeL/Clover - Java based ETL application framework.
 * Copyright (c) Opensys TM by Javlin, a.s. (www.opensys.com)
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
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307  USA
 */
package org.jetel.ctl.extensions;

import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jetel.ctl.Stack;
import org.jetel.ctl.TransformLangExecutorRuntimeException;
import org.jetel.ctl.data.TLType;
import org.jetel.data.DataRecord;
import org.jetel.exception.JetelException;
import org.jetel.util.DataGenerator;
import org.jetel.util.MiscUtils;
import org.jetel.util.string.StringAproxComparator;
import org.jetel.util.string.StringUtils;

public class StringLib extends TLFunctionLibrary {

	private static Map<Thread, DataGenerator> dataGenerators = new HashMap<Thread, DataGenerator>();

	private static synchronized DataGenerator getGenerator(Thread key) {
		DataGenerator generator = dataGenerators.get(key);
		if (generator == null) {
			generator = new DataGenerator();
			dataGenerators.put(key, generator);
		}
		return generator;
	}

	@Override
	public TLFunctionPrototype getExecutable(String functionName) {
		TLFunctionPrototype ret = "concat".equals(functionName) ? new ConcatFunction() : "uppercase".equals(functionName) ? new UpperCaseFunction() : "lowercase".equals(functionName) ? new LowerCaseFunction() : "substring".equals(functionName) ? new SubstringFunction() : "left".equals(functionName) ? new LeftFunction() : "right".equals(functionName) ? new RightFunction() : "trim".equals(functionName) ? new TrimFunction() : "length".equals(functionName) ? new LengthFunction() : "soundex".equals(functionName) ? new SoundexFunction() : "replace".equals(functionName) ? new ReplaceFunction() : "split".equals(functionName) ? new SplitFunction() : "char_at".equals(functionName) ? new CharAtFunction() : "is_blank".equals(functionName) ? new IsBlankFunction() : "is_ascii".equals(functionName) ? new IsAsciiFunction() : "is_number".equals(functionName) ? new IsNumberFunction() : "is_integer".equals(functionName) ? new IsIntegerFunction() : "is_long".equals(functionName) ? new IsLongFunction() : "is_date".equals(functionName) ? new IsDateFunction() : "remove_diacritic".equals(functionName) ? new RemoveDiacriticFunction() : "remove_blank_space".equals(functionName) ? new RemoveBlankSpaceFunction() : "remove_nonprintable".equals(functionName) ? new RemoveNonPrintableFunction() : "remove_nonascii".equals(functionName) ? new RemoveNonAsciiFunction() : "get_alphanumeric_chars".equals(functionName) ? new GetAlphanumericCharsFunction() : "translate".equals(functionName) ? new TranslateFunction() : "join".equals(functionName) ? new JoinFunction() : "index_of".equals(functionName) ? new IndexOfFunction() : "count_char".equals(functionName) ? new CountCharFunction() : "find".equals(functionName) ? new FindFunction() : "chop".equals(functionName) ? new ChopFunction() : "cut".equals(functionName) ? new CutFunction() : "edit_distance".equals(functionName) ? new EditDistanceFunction() : "metaphone".equals(functionName) ? new MetaphoneFunction() : "nysiis".equals(functionName) ? new NYSIISFunction() : "random_string".equals(functionName) ? new RandomStringFunction() : null;

		if (ret == null) {
			throw new IllegalArgumentException("Unknown function '" + functionName + "'");
		}

		return ret;
	}

	// CONCAT
	@TLFunctionAnnotation("Concatenates two or more strings.")
	public static final String concat(String... operands) {
		final StringBuilder buf = new StringBuilder();

		for (int i = 0; i < operands.length; i++) {
			buf.append(operands[i]);
		}

		return buf.toString();
	}

	class ConcatFunction implements TLFunctionPrototype {

		public void execute(Stack stack, TLType[] actualParams) {
			String[] args = new String[actualParams.length];
			for (int i = args.length - 1; i >= 0; i--) {
				args[i] = stack.popString();
			}
			stack.push(concat(args));
		}
	}

	// UPPERCASE
	@TLFunctionAnnotation("Returns input string in uppercase")
	public static final String uppercase(String input) {
		return input.toUpperCase();
	}

	class UpperCaseFunction implements TLFunctionPrototype {

		public void execute(Stack stack, TLType[] actualParams) {
			stack.push(uppercase(stack.popString()));
		}
	}

	// LOWERCASE
	@TLFunctionAnnotation("Returns input string in lowercase")
	public static final String lowercase(String input) {
		return input.toLowerCase();
	}

	class LowerCaseFunction implements TLFunctionPrototype {

		public void execute(Stack stack, TLType[] actualParams) {
			stack.push(lowercase(stack.popString()));
		}

	}

	// SUBSTRING
	@TLFunctionAnnotation("Returns a substring of a given string")
	public static final String substring(String input, int from, int length) {
		return input.substring(from, from + length);
	}

	class SubstringFunction implements TLFunctionPrototype {

		public void execute(Stack stack, TLType[] actualParams) {
			final int length = stack.popInt();
			final int from = stack.popInt();
			final String input = stack.popString();
			stack.push(substring(input, from, length));
		}
	}

	// LEFT
	@TLFunctionAnnotation("Returns prefix of the specified length")
	public static final String left(String input, int length) {
		return input.length() < length ? "" : input.substring(0, length);
	}

	class LeftFunction implements TLFunctionPrototype {

		public void execute(Stack stack, TLType[] actualParams) {
			final int length = stack.popInt();
			final String input = stack.popString();
			stack.push(left(input, length));
		}
	}

	// RIGHT
	@TLFunctionAnnotation("Returns suffix of the specified length")
	public static final String right(String input, int length) {
		return input.substring(input.length() - length, input.length());
	}

	class RightFunction implements TLFunctionPrototype {

		public void execute(Stack stack, TLType[] actualParams) {
			final int length = stack.popInt();
			final String input = stack.popString();
			stack.push(right(input, length));
		}
	}

	// TRIM
	@TLFunctionAnnotation("Removes leading and trailing whitespaces from a string.")
	public static final String trim(String input) {
		StringBuilder buf = new StringBuilder(input);
		return StringUtils.trim(buf).toString();

	}

	class TrimFunction implements TLFunctionPrototype {

		public void execute(Stack stack, TLType[] actualParams) {
			stack.push(trim(stack.popString()));
		}
	}

	// LENGTH
	@TLFunctionAnnotation("Returns number of characters in the input string")
	public static final Integer length(String input) {
		return input.length();
	}

	@TLFunctionAnnotation("Returns number of elements in the input list")
	public static final <E> Integer length(List<E> input) {
		return input.size();
	}

	@TLFunctionAnnotation("Returns number of mappings in the input map")
	public static final <K, V> Integer length(Map<K, V> input) {
		return input.size();
	}

	public static Integer length(byte[] input) {
		return input.length;
	}

	@TLFunctionAnnotation("Returns number of fields in the input record")
	public static final Integer length(DataRecord input) {
		return input.getNumFields();
	}

	class LengthFunction implements TLFunctionPrototype {

		public void execute(Stack stack, TLType[] actual) {
			if (actual[0].isString()) {
				stack.push(length(stack.popString()));
				return;
			}

			if (actual[0].isList()) {
				stack.push(length(stack.popList()));
				return;
			}

			if (actual[0].isMap()) {
				stack.push(length(stack.popMap()));
				return;
			}

			if (actual[0].isRecord()) {
				stack.push(length(stack.popRecord()));
				return;
			}

			// FIXME: handle bytearray
			throw new TransformLangExecutorRuntimeException("length - Unknown type: " + actual[0].name());
		}

	}

	// SOUNDEX
	@TLFunctionAnnotation("Calculates string index based on its sound")
	public static final String soundex(String input) {
		StringBuilder targetStrBuf = new StringBuilder(input.toUpperCase());
		targetStrBuf.setLength(0);

		CharSequence src = input;
		final int length = src.length();
		char srcChars[] = new char[length];
		for (int i = 0; i < length; i++) {
			srcChars[i] = Character.toUpperCase(src.charAt(i++));
		}
		char firstLetter = srcChars[0];

		// convert letters to numeric code
		for (int i = 0; i < srcChars.length; i++) {
			switch (srcChars[i]) {
			case 'B':
			case 'F':
			case 'P':
			case 'V':
				srcChars[i] = '1';
				break;
			case 'C':
			case 'G':
			case 'J':
			case 'K':
			case 'Q':
			case 'S':
			case 'X':
			case 'Z':
				srcChars[i] = '2';
				break;
			case 'D':
			case 'T':
				srcChars[i] = '3';
				break;
			case 'L':
				srcChars[i] = '4';
				break;
			case 'M':
			case 'N':
				srcChars[i] = '5';
				break;
			case 'R':
				srcChars[i] = '6';
				break;
			default:
				srcChars[i] = '0';
				break;
			}
		}

		// remove duplicates
		targetStrBuf.append(firstLetter);
		char last = srcChars[0];
		for (int i = 1; i < srcChars.length; i++) {
			if (srcChars[i] != '0' && srcChars[i] != last) {
				last = srcChars[i];
				targetStrBuf.append(last);
			}
		}

		// pad with 0's or truncate
		for (int i = targetStrBuf.length(); i < SoundexFunction.SIZE; i++) {
			targetStrBuf.append('0');
		}
		targetStrBuf.setLength(SoundexFunction.SIZE);
		return targetStrBuf.toString();

	}

	class SoundexFunction implements TLFunctionPrototype {

		public static final int SIZE = 4;

		public void execute(Stack stack, TLType[] actualParams) {
			stack.push(soundex(stack.popString()));
		}
	}

	// REPLACE
	@TLFunctionAnnotation("Replaces matches of a regular expression")
	public static final String replace(String input, String regex, String replacement) {
		Pattern p = Pattern.compile(regex);
		Matcher m = p.matcher(input);
		return m.replaceAll(replacement);
	}

	class ReplaceFunction implements TLFunctionPrototype {

		public void execute(Stack stack, TLType[] actualParams) {
			final String replacement = stack.popString();
			final String regex = stack.popString();
			final String input = stack.popString();
			stack.push(replace(input, regex, replacement));
		}

	}

	// SPLIT
	@TLFunctionAnnotation("Splits the string around regular expression matches")
	public static final List<String> split(String input, String regex) {
		final Pattern p = Pattern.compile(regex);
		final String[] strArray = p.split(input);
		final List<String> list = new ArrayList<String>();
		for (String item : strArray) {
			list.add(item);
		}
		return list;
	}

	class SplitFunction implements TLFunctionPrototype {

		public void execute(Stack stack, TLType[] actualParams) {
			final String regex = stack.popString();
			final String input = stack.popString();
			stack.push(split(input, regex));
		}

	}

	// CHAR AT
	@TLFunctionAnnotation("Returns character at the specified position of input string")
	public static final String char_at(String input, int position) {
		return String.valueOf(input.charAt(position));
	}

	class CharAtFunction implements TLFunctionPrototype {

		public void execute(Stack stack, TLType[] actualParams) {
			int pos = stack.popInt();
			String input = stack.popString();
			stack.push(char_at(input, pos));
		}
	}

	// IS BLANK
	@TLFunctionAnnotation("Checks if the string contains only whitespace characters")
	public static final boolean is_blank(String input) {
		return input == null || input.length() == 0 || StringUtils.isBlank(input);
	}

	class IsBlankFunction implements TLFunctionPrototype {

		public void execute(Stack stack, TLType[] actualParams) {
			stack.push(is_blank(stack.popString()));
		}

	}

	// IS ASCII
	@TLFunctionAnnotation("Checks if the string contains only characters from the US-ASCII encoding")
	public static final boolean is_ascii(String input) {
		return StringUtils.isAscii(input);

	}

	class IsAsciiFunction implements TLFunctionPrototype {

		public void execute(Stack stack, TLType[] actualParams) {
			stack.push(is_ascii(stack.popString()));
		}

	}

	// IS NUMBER
	@TLFunctionAnnotation("Checks if the string can be parsed into a double number")
	public static final boolean is_number(String input) {
		return StringUtils.isNumber(input);
	}

	class IsNumberFunction implements TLFunctionPrototype {

		public void execute(Stack stack, TLType[] actualParams) {
			stack.push(is_number(stack.popString()));
		}
	}

	// IS INTEGER
	@TLFunctionAnnotation("Checks if the string can be parsed into an integer number")
	public static final boolean is_integer(String input) {
		int result = StringUtils.isInteger(input);
		return result == 0 || result == 1;
	}

	class IsIntegerFunction implements TLFunctionPrototype {

		public void execute(Stack stack, TLType[] actualParams) {
			stack.push(is_integer(stack.popString()));
		}
	}

	// IS LONG
	@TLFunctionAnnotation("Checks if the string can be parsed into a long number")
	public static final boolean is_long(String input) {
		int result = StringUtils.isInteger(input);
		return result > 0 || result < 3;
	}

	class IsLongFunction implements TLFunctionPrototype {

		public void execute(Stack stack, TLType[] actualParams) {
			stack.push(is_long(stack.popString()));
		}

	}

	// IS DATE
	@TLFunctionAnnotation("Checks if the string can be parsed into a date with specified pattern")
	public static final boolean is_date(String input, String pattern) {
		return is_date(input, pattern, Locale.getDefault().getDisplayName(), false);
	}

	@TLFunctionAnnotation("Checks if the string can be parsed into a date with specified pattern. Allows changing parser strictness")
	public static final boolean is_date(String input, String pattern, boolean lenient) {
		return is_date(input, pattern, Locale.getDefault().getDisplayName(), lenient);
	}

	@TLFunctionAnnotation("Checks if the string can be parsed into a date with specified pattern and locale.")
	public static final boolean is_date(String input, String pattern, String locale) {
		return is_date(input, pattern, locale, false);
	}

	@TLFunctionAnnotation("Checks if the string can be parsed into a date with specified pattern and locale. Allows changing parser strictness.")
	public static final boolean is_date(String input, String pattern, String locale, boolean lenient) {
		final SimpleDateFormat formatter = new SimpleDateFormat(pattern, MiscUtils.createLocale(locale));
		formatter.setLenient(lenient);
		final ParsePosition p = new ParsePosition(0);

		formatter.parse(input, p);
		// in lenient mode, empty string is not a valid date
		if ("".equals(input)) {
			return lenient;
		}

		return p.getIndex() == input.length();
	}

	class IsDateFunction implements TLFunctionPrototype {

		public void execute(Stack stack, TLType[] parameters) {

			boolean lenient = false;
			String locale = Locale.getDefault().getDisplayName();

			if (parameters.length > 2) {

				if (parameters.length > 3) {
					// if 4 params, it's (string,string,string,bool)
					lenient = stack.popBoolean();
					locale = stack.popString();
				} else {
					// if 3 params it's (string,string,string) or
					// (string,string,bool) if locale is skipped
					if (parameters[2].isString()) {
						locale = stack.popString();
					} else {
						lenient = stack.popBoolean();
					}
				}
			}

			final String pattern = stack.popString();
			final String input = stack.popString();

			stack.push(is_date(input, pattern, locale, lenient));
		}

	}

	// REMOVE DIACRITIC
	@TLFunctionAnnotation("Strips diacritic from characters.")
	public static final String remove_diacritic(String input) {
		return StringUtils.removeDiacritic(input);
	}

	class RemoveDiacriticFunction implements TLFunctionPrototype {

		public void execute(Stack stack, TLType[] actualParams) {
			stack.push(remove_diacritic(stack.popString()));
		}
	}

	// REMOVE BLANK SPACE
	@TLFunctionAnnotation("Removes whitespace characters")
	public static final String remove_blank_space(String input) {
		return StringUtils.removeBlankSpace(input);
	}

	class RemoveBlankSpaceFunction implements TLFunctionPrototype {

		public void execute(Stack stack, TLType[] actualParams) {
			stack.push(remove_blank_space(stack.popString()));
		}
	}

	// REMOVE NONPRINTABLE CHARS
	@TLFunctionAnnotation("Removes nonprintable characters")
	public static final String remove_nonprintable(String input) {
		return StringUtils.removeNonPrintable(input);
	}

	class RemoveNonPrintableFunction implements TLFunctionPrototype {

		public void execute(Stack stack, TLType[] actualParams) {
			stack.push(remove_nonprintable(stack.popString()));
		}

	}

	// REMOVE NONASCII CHARS
	@TLFunctionAnnotation("Removes nonascii characters")
	public static final String remove_nonascii(String input) {
		return StringUtils.removeNonAscii(input);
	}

	class RemoveNonAsciiFunction implements TLFunctionPrototype {

		public void execute(Stack stack, TLType[] actualParams) {
			stack.push(remove_nonascii(stack.popString()));
		}

	}

	// GET ALPHANUMERIC CHARS
	@TLFunctionAnnotation("Extracts only alphanumeric characters from input string")
	public static final String get_alphanumeric_chars(String input) {
		return get_alphanumeric_chars(input, true, true);
	}

	@TLFunctionAnnotation("Extracts letters, numbers or both from input string")
	public static final String get_alphanumeric_chars(String input, boolean takeAlpha, boolean takeNumeric) {
		return StringUtils.getOnlyAlphaNumericChars(input, takeAlpha, takeNumeric);
	}

	class GetAlphanumericCharsFunction implements TLFunctionPrototype {

		public void execute(Stack stack, TLType[] actualParams) {
			if (actualParams.length == 1) {
				stack.push(get_alphanumeric_chars(stack.popString()));
			} else {
				final boolean takeNumeric = stack.popBoolean();
				final boolean takeAlpha = stack.popBoolean();
				final String input = stack.popString();
				stack.push(get_alphanumeric_chars(input, takeAlpha, takeNumeric));
			}
		}

	}

	// TRANSLATE
	@TLFunctionAnnotation("Replaces occurences of characters")
	public static final String translate(String input, String match, String replacement) {
		return String.valueOf(StringUtils.translateSequentialSearch(input, match, replacement));
	}

	class TranslateFunction implements TLFunctionPrototype {

		public void execute(Stack stack, TLType[] actualParams) {
			final String replacement = stack.popString();
			final String match = stack.popString();
			final String input = stack.popString();

			stack.push(translate(input, match, replacement));
		}

	}

	// JOIN
	@TLFunctionAnnotation("Concatenets list elements into a string using delimiter.")
	public static final <E> String join(String delimiter, List<E> values) {
		StringBuffer buf = new StringBuffer();
		for (int i = 0; i < values.size(); i++) {
			buf.append(values.get(i) == null ? "null" : values.get(i).toString());
			if (i < values.size() - 1) {
				buf.append(delimiter);
			}
		}

		return buf.toString();
	}

	@TLFunctionAnnotation("Concatenates all mappings into a string using delimiter.")
	public static final <K, V> String join(String delimiter, Map<K, V> values) {
		StringBuffer buf = new StringBuffer();
		Set<K> keys = values.keySet();
		for (Iterator<K> it = keys.iterator(); it.hasNext();) {
			K key = it.next();
			V value = values.get(key);
			buf.append(key.toString()).append("=").append(value == null ? "null" : value.toString());
			if (it.hasNext()) {
				buf.append(delimiter);
			}

		}

		return buf.toString();
	}

	class JoinFunction implements TLFunctionPrototype {

		public void execute(Stack stack, TLType[] actualParams) {
			if (actualParams[1].isList()) {
				final List<Object> values = stack.popList();
				final String delim = stack.popString();
				stack.push(join(delim, values));
			} else {
				final Map<Object, Object> values = stack.popMap();
				final String delim = stack.popString();
				stack.push(join(delim, values));
			}
		}

	}

	// INDEX OF
	@TLFunctionAnnotation("Returns the first occurence of a specified string")
	public static final Integer index_of(String input, String pattern) {
		return index_of(input, pattern, 0);
	}

	@TLFunctionAnnotation("Returns the first occurence of a specified string")
	public static final Integer index_of(String input, String pattern, int from) {
		return StringUtils.indexOf(input, pattern, from);
	}

	class IndexOfFunction implements TLFunctionPrototype {

		public void execute(Stack stack, TLType[] actualParams) {
			int from = 0;

			if (actualParams.length > 2) {
				from = stack.popInt();
			}

			final String pattern = stack.popString();
			final String input = stack.popString();
			stack.push(index_of(input, pattern, from));
		}

	}

	// COUNT_CHAR
	@TLFunctionAnnotation("Calculates the number of occurences of the specified character")
	public static final Integer count_char(String input, String character) {
		return StringUtils.count(input, character.charAt(0));
	}

	class CountCharFunction implements TLFunctionPrototype {

		public void execute(Stack stack, TLType[] actualParams) {
			String character = stack.popString();
			String input = stack.popString();
			stack.push(count_char(input, character));
		}
	}

	// FIND
	@TLFunctionAnnotation("Finds and returns all occurences of regex in specified string")
	public static final List<String> find(String input, String pattern) {
		final Pattern p = Pattern.compile(pattern);
		final Matcher m = p.matcher(input);
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

		public void execute(Stack stack, TLType[] actualParams) {
			final String pattern = stack.popString();
			final String input = stack.popString();
			stack.push(find(input, pattern));
		}
	}

	// CHOP
	@TLFunctionAnnotation("Removes new line characters from input string")
	public static final String chop(String input) {
		return chop(input, "[\r\n]+$");
	}

	@TLFunctionAnnotation("Removes pattern  from input string")
	public static final String chop(String input, String pattern) {
		final Pattern p = Pattern.compile(pattern);
		final Matcher m = p.matcher(input);
		return m.replaceAll("");

	}

	class ChopFunction implements TLFunctionPrototype {

		public void execute(Stack stack, TLType[] actualParams) {
			if (actualParams.length == 1) {
				stack.push(chop(stack.popString()));
			} else {
				final String pattern = stack.popString();
				final String input = stack.popString();
				stack.push(chop(input, pattern));

			}
		}

	}

	// CUT
	@TLFunctionAnnotation("Cuts substring from specified string based on list consisting of pairs position,length")
	public static final List<String> cut(String input, List<Integer> indexes) {
		if (indexes.size() % 2 != 0) {
			throw new TransformLangExecutorRuntimeException("Incorrect number of indices: " + indexes.size() + ". Must be an even number");
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

		public void execute(Stack stack, TLType[] actualParams) {
			final List<Object> indices = stack.popList();
			final String input = stack.popString();
			stack.push(cut(input, TLFunctionLibrary.<Integer> convertTo(indices)));
		}
	}

	// EDIT DISTANCE
	@TLFunctionAnnotation("Calculates edit distance between two strings.")
	public static final Integer edit_distance(String str1, String str2) {
		return edit_distance(str1, str2, StringAproxComparator.IDENTICAL, null, -1);
	}

	@TLFunctionAnnotation("Calculates edit distance between two strings. Allows changing strength of comparsion.")
	public static final Integer edit_distance(String str1, String str2, int strength) {
		return edit_distance(str1, str2, strength, null, -1);
	}

	@TLFunctionAnnotation("Calculates edit distance between two strings. Allows changing locale for comparsion.")
	public static final Integer edit_distance(String str1, String str2, String locale) {
		return edit_distance(str1, str2, StringAproxComparator.IDENTICAL, locale, -1);
	}

	@TLFunctionAnnotation("Calculates edit distance between two strings. Allows changing locale for comparsion and maximum amount of letters to be changed.")
	public static final Integer edit_distance(String str1, String str2, String locale, int maxDifference) {

		return edit_distance(str1, str2, StringAproxComparator.IDENTICAL, locale, maxDifference);
	}

	@TLFunctionAnnotation("Calculates edit distance between two strings. Allows changing strenght of comparsion and locale for comparsion.")
	public static final Integer edit_distance(String str1, String str2, int strength, String locale) {

		return edit_distance(str1, str2, strength, locale, -1);
	}

	@TLFunctionAnnotation("Calculates edit distance between two strings. Allows changing strenght of comparsion and maximum amount of letters to be changed.")
	public static final Integer edit_distance(String str1, String str2, int strength, int maxDifference)
			throws JetelException {

		return edit_distance(str1, str2, strength, null, maxDifference);
	}

	@TLFunctionAnnotation("Calculates edit distance between two strings. Allows changing strenght of comparsion, locale for comparsion and maximum amount of letters to be changed.")
	public static final Integer edit_distance(String str1, String str2, int strength, String locale, int maxDifference) {

		boolean[] s = new boolean[4];
		Arrays.fill(s, false);
		s[4 - strength] = true;
		StringAproxComparator comparator = null;
		try {
			comparator = StringAproxComparator.createComparator(locale, s);
		} catch (JetelException e) {
			throw new TransformLangExecutorRuntimeException(e.getMessage());
		}
		if (maxDifference > -1) {
			comparator.setMaxLettersToChange(maxDifference);
		}
		return comparator.distance(str1, str2) / comparator.getMaxCostForOneLetter();
	}

	class EditDistanceFunction implements TLFunctionPrototype {

		public void execute(Stack stack, TLType[] parameters) {
			Integer maxDifference = -1;
			String locale = null;
			Integer strength = StringAproxComparator.IDENTICAL;
			switch (parameters.length) {
			case 5:
				maxDifference = stack.popInt();
			case 4:
				if (parameters[3].isString())
					locale = stack.popString();
				else
					maxDifference = stack.popInt();
			case 3:
				if (parameters[2].isString())
					locale = stack.popString();
				else
					strength = stack.popInt();

			}
			final String string2 = stack.popString();
			final String string1 = stack.popString();
			stack.push(edit_distance(string1, string2, strength, locale, maxDifference));
		}
	}

	@TLFunctionAnnotation("Finds the metaphone value of a String.")
	public static final String metaphone(String input) {
		return StringUtils.metaphone(input);
	}

	@TLFunctionAnnotation("Finds the metaphone value of a String. Allows changing maximal length of output string.")
	public static final String metaphone(String input, int maxLength) {
		return StringUtils.metaphone(input, maxLength);
	}

	class MetaphoneFunction implements TLFunctionPrototype {

		public void execute(Stack stack, TLType[] actualParams) {
			if (actualParams.length > 1) {
				Integer maxLength = stack.popInt();
				stack.push(metaphone(stack.popString(), maxLength));
			} else {
				stack.push(metaphone(stack.popString()));
			}
		}
	}

	@TLFunctionAnnotation("Finds The New York State Identification and Intelligence System Phonetic Code.")
	public static final String nysiis(String input) {
		return StringUtils.NYSIIS(input);
	}

	class NYSIISFunction implements TLFunctionPrototype {

		public void execute(Stack stack, TLType[] actualParams) {
			stack.push(nysiis(stack.popString()));
		}
	}

	@TLFunctionAnnotation("Generates a random string.")
	public static String random_string(int minLength, int maxLength) {
		return getGenerator(Thread.currentThread()).nextString(minLength, maxLength);
	}

	@TLFunctionAnnotation("Generates a random string. Allows changing seed.")
	public static String random_string(int minLength, int maxLength, long randomSeed) {
		DataGenerator generator = getGenerator(Thread.currentThread());
		generator.setSeed(randomSeed);
		return generator.nextString(minLength, maxLength);
	}

	class RandomStringFunction implements TLFunctionPrototype {

		public void execute(Stack stack, TLType[] actualParams) {
			Integer maxLength;
			Integer minLength;
			if (actualParams.length > 2) {
				Long seed = stack.popLong();
				maxLength = stack.popInt();
				minLength = stack.popInt();
				stack.push(random_string(minLength, maxLength, seed));
			} else {
				maxLength = stack.popInt();
				minLength = stack.popInt();
				stack.push(random_string(minLength, maxLength));
			}
		}
	}
}
