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
package org.jetel.util.spreadsheet;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jetel.util.file.WcardPattern;
import org.jetel.util.string.StringUtils;

/**
 * @author lkrejci (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 26 Aug 2011
 */
public class SpreadsheetIndexIterator implements Iterator<Integer> {

	private static String UNLIMITED = "*";

	private static final String ESCAPED_SHEET = "\\" + SpreadsheetUtils.SHEET_NAME_ESCAPE_START + "[^" + SpreadsheetUtils.INVALID_SHEET_CHARACTERS + "]{1,31}\\" + SpreadsheetUtils.SHEET_NAME_ESCAPE_END;
	private static final String UNESCAPED_SHEET = "[^" + SpreadsheetUtils.INVALID_UNESCAPED_CHARACTERS + "]{1,31}";
	private static final Pattern SHEET = Pattern.compile(ESCAPED_SHEET + "|" + UNESCAPED_SHEET + "|\\*|[\\d]+");

	private final List<String> sheetNames;
	private final String pattern;
	private final int first;
	private final int last;

	private int patternIndex = 0;

	private Integer rangeLast;
	private Pattern sheetWcardPattern;
	private int sheetCounter;

	private Integer next = null;
	private Integer tmp;

	public SpreadsheetIndexIterator(List<String> sheetNames, String pattern, int start, int end) {
		this.sheetNames = sheetNames;
		this.pattern = pattern;
		this.first = start;
		this.last = end;

		next = getNext();
	}

	private Integer getNext() {
		if (rangeLast != null && next < rangeLast) {
			return ++next;
		}

		if (sheetWcardPattern != null) {
			Integer result = getWildcardSheet();
			if (result != null) {
				return result;
			}
		}

		if (patternIndex < pattern.length()) {
			return parseNextPattern();
		}

		return null;
	}

	private Integer parseNextPattern() {
		int commaIndex = pattern.indexOf(',', patternIndex);
		int escapeStartIndex = pattern.indexOf(SpreadsheetUtils.SHEET_NAME_ESCAPE_START, patternIndex);
		if (escapeStartIndex != -1) {
			if (commaIndex > escapeStartIndex) {
				int escapeEndIndex = pattern.indexOf(SpreadsheetUtils.SHEET_NAME_ESCAPE_END, patternIndex);
				if (commaIndex < escapeEndIndex) {
					commaIndex = pattern.indexOf(',', escapeEndIndex);
				}
			}
		}
		String patternPart;
		if (commaIndex == -1) {
			patternPart = pattern.substring(patternIndex);
			patternIndex = pattern.length();
		} else {
			patternPart = pattern.substring(patternIndex, commaIndex);
			patternIndex = commaIndex + 1;
		}

		Matcher matcher = SHEET.matcher(patternPart);
		if (matcher.find()) {
			String intervalStart = patternPart.substring(matcher.start(), matcher.end());
			if (matcher.find(matcher.end())) {
				String intervalEnd = patternPart.substring(matcher.start(), matcher.end());

				if (intervalEnd.equals(intervalStart)) {
					throw new NoSuchElementException("Incorrect format of range: '" + patternPart + "'");
				}

				rangeLast = getSheetIndex(intervalEnd, false);
				return getSheetIndex(intervalStart, true);
			} else {
				String toCompile = intervalStart;
				if (UNLIMITED.equals(toCompile)) {
					rangeLast = getSheetIndex(toCompile, false);
					return getSheetIndex(intervalStart, true);
				} else {
					int isInteger = StringUtils.isInteger(toCompile);
					if (isInteger == 0 || isInteger == 1) {
						int toReturn = Integer.parseInt(toCompile);
						if (toReturn >= first && toReturn < last) {
							rangeLast = null;
							return toReturn;
						} else {
							throw new NoSuchElementException();
						}
					}
					
					if (intervalStart.charAt(0) == SpreadsheetUtils.SHEET_NAME_ESCAPE_START) {
						toCompile = intervalStart.substring(1, intervalStart.length() - 1);
					}
					
					sheetWcardPattern = WcardPattern.compileSimplifiedPattern(toCompile);
					sheetCounter = 0;
					return getWildcardSheet();
				}
			}
		}
		rangeLast = null;
		return null;
	}

	private int getSheetIndex(String sheetName, boolean start) {
		if (sheetName.equals(UNLIMITED)) {
			return start ? first : last;
		} else {
			int isInteger = StringUtils.isInteger(sheetName);
			if (isInteger == 0 || isInteger == 1) {
				return Integer.parseInt(sheetName);
			} else {
				String sheet = sheetName;
				if (sheetName.charAt(0) == SpreadsheetUtils.SHEET_NAME_ESCAPE_START) {
					sheet = sheetName.substring(1, sheetName.length() - 1);
				}

				int sheetIndex = sheetNames.indexOf(sheet);

				if (sheetIndex == -1) {
					throw new NoSuchElementException("Sheet " + sheet + " could not be found");
				}

				return sheetIndex;
			}
		}
	}

	private Integer getWildcardSheet() {
		for (; sheetCounter < sheetNames.size(); sheetCounter++) {
			if (sheetWcardPattern.matcher(sheetNames.get(sheetCounter)).matches()) {
				return sheetCounter++;
			}
		}
		sheetWcardPattern = null;
		return null;
	}

	@Override
	public boolean hasNext() {
		return next != null;
	}

	@Override
	public Integer next() {
		if (next == null) {
			throw new NoSuchElementException();
		}

		tmp = next;
		next = getNext();

		return tmp;
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}
}
