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
package org.jetel.data.parser;

import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;

import org.jetel.test.CloverTestCase;
import org.jetel.util.SpreadsheetIndexIterator;

/**
 * @author lkrejci (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 26 Aug 2011
 */
public class SpreadsheetIndexIteratorTest extends CloverTestCase {
	
	private static final List<String> SHEET_NAMES = Arrays.asList("BAGR", "bagr", "bagr45", "454", "poslednibagr", "2", "2,1-5");
	
	public void testWildcard() {
		int maximum = 20;
		SpreadsheetIndexIterator iterator = new SpreadsheetIndexIterator(null, "*", 0, maximum);
		Integer expected = 0;
		while (iterator.hasNext()) {
			if (expected > maximum) {
				fail("Unexpected result: " + iterator.next());
				
			}
			assertEquals(expected++, iterator.next());
		}
	}
	
	public void testNumbers() {
		try {
			testIterator(null, 0, 10, "2,6,7,18", 2, 6, 7);
			fail();
		} catch (NoSuchElementException e) {
			// this is correct
		}
	}
	
	public void testNumberRange() {
		testIterator(null, 0, 10, "2-4,3-8", 2, 3, 4, 3, 4, 5, 6, 7, 8);
	}
	
	public void testNumberRangeWildcardStart() {
		testIterator(null, -1, 10, "*-8", -1, 0, 1, 2, 3, 4, 5, 6, 7, 8);
	}
	
	public void testNumberRangeWildcardEnd() {
		testIterator(null, 0, 3, "0-*", 0, 1, 2, 3);
	}
	
	public void testSheetName() {
		testIterator(SHEET_NAMES, 0, SHEET_NAMES.size() - 1, "bagr45", 2);
	}
	
	public void testMultipleSheetName() {
		testIterator(SHEET_NAMES, 0, SHEET_NAMES.size() - 1, "bagr45,BAGR,bagr", 2,0,1);
	}
	
	public void testEscapedSheetName() {
		testIterator(SHEET_NAMES, 0, SHEET_NAMES.size() - 1, "[454]", 3);
	}
	
	public void testUnEscapedNumberSheetNameNonExisting() {
		try {
			testIterator(SHEET_NAMES, 0, SHEET_NAMES.size() - 1, "454");
			fail();
		} catch (NoSuchElementException e) {
			// this is correct
		}
	}
	
	public void testUnEscapedNumberSheetName() {
		testIterator(SHEET_NAMES, 0, SHEET_NAMES.size() - 1, "2", 2);
	}
	
	public void testNameRangeStart() {
		testIterator(SHEET_NAMES, 0, SHEET_NAMES.size() - 1, "*-[454]", 0, 1, 2, 3);
	}
	
	public void testNameRangeEnd() {
		testIterator(SHEET_NAMES, 0, SHEET_NAMES.size() - 1, "[454]-*", 3, 4, 5, 6);
	}
	
	public void testComplexCombination() {
		testIterator(SHEET_NAMES, 0, SHEET_NAMES.size() - 1, "2,*bagr*,*-[2],2-[2,1-5],0,1-5", 2, 1, 2, 4, 0, 1, 2, 3, 4, 5, 2, 3, 4, 5, 6, 0, 1, 2, 3, 4, 5);
	}

	private void testIterator(List<String> sheets, int min, int max, String pattern, Integer... expectedResult) {
		SpreadsheetIndexIterator iterator = new SpreadsheetIndexIterator(sheets, pattern, min, max);
		int expectedIndex = 0;
		while (iterator.hasNext()) {
			if (expectedIndex >= expectedResult.length) {
				fail("Unexpected result: " + iterator.next());
			}
			assertEquals("Unexpected result at index " + expectedIndex, expectedResult[expectedIndex++], iterator.next());
		}

		if (expectedIndex < expectedResult.length) {
			fail("Missing result: " + expectedResult[expectedIndex] + " at index " + expectedIndex);
		}
	}

}
