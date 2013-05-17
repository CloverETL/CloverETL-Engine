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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import junit.framework.Assert;

import org.junit.Test;

/**
 * @author tkramolis (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 17.5.2013
 */
public class AhoCorasickTest {
	
	@Test
	public void test() {
		List<String[]> patterns = new ArrayList<String[]>();
		
		patterns.add(new String[] {"a", "ab", "bc", "bca", "c", "caa"});
		patterns.add(new String[] {"cc", "abc", "cab", "b", "bccab", "bc", "abcca", "cca"});
		patterns.add(new String[] {"ab", "cc", "a", "cab", "bccab", "bcca", "abcca", "bc", "bcc", "cca", "abcc"});
		patterns.add(new String[] {"ab", "c", "abccab", "cc", "cab", "bcca", "bccab", "bc"});
		patterns.add(new String[] {"ab", "abcca", "c", "abcca", "bc", "bcc", "cca"});
		patterns.add(new String[] {"c", "abc", "ca", "a", "b", "bccab", "bc", "cca", "bcc", "abcc"});

		for (String[] pattern : patterns) {
			AhoCorasickTester ahoCorasickTest = new AhoCorasickTester("abccab");
			ahoCorasickTest.addPatterns(pattern);
			ahoCorasickTest.runTest();
		}
	}

	private static class AhoCorasickTester {
		
		private List<Set<Integer>> patternsEndingOnPosition;
		private List<Integer> allPatternIndexes = new ArrayList<Integer>();
		private List<String> allPatterns = new ArrayList<String>();
		private String testInput;
		private AhoCorasick ahoCorasick;
		
		public AhoCorasickTester(String testInput) {
			this.testInput = testInput;
			this.ahoCorasick = new AhoCorasick();
			patternsEndingOnPosition = new ArrayList<Set<Integer>>();
			
			for (int i = 0; i < testInput.length(); i++) {
				patternsEndingOnPosition.add(new HashSet<Integer>());
			}
		}
		
		private void addPatterns(String... patterns) {
			for (int i = 0; i < patterns.length; i++) {
				addPattern(patterns[i], i);
			}
		}
		
		/**
		 * @param pattern
		 * @param idx ID of the pattern  
		 * @param matchPositions pattern occurrence indexes in the test input string 
		 */
		private void addPattern(String pattern, int idx) {
			ahoCorasick.addPattern(pattern, idx);
			
			allPatternIndexes.add(idx);
			allPatterns.add(pattern);
			
			int matchIndex = testInput.indexOf(pattern);
			while (matchIndex >= 0) {
				patternsEndingOnPosition.get(matchIndex + pattern.length() - 1).add(idx);
			    matchIndex = testInput.indexOf(pattern, matchIndex + 1);
			}			
		}
		
		private void runTest() {
			ahoCorasick.compile();
			
			for (int i = 0; i < testInput.length(); i++) {
				ahoCorasick.update(testInput.charAt(i));
				Set<Integer> expectedMatchingPatterns = patternsEndingOnPosition.get(i);
				for (int j = 0; j < allPatternIndexes.size(); j++) {
					int patternIdx = allPatternIndexes.get(j);
					Assert.assertEquals("Unexpected result of isPattern() for pattern '" + allPatterns.get(j) + "' at index " + i + " of input string '" + testInput + "';", expectedMatchingPatterns.contains(patternIdx), ahoCorasick.isPattern(patternIdx));
				}
			}
		}

	}
}
