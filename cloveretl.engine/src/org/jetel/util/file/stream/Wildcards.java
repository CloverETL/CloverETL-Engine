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
package org.jetel.util.file.stream;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.DirectoryIteratorException;
import java.nio.file.DirectoryStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import org.jetel.data.Defaults;
import org.jetel.util.file.FileUtils;
import org.jetel.util.file.SandboxUrlUtils;

/**
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 6. 9. 2016
 */
public class Wildcards {

	// Wildcard characters.
	public final static char[] WCARD_CHAR = {'*', '?'};

	// Regex substitutions for wildcards. 
	private final static String[] REGEX_SUBST = {".*", "."};

	public static class AcceptAllFilter<T> implements DirectoryStream.Filter<T> {
		private AcceptAllFilter() {
		}

		@Override
		public boolean accept(T entry) {
			return true;
		}

		private static final AcceptAllFilter<Object> FILTER = new AcceptAllFilter<Object>();
		
		@SuppressWarnings("unchecked")
		public static final <T> AcceptAllFilter<T> getInstance() {
			return (AcceptAllFilter<T>) FILTER;
		}
	}
	
	public static class CheckConfigFilter implements DirectoryStream.Filter<String> {
		
		private final URL contextUrl;
		
		public CheckConfigFilter(URL contextUrl) {
			this.contextUrl = contextUrl;
		}
		
		@Override
		public boolean accept(String input) throws IOException {
			if (FileUtils.STD_CONSOLE.equals(input)) {
				return false;
			}
			URL url = FileUtils.getFileURL(contextUrl, input);
			return accept(url);
		}
		
		private boolean accept(URL url) throws IOException {
			return SandboxUrlUtils.isSandboxUrl(url) || !FileUtils.isServerURL(url);
		}
		
		public List<String> filter(String... items) throws IOException {
			List<String> result = new ArrayList<>(items.length);
			for (String item: items) {
				if (accept(item)) {
					result.add(item);
				}
			}
			return result;
		}

	}

	public static DirectoryStream<Input> newDirectoryStream(URL contextUrl, Iterable<String> patterns) {
		WildcardDirectoryStream stream = new WildcardDirectoryStream(contextUrl, patterns);
		return new CompoundDirectoryStream(stream);
	}

	public static DirectoryStream<Input> newDirectoryStream(URL contextUrl, String[] patterns) {
		return newDirectoryStream(contextUrl, Arrays.asList(patterns));
	}

	public static DirectoryStream<Input> newDirectoryStream(URL contextUrl, String patterns) {
		String[] parts = patterns.isEmpty() ? new String[0] : patterns.split(Defaults.DEFAULT_PATH_SEPARATOR_REGEX);
		return newDirectoryStream(contextUrl, parts);
	}
	
	private static class CompoundDirectoryStream extends AbstractDirectoryStream<Input> {
		
		private final WildcardDirectoryStream parent;
		private DirectoryStream<Input> currentStream;
		private Iterator<Input> currentIterator;
		
		private Input next;

		/**
		 * @param parent
		 */
		public CompoundDirectoryStream(WildcardDirectoryStream parent) {
			this.parent = parent;
		}

		@Override
		public void close() throws IOException {
			try {
				super.close();
			} finally {
				parent.close();
			}
		}
		
		private Input fillCache() throws IOException {
			if (currentStream == null) {
				if (parent.hasNext()) {
					currentStream = parent.next();
					currentIterator = currentStream.iterator();
				} else {
					return null;
				}
			}
			
			while (!currentIterator.hasNext() && parent.hasNext()) {
				currentStream.close();
				currentStream = parent.next();
				currentIterator = currentStream.iterator();
			}
			
			if (currentIterator.hasNext()) {
				next = currentIterator.next();
			}
			
			return next;
		}
		
		private Input getNext() throws IOException {
			if (next == null) {
				next = fillCache();
			}
			return next;
		}

		@Override
		public boolean hasNext() {
			try {
				return (getNext() != null);
			} catch (IOException e) {
				FileUtils.closeQuietly(this);
				throw new DirectoryIteratorException(e);
			}
		}

		@Override
		public Input next() {
			Input result;
			try {
				result = getNext();
			} catch (IOException e) {
				throw new DirectoryIteratorException(e);
			}
			next = null;
			return result;
		}

	}

	/**
	 * Returns true if the URL, in the concrete his file, has a wildcard. 
	 * @param url
	 * @return
	 */
	public static boolean hasWildcard(URL url) {
		return hasWildcard(url.getFile());
	}
	
	public static boolean hasWildcard(String fileURL) {
		// check if the url has wildcards
		String fileName = new File(fileURL).getName();
		for (int wcardIdx = 0; wcardIdx < WCARD_CHAR.length; wcardIdx++) {
			if (fileName.indexOf("" + WCARD_CHAR[wcardIdx]) >= 0) { // wildcard found
				return true;
			}
		}
		return false;
	}

	/**
	 * Creates compiled Pattern from String pattern with simplified syntax -- containing '*' and '?' symbols.
	 * 
	 * @param mask
	 * @return
	 */
	public static Pattern compileSimplifiedPattern(String mask) {
		return compileSimplifiedPattern(mask, WCARD_CHAR, REGEX_SUBST);
	}

	/**
	 * Creates compiled Pattern from String pattern. Replaces characters from wildcardCharacters with strings from regexSubstitutions.
	 * @param mask
	 * @param wildcardCharacters eg. {'*', '?'}
	 * @param regexSubstitutions eg. {".*", "."}
	 * @return
	 */
	public static Pattern compileSimplifiedPattern(String mask, char[] wildcardCharacters, String[] regexSubstitutions) {
		StringBuilder regex = new StringBuilder(mask);
		regex.insert(0, REGEX_START_ANCHOR + REGEX_START_QUOTE);
		for (int wcardIdx = 0; wcardIdx < wildcardCharacters.length; wcardIdx++) {
			regex.replace(0, regex.length(), regex.toString().replace("" + wildcardCharacters[wcardIdx],
					REGEX_END_QUOTE + regexSubstitutions[wcardIdx] + REGEX_START_QUOTE));
		}
		regex.append(REGEX_END_QUOTE + REGEX_END_ANCHOR);

		return Pattern.compile(regex.toString());
	}

	// Start sequence for regex quoting.
	private final static String REGEX_START_QUOTE = "\\Q";

	// End sequence for regex quoting
	private final static String REGEX_END_QUOTE = "\\E";

	// Regex start anchor.
	private final static char REGEX_START_ANCHOR = '^';

	// Regex end anchor.
	private final static char REGEX_END_ANCHOR = '$';

}
