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

import java.io.IOException;
import java.net.URL;
import java.nio.file.DirectoryIteratorException;
import java.nio.file.DirectoryStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

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
	
	/**
	 * Flattens DirectoryStream<DirectoryStream<Input>> into DirectoryStream<Input>.
	 * 
	 * @author krivanekm (info@cloveretl.com)
	 *         (c) Javlin, a.s. (www.cloveretl.com)
	 *
	 * @created 5. 10. 2016
	 */
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

}
