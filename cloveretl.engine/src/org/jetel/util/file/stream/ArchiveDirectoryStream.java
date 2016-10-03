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
import java.io.InputStream;
import java.nio.file.DirectoryIteratorException;
import java.nio.file.DirectoryStream;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.regex.Pattern;

import org.jetel.component.fileoperation.URIUtils;
import org.jetel.util.ExceptionUtils;
import org.jetel.util.file.FileUtils;

/**
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 5. 9. 2016
 */
public abstract class ArchiveDirectoryStream<Stream extends InputStream, Entry> extends AbstractDirectoryStream<Input> {
	
	private final DirectoryStream<Input> parentStream;
	private final Iterator<Input> parent;
	private final String glob;
	private final Pattern pattern;
	
	protected Stream archiveInputStream;
	protected Input currentInput;
	
	private Entry cachedEntry;
	
	private boolean firstEntry = true;

	/**
	 * 
	 */
	protected ArchiveDirectoryStream(DirectoryStream<Input> parent, String glob) {
		this.parentStream = parent;
		this.parent = parent.iterator();
		if (glob == null) {
			glob = "";
		}
		this.glob = glob;
		boolean containsWildcard = (glob.contains("?") || glob.contains("*"));
		pattern = containsWildcard ? Pattern.compile(glob.replaceAll("\\\\", "\\\\\\\\").replaceAll("\\.", "\\\\.").replaceAll("\\?", "\\.").replaceAll("\\*", ".*")) : null; 
	}

	@Override
	public void close() throws IOException {
		try {
			super.close();
			FileUtils.close(archiveInputStream);
			archiveInputStream = null;
		} finally {
			parentStream.close();
		}
	}
	
	/**
	 * Reads an entry from the current stream.
	 * Closes the stream if the entry is <code>null</code>.
	 * 
	 * @return
	 * @throws IOException
	 */
	private Entry readEntry() throws IOException {
		if ((archiveInputStream == null) && parent.hasNext()) {
			getNextArchiveInputStream();
		}
		
		Entry entry = null;
		if (archiveInputStream != null) {
			try {
				entry = getNextEntry();
			} finally {
				if (entry == null) {
					archiveInputStream.close();
					archiveInputStream = null;
				}
			}
		}
		return entry;
	}
	
	protected abstract Entry getNextEntry() throws IOException;
	
	/**
	 * Reads an entry, iterating through the available streams if necessary.
	 * 
	 * @return
	 * @throws IOException
	 */
	private Entry readNextEntry() throws IOException {
		Entry entry = readEntry();
		
		for (; (entry == null) && parent.hasNext(); entry = readEntry()) {
			getNextArchiveInputStream();
		}
		
		return entry;
	}

	protected abstract Stream newArchiveInputStream(InputStream is) throws IOException;
	
	private void getNextArchiveInputStream() throws IOException {
		Input input = parent.next();
		currentInput = input;
		InputStream is = null;
		try {
			is = input.getInputStream();
			archiveInputStream = newArchiveInputStream(is);
		} catch (Exception e) {
			FileUtils.closeQuietly(is);
			throw ExceptionUtils.getIOException(e);
		}
	}
	
	protected abstract boolean isDirectory(Entry entry);
	
	protected abstract String getName(Entry entry);
	
	/**
	 * Reads entries until it finds a match.
	 * Returns the first encountered matching entry.
	 * Skips directories.
	 * 
	 * @return
	 * @throws IOException
	 */
	/**
	 * Reads entries until it finds a match.
	 * Returns the first encountered matching entry.
	 * Skips directories.
	 * 
	 * @return
	 * @throws IOException
	 */
	private Entry readMatchingEntry() throws IOException {
		for (Entry entry = readNextEntry(); entry != null; entry = readNextEntry()) {
			if (isDirectory(entry)) {
				continue; // skip directories
			}
			if (pattern != null) {
				if (pattern.matcher(getName(entry)).matches()) {
					return entry;
				}
			} else if ((glob.isEmpty() && firstEntry) || glob.equals(getName(entry))) {
				return entry;
			}
		}
		
		return null;
	}
	
	/**
	 * Returns the cached entry.
	 * Searches for the next matching entry if the cache is empty.
	 * 
	 * @return
	 * @throws IOException
	 */
	private Entry getNextMatchingEntry() throws IOException {
		if (cachedEntry == null) {
			cachedEntry = readMatchingEntry();
			firstEntry = false;
		}
		
		return cachedEntry;
	}
	
	@Override
	public boolean hasNext() {
		try {
			return (getNextMatchingEntry() != null);
		} catch (IOException e) {
			throw new DirectoryIteratorException(e);
		}
	}
	
	protected abstract String getEntryUrl(String innerUrl, String entryNameUrlEncoded);

	@Override
	public Input next() {
		try {
			Entry entry = getNextMatchingEntry();
			if (entry == null) {
				throw new NoSuchElementException();
			}
			Input input = getEntryInput(entry);
			cachedEntry = null;
			return input;
		} catch (IOException e) {
			throw new DirectoryIteratorException(e);
		}
	}

	protected Input getEntryInput(Entry entry) {
		String entryName = getName(entry);
		return new ArchiveInput(getEntryUrl(currentInput.getAbsolutePath(), URIUtils.urlEncodePath(entryName)), archiveInputStream);
	}

}
