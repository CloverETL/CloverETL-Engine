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
import java.net.URLDecoder;
import java.nio.file.DirectoryIteratorException;
import java.nio.file.DirectoryStream;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;

import org.jetel.enums.ArchiveType;
import org.jetel.util.file.CustomPathResolver;
import org.jetel.util.file.FileURLParser;
import org.jetel.util.file.FileUtils;
import org.jetel.util.file.WcardPattern;

public class WildcardDirectoryStream extends AbstractDirectoryStream<DirectoryStream<Input>> {

	private final URL contextUrl;
	// Collection of filename patterns.
	private final Iterator<String> patterns;
	
	private DirectoryStream<Input> currentIterator;
	
	/**
	 * @param patterns
	 */
	public WildcardDirectoryStream(URL contextUrl, Iterable<String> patterns) {
		Objects.requireNonNull(patterns);
		this.contextUrl = contextUrl;
		this.patterns = patterns.iterator();
	}

	@Override
	public void close() throws IOException {
		FileUtils.close(currentIterator);
	}

	@Override
	public boolean hasNext() {
		return patterns.hasNext();
	}
	
	protected DirectoryStream<Input> newDirectoryStream(String fileName) throws IOException {
		URL url = FileUtils.getFileURL(contextUrl, fileName); // CL-2667: may throw MalformedURLException

		// try CustomPathResolvers first
		for (CustomPathResolver resolver : FileUtils.getCustompathresolvers()) {
			if (resolver.handlesURL(contextUrl, fileName)) {
				try {
					List<String> resolved = resolver.resolveWildcardURL(contextUrl, fileName);
					if (resolved != null) {
						return new CollectionDirectoryStream(contextUrl, resolved);
					}
				} catch (IOException e) {
					// NOTHING - will be handled the standard way below
				}
			}
		}
		
		// get inner source
		String originalFileName = fileName;
		Matcher matcher = FileURLParser.getURLMatcher(fileName);
		String innerSource = null;
		int iPreName = 0;
		int iPostName = 0;
		if (matcher != null && (innerSource = matcher.group(5)) != null) {
			iPreName = (matcher.group(2) + matcher.group(3)).length()+1;
			iPostName = iPreName + innerSource.length();
		} else {
			// for archives without ...:(......), just ...:......
			Matcher archMatcher = WcardPattern.getArchiveURLMatcher(fileName);
			if (archMatcher != null && (innerSource = archMatcher.group(3)) != null) {
				iPreName = archMatcher.group(2).length()+1;
				iPostName = iPreName + innerSource.length();
			} else if (archMatcher != null && (innerSource = archMatcher.group(7)) != null) {
				iPreName = archMatcher.group(6).length()+1;
				iPostName = iPreName + innerSource.length();
			}
		}
		
		StringBuilder sbAnchor = new StringBuilder();
		StringBuilder sbInnerInput = new StringBuilder();
		ArchiveType archiveType = FileUtils.getArchiveType(fileName, sbInnerInput, sbAnchor);
		if (innerSource == null) {
			innerSource = sbInnerInput.toString();
		}

		if (archiveType != null) {
			DirectoryStream<Input> innerStream = newDirectoryStream(innerSource);
			String anchor = sbAnchor.toString();
			anchor = URLDecoder.decode(anchor, FileUtils.UTF8); // CL-2579
			switch (archiveType) {
			case ZIP:
				return new ZipDirectoryStream(innerStream, anchor);
			case TAR:
				return new TarDirectoryStream(innerStream, anchor);
			case TGZ:
				return new TgzDirectoryStream(innerStream, anchor);
			case GZIP:
				return new GzipDirectoryStream(innerStream);
			default:
				throw new UnsupportedOperationException("Unsupported archive type: " + archiveType);
			}
		}

		// most protocols are handled by making a list of file URLs and iterating through the list
		Collection<String> filenames = listFiles(url, fileName);
		return new CollectionDirectoryStream(contextUrl, filenames);
	}
	
	private Collection<String> listFiles(URL url, String fileName) throws IOException {
		if (FileUtils.STD_CONSOLE.equals(fileName)) {
			return Collections.singletonList(fileName);
		}
		
		String protocol = url.getProtocol().toLowerCase();
		switch (protocol) {
			case FileUtils.PORT_PROTOCOL:
				return Collections.<String>emptyList(); // returning fileName causes infinite loop, why?
			default:
				WcardPattern wcardPattern = new WcardPattern();
				wcardPattern.setParent(contextUrl);
				wcardPattern.addPattern(fileName);
				return wcardPattern.filenames();
		}
	}

	@Override
	public DirectoryStream<Input> next() {
		try {
			String fileName = patterns.next();
			currentIterator = newDirectoryStream(fileName);
			return currentIterator;
		} catch (IOException ioe) {
			throw new DirectoryIteratorException(ioe);
		}
	}

}