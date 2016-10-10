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
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;

import org.jetel.component.fileoperation.URIUtils;
import org.jetel.enums.ArchiveType;
import org.jetel.util.file.CustomPathResolver;
import org.jetel.util.file.FileURLParser;
import org.jetel.util.file.FileUtils;
import org.jetel.util.file.WcardPattern;

public class WildcardDirectoryStream extends AbstractDirectoryStream<DirectoryStream<Input>> {

	private final URL contextUrl;
	// Collection of filename patterns.
	private final Iterator<String> patterns;
	
	private DirectoryStream<Input> directoryStream;
	
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
		try {
			super.close();
		} finally {
			FileUtils.close(directoryStream);
		}
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
		
		ArchiveDescriptor archiveDescriptor = getArchiveDescriptor(fileName);

		if (archiveDescriptor != null) {
			String anchor = URIUtils.urlDecode(archiveDescriptor.anchor); // CL-2579
			String innerSource = archiveDescriptor.innerSource;
			DirectoryStream<Input> innerStream = newDirectoryStream(innerSource);
			switch (archiveDescriptor.archiveType) {
			case ZIP:
				return new ZipDirectoryStream(innerStream, anchor);
			case TAR:
				return new TarDirectoryStream(innerStream, anchor);
			case TGZ:
				return new TgzDirectoryStream(innerStream, anchor);
			case GZIP:
				return new GzipDirectoryStream(innerStream);
			default:
				throw new UnsupportedOperationException("Unsupported archive type: " + archiveDescriptor.archiveType);
			}
		}
		
		// TODO implement DirectoryStream with real streaming for local files and for S3

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
	
	private ArchiveDescriptor getArchiveDescriptor(String fileName) {
		StringBuilder sbInnerInput = new StringBuilder();
		StringBuilder sbAnchor = new StringBuilder();
		ArchiveType archiveType = FileUtils.getArchiveType(fileName, sbInnerInput, sbAnchor);
		if (archiveType == null) {
			return null;
		}
		
		// get inner source
		String innerSource = null;
		Matcher matcher = FileURLParser.getURLMatcher(fileName);
		if ((matcher != null) && (matcher.group(5) != null)) {
			innerSource = matcher.group(5);
		} else {
			// for archives without ...:(......), just ...:......
			Matcher archMatcher = WcardPattern.getArchiveURLMatcher(fileName);
			if (archMatcher != null) {
				if (archMatcher.group(3) != null) {
					innerSource = archMatcher.group(3);
				} else if (archMatcher.group(7) != null) {
					innerSource = archMatcher.group(7);
				}
			}
		}
		
		if (innerSource == null) {
			// failed to parse archive inner source, the code should be fixed 
			throw new IllegalArgumentException("Failed to parse URL: " + fileName);
		}
		
		return new ArchiveDescriptor(archiveType, innerSource, sbAnchor.toString());
	}
	
	private static class ArchiveDescriptor {
		
		public final ArchiveType archiveType;
		public final String innerSource;
		public final String anchor;

		/**
		 * @param archiveType
		 * @param innerSource
		 * @param anchor
		 */
		public ArchiveDescriptor(ArchiveType archiveType, String innerSource, String anchor) {
			this.archiveType = Objects.requireNonNull(archiveType);
			this.innerSource = Objects.requireNonNull(innerSource);
			this.anchor = anchor;
		}
		
	}

	@Override
	public DirectoryStream<Input> next() {
		try {
			FileUtils.close(directoryStream); // close the previous stream
			String fileName = patterns.next();
			directoryStream = newDirectoryStream(fileName);
			return directoryStream;
		} catch (IOException ioe) {
			throw new DirectoryIteratorException(ioe);
		}
	}

}