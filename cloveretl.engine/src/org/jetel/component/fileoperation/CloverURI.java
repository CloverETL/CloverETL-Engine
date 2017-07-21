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
package org.jetel.component.fileoperation;

import static java.text.MessageFormat.format;

import java.io.File;
import java.net.URI;
import java.net.URL;
import java.util.List;
import java.util.regex.Pattern;

import org.jetel.data.Defaults;

/**
 * The FileManager and related classes
 * are considered internal and may change in the future.
 */
public abstract class CloverURI {
	
	public static final String SEPARATOR = Defaults.DEFAULT_PATH_SEPARATOR_REGEX;
	
	public static final String PATH_SEPARATOR = "/"; //$NON-NLS-1$
	
	private static final Pattern BACKSLASH_PATTERN = Pattern.compile("\\\\"); //$NON-NLS-1$
	
	private static final char BACKSLASH_CHAR = '\\';
	
	private static final String SINGLE_QUOTE = "'"; //$NON-NLS-1$
	
	private static final String SLASH = "/"; //$NON-NLS-1$
	
	private static final char SLASH_CHAR = '/';
	
	private static final Pattern SPACE_PATTERN = Pattern.compile(" "); //$NON-NLS-1$
	
	protected URI context;
	
	protected CloverURI() {
		
	}
	
	protected CloverURI(URI context) {
		this.context = context;
	}
	
	protected static String preprocess(String uriString) {
		uriString = uriString.replace(BACKSLASH_CHAR, SLASH_CHAR);
		uriString = SPACE_PATTERN.matcher(uriString).replaceAll("%20"); //$NON-NLS-1$
		if (uriString.startsWith(PATH_SEPARATOR)) { // handles Linux absolute paths and UNC pathnames on Windows
			return "file://" + uriString; //$NON-NLS-1$
		}
		String lowerCaseUri = uriString.toLowerCase();
		for (File root: File.listRoots()) { // handles drive letters on Windows 
			String rootString = BACKSLASH_PATTERN.matcher(root.toString()).replaceFirst(PATH_SEPARATOR).toLowerCase();
			if (lowerCaseUri.startsWith(rootString)) {
				return "file:/" + uriString; //$NON-NLS-1$
			}
		}
		return uriString;
	}
	
	protected static String quote(String str) {
		if (isQuoted(str)) {
			return str;
		}
		return String.format("'%s'", str); //$NON-NLS-1$
	}
	
	protected static boolean isQuoted(String str) {
		return str.startsWith(SINGLE_QUOTE) && str.endsWith(SINGLE_QUOTE) && str.length() >= 2;
	}
	
	public static CloverURI createRelativeURI(URI base, String uriString) {
		if (uriString == null) {
			throw new NullPointerException("uriString"); //$NON-NLS-1$
		}
		CloverURI result = null;
		uriString = preprocess(uriString);
		if (uriString.contains(SEPARATOR)) {
			String[] uris = uriString.split(SEPARATOR);
			result = new MultiCloverURI(uris); 
			result.context = base;
		} else {
			result = new SingleCloverURI(base, uriString);
		}
		return result;
	}

	public static CloverURI createURI(String uriString) {
		return createRelativeURI(null, uriString);
	}

	public static CloverURI createURI(URI... uris) {
		if (uris.length == 1) {
			return createSingleURI(uris[0]);
		} else {
			return new MultiCloverURI(uris);
		}
	}
	
	public static CloverURI createRelativeURI(URI base, URI... uris) {
		if (uris.length == 1) {
			return createSingleURI(base, uris[0]);
		} else {
			MultiCloverURI uri = new MultiCloverURI(uris);
			uri.context = base;
			return uri;
		}
	}
	
	public static SingleCloverURI createSingleURI(URI uri) {
		return new SingleCloverURI(uri);
	}
	
	public static SingleCloverURI createSingleURI(URI context, URI uri) {
		return new SingleCloverURI(context, uri);
	}
	
	public static SingleCloverURI createSingleURI(URI context, String uri) {
		if (uri.contains(SEPARATOR)) {
			throw new IllegalArgumentException(FileOperationMessages.getString("CloverURI.not_a_single_URI")); //$NON-NLS-1$
		}
		uri = preprocess(uri);
		return new SingleCloverURI(context, uri);
	}
	
	protected URI getContext() {
		if (context != null) {
			return context;
		}
		return FileManager.getInstance().getContextURI();
	}
	
	protected static URI resolve(URI context, URI spec) {
		URI result = context.resolve(spec);
		if (!result.isAbsolute()) {
			try {
				result = new URL(context.toURL(), spec.toString()).toURI();
			} catch (Exception ex) {
				//FIXME ex.printStackTrace();
			}
		}
		if (!result.isAbsolute()) {
			throw new RuntimeException(format(FileOperationMessages.getString("CloverURI.failed_to_resolve"), spec, context)); //$NON-NLS-1$
		}
		return result;
	}
	
	protected static String resolve(URI context, String spec) {
		String result = null;
		if (context.toString().endsWith(SLASH) || spec.startsWith(SLASH)) {
			result = context + spec;
		} else {
			result = context + SLASH + spec;
		}
		return result;
	}

	public abstract CloverURI getAbsoluteURI();

	public abstract boolean isSingle();
	
	@Override
	public abstract String toString();
	
	public abstract List<SingleCloverURI> split();
	
	public abstract SingleCloverURI getSingleURI();
	
}
