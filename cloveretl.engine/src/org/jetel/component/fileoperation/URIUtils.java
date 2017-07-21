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

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.regex.Pattern;

import org.jetel.util.string.StringUtils;

/**
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 22.3.2012
 */
public class URIUtils {
	
	public static final String PATH_SEPARATOR = "/"; //$NON-NLS-1$

	public static final String CURRENT_DIR_NAME = "."; //$NON-NLS-1$
	
	public static final String PARENT_DIR_NAME = ".."; //$NON-NLS-1$
	
	public static final String CHARSET = "US-ASCII"; //$NON-NLS-1$
	
	private static final Pattern PLUS_PATTERN = Pattern.compile("\\+"); //$NON-NLS-1$
	
	private static final String ENCODED_SPACE = "%20"; //$NON-NLS-1$

	public static URI getChildURI(URI parentDir, URI name) {
		String uriString = parentDir.toString();
		if (!uriString.endsWith(PATH_SEPARATOR)) {
			parentDir = URI.create(uriString + PATH_SEPARATOR);
		}
		return parentDir.resolve(name);
	}

	public static URI getChildURI(URI parentDir, String name) {
		StringBuilder sb = new StringBuilder(name);
		int end = name.endsWith(PATH_SEPARATOR) ? name.length() - 1 : name.length();
		// prevent trailing slash from being encoded
		// there mustn't be any other slashes 
		sb.replace(0, end, urlEncode(sb.substring(0, end)));
		String uriString = parentDir.toString();
		if (!uriString.endsWith(PATH_SEPARATOR)) {
			parentDir = URI.create(uriString + PATH_SEPARATOR);
		}
		return parentDir.resolve(sb.toString());
	}
	
	public static URI getParentURI(URI uri) {
		String path = uri.getPath();
		if (StringUtils.isEmpty(path) || path.equals(PATH_SEPARATOR)) {
			return null;
		}
		return uri.toString().endsWith(PATH_SEPARATOR) ? uri.resolve(PARENT_DIR_NAME) : uri.resolve(CURRENT_DIR_NAME);
	}
	
	public static URI trimToLastSlash(URI uri) {
		String path = uri.getPath();
		if ((path == null) || path.indexOf(PATH_SEPARATOR) < 0) {
			return null;
		}
		path = path.substring(0, path.lastIndexOf(PATH_SEPARATOR) + 1);
		try {
			return new URI(uri.getScheme(), uri.getUserInfo(), uri.getHost(), uri.getPort(), path, uri.getQuery(), uri.getFragment());
		} catch (URISyntaxException e) {
			return null;
		}
	}
	
	public static String getFileName(URI uri) {
		URI tmpUri = uri.normalize();
		String path = tmpUri.getRawPath(); // it is assumed that the URI does not contain ?
		// TODO how about #
		if (path == null) {
			return ""; // root
		}
		
		if (path.endsWith("/")) {
			path = path.substring(0, path.length() - 1);
		}
		int slashIdx = path.lastIndexOf('/');
		if (slashIdx >= 0) {
			path = path.substring(slashIdx + 1);
		}
		if (path.equals("..")) {
			return ""; // relative path with too many ".." segments and no filename
		}
		
		return urlDecode(path);
	}

	public static String urlEncode(String str) {
		try {
			return PLUS_PATTERN.matcher(URLEncoder.encode(str, CHARSET)).replaceAll(ENCODED_SPACE);
		} catch (UnsupportedEncodingException e) {
			return str;
		}
	}

	public static String urlDecode(String str) {
		try {
			return URLDecoder.decode(str, CHARSET);
		} catch (UnsupportedEncodingException e) {
			return str;
		}
	}
}
