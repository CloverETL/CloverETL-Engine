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
package org.jetel.util.file;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;

/**
 * Utility class for working with sandbox URLs.
 *
 * @author Martin Janik, Javlin a.s. &lt;martin.janik@javlin.eu&gt;
 *
 * @version 3.1.0
 * @since 3.1.0
 */
public final class SandboxUrlUtils {

	/** The name of sandbox protocol. */
	public static final String SANDBOX_PROTOCOL = "sandbox";
	/** The URL prefix of sandbox URLs. */
	public static final String SANDBOX_PROTOCOL_URL_PREFIX = SANDBOX_PROTOCOL + "://";

	/**
	 * Checks whether or not a given URL string is a sandbox URL.
	 *
	 * @param url a string URL to be checked
	 *
	 * @return <code>true</code> if the given URL is a sandbox URL, <code>false</code> otherwise 
	 */
	public static boolean isSandboxUrl(String url) {
		return (url != null && url.startsWith(SANDBOX_PROTOCOL_URL_PREFIX));
	}

	/**
	 * Checks whether or not an URL is a sandbox URL.
	 *
	 * @param url an URL to be checked
	 *
	 * @return <code>true</code> if the given URL is a sandbox URL, <code>false</code> otherwise 
	 */
	public static boolean isSandboxUrl(URL url) {
		return url != null && SANDBOX_PROTOCOL.equals(url.getProtocol());
	}

	/**
	 * Checks whether or not an URI is a sandbox URI.
	 *
	 * @param uri an URI to be checked
	 *
	 * @return <code>true</code> if the given URI is a sandbox URI, <code>false</code> otherwise 
	 */
	public static boolean isSandboxUri(URI uri) {
		return uri != null && SANDBOX_PROTOCOL.equals(uri.getScheme());
	}
	
	/**
	 * Extracts a sandbox name from a given sandbox URL.
	 *
	 * @param sandboxUrl a sandbox URL
	 *
	 * @return a sandbox name extracted from the given sandbox URL
	 *
	 * @throws IllegalArgumentException if the given url is not a sanbox URL  
	 */
	public static String getSandboxName(String sandboxUrl) {
		if (!isSandboxUrl(sandboxUrl)) {
			throw new IllegalArgumentException("sandboxUrl");
		}

		int slashIndex = sandboxUrl.indexOf('/', SANDBOX_PROTOCOL_URL_PREFIX.length());

		if (slashIndex < 0) {
			slashIndex = sandboxUrl.length();
		}

		return sandboxUrl.substring(SANDBOX_PROTOCOL_URL_PREFIX.length(), slashIndex);
	}

	/**
	 * Extracts a sandbox name from a given sandbox URL.
	 * 
	 * @param url sandbox url
	 * @return sandbox name extracted from the given sandbox URL
	 */
	public static String getSandboxName(URL url) {
		if (!isSandboxUrl(url)) {
			throw new IllegalArgumentException("sandboxUrl");
		}
		
		return url.getHost();
	}

	/**
	 * Extracts a relative URL from a given sandbox URL.
	 *
	 * @param sandboxUrl a sandbox URL
	 *
	 * @return a relative URL extracted from the given sandbox URL
	 *
	 * @throws IllegalArgumentException if the given url is not a sanbox URL  
	 */
	public static String getRelativeUrl(String sandboxUrl) {
		if (!isSandboxUrl(sandboxUrl)) {
			throw new IllegalArgumentException("sandboxUrl");
		}

		int slashIndex = sandboxUrl.indexOf('/', SANDBOX_PROTOCOL_URL_PREFIX.length());

		if (slashIndex < 0 || slashIndex == sandboxUrl.length() - 1) {
			// there is just a sandbox name (and possibly a slash) present within the sandbox URL
			return ".";
		}

		return sandboxUrl.substring(slashIndex + 1);
	}
	
	/**
	 * Returns a URL of the form "sandbox://${storageCode}/".
	 * 
	 * @param storageCode
	 * @return sandbox root URL
	 * @throws MalformedURLException
	 */
	public static URL getSandboxUrl(String storageCode) throws MalformedURLException {
		return getSandboxUrl(storageCode, null);
	}

	/**
	 * Returns a URL of the form "sandbox://${storageCode}/${relativePath}".
	 * 
	 * @param storageCode
	 * @return sandbox root URL
	 * @throws MalformedURLException
	 */
	public static URL getSandboxUrl(String storageCode, String relativePath) throws MalformedURLException {
		return FileUtils.getFileURL(FileUtils.appendSlash(SandboxUrlUtils.SANDBOX_PROTOCOL_URL_PREFIX + storageCode), (relativePath != null ? relativePath : ""));
	}
	
	/**
	 * Simply concatenates sandbox protocol with given storageCode and relative path.
	 * @param storageCode
	 * @param relativePath
	 * @return
	 */
	public static String getSandboxPath(String storageCode, String relativePath) {
		return FileUtils.appendSlash(SandboxUrlUtils.SANDBOX_PROTOCOL_URL_PREFIX + storageCode) + (relativePath != null ? relativePath : "");
	}
	
	/**
	 * Answers file:// URL corresponding to the given sandbox:// URL.
	 * @param sandboxUrl
	 * @return resolved local URL or <code>null</code> if referenced resource is not available locally or the URL is ambiguous
	 */
	public static URL toLocalFileUrl(URL sandboxUrl) {
		
		if (!isSandboxUrl(sandboxUrl)) {
			return null; // or throw an exception?
		}
		try {
			 File resolved = FileUtils.convertUrlToFile(sandboxUrl);
			 return resolved == null ? null : resolved.toURI().toURL();
		} catch (MalformedURLException e) {
			return null;
		}
	}

	private SandboxUrlUtils() {
		throw new UnsupportedOperationException();
	}

}
