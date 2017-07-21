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
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.StringTokenizer;

import org.jetel.component.fileoperation.URIUtils;
import org.jetel.graph.ContextProvider;
import org.jetel.graph.runtime.IAuthorityProxy;

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

		// CLO-6374: decode escape sequences
		return URIUtils.urlDecode(sandboxUrl.substring(SANDBOX_PROTOCOL_URL_PREFIX.length(), slashIndex));
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
		
		// CLO-6374: decode escape sequences
		return URIUtils.urlDecode(url.getHost());
	}

	/**
	 * Extracts a path from a given sandbox URL.
	 * 
	 * @param url sandbox url
	 * @return path extracted from the given sandbox URL
	 */
	public static String getSandboxPath(URL url) {
		if (!isSandboxUrl(url)) {
			throw new IllegalArgumentException("sandboxUrl");
		}
		
		return url.getPath();
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

    /**
     * CLO-5660: use getUrlFile() to decode the path
     * 
     * @param url			- target URL
     * @param appendData	- <code>true</code> to open the stream for appending
     * @return new sandbox {@link OutputStream}
     * @throws IOException
     */
    public static OutputStream getSandboxOutputStream(URL url, boolean appendData) throws IOException {
		String filePath = FileUtils.getUrlFile(url);
		if (filePath.startsWith("/")) {
			filePath = filePath.substring(1);
		}
		IAuthorityProxy authorityProxy = IAuthorityProxy.getAuthorityProxy(ContextProvider.getGraph());
		try {
			return authorityProxy.getSandboxResourceOutput(ContextProvider.getComponentId(), getSandboxName(url), filePath, appendData);
		} catch (UnsupportedOperationException uoe) {
			throw new IOException("Failed to open sandbox output stream", uoe);
		}
    }
    
    /**
     * Utility method to open sandbox {@link InputStream}.
     * 
     * @param url	- source URL
     * @return new sandbox {@link InputStream}
     * @throws IOException
     */
    public static InputStream getSandboxInputStream(URL url) throws IOException {
		String filePath = FileUtils.getUrlFile(url);
		if (filePath.startsWith("/")) {
			filePath = filePath.substring(1);
		}
		IAuthorityProxy authorityProxy = IAuthorityProxy.getAuthorityProxy(ContextProvider.getGraph());
		try {
			return authorityProxy.getSandboxResourceInput(ContextProvider.getComponentId(), getSandboxName(url), filePath);
		} catch (UnsupportedOperationException uoe) {
			throw new IOException("Failed to open sandbox input stream", uoe);
		}
    }

	private SandboxUrlUtils() {
		throw new UnsupportedOperationException();
	}

	/**
	 * Joins context URL with an other URL. If the second URL is regular
	 * sandbox URL, only the secon attribute is returned. But if the second URL
	 * is relative path, first context URL is taken into consideration and
	 * the relative path is joined with the sandbox given by the first attribute.
	 * For example:
	 * joinSandboxUrls("xxx", "sandbox://test/data.txt") -> "sandbox://test/data.txt" 
	 * joinSandboxUrls("sandbox://test/foo.txt"", "./some/data.txt") -> "sandbox://test/some/data.txt" 
	 */
	public static String joinSandboxUrls(String contextSandboxUrl, String sandboxUrl) {
		if (isSandboxUrl(sandboxUrl) || !isSandboxUrl(contextSandboxUrl)) {
			return sandboxUrl;
		}
		String sandboxName = getSandboxName(contextSandboxUrl);
		return getSandboxPath(sandboxName, sandboxUrl);
	}
	
	/**
	 * CLO-6374: encode spaces in sandbox name to prevent URISyntaxException.
	 * TODO encode all spaces?
	 * 
	 * @param url
	 * @return
	 * @throws URISyntaxException
	 */
	public static URI toURI(URL url) throws URISyntaxException {
		// CLO-6374: encode spaces in sandbox name to prevent URISyntaxException
		String urlString = url.toString();
		String sandboxName = SandboxUrlUtils.getSandboxName(url);
		sandboxName = sandboxName.replace(" ", "%20");
		String relativePath = SandboxUrlUtils.getRelativeUrl(url.toString());
		relativePath = relativePath.replace(" ", "%20");
		try {
			urlString = SandboxUrlUtils.getSandboxUrl(sandboxName, relativePath).toString();
		} catch (MalformedURLException e) {
			URISyntaxException ex = new URISyntaxException(urlString, e.getMessage());
			ex.initCause(e);
			throw ex;
		}
		return new URI(urlString);
	}
	
	/**
	 * Escapes path segments in URL. The URL is not expected to have a query.
	 * @param url
	 * @return
	 */
	public static String escapeUrlPath(String url) {
		return processUrlPath(url, true);
	}
	
	public static String unescapeUrlPath(String url) {
		return processUrlPath(url, false);
	}
	
	private static String processUrlPath(String url, final boolean escape) {
		if (url == null) {
			return null;
		}
		StringBuilder result = new StringBuilder();
		int protocolPos = url.indexOf("://");
		if (protocolPos >= 0) {
			result.append(url.substring(0, protocolPos + 3));
			url = url.substring(protocolPos + 3);
		}
		StringTokenizer strtok = new StringTokenizer(url, "/");
		if (url.startsWith("/")) {
			result.append('/');
		}
		while (strtok.hasMoreElements()) {
			String segment = strtok.nextToken();
			result.append(escape ? URIUtils.urlEncode(segment) : URIUtils.urlDecode(segment));
			if (strtok.hasMoreElements()) {
				result.append('/');
			}
		}
		if (url.endsWith("/")) {
			result.append('/');
		}
		return result.toString();
	}
}
