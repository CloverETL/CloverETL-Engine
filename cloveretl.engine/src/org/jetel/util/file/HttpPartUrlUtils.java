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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;

import org.jetel.graph.ContextProvider;
import org.jetel.graph.runtime.IAuthorityProxy;

/**
 * @author reichman (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created May 5, 2017
 */
public class HttpPartUrlUtils {

	public static final String REQUEST_PROTOCOL = "request";
	public static final String REQUEST_BODY = "body";
	public static final String REQUEST_PART = "part";
	public static final String REQUEST_PART_PREFIX = REQUEST_PART+":";
	
	public static final String REQUEST_PROTOCOL_URL_PREFIX = REQUEST_PROTOCOL + ":";
	
	public static final String REQUEST_PROTOCOL_URL_PLAIN_PREFIX = REQUEST_PROTOCOL_URL_PREFIX + REQUEST_BODY;
	public static final String REQUEST_PROTOCOL_URL_MUTLTIPART_PREFIX = REQUEST_PROTOCOL_URL_PREFIX + REQUEST_PART_PREFIX;
	
	private HttpPartUrlUtils() {
	}
	
	public static boolean isRequestUrl(String url) {
		return (url != null && url.startsWith(REQUEST_PROTOCOL_URL_PREFIX));
	}
	
	public static boolean isRequestUrl(URL url) {
		return url != null && REQUEST_PROTOCOL.equals(url.getProtocol());
	}
	
	public static boolean isPlainRequestUrl(String url) {
		if (isRequestUrl(url)) {
			return url.startsWith(REQUEST_PROTOCOL_URL_PLAIN_PREFIX);
		}
		return false;
	}
	
	public static boolean isMultipartRequestUrl(String url) {
		if (isRequestUrl(url)) {
			return url.startsWith(REQUEST_PROTOCOL_URL_MUTLTIPART_PREFIX);
		}
		return false;
	}
		
	public static InputStream getRequestInputStream(URL url) throws IOException {
		IAuthorityProxy authorityProxy = IAuthorityProxy.getAuthorityProxy(ContextProvider.getGraph());
		String urlPath = url.getPath();
		if (REQUEST_BODY.equals(urlPath)) {
			return authorityProxy.getHttpContext().getRequestInputStream();
		} else {
			String param = getPartName(urlPath);
			return authorityProxy.getHttpContext().getRequestInputStream(param);
		}
	}
	
	public static OutputStream getRequestOutputStream(URL url) throws IOException {
		/** TODO implement **/
		throw new UnsupportedOperationException("getRequestOutputStream is not supported yet");
	}
	
	private static String getPartName(String urlPath) throws IOException {
		if (urlPath == null || !urlPath.startsWith(REQUEST_PART_PREFIX)) {
			throw new IOException("Invalid HTTP request path "+urlPath);
		}
		
		return urlPath.substring(REQUEST_PART_PREFIX.length());
	}
}
