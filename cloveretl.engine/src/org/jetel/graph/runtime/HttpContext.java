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
package org.jetel.graph.runtime;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

/**
 * Provides methods to access HTTP request/response
 * 
 * @author David Jedlicka (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 21. 3. 2017
 */
public interface HttpContext {
	
	/**
	 * Answers IP of the client that performed the HTTP request.
	 * @return
	 */
	String getClientIPAddress();
	
	/**
	 * Answers name of HTTP method of the request.
	 * @return
	 */
	String getRequestMethod();
	
	/** Returns names of parameters contained in this request and on URL path */
	List<String> getRequestParameterNames();

	/**
	 * Answers value of the specified request parameter, <code>null</code> if not such parameter exists,
	 * and first value if it is a multi-value parameter.
	 * @param name
	 * @return
	 */
	String getRequestParameter(String name);
	
	/** Returns values of the given parameter name */
	List<String> getRequestParameters(String name);
	
	/**
	 * Answers parameters as a map. For multi-value parameters, first value is returned.
	 * @return
	 */
	Map<String, String> getRequestParameters();
	
	/**
	 * Answers value of given HTTP request header.
	 * @param name
	 * @return
	 */
	String getRequestHeader(String name);
	
	/**
	 * Answers names of the HTTP header present in the request.
	 * @return
	 */
	List<String> getRequestHeaderNames();
	
	/**
	 * Answers headers of the HTTP request as a map.
	 * @return
	 */
	Map<String, String> getRequestHeaders();
	
	/**
	 * Answers all values of given HTTP request header.
	 * @return
	 */
	List<String> getRequestHeaderValues(String header);
	
	/**
	 * Specifies character encoding to use to parse HTTP request body.
	 * @param encoding
	 */
	void setRequestEncoding(String encoding);
	
	/**
	 * Answers the character encoding of the HTTP request.
	 * @return
	 */
	String getRequestEncoding();
	
	/**
	 * Answers HTTP request body as a string.
	 * 
	 * @see {@link #setRequestEncoding(String)}
	 * @return
	 */
	String getRequestBody();
	
	/**
	 * Specifies HTTP response body content.
	 * 
	 * @see {@link #setResponseEncoding(String)}
	 * @param body
	 */
	void setResponseBody(String body);
	
	/**
	 * Specifies character encoding of HTTP response body.
	 * 
	 * @param encoding
	 */
	void setResponseEncoding(String encoding);
	
	/**
	 * Answers character encoding of HTTP response body.
	 * @return
	 */
	String getResponseEncoding();
	
	/**
	 * Answers <code>true</true> if the HTTP response contains given header.
	 * @param name
	 * @return
	 */
	boolean containsResponseHeader(String name);
	
	/**
	 * Specifies value for given HTTP response header.
	 * 
	 * @param name
	 * @param value
	 */
	void setResponseHeader(String name, String value);
	
	/**
	 * Adds next value to given HTTP response header.
	 * @param name
	 * @param value
	 */
	void addResponseHeader(String name, String value);
	
	/**
	 * Answers content type of the HTTP request body.
	 * @return
	 */
	String getRequestContentType();
	
	/**
	 * Answers content type of the HTTP response body.
	 * @return
	 */
	String getResponseContentType();
	
	/**
	 * Specifies content type of the HTTP response body.
	 * @param mimeType
	 */
	void setResponseContentType(String mimeType);
	
	/** Retrieves the body of the request */
	InputStream getRequestInputStream() throws IOException;
	
	/** Set status code for this response */
	void setResponseStatus(int status);
	
	/** Sends error response to the client */
	void sendResponseError(int status, String message) throws IOException;
	
	/** Returns input stream for the given part
	 * @param partName 
	 **/
	InputStream getRequestInputStream(String partName) throws IOException;
	
	/**
	 * Answers filename of the first request body part with given name.
	 * @param partName
	 * @return
	 */
	String getRequestPartFilename(String partName);
	
	/**
	 * Answers output stream to write response body.
	 * @return
	 * @throws IOException
	 */
	OutputStream getResponseOutputStream() throws IOException;
}
