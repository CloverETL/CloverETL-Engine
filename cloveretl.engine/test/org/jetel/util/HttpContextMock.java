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
package org.jetel.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jetel.graph.runtime.HttpContext;
import org.jetel.graph.runtime.RestJobOutputData;

/**
 * @author Roland (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 15. 5. 2017
 */
public class HttpContextMock implements HttpContext {

	private static final Map<String, String> REQUEST_PARAMETERES = new HashMap<String, String>();
	
	static {
		REQUEST_PARAMETERES.put("a", "123");
		REQUEST_PARAMETERES.put("a--", "A++");
		REQUEST_PARAMETERES.put("///", "${a--}");
	}
	
	@Override
	public String getClientIPAddress() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getRequestMethod() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<String> getRequestParameterNames() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getRequestParameter(String name) {
		return REQUEST_PARAMETERES.get(name);
	}

	@Override
	public List<String> getRequestParameters(String name) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, String> getRequestParameters() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getRequestHeader(String name) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<String> getRequestHeaderNames() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, String> getRequestHeaders() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<String> getRequestHeaderValues(String header) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setRequestEncoding(String encoding) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String getRequestEncoding() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getRequestBody() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setResponseBody(String body) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setResponseEncoding(String encoding) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String getResponseEncoding() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean containsResponseHeader(String name) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void setResponseHeader(String name, String value) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void addResponseHeader(String name, String value) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String getRequestContentType() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getResponseContentType() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setResponseContentType(String mimeType) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public InputStream getRequestInputStream() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setResponseStatus(int status) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setResponseStatus(int status, String message) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public InputStream getRequestInputStream(String partName) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getRequestPartFilename(String partName) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public OutputStream getResponseOutputStream() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, List<String>> getAllRequestParametersList() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void performFileWrite(RestJobOutputData fileData) {
		// TODO Auto-generated method stub		
	}

}
