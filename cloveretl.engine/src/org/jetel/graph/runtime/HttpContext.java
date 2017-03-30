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
import java.util.Collection;


/**
 * Declares functions for working with HTTP request/response
 * 
 * @author David Jedlicka (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 21. 3. 2017
 */
public interface HttpContext {

	/** Returns names of parameters contained in this request and on URL path */
	Collection<String> getRequestParameterNames();

	/** Returns values of the given parameter name */
	Collection<String> getRequestParameterValues(String name);
	
	/** Retrieves the body of the request */
	InputStream getRequestInputStream() throws IOException;
	
	/** Set status code for this response */
	void setResponseStatus(int status);
	
	/** Sends error response to the client */
	void sendResponseError(ErrorMessage message) throws IOException;
}
