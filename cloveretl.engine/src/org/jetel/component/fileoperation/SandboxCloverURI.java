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

import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 23.3.2012
 */
public class SandboxCloverURI extends SingleCloverURI {

	/**
	 * @param uri
	 */
	public SandboxCloverURI(URI uri) {
		super(uri);
	}

	/**
	 * @param context
	 * @param uri
	 */
	public SandboxCloverURI(URI context, URI uri) {
		super(context, uri);
	}

	/**
	 * @param uri
	 * @throws URISyntaxException
	 */
	public SandboxCloverURI(String uri) throws URISyntaxException {
		super(uri);
	}

	/**
	 * @param context
	 * @param uri
	 * @throws URISyntaxException
	 */
	public SandboxCloverURI(URI context, String uri) throws URISyntaxException {
		super(context, uri);
	}
	
	public String getSandbox() {
		return null;
	}
	
	public String getResourceId() {
		return null;
	}

}
