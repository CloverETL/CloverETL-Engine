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
package org.jetel.component.fileoperation.pool;

import java.net.URI;

/**
 * Authority for S3 connection,
 * also stores the original URI.
 * 
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 23. 3. 2015
 */
public class S3Authority extends DefaultAuthority {
	
	private final URI noProxyUri;
	private final URI proxyUri;
	
	private S3Authority(URI proxyUri, ProxyHelper proxyHelper) {
		super(proxyHelper);
		this.proxyUri = proxyUri;
		this.noProxyUri = proxyHelper.uri;
	}

	public S3Authority(URI uri) {
		this(uri, ProxyHelper.getInstance(uri));
	}

	/**
	 * Returns URI that may contain proxy configuration.
	 * 
	 * @return URI with proxy configuration
	 */
	public URI getUri() {
		return proxyUri;
	}
	
	/**
	 * Returns URI that does not contain proxy configuration.
	 * 
	 * @return URI without proxy configuration
	 */
	public URI getPlainUri() {
		return noProxyUri;
	}

}
