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
import java.util.Arrays;
import java.util.List;


/**
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 8.3.2012
 */
public class SingleCloverURI extends CloverURI {
	
	private final String path;
	
	private boolean quoted = false;
	
	private final String scheme;
	
	private URI uri;
	
	private URISyntaxException exception;

	/**
	 * @param uris
	 */
	protected SingleCloverURI(URI uri) {
		this(null, uri);
	}

	/**
	 * @param uris
	 */
	protected SingleCloverURI(URI context, URI uri) {
		super(context);
		this.path = uri.toString();
		this.quoted = true;
		this.scheme = uri.getScheme();
		this.uri = uri;
	}
	
	/**
	 * @param uris
	 * @throws URISyntaxException
	 */
	protected SingleCloverURI(String uri) {
		this(null, uri); 
	}

	/**
	 * @param uris
	 * @throws URISyntaxException
	 */
	protected SingleCloverURI(URI context, String uri) {
		super(context);
		this.quoted = isQuoted(uri); 
		if (quoted) {
			uri = uri.substring(1, uri.length() - 1);
		}
		this.path = uri;
		int colonIndex = uri.indexOf(":"); //$NON-NLS-1$
		this.scheme = colonIndex >= 0 ? uri.substring(0, colonIndex) : null;
		URI tmp = null;
		try {
			tmp = new URI(uri); 
		} catch (URISyntaxException ex) {
		}
		this.uri = tmp;
	}
	
	public boolean isQuoted() {
		return quoted;
	}

	public void setQuoted(boolean quoted) {
		this.quoted = quoted;
	}

	public URI toURI() {
		if ((uri == null) && (exception == null)) {
			try {
				this.uri = new URI(path);
				return uri;
			} catch (URISyntaxException ex) {
				this.exception = ex;
			}
		} 
		if (exception != null) {
			throw new IllegalStateException(FileOperationMessages.getString("SingleCloverURI.invalid_URI"), exception); //$NON-NLS-1$
		}
		return uri;
	}

	public String getScheme() {
		return scheme;
	}

	@Override
	public boolean isSingle() {
		return true;
	}

	public boolean isRelative() {
		return scheme == null;
	}

	@Override
	public String toString() {
		return quoted ? quote(path) : path;
	}

	@Override
	public List<SingleCloverURI> split() {
		return Arrays.asList(this);
	}

	@Override
	public SingleCloverURI getSingleURI() {
		return this;
	}

	@Override
	public SingleCloverURI getAbsoluteURI() {
		if (!isRelative()) {
			return this;
		} else {
			SingleCloverURI result = createSingleURI(null, resolve(getContext(), path));
			result.setQuoted(this.isQuoted());
			return result;
		}
	}

	public String getPath() {
		return path;
	}

}
