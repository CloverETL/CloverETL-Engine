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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;


/**
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 8.3.2012
 */
public class MultiCloverURI extends CloverURI {

	private final String[] paths;
	
	private final boolean[] quoted;
	
	/**
	 * @param uris
	 * @throws URISyntaxException
	 */
	protected MultiCloverURI(String[] singleUris) {
		this.paths = new String[singleUris.length];
		this.quoted = new boolean[singleUris.length];
		for (int i = 0; i < singleUris.length; i++) {
			String uri = singleUris[i];
			boolean quoted = isQuoted(uri);
			this.quoted[i] = quoted;
			if (quoted) {
				uri = uri.substring(1, uri.length() - 1);
			}
			this.paths[i] = uri;
		}
	}

	/**
	 * @param uris
	 */
	protected MultiCloverURI(List<URI> uris) {
		this.paths = new String[uris.size()];
		this.quoted = new boolean[uris.size()];
		int i = 0;
		for (Iterator<URI> it = uris.iterator(); it.hasNext(); ) {
			URI uri = it.next();
			this.paths[i] = uri.toString();
			this.quoted[i] = true;
			i++;
		}
	}

	/**
	 * @param uris
	 */
	protected MultiCloverURI(URI... uris) {
		this(Arrays.asList(uris));
	}

	/**
	 * @param uris
	 */
	protected MultiCloverURI(SingleCloverURI... uris) {
		this.paths = new String[uris.length];
		this.quoted = new boolean[uris.length];
		for (int i = 0; i < uris.length; i++) {
			SingleCloverURI uri = uris[i];
			this.paths[i] = uri.getPath();
			this.quoted[i] = uri.isQuoted();
		}
	}

	@Override
	public boolean isSingle() {
		return paths.length == 1;
	}

	@Override
	public List<SingleCloverURI> split() {
		List<SingleCloverURI> result = new ArrayList<SingleCloverURI>(paths.length);
		for (int i = 0; i < paths.length; i++) {
			SingleCloverURI cloverUri = new SingleCloverURI(this.context, paths[i]);
			cloverUri.setQuoted(quoted[i]);
			result.add(cloverUri);
		}
		return result;
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < paths.length; i++) {
			String path = paths[i];
			sb.append(quoted[i] ? quote(path) : path).append(SEPARATOR);
		}
		if (sb.length() > 0) {
			sb.deleteCharAt(sb.length() - 1);
		}
		return sb.toString();
	}

	@Override
	public SingleCloverURI getSingleURI() {
		throw new IllegalStateException(FileOperationMessages.getString("MultiCloverURI.multiple_URIs")); //$NON-NLS-1$
	}

	@Override
	public CloverURI getAbsoluteURI() {
		if (this.isSingle()) {
			SingleCloverURI singleUri = this.getSingleURI();
			if (singleUri.isRelative()) {
				SingleCloverURI result = new SingleCloverURI(resolve(getContext(), singleUri.getPath()));
				result.setQuoted(singleUri.isQuoted());
				return result;
			} else {
				return this;
			}
		} else {
			List<SingleCloverURI> relative = this.split();
			SingleCloverURI[] absolute = new SingleCloverURI[relative.size()];
			for (int i = 0; i < absolute.length; i++) {
				SingleCloverURI uri = relative.get(i); 
				if (!uri.isRelative()) {
					absolute[i] = uri;
				} else {
					absolute[i] = new SingleCloverURI(resolve(getContext(), uri.getPath()));
				}
			}
			MultiCloverURI result = new MultiCloverURI(absolute);
			System.arraycopy(this.quoted, 0, result.quoted, 0, this.quoted.length);
			return result;
		}
	}

}
