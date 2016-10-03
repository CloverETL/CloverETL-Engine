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
package org.jetel.util.file.stream;

import java.net.URL;
import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;

/**
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 7. 9. 2016
 */
public class CollectionDirectoryStream extends AbstractDirectoryStream<Input> {
	
	private final URL contextUrl;
	private final Iterator<String> it;

	public CollectionDirectoryStream(URL contextUrl, String url) {
		this(contextUrl, Collections.singletonList(url));
	}

	/**
	 * 
	 */
	public CollectionDirectoryStream(URL contextUrl, Iterable<String> urls) {
		Objects.requireNonNull(urls);
		this.contextUrl = contextUrl;
		this.it = urls.iterator();
	}

	@Override
	public boolean hasNext() {
		return it.hasNext();
	}

	@Override
	public Input next() {
		String url = it.next();
		return createInput(contextUrl, url);
	}
	
	protected Input createInput(URL contextUrl, String url) {
		return new URLInput(contextUrl, url);
	}

}
