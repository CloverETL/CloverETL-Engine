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
package org.jetel.util.stream;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A wrapper for {@link InputStream} that guarantees that 
 * the wrapped {@link InputStream#close()} method is called at most once.
 * 
 * Subclasses are encouraged to override
 * {@link #doClose()} instead of {@link #close()}.
 * 
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 21. 8. 2017
 */
public class CloseOnceInputStream extends FilterInputStream {

	protected AtomicBoolean closed = new AtomicBoolean(false);

	/**
	 * @param in
	 */
	public CloseOnceInputStream(InputStream in) {
		super(in);
	}

	/**
	 * @see #doClose()
	 */
	@Override
	public void close() throws IOException {
		if (!closed.getAndSet(true)) {
			doClose();
		}
	}

	/**
	 * Closes the stream and releases any other related resources.
	 * It is guaranteed that this method is called only once.
	 * 
	 * @throws IOException
	 */
	protected void doClose() throws IOException {
		super.close();
	}

}
