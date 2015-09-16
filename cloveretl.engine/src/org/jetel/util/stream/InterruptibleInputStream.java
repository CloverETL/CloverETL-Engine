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

import org.jetel.component.fileoperation.FileOperationMessages;

/**
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 17. 4. 2015
 */
public class InterruptibleInputStream extends FilterInputStream {

	/**
	 * @param in
	 */
	public InterruptibleInputStream(InputStream in) {
		super(in);
	}

	private void checkInterrupted() throws IOException {
		if (Thread.currentThread().isInterrupted()) {
			throw new IOException(FileOperationMessages.getString("IOperationHandler.interrupted")); //$NON-NLS-1$
		}
	}

	@Override
	public int read() throws IOException {
		checkInterrupted();
		return super.read();
	}

	@Override
	public int read(byte[] b, int off, int len) throws IOException {
		checkInterrupted();
		return super.read(b, off, len);
	}

}
