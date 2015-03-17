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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.zip.Deflater;

import de.schlichtherle.truezip.zip.ZipFile;
import de.schlichtherle.truezip.zip.ZipOutputStream;

/**
 * CLO-2572:
 * 
 * Replacement for {@link de.schlichtherle.truezip.zip.ZipOutputStream},
 * prevents active deadlock in {@link Deflater}.
 * 
 * Each write operation should check the {@link #closed} flag
 * to prevent active deadlock if the thread has been
 * asynchronously interrupted.
 * 
 * @see <a href="https://bug.javlin.eu/browse/CLO-2572">CLO-2572</a>
 * 
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 16. 3. 2015
 */
public class TZipOutputStream extends ZipOutputStream {

	private volatile boolean closed = false;

	public TZipOutputStream(OutputStream out) {
		super(out);
	}

	public TZipOutputStream(OutputStream out, Charset charset) {
		super(out, charset);
	}

	public TZipOutputStream(OutputStream out, ZipFile appendee) {
		super(out, appendee);
	}

	@Override
	public void write(byte[] b, int off, int len) throws IOException {
		ensureOpen();
		super.write(b, off, len);
	}

	@Override
	public void write(int b) throws IOException {
		ensureOpen();
		super.write(b);
	}
	
	@Override
	public void flush() throws IOException {
		ensureOpen();
		super.flush();
	}

	@Override
	public void close() throws IOException {
		this.closed = true;
		super.close();
	}

	private void ensureOpen() throws IOException {
		if (closed) {
			throw new IOException("Output stream closed");
		}
	}
	
}
