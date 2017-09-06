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
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

/**
 * This OutputStream is used as decorator for ServletOutputStream to ensure, that the wrapped OS will be closed just once.
 * 
 * Anyway, OutputStream must be explicitly closed to fix CLO-2203. 
 * OutputStream can't be closed more then once, 
 * otherwise the other HTTP connection will fail with ConnectionReset or Pipe closed (see CLO-2466).
 * 
 * @author mvarecha (info@cloveretl.com)
 *         (c) Opensys TM by Javlin, a.s. (www.cloveretl.com)
 *
 * @created Dec 4, 2013
 */
public class CloseOnceOutputStream extends OutputStream {
	private static Logger log = Logger.getLogger(CloseOnceOutputStream.class);

	protected OutputStream os = null;
	protected Object objectToNotifyWhenClosed = null;
	
	/**
	 * @see #close()
	 */
	private AtomicBoolean isClosed = new AtomicBoolean(false);
	private AtomicBoolean isClosing = new AtomicBoolean(false);
	
	public CloseOnceOutputStream(OutputStream os) {
		this(os, null);
	}

	public CloseOnceOutputStream(OutputStream os, Object objectToNotifyWhenClosed) {
		super();
		this.os = os;
		this.objectToNotifyWhenClosed = objectToNotifyWhenClosed;
	}

	/**
	 * Closes the underlying stream, if it hasn't been already closed or is not closing right now.
	 * 
	 * The method is thread-safe, uses AtomicBoolean to prevent race condition,
	 * e.g. when the stream is asynchronously closed from an interruptible Channel.  
	 */
	@Override
	public void close() throws IOException {
		if (isClosing.getAndSet(true)) {
			log.debug("Stream "+this+" already closing or is closed. Skipping.");
		} else {
			log.debug("Stream "+this+" closing...");
			try {
				doClose();
				log.debug("Stream "+this+" closed.");
			} finally {
				isClosed.set(true);
				if (objectToNotifyWhenClosed != null) {
					synchronized (objectToNotifyWhenClosed) {
						objectToNotifyWhenClosed.notifyAll();
					}
				}
			}
		}
	}

	/**
	 * Closes the underlying stream and releases related resources.
	 * It is guaranteed that this method is called only once.
	 * 
	 * @throws IOException
	 */
	protected void doClose() throws IOException {
		os.close();
	}

	@Override
	public void flush() throws IOException {
		os.flush();
	}

	@Override
	public void write(byte[] b, int off, int len) throws IOException {
		os.write(b, off, len);
	}

	@Override
	public void write(byte[] b) throws IOException {
		os.write(b);
	}
 
	@Override
	public void write(int b) throws IOException {
		os.write(b);
	}
	
	@Override
	public String toString() {
		return "CloseOnceOutputStream["+this.os+"]:"+(this.isClosed.get()?"closed":"opened");
	}

	/**
	 * @return
	 */
	public boolean isAlreadyClosed() {
		return this.isClosed.get();
	}

	/**
	 * Returns instance which is to be notified when the stream is closed.
	 */
	public Object getClosingMonitor() {
		return objectToNotifyWhenClosed;
	}
	
	
}
