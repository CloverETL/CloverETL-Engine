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
package org.jetel.data.parser;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import org.jetel.test.CloverTestCase;

/**
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created Mar 14, 2013
 */
public abstract class AbstractParserTestCase extends CloverTestCase {
	
	private class CloseableStream extends ByteArrayInputStream {
		
		private boolean closed = false;

		/**
		 * @param buf
		 */
		public CloseableStream() {
			super(getBytes());
		}

		@Override
		public void close() throws IOException {
			this.closed = true;
			super.close();
		}

		public boolean isClosed() {
			return closed;
		}

	}
	
	private CloseableStream[] streams = null;
	private CloseableStream stream = null;
	private Parser parser = null;

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		
		stream = new CloseableStream();
		streams = new CloseableStream[] {
				new CloseableStream(),
				new CloseableStream(),
				new CloseableStream()
		};
		parser = createParser();
		parser.setReleaseDataSource(true);
		parser.init();
		parser.preExecute();
	}


	@Override
	protected void tearDown() throws Exception {
		super.tearDown();
		stream = null;
		streams = null;
		parser = null;
	}
	
	protected abstract Parser createParser() throws Exception;
	
	protected byte[] getBytes() {
		return new byte[0];
	}

	public void testSetDataSource() throws Exception {
		for (CloseableStream is: streams) {
			parser.setDataSource(is);
		}
		parser.postExecute();
		parser.free();
		for (CloseableStream is: streams) {
			assertTrue(is.isClosed());
		}
	}
	
	@SuppressWarnings("deprecation")
	public void testClose() throws Exception {
		parser.setDataSource(stream);
		parser.close();
		assertTrue(stream.isClosed());
	}
	
	public void testPostExecute() throws Exception {
		parser.setDataSource(stream);
		parser.postExecute();
		assertTrue(stream.isClosed());
	}
	
	public void testFree() throws Exception {
		parser.setDataSource(stream);
		parser.free();
		assertTrue(stream.isClosed());
	}
}
