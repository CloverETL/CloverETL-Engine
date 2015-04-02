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
package org.jetel.data.formatter;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.NonWritableChannelException;
import java.nio.channels.SeekableByteChannel;

import org.jetel.data.parser.CloverDataParser;
import org.jetel.data.parser.CloverDataParser.FileConfig;
import org.jetel.test.CloverTestCase;
import org.jetel.util.file.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.internal.AssumptionViolatedException;

/**
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 1. 4. 2015
 */
public class CloverDataFormatterTest extends CloverTestCase {

	@Override
	@Before
	protected void setUp() throws Exception {
		super.setUp();
	}

	@Override
	@After
	protected void tearDown() throws Exception {
		super.tearDown();
	}

	/**
	 * {@link #read(ByteBuffer)} always reads at most one byte.
	 * 
	 * @author krivanekm (info@cloveretl.com)
	 *         (c) Javlin, a.s. (www.cloveretl.com)
	 *
	 * @created 1. 4. 2015
	 */
	private static class TestChannel implements SeekableByteChannel {
		
		private final SeekableByteChannel delegate;
		
		private final ByteBuffer bb = ByteBuffer.allocate(1);

		public TestChannel(SeekableByteChannel delegate) {
			this.delegate = delegate;
		}

		@Override
		public boolean isOpen() {
			return delegate.isOpen();
		}

		@Override
		public void close() throws IOException {
			delegate.close();
		}

		@Override
		public int read(ByteBuffer dst) throws IOException {
			bb.clear();
			int count = delegate.read(bb);
			if (count > 0) {
				bb.flip();
				dst.put(bb);
				bb.flip();
			}
			return count;
		}

		@Override
		public int write(ByteBuffer src) throws IOException {
			return delegate.write(src);
		}

		@Override
		public long position() throws IOException {
			return delegate.position();
		}

		@Override
		public SeekableByteChannel position(long newPosition) throws IOException {
			return delegate.position(newPosition);
		}

		@Override
		public long size() throws IOException {
			return delegate.size();
		}

		@Override
		public SeekableByteChannel truncate(long size) throws IOException {
			return delegate.truncate(size);
		}
		
		
	}

	/**
	 * CLO-6015:
	 */
	@Test
	public void testAppend() throws Exception {
		File file = new File("test/CDW_append_checksum.cdf");
		// check that the file is not corrupted
		long checksum = FileUtils.calculateFileCheckSum(file.getAbsolutePath());
		if (checksum != 725508159L) {
			throw new AssumptionViolatedException(String.valueOf(checksum));
		}
		try (
			// open the file in read-only mode to prevent it from being modified
			RandomAccessFile raf = new RandomAccessFile(file, "r");
			FileChannel channel = raf.getChannel();
			SeekableByteChannel testChannel = new TestChannel(channel);
			CloverDataFormatter formatter = new CloverDataFormatter();
		) {
			formatter.setAppend(true);
			FileConfig config = CloverDataParser.checkCompatibilityHeader(testChannel, null);
			testChannel.position(0);
			formatter.init(config.metadata);
			formatter.setDataTarget(testChannel);
			assertTrue(channel.position() < channel.size() - 1);
		} catch (NonWritableChannelException ex) {
			// thrown by CloverDataFormatter.close() - expected, ignore
		}
	}

}
