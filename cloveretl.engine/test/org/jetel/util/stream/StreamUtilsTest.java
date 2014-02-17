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
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

import org.jetel.test.CloverTestCase;

/**
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 20. 1. 2014
 */
public class StreamUtilsTest extends CloverTestCase {

	private class NoDataChannel implements ReadableByteChannel {
		@Override
		public boolean isOpen() {
			return true;
		}

		@Override
		public void close() throws IOException {
		}

		@Override
		public int read(ByteBuffer dst) throws IOException {
			return -1;
		}
	}

	private class OneByteChannel implements ReadableByteChannel {
		private boolean empty = false;
		@Override
		public boolean isOpen() {
			return true;
		}

		@Override
		public void close() throws IOException {
		}

		@Override
		public int read(ByteBuffer dst) throws IOException {
			if (!empty) {
				if (dst.remaining() > 0) {
					empty = true;
					dst.put((byte) 123);
					return 1;
				} else {
					return 0;
				}
			} else {
				return -1;
			}
		}
	}

	private class LazyThreeBytesChannel implements ReadableByteChannel {
		private int counter = 0;
		@Override
		public boolean isOpen() {
			return true;
		}

		@Override
		public void close() throws IOException {
		}

		@Override
		public int read(ByteBuffer dst) throws IOException {
			if (counter < 10) {
				counter++;
				return 0;
			} else if (counter < 13) {
				int written = 0;
				if (counter == 10 && dst.remaining() > 0) {
					counter++;
					dst.put((byte) 1);
					written++;
				}
				if (counter == 11 && dst.remaining() > 0) {
					counter++;
					dst.put((byte) 2);
					written++;
				}
				if (counter == 12 && dst.remaining() > 0) {
					counter++;
					dst.put((byte) 3);
					written++;
				}
				return written;
			} else {
				return -1;
			}
		}
	}

    public void testReadBlocking() throws IOException {
    	ByteBuffer buffer5 = ByteBuffer.allocate(5);
    	ByteBuffer buffer0 = ByteBuffer.allocate(0);
    	ReadableByteChannel channel;
    	
    	//NoDataChannel
    	buffer5.clear();
    	assertEquals(-1, StreamUtils.readBlocking(new NoDataChannel(), buffer5));
    	
    	buffer0.clear();
    	assertEquals(-1, StreamUtils.readBlocking(new NoDataChannel(), buffer0));
    	
    	buffer5.clear();
    	buffer5.limit(0);
    	assertEquals(-1, StreamUtils.readBlocking(new NoDataChannel(), buffer5));

    	buffer5.clear();
    	buffer5.position(5);
    	assertEquals(-1, StreamUtils.readBlocking(new NoDataChannel(), buffer5));
    	
    	//OneByteChannel
    	buffer5.clear();
    	channel = new OneByteChannel();
    	assertEquals(1, StreamUtils.readBlocking(channel, buffer5));
    	buffer5.flip();
    	assertEquals(123, buffer5.get());
    	buffer5.compact();
    	assertEquals(-1, StreamUtils.readBlocking(channel, buffer5));
    	
    	buffer0.clear();
    	channel = new OneByteChannel();
    	assertEquals(0, StreamUtils.readBlocking(channel, buffer0));
    	assertEquals(0, StreamUtils.readBlocking(channel, buffer0));
    	
    	buffer5.clear();
    	buffer5.limit(0);
    	channel = new OneByteChannel();
    	assertEquals(0, StreamUtils.readBlocking(channel, buffer5));
    	assertEquals(0, StreamUtils.readBlocking(channel, buffer5));

    	buffer5.clear();
    	buffer5.position(5);
    	channel = new OneByteChannel();
    	assertEquals(0, StreamUtils.readBlocking(channel, buffer5));
    	assertEquals(0, StreamUtils.readBlocking(channel, buffer5));

    	buffer5.clear();
    	buffer5.position(4);
    	channel = new OneByteChannel();
    	assertEquals(1, StreamUtils.readBlocking(channel, buffer5));
    	buffer5.flip();
    	assertEquals(123, buffer5.get());
    	buffer5.clear();
    	assertEquals(-1, StreamUtils.readBlocking(channel, buffer5));

    	//LazyOneByteChannel
    	buffer5.clear();
    	channel = new LazyThreeBytesChannel();
    	assertEquals(3, StreamUtils.readBlocking(channel, buffer5));
    	buffer5.flip();
    	assertEquals(1, buffer5.get());
    	assertEquals(2, buffer5.get());
    	assertEquals(3, buffer5.get());
    	buffer5.compact();
    	assertEquals(-1, StreamUtils.readBlocking(channel, buffer5));
    	
    	buffer5.clear();
    	channel = new LazyThreeBytesChannel();
    	buffer5.limit(2);
    	assertEquals(2, StreamUtils.readBlocking(channel, buffer5));
    	buffer5.flip();
    	assertEquals(1, buffer5.get());
    	assertEquals(2, buffer5.get());
    	buffer5.compact();
    	assertEquals(1, StreamUtils.readBlocking(channel, buffer5));
    	buffer5.flip();
    	assertEquals(3, buffer5.get());
    	buffer5.compact();
    	assertEquals(-1, StreamUtils.readBlocking(channel, buffer5));
    	buffer0.clear();
    	assertEquals(-1, StreamUtils.readBlocking(channel, buffer0));
    	
    	buffer0.clear();
    	channel = new LazyThreeBytesChannel();
    	assertEquals(0, StreamUtils.readBlocking(channel, buffer0));

    	buffer5.clear();
    	channel = new LazyThreeBytesChannel();
    	buffer5.position(5);
    	assertEquals(0, StreamUtils.readBlocking(channel, buffer5));
    	assertEquals(0, StreamUtils.readBlocking(channel, buffer5));
    	buffer5.position(4);
    	assertEquals(1, StreamUtils.readBlocking(channel, buffer5));
    	buffer5.position(4);
    	assertEquals(1, buffer5.get());
    	buffer5.clear();
    	assertEquals(2, StreamUtils.readBlocking(channel, buffer5));
    	buffer5.flip();
    	assertEquals(2, buffer5.get());
    	assertEquals(3, buffer5.get());
    }

}
