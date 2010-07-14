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
package org.jetel.util.bytes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Simple wrapper around FileChannel, so now implements our SeekableByteChannel interface.
 * JDK1.7 already has this type of interface and so it could be removed in the future.
 * 
 * @author Martin Zatopek (info@cloveretl.com)
 *         (c) (c) Javlin, a.s. (www.javlin.eu) (www.cloveretl.com)
 *
 * @created 2.12.2009
 */
public class FileSeekableByteChannel implements SeekableByteChannel {

	private FileChannel fileChannel;
	
	public FileSeekableByteChannel(FileChannel fileChannel) {
		this.fileChannel = fileChannel;
	}

	/* (non-Javadoc)
	 * @see org.jetel.util.bytes.SeekableByteChannel#position()
	 */
	public long position() throws IOException {
		return fileChannel.position();
	}

	/* (non-Javadoc)
	 * @see org.jetel.util.bytes.SeekableByteChannel#position(long)
	 */
	public SeekableByteChannel position(long newPosition) throws IOException {
		fileChannel.position(newPosition);
		return this;
	}

	/* (non-Javadoc)
	 * @see org.jetel.util.bytes.SeekableByteChannel#size()
	 */
	public long size() throws IOException {
		return fileChannel.size();
	}

	/* (non-Javadoc)
	 * @see org.jetel.util.bytes.SeekableByteChannel#truncate(long)
	 */
	public SeekableByteChannel truncate(long size) throws IOException {
		fileChannel.truncate(size);
		return this;
	}

	/* (non-Javadoc)
	 * @see java.nio.channels.ReadableByteChannel#read(java.nio.ByteBuffer)
	 */
	public int read(ByteBuffer dst) throws IOException {
		return fileChannel.read(dst);
	}

	/* (non-Javadoc)
	 * @see java.nio.channels.Channel#close()
	 */
	public void close() throws IOException {
		fileChannel.close();
	}

	/* (non-Javadoc)
	 * @see java.nio.channels.Channel#isOpen()
	 */
	public boolean isOpen() {
		return fileChannel.isOpen();
	}

	/* (non-Javadoc)
	 * @see java.nio.channels.WritableByteChannel#write(java.nio.ByteBuffer)
	 */
	public int write(ByteBuffer src) throws IOException {
		return fileChannel.write(src);
	}
	
}
