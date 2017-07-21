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
package org.jetel.util.protocols.amazon;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.NonWritableChannelException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.util.Objects;

import org.jetel.component.fileoperation.PrimitiveS3OperationHandler;
import org.jetel.component.fileoperation.pool.Authority;
import org.jetel.component.fileoperation.pool.ConnectionPool;
import org.jetel.component.fileoperation.pool.PooledS3Connection;
import org.jetel.component.fileoperation.pool.S3Authority;
import org.jetel.util.file.FileUtils;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

/**
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 8. 9. 2016
 */
public class S3SeekableByteChannel implements SeekableByteChannel {
	
	private final PooledS3Connection connection;
	
	private final URI uri;
	
	private boolean open = true;
	
	private S3ObjectInputStream is;
	private ReadableByteChannel channel;
	
	private long size;
	
	private long position = 0;

	private boolean eof = false;
	
	
	/**
	 * 
	 */
	public S3SeekableByteChannel(URI uri) throws IOException {
		Objects.requireNonNull(uri);
		this.uri = uri;
		
		try {
			Authority authority = new S3Authority(uri);
			connection = (PooledS3Connection) ConnectionPool.getInstance().borrowObject(authority);
		} catch (IOException ioe) {
			throw ioe;
		} catch (Exception e) {
			throw new IOException(e);
		}
		
		ObjectMetadata metadata = PrimitiveS3OperationHandler.getObjectMetadata(uri, connection.getService());
		this.size = metadata.getInstanceLength();
	}

	private void openChannel(long position) throws IOException {
		try {
			S3Object object = S3Utils.getObject(uri, connection.getService(), position);
			is = S3Utils.getObjectInputStream(object);
			channel = Channels.newChannel(is);
			// use getInstanceLength(), not getContentLength()!
			this.size = object.getObjectMetadata().getInstanceLength();
		} catch (IOException ioe) {
			try {
				closeChannel();
			} catch (Exception e2) {
				ioe.addSuppressed(e2);
			}
			throw ioe;
		}
	}

	private void closeChannel() throws IOException {
		if (is != null) {
			is.abort(); // prevent the stream from reading remaining data
		}
		FileUtils.closeAll(channel, is);
		channel = null;
		is = null;
	}

	@Override
	public boolean isOpen() {
		return open;
	}

	@Override
	public void close() throws IOException {
		try {
			closeChannel();
		} finally {
			open = false;
			connection.returnToPool();
		}
	}

	@Override
	public int read(ByteBuffer dst) throws IOException {
		checkOpen();
		if (eof) {
			return -1;
		}
		if (channel == null) {
			openChannel(position);
		}
		int count = channel.read(dst);
		if (count > 0) { // -1 indicates EOF
			position += count;
		}
		return count;
	}

	/**
	 * @throws ClosedChannelException
	 */
	private void checkOpen() throws ClosedChannelException {
		if (!isOpen()) {
			throw new ClosedChannelException();
		}
	}

	@Override
	public long position() throws IOException {
		checkOpen();
		return position;
	}

	@Override
	public SeekableByteChannel position(long newPosition) throws IOException {
		if (newPosition < 0) {
			throw new IllegalArgumentException("The position can't be negative");
		}
		checkOpen();
		closeChannel();
		if (newPosition >= size) {
			eof = true;
		} else {
			eof = false;
			openChannel(newPosition);
		}
		this.position = newPosition;
		return this;
	}

	@Override
	public long size() throws IOException {
		checkOpen();
		return size;
	}

	/**
	 * The channel is read-only.
	 */
	@Override
	public int write(ByteBuffer src) throws IOException {
		throw new NonWritableChannelException();
	}

	/**
	 * The channel is read-only.
	 */
	@Override
	public SeekableByteChannel truncate(long size) throws IOException {
		throw new NonWritableChannelException();
	}

}
