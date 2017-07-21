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
package org.jetel.hadoop.provider.filesystem;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.apache.hadoop.fs.FSDataInputStream;
import org.jetel.hadoop.service.filesystem.HadoopDataInput;
import org.jetel.util.bytes.SeekableByteChannel;

public class HadoopDataInputStream implements HadoopDataInput, SeekableByteChannel {

	private final static int INTERNAL_IO_BUFFER_SIZE=512;
	
	private FSDataInputStream stream;
	private boolean isOpen;
	private long size;
	private byte[] IObuffer;
	
	public HadoopDataInputStream(FSDataInputStream stream,long lenght){
		this.stream = stream;
		this.isOpen=true;
		this.size=lenght;
	}
	
	
	@Override
	public final boolean readBoolean() throws IOException {
		return stream.readBoolean();
	}

	@Override
	public final byte readByte() throws IOException {
		return stream.readByte();
	}

	@Override
	public final char readChar() throws IOException {
		return stream.readChar();
	}

	@Override
	public final double readDouble() throws IOException {
		return stream.readDouble();
	}

	@Override
	public final float readFloat() throws IOException {
		return stream.readFloat();
	}

	@Override
	public final void readFully(byte[] arg0) throws IOException {
		stream.readFully(arg0);
	}

	@Override
	public final void readFully(byte[] arg0, int arg1, int arg2) throws IOException {
		stream.readFully(arg0, arg1, arg2);
	}

	@Override
	public final int readInt() throws IOException {
		return stream.readInt();
	}

	@SuppressWarnings("deprecation")
	@Override
	public final String readLine() throws IOException {
		return stream.readLine();
	}

	@Override
	public final long readLong() throws IOException {
		return stream.readLong();
	}

	@Override
	public final short readShort() throws IOException {
		return stream.readShort();
	}

	@Override
	public final String readUTF() throws IOException {
		return stream.readUTF();
	}

	@Override
	public final int readUnsignedByte() throws IOException {
		return stream.readUnsignedByte();
	}

	@Override
	public final int readUnsignedShort() throws IOException {
		return stream.readUnsignedShort();
	}

	@Override
	public final int skipBytes(int arg0) throws IOException {
		return stream.skipBytes(arg0);
	}

	@Override
	public final DataInputStream getDataInputStream() {
		try{
			IObuffer=new byte[INTERNAL_IO_BUFFER_SIZE];
			return new HadoopInputStream(stream);
		}catch(IOException ex){
			return null;
		}
	}

	@Override
	public final void seek(long position) throws IOException {
		stream.seek(position);
	}

	@Override
	public final boolean seekToNewSource(long targetPos)  throws IOException {
		return stream.seekToNewSource(targetPos);
	}

	@Override
	public final long getPos() throws IOException {
		return stream.getPos();
	}


	@Override
	public final void close() throws IOException {
		stream.close();
		isOpen=false;
	}


	@Override
	public int read(ByteBuffer dst) throws IOException {
		if (dst.hasArray()) {
			// the starting position may not be 0, the limit is the number of remaining bytes before the limit
			int n = stream.read(dst.array(), dst.position(), dst.remaining());
			// ReadableByteChannel.read(ByteBuffer) must update the buffer position
			if (n > 0) {
				dst.position(dst.position() + n);
			}
			return n;
		}else{
			int len;
			int remaining;
			while((remaining=dst.remaining())>0){
				len=stream.read(IObuffer, 0, Math.min(remaining,INTERNAL_IO_BUFFER_SIZE));
				if (len>=0){
					dst.put(IObuffer, 0, len);
				}else{
					return dst.position()>0 ? dst.position() : -1;
				}
			}
			return dst.position();
		}
	}


	@Override
	public boolean isOpen() {
		return isOpen;
	}


	@Override
	public int write(ByteBuffer src) throws IOException {
		throw new UnsupportedOperationException();
	}


	@Override
	public long position() throws IOException {
		return getPos();
	}


	@Override
	public SeekableByteChannel position(long newPosition) throws IOException {
		stream.seek(newPosition);
		return this;
	}


	@Override
	public long size() throws IOException {
		return size;
	}


	@Override
	public SeekableByteChannel truncate(long size) throws IOException {
		//do nothing
		return this;
	}

	private class HadoopInputStream extends FSDataInputStream implements SeekableByteChannel{
		
		public HadoopInputStream(InputStream in) throws IOException {
			super(in);
		}

		@Override
		public int read(ByteBuffer dst) throws IOException {
			return HadoopDataInputStream.this.read(dst);
		}

		@Override
		public boolean isOpen() {
			return HadoopDataInputStream.this.isOpen;
		}

		@Override
		public int write(ByteBuffer src) throws IOException {
			return HadoopDataInputStream.this.write(src);
		}

		@Override
		public long position() throws IOException {
			return HadoopDataInputStream.this.position();
		}

		@Override
		public SeekableByteChannel position(long newPosition)
				throws IOException {
			return HadoopDataInputStream.this.position(newPosition);
		}

		@Override
		public long size() throws IOException {
			return HadoopDataInputStream.this.size;
		}

		@Override
		public SeekableByteChannel truncate(long size) throws IOException {
			return HadoopDataInputStream.this.truncate(size);
		}
		
	}
	
}
