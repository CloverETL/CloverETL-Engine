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
import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.jetel.hadoop.service.filesystem.HadoopDataInput;

public class HadoopDataInputStream implements HadoopDataInput {

	private FSDataInputStream stream;
	
	public HadoopDataInputStream(FSDataInputStream stream){
		this.stream = stream;
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
		return stream;
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
	}

}
