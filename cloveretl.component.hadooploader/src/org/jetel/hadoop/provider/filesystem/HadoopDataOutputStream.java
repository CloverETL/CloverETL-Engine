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

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.jetel.hadoop.service.filesystem.HadoopDataOutput;

public class HadoopDataOutputStream implements HadoopDataOutput {

	private FSDataOutputStream stream;
	
	public HadoopDataOutputStream(FSDataOutputStream stream){
		this.stream = stream;
	}
	
	@Override
	public final void write(int arg0) throws IOException {
		stream.write(arg0);
	}

	@Override
	public final void write(byte[] arg0) throws IOException {
		stream.write(arg0);
	}

	@Override
	public final void write(byte[] arg0, int arg1, int arg2) throws IOException {
		stream.write(arg0, arg1, arg2);
	}

	@Override
	public final void writeBoolean(boolean arg0) throws IOException {
		stream.writeBoolean(arg0);
	}

	@Override
	public final void writeByte(int arg0) throws IOException {
		stream.writeByte(arg0);
	}

	@Override
	public final void writeBytes(String arg0) throws IOException {
		stream.writeBytes(arg0);
	}

	@Override
	public final void writeChar(int arg0) throws IOException {
		stream.writeChar(arg0);
	}

	@Override
	public final void writeChars(String arg0) throws IOException {
		stream.writeChars(arg0);
	}

	@Override
	public final void writeDouble(double arg0) throws IOException {
		stream.writeDouble(arg0);
	}

	@Override
	public final void writeFloat(float arg0) throws IOException {
		stream.writeFloat(arg0);
	}

	@Override
	public final void writeInt(int arg0) throws IOException {
		stream.writeInt(arg0);
	}

	@Override
	public final void writeLong(long arg0) throws IOException {
		stream.writeLong(arg0);
	}

	@Override
	public final void writeShort(int arg0) throws IOException {
		stream.writeShort(arg0);
	}

	@Override
	public final void writeUTF(String arg0) throws IOException {
		stream.writeUTF(arg0);
	}

	@Override
	public final DataOutputStream getDataOutputStream() {
		return stream;
	}

	@Override
	public final long getPos() throws IOException {
		return stream.getPos();
	}

	public final void close() throws IOException {
		stream.close();
	}

}
