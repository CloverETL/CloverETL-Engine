package org.jetel.component.hadooploader;

import java.io.DataInputStream;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.jetel.hadoop.connection.IHadoopInputStream;

public class HadoopInputStream implements IHadoopInputStream {

	private FSDataInputStream stream;
	
	public HadoopInputStream(FSDataInputStream stream){
		this.stream=stream;
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

	public final long getPos()throws IOException {
		return stream.getPos();
	}


	public final void close() throws IOException {
		stream.close();
		
	}

}
