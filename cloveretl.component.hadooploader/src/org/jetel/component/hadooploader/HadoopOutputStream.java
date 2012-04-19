package org.jetel.component.hadooploader;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.jetel.hadoop.connection.IHadoopOutputStream;

public class HadoopOutputStream implements IHadoopOutputStream {

	FSDataOutputStream stream;
	
	public HadoopOutputStream(FSDataOutputStream stream){
		this.stream=stream;
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
