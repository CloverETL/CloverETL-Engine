
package org.jetel.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

public class ByteBufferUtils {
	
	public static int flusch(ByteBuffer buffer, WritableByteChannel writer)
			throws IOException {
		int write;
		buffer.flip();
		write = writer.write(buffer);
		buffer.clear();
		return write;
	}
	
	public static int reload(ByteBuffer buffer, ReadableByteChannel reader) throws IOException{
		int read;
		buffer.compact();
		read = reader.read(buffer);
		buffer.flip();
		return read;
	}

}
