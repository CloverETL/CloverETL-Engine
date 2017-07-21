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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

import org.jetel.data.Defaults;

/**
 * Simple routines for streams manipulating.
 *
 * @author Martin Zatopek (info@cloveretl.com)
 *         (c) (c) Javlin, a.s. (www.javlin.eu) (www.cloveretl.com)
 *
 * @created 23.11.2009
 */
public class StreamUtils {

	/**
	 * Private constructor.
	 */
	private StreamUtils() {
		//unreachable code
	}
	
    /**
     * Read data from an InputStream and convert it to a String.
     * @param is data source
     * @param charset charset for input byte stream
     * @param closeStream should be the given stream closed on exit?
     * @return string representation of given input stream
     * @throws IOException
     */
    public static String convertStreamToString(InputStream is, String charset, boolean closeStream) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(is, charset));
        StringBuilder sb = new StringBuilder();

        int ch;
        try {
            while ((ch = reader.read()) != -1) {
                sb.append((char) ch);
            }
        } finally {
        	if (closeStream) {
        		is.close();
        	}
        }

        return sb.toString();
    }
    
    /**
     * Read all available bytes from one channel and copy them to the other.
     * Can be more efficient when the target is a {@link FileChannel} 
     * and the size of the source is known.
     * 
     * @param in
     * @param out
     * @param sourceSize the size of the source or 0 when unknown
     * 
     * @throws IOException
     */
	public static void copy(ReadableByteChannel in, WritableByteChannel out, long sourceSize) throws IOException {
		if ((out instanceof FileChannel) && (sourceSize > 0)) {
			FileChannel outputFileChannel = (FileChannel) out;
			long pos = 0;
			long transferred = 0;
			long count = 0;
			while (pos < sourceSize) {
				count = Math.min(Defaults.MAX_MAPPED_FILE_TRANSFER_SIZE, sourceSize - pos); // CL-2313
				transferred = outputFileChannel.transferFrom(in, pos, count);
				if (transferred == 0) {
					break;
				}
				pos += transferred;
			}
			if (pos != sourceSize) {
				throw new IOException(String.format("Failed to copy the whole content: expected %d, transferred %d bytes", sourceSize, pos));
			}
		} else {
			copy(in, out);
		}
	}
    
    /**
     * Read all available bytes from one channel and copy them to the other.
     * 
     * The method should correctly check for current thread's interruption status,
     * which is probably guaranteed by using NIO Channels.
     * 
     * @param in
     * @param out
     * @throws IOException
     */
    public static void copy(ReadableByteChannel in, WritableByteChannel out) throws IOException {
		if (in instanceof FileChannel) { // possibly more efficient
			FileChannel inputFileChannel = (FileChannel) in;
			long size = inputFileChannel.size();
	        long pos = 0;
	        long count = 0;
	        long transferred = 0;
	        while (pos < size) {
				count = Math.min(Defaults.MAX_MAPPED_FILE_TRANSFER_SIZE, size - pos); // CL-2313
				transferred = inputFileChannel.transferTo(pos, count, out);
				if (transferred == 0) {
					break;
				}
				pos += transferred;
	        }
	        if (pos != size) {
	        	throw new IOException(String.format("Failed to copy the whole content: expected %d, transferred %d bytes", size, pos));
	        }
		} else {
	    	// First, we need a buffer to hold blocks of copied bytes.
	    	ByteBuffer buffer = ByteBuffer.allocateDirect(32 * 1024);

	    	// Now loop until no more bytes to read and the buffer is empty
	    	while (in.read(buffer) != -1 || buffer.position() > 0) {
	    		// The read() call leaves the buffer in "fill mode". To prepare
	    		// to write bytes from the bufferwe have to put it in "drain mode"
	    		// by flipping it: setting limit to position and position to zero
	    		buffer.flip();

	    		// Now write some or all of the bytes out to the output channel
	    		out.write(buffer);

	    		// Compact the buffer by discarding bytes that were written,
	    		// and shifting any remaining bytes. This method also
	    		// prepares the buffer for the next call to read() by setting the
	    		// position to the limit and the limit to the buffer capacity.
	    		buffer.compact();
	    	}
		}
    }

    private static final int IO_BUFFER_SIZE = 4 * 1024;  

    /**
     * Read all available bytes from one stream and copy them to the other.
     * @param in
     * @param out
     * @throws IOException
     */
    public static void copy(InputStream in, OutputStream out) throws IOException {
    	copy(in, out, false, false);
    }

    /**
     * Read all available bytes from one stream and copy them to the other stream or to nothing.
 	 * Stream may be automatic closed if specified. 
     * @param in input stream
     * @param out target stream, may be null
     * @param closeSrc if true, close input stream finally
     * @param closeDst if true, close output stream finally
     * @throws IOException
     */
    public static void copy(InputStream src, OutputStream dst, boolean closeSrc, boolean closeDst) throws IOException {
    	try {
			try {
				final byte[] buffer = new byte[IO_BUFFER_SIZE];
				for (int l; -1 != (l = src.read(buffer));) {
					// check for interruption
					// TODO do not do it every iteration
					if (Thread.currentThread().isInterrupted()) {
						throw new IOException("Interrupted");
					}
					if ( dst != null ) {
						dst.write(buffer, 0, l);
					}
				}
			} finally {
				if(closeSrc)
				    src.close();
			}
    	} finally {
    	    if(closeDst && dst != null)
    	        dst.close();
    	}
    }
    
	/**
	 * Tries to read as much as possible bytes to the given buffer from the given
	 * channel. It is somehow blocking operation, even incoming data are not
	 * ready in channel, operation tries to populate complete remaining bytes
	 * in buffer. Only end of stream can cause the buffer is not completely populated.
	 * @param channel source channel
	 * @param buffer populated byte buffer
	 * @return number of read bytes
	 * @throws IOException
	 */
	public static int readBlocking(ReadableByteChannel channel, ByteBuffer buffer) throws IOException {
		int result = 0;
		int size;
		
		while ((size = channel.read(buffer)) != -1) {
			result += size;
			if (buffer.remaining() == 0) {
				return result;
			}
		}
		
		return result != 0 ? result : -1;
	}

}
